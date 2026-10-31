import asyncio
import json
import random
from dataclasses import dataclass
from datetime import datetime, timezone

from cachetools import TTLCache

from .config import settings
from .ctx import ctx
from .errors import Overloaded
from .guard import db_guard
from .metrics import DB_ERRORS, HITS
from .redis_client import redis_call

l1 = TTLCache(maxsize=settings.l1_size, ttl=settings.l1_ttl_s)

SELECT_LINK = "SELECT long_url, expires_at, deleted_at FROM links WHERE code = $1"


def utcnow():
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Entry:
    kind: str                  # "ok" | "missing" | "gone"
    url: str = None
    expires_at: datetime = None


def encode(e):
    if e.kind != "ok":
        return "!" + e.kind
    return json.dumps({"u": e.url, "e": e.expires_at.isoformat() if e.expires_at else None})


def decode(raw):
    if raw.startswith("!"):
        return Entry(raw[1:])
    d = json.loads(raw)
    return Entry("ok", d["u"], datetime.fromisoformat(d["e"]) if d["e"] else None)


def to_entry(row):
    if row is None:
        return Entry("missing")
    if row["deleted_at"] is not None:
        return Entry("gone")
    return Entry("ok", row["long_url"], row["expires_at"])


def ttl_for(e):
    if e.kind == "missing":
        return settings.l2_negative_ttl_s
    if e.kind == "gone":
        return settings.l2_gone_ttl_s
    ttl = int(settings.l2_ttl_s * random.uniform(0.9, 1.1))
    if e.expires_at:
        ttl = min(ttl, int((e.expires_at - utcnow()).total_seconds()))
    return ttl


class SingleFlight:
    def __init__(self):
        self._inflight = {}

    async def do(self, key, fn):
        if (fut := self._inflight.get(key)) is not None:
            return await asyncio.shield(fut)              # wait for the first caller's result
        fut = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        try:
            res = await fn()
            fut.set_result(res)
            return res
        except asyncio.CancelledError:
            fut.cancel()      # shortcut: waiters are cancelled too, upgrade if that shows up in prod
            raise
        except Exception as e:
            fut.set_exception(e)
            fut.exception()                               # mark as retrieved when nobody waits
            raise
        finally:
            self._inflight.pop(key, None)


class Resolver:
    def __init__(self):
        self.sf = SingleFlight()

    async def resolve(self, code):
        if (e := l1.get(code)) is not None:
            HITS.labels("l1").inc()
            return e
        return await self.sf.do(code, lambda: self._load(code))

    async def _load(self, code):
        e = await self._redis_get(code)               # None on miss OR on Redis error/timeout
        if e is not None:
            HITS.labels("l2").inc()
        else:
            HITS.labels("db").inc()
            e = await self._db_get(code)
            e = await self._redis_set(code, e)        # best effort
        l1[code] = e
        return e

    async def _redis_get(self, code):
        try:
            raw = await redis_call("get", f"u:{code}")
        except Exception:
            return None
        return decode(raw) if raw is not None else None

    async def _redis_set(self, code, e):
        """NX: a stale read must not overwrite the tombstone left by a delete. Returns what L2 holds."""
        ttl = ttl_for(e)
        if ttl <= 0:
            return e
        try:
            if await redis_call("set", f"u:{code}", encode(e), ex=ttl, nx=True):
                return e
            raw = await redis_call("get", f"u:{code}")
        except Exception:
            return e
        return decode(raw) if raw else e

    async def _db_get(self, code):
        try:
            row = await self._read(ctx.replica, "replica", code)
        except Overloaded:
            raise
        except Exception:
            row = None
        if row is None:
            row = await self._read(ctx.primary, "primary", code)   # the replica may lag
        return to_entry(row)

    async def _read(self, pool, name, code):
        async def query():
            async with pool.acquire() as conn:
                return await conn.fetchrow(SELECT_LINK, code)
        try:
            async with db_guard.slot():
                return await asyncio.wait_for(query(), settings.db_read_timeout_s)
        except Overloaded:
            raise
        except Exception:
            DB_ERRORS.labels(name).inc()
            raise


resolver = Resolver()
