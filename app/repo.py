import hashlib
import json

import asyncpg

from .codegen import new_code
from .config import settings
from .ctx import ctx
from .errors import AliasTaken, IdemRace, Overloaded
from .guard import db_guard
from .invalidation import invalidate, tombstone

RETRYABLE = (asyncpg.SerializationError, asyncpg.DeadlockDetectedError)

INSERT_LINK = """
INSERT INTO links (code, long_url, owner_id, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (code) DO NOTHING
RETURNING created_at"""

INSERT_IDEM = """
INSERT INTO idempotency_keys (key, code, response_status, response_body)
VALUES ($1, $2, 201, $3::jsonb)
ON CONFLICT (key) DO NOTHING
RETURNING 1"""

SOFT_DELETE = """
UPDATE links SET deleted_at = now()
WHERE code = $1 AND owner_id = $2 AND deleted_at IS NULL
RETURNING code"""


async def retry_on(fn, errors, attempts):
    for i in range(attempts):
        try:
            return await fn()
        except errors:
            if i == attempts - 1:
                raise


def idem_hash(user_id, key):
    """Scope the key to the user so one user cannot replay another's response."""
    return hashlib.sha256(f"{user_id}:{key}".encode()).hexdigest()


def make_body(code, req):
    return {"code": code, "short_url": f"{settings.base_url}/{code}", "long_url": req.long_url,
            "expires_at": req.expires_at.isoformat() if req.expires_at else None}


async def get_idem(idem):
    async with db_guard.slot(), ctx.primary.acquire() as conn:
        raw = await conn.fetchval("SELECT response_body FROM idempotency_keys WHERE key = $1", idem)
    return json.loads(raw) if raw else None


async def _insert_link(conn, req, user_id):
    for _ in range(1 if req.alias else 3):
        code = req.alias or new_code()
        if await conn.fetchrow(INSERT_LINK, code, req.long_url, user_id, req.expires_at):
            return code
    raise AliasTaken() if req.alias else Overloaded()


async def _create_txn(req, user_id, idem):
    async with db_guard.slot(), ctx.primary.acquire() as conn, conn.transaction():
        code = await _insert_link(conn, req, user_id)
        body = make_body(code, req)
        if idem and not await conn.fetchval(INSERT_IDEM, idem, code, json.dumps(body)):
            raise IdemRace()                  # the other request won -> roll back our link
        return body


async def create_link(req, user_id, idem_key=None):
    idem = idem_hash(user_id, idem_key) if idem_key else None
    if idem and (stored := await get_idem(idem)):          # fast path for a retry
        return stored
    try:
        body = await retry_on(lambda: _create_txn(req, user_id, idem), RETRYABLE, 3)
    except (IdemRace, AliasTaken):
        if idem and (stored := await get_idem(idem)):      # concurrent twin finished first
            return stored
        raise
    await invalidate(body["code"])             # drop a cached "missing" for this code
    return body


async def get_link(code):
    async with db_guard.slot(), ctx.primary.acquire() as conn:
        return await conn.fetchrow(
            "SELECT code, long_url, owner_id, created_at, expires_at, deleted_at FROM links WHERE code = $1",
            code)


async def delete_link(code, user_id):
    """Returns 'ok', 'missing' or 'forbidden'."""
    async with db_guard.slot(), ctx.primary.acquire() as conn:
        if await conn.fetchval(SOFT_DELETE, code, user_id):
            result = "ok"
        else:
            row = await conn.fetchrow("SELECT owner_id, deleted_at FROM links WHERE code = $1", code)
            result = "missing" if row is None or row["deleted_at"] else "forbidden"
    if result == "ok":
        await tombstone(code)
    return result
