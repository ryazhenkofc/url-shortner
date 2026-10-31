import asyncio
import logging
from contextlib import suppress

from .cache import Entry, encode, l1
from .config import settings
from .redis_client import make_redis, redis_call

log = logging.getLogger("invalidation")
CHANNEL = "links:inv"


async def invalidate(code):
    """Call after the DB commit: drop L1, delete the L2 key, tell the other nodes."""
    l1.pop(code, None)
    with suppress(Exception):                    # Redis is down -> the TTL will clean up
        await redis_call("delete", f"u:{code}")
        await redis_call("publish", CHANNEL, code)


async def tombstone(code):
    """Call after a delete commit. A plain DEL is not enough: a lagging replica would refill L2
    with the old row for a day. shortcut: lost if Redis is down at this moment, or if replica
    lag exceeds l2_gone_ttl_s; upgrade by shortening l2_ttl_s or reading the primary on fill."""
    l1.pop(code, None)
    with suppress(Exception):
        await redis_call("setex", f"u:{code}", settings.l2_gone_ttl_s, encode(Entry("gone")))
        await redis_call("publish", CHANNEL, code)


async def listen_invalidations():
    """Background task on every node. Pub/sub needs a client without a read timeout."""
    client = make_redis(settings, None)
    while True:
        try:
            async with client.pubsub() as ps:
                await ps.subscribe(CHANNEL)
                l1.clear()                       # messages may have been missed while disconnected
                async for msg in ps.listen():
                    if msg["type"] == "message":
                        l1.pop(msg["data"], None)
        except Exception as e:
            log.warning("invalidation listener reconnecting: %s", e)
            await asyncio.sleep(1)
