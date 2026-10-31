import asyncio
import logging

from .ctx import ctx
from .guard import db_guard

log = logging.getLogger("jobs")
LOCK_ID = 4242


async def cleanup_idempotency():
    """Runs on every node but only one wins the advisory lock."""
    async with db_guard.slot(), ctx.primary.acquire() as conn, conn.transaction():
        if await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", LOCK_ID):
            await conn.execute("DELETE FROM idempotency_keys WHERE created_at < now() - interval '24 hours'")


async def cleanup_loop(period_s=600):
    while True:
        await asyncio.sleep(period_s)
        try:
            await cleanup_idempotency()
        except Exception as e:
            log.warning("idempotency cleanup failed: %s", e)
