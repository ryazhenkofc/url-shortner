import asyncio
import logging

import asyncpg

log = logging.getLogger("db")

SERVER_SETTINGS = {
    "statement_timeout": "5000",
    "idle_in_transaction_session_timeout": "30000",
}


async def make_pool(dsn, size, attempts=30):
    """The DB may come up later than the app, so retry with backoff."""
    for i in range(attempts):
        try:
            # min_size=size: do not spend the redirect path on opening connections during a burst
            return await asyncpg.create_pool(dsn, min_size=size, max_size=size,
                                             server_settings=SERVER_SETTINGS, timeout=2)
        except (OSError, asyncpg.PostgresError, asyncio.TimeoutError) as e:
            log.warning("pool not ready (%s), attempt %d/%d", e, i + 1, attempts)
            await asyncio.sleep(min(0.2 * 2 ** i, 3))
    raise RuntimeError("database is unavailable")


async def make_pools(s):
    return await make_pool(s.pg_primary_dsn, s.pool_size), await make_pool(s.pg_replica_dsn, s.pool_size)
