import asyncio
from contextlib import asynccontextmanager

from .config import settings
from .errors import Overloaded
from .metrics import OVERLOAD_REJECTS


class Guard:
    """Semaphore with a short acquire timeout: fail fast instead of queueing."""

    def __init__(self, limit, acquire_timeout):
        self._sem = asyncio.Semaphore(limit)
        self._t = acquire_timeout

    @asynccontextmanager
    async def slot(self):
        try:
            await asyncio.wait_for(self._sem.acquire(), self._t)
        except asyncio.TimeoutError:
            OVERLOAD_REJECTS.inc()
            raise Overloaded()
        try:
            yield
        finally:
            self._sem.release()


db_guard = Guard(settings.pool_size, settings.acquire_timeout_s)        # = pool size
redis_guard = Guard(settings.redis_concurrency, settings.acquire_timeout_s)
