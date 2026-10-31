import asyncio

from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel

from .config import settings
from .ctx import ctx
from .guard import redis_guard
from .metrics import REDIS_ERRORS


def make_redis(s, timeout):
    """Master client; timeout=None is needed for pub/sub, which idles on purpose."""
    if not s.redis_sentinels:
        return Redis.from_url(s.redis_url, socket_timeout=timeout, decode_responses=True)
    hosts = [(h, int(p)) for h, p in (x.split(":") for x in s.redis_sentinels.split(","))]
    sentinel = Sentinel(hosts, sentinel_kwargs={"socket_timeout": 1}, socket_timeout=timeout,
                        decode_responses=True)
    return sentinel.master_for(s.redis_master_name)


async def redis_call(cmd, *args, **kwargs):
    """One Redis command under the concurrency guard and the timeout."""
    try:
        async with redis_guard.slot():
            return await asyncio.wait_for(getattr(ctx.redis, cmd)(*args, **kwargs), settings.redis_timeout_s)
    except Exception:
        REDIS_ERRORS.inc()
        raise


async def warm_up(client, n):
    """Open n pooled connections up front: a burst on an empty pool overruns redis_timeout_s."""
    try:
        await asyncio.wait_for(asyncio.gather(*(client.ping() for _ in range(n))), 5)
    except Exception:
        pass            # Redis down at boot is fine: the service runs degraded
