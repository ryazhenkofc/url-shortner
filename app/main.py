import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from . import errors
from .config import settings
from .ctx import ctx
from .db import make_pools
from .invalidation import listen_invalidations
from .jobs import cleanup_loop
from .metrics import REQ_DURATION
from .redis_client import make_redis, warm_up
from .routes import health, links, redirect

logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s [{settings.node_id}] %(name)s: %(message)s")
INDEX = Path(__file__).parent / "static" / "index.html"


@asynccontextmanager
async def lifespan(app):
    ctx.primary, ctx.replica = await make_pools(settings)
    ctx.redis = make_redis(settings, settings.redis_timeout_s)
    await warm_up(ctx.redis, settings.redis_concurrency)
    tasks = [asyncio.create_task(listen_invalidations()), asyncio.create_task(cleanup_loop())]
    yield
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await ctx.redis.aclose()
    await asyncio.gather(ctx.primary.close(), ctx.replica.close())


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
errors.install(app)


@app.middleware("http")
async def timing(request, call_next):
    start = time.perf_counter()
    resp = await call_next(request)
    route = request.scope.get("route")
    REQ_DURATION.labels(route.path if route else "unmatched", resp.status_code).observe(
        time.perf_counter() - start)
    return resp


@app.get("/", include_in_schema=False)
async def ui():
    return FileResponse(INDEX)


# Order matters: the catch-all /{code} must come last.
app.include_router(health.router)
app.include_router(links.router)
app.include_router(redirect.router)
