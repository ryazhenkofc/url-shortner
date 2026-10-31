import asyncio

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ..ctx import ctx

router = APIRouter()


async def _ping(pool):
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")


@router.get("/health/live")
async def live():
    return {"status": "ok"}


@router.get("/health/ready")
async def ready():
    """Redis is not checked: without it the service runs degraded, not down."""
    for pool in (ctx.primary, ctx.replica):
        try:
            await asyncio.wait_for(_ping(pool), 0.2)
            return {"status": "ok"}
        except Exception:
            continue
    raise HTTPException(503, "no database reachable")


@router.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
