import hashlib

from cachetools import TTLCache
from fastapi import Header, HTTPException

from .ctx import ctx
from .guard import db_guard

_users = TTLCache(maxsize=1024, ttl=60)   # shortcut: a revoked key stays valid up to 60 s


async def current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    key_hash = hashlib.sha256(authorization[7:].encode()).digest()
    if key_hash not in _users:
        async with db_guard.slot(), ctx.primary.acquire() as conn:
            user_id = await conn.fetchval("SELECT user_id FROM api_keys WHERE key_hash = $1", key_hash)
        if user_id is None:
            raise HTTPException(401, "invalid token")
        _users[key_hash] = user_id
    return _users[key_hash]
