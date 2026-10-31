from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from .. import repo
from ..auth import current_user
from ..cache import utcnow
from ..codegen import validate_alias, validate_url
from ..config import settings
from ..errors import BadRequest

router = APIRouter(prefix="/api/v1/links")

OWN_HOST = settings.base_url.split("//")[-1].split(":")[0].split("/")[0]
BLOCKLIST = {h.strip() for h in settings.url_blocklist.split(",") if h.strip()}


class CreateLink(BaseModel):
    long_url: str
    alias: str = None
    expires_at: datetime = None


def validate(req):
    validate_url(req.long_url, OWN_HOST, BLOCKLIST)
    if req.alias:
        validate_alias(req.alias)
    if req.expires_at:
        if req.expires_at.tzinfo is None:
            req.expires_at = req.expires_at.replace(tzinfo=timezone.utc)
        if req.expires_at <= utcnow():
            raise BadRequest("expires_at must be in the future")


@router.post("", status_code=201)
async def create(req: CreateLink, user_id=Depends(current_user), idempotency_key: str = Header(None)):
    if idempotency_key is not None and not 0 < len(idempotency_key) <= 64:
        raise BadRequest("Idempotency-Key must be 1-64 characters")
    validate(req)
    return await repo.create_link(req, user_id, idempotency_key)


@router.get("/{code}")
async def info(code: str, user_id=Depends(current_user)):
    row = await repo.get_link(code)
    if row is None or row["deleted_at"]:
        raise HTTPException(404)
    if row["owner_id"] != user_id:
        raise HTTPException(403)
    return {"code": row["code"], "long_url": row["long_url"],
            "created_at": row["created_at"], "expires_at": row["expires_at"]}


@router.delete("/{code}", status_code=204)
async def delete(code: str, user_id=Depends(current_user)):
    result = await repo.delete_link(code, user_id)
    if result == "missing":
        raise HTTPException(404)
    if result == "forbidden":
        raise HTTPException(403)
