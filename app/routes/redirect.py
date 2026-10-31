from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse

from ..cache import resolver, utcnow
from ..codegen import ALIAS_RE

router = APIRouter()


@router.get("/{code}")
async def redirect(code: str):
    if not ALIAS_RE.fullmatch(code):
        raise HTTPException(404)
    e = await resolver.resolve(code)
    if e.kind == "missing":
        raise HTTPException(404)
    if e.kind == "gone" or (e.expires_at and e.expires_at <= utcnow()):
        raise HTTPException(410)
    # 302 + no-store semantics so browsers never pin a deleted link
    return RedirectResponse(e.url, status_code=302, headers={"Cache-Control": "private, max-age=0"})
