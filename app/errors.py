import asyncio

import asyncpg
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


class Overloaded(Exception):
    """A guard or the retry budget ran out; the client should retry later."""


class IdemRace(Exception):
    """Another request with the same Idempotency-Key committed first."""


class BadRequest(HTTPException):
    def __init__(self, detail):
        super().__init__(400, detail)


class AliasTaken(HTTPException):
    def __init__(self):
        super().__init__(409, "alias already taken")


DB_DOWN = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError, asyncio.TimeoutError)


def install(app):
    async def unavailable(request, exc):
        return JSONResponse({"detail": "service unavailable"}, status_code=503,
                            headers={"Retry-After": "1"})

    async def invalid(request, exc):
        return JSONResponse({"detail": str(exc.errors()[0]["msg"])}, status_code=400)

    for exc_type in (Overloaded, *DB_DOWN):
        app.add_exception_handler(exc_type, unavailable)
    app.add_exception_handler(RequestValidationError, invalid)
