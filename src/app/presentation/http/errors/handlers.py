from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse

from app.domain.exceptions.base import ApplicationError

logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApplicationError)
    async def application_error_handler(
        request: Request, exc: ApplicationError
    ) -> ORJSONResponse:
        logger.warning(f"Application error: {exc.error_code.name} - {exc}")
        return ORJSONResponse(
            status_code=exc.http_status,
            content={"error": exc.to_dict()},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(
        request: Request, exc: Exception
    ) -> ORJSONResponse:
        logger.exception(f"Unhandled error: {exc}")
        return ORJSONResponse(
            status_code=500,
            content={"error": {"code": "INTERNAL", "message": "Internal server error"}},
        )
