from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.domain.exceptions.base import ApplicationError
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.infrastructure.audit.audit_logger import audit_injection_blocked

logger = logging.getLogger(__name__)


class PromptInjectionError(ApplicationError):
    """Raised when prompt injection is detected."""

    def __init__(self, score: float, user: str = "unknown") -> None:
        self.injection_score = score
        self.user = user
        super().__init__(
            error_code=ErrorCode.SEC_INJECTION_DETECTED,
            details={"score": round(score, 3)},
        )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PromptInjectionError)
    async def prompt_injection_handler(
        request: Request, exc: PromptInjectionError
    ) -> JSONResponse:
        audit_injection_blocked(
            user=exc.user,
            question=str(exc.details.get("question", "")),
            score=exc.injection_score,
        )
        logger.warning(
            "Prompt injection blocked: score=%.3f user=%s",
            exc.injection_score,
            exc.user,
        )
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.to_dict()},
        )

    @app.exception_handler(ConnectorError)
    async def connector_error_handler(request: Request, exc: ConnectorError) -> JSONResponse:
        logger.warning(
            "Connector error on %s %s: %s - %s",
            request.method,
            request.url.path,
            exc.error_code.name,
            exc,
        )
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.to_dict()},
        )

    @app.exception_handler(ApplicationError)
    async def application_error_handler(request: Request, exc: ApplicationError) -> JSONResponse:
        logger.warning(
            "Application error on %s %s: %s - %s",
            request.method,
            request.url.path,
            exc.error_code.name,
            exc,
        )
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.to_dict()},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = []
        for err in exc.errors():
            loc = " → ".join(str(part) for part in err.get("loc", []))
            errors.append(
                {
                    "field": loc,
                    "message": err.get("msg", "Validation error"),
                    "type": err.get("type", ""),
                }
            )
        logger.warning("Validation error: %s", errors)
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Request validation failed",
                    "details": errors,
                }
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(f"Unhandled error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "INTERNAL", "message": "Internal server error"}},
        )
