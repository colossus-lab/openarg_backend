from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.infrastructure.monitoring.middleware import MetricsMiddleware
from app.infrastructure.persistence_sqla.mappings.all import map_tables
from app.presentation.http.errors.handlers import register_exception_handlers
from app.presentation.http.middleware.auth_middleware import APIKeyMiddleware
from app.presentation.http.middleware.rate_limit_key import get_rate_limit_identifier
from app.presentation.http.middleware.security_headers import SecurityHeadersMiddleware
from app.setup.config.settings import AppSettings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    map_tables()

    # Security validation at startup
    settings: AppSettings | None = getattr(app.state, "settings", None)
    environment: str = getattr(app.state, "environment", "local")
    if environment == "prod" and settings:
        if settings.security.JWT_SECRET_KEY == "change-me":
            raise RuntimeError("JWT_SECRET_KEY must be changed in production")
        if not settings.security.BACKEND_API_KEY:
            raise RuntimeError("BACKEND_API_KEY must be set in production")

    logger.info("OpenArg API started")
    yield
    if hasattr(app.state, "dishka_container"):
        await app.state.dishka_container.close()
    logger.info("OpenArg API shutdown")


limiter = Limiter(
    key_func=get_rate_limit_identifier,
    storage_uri=os.getenv("REDIS_CACHE_URL", "memory://"),
)


def create_app() -> FastAPI:
    app = FastAPI(
        title="OpenArg API",
        description="Inteligencia sobre Datos Abiertos de Argentina",
        version="0.1.0",
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    return app


def configure_app(
    app: FastAPI,
    root_router: APIRouter,
    environment: str = "local",
    settings: AppSettings | None = None,
) -> None:
    app.include_router(root_router)
    register_exception_handlers(app)

    app.add_middleware(MetricsMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)

    # API key middleware (if configured)
    if settings and settings.security.BACKEND_API_KEY:
        app.add_middleware(APIKeyMiddleware, api_key=settings.security.BACKEND_API_KEY)

    # CORS: use settings origins if defined, else permissive in dev, restrictive in prod
    if settings and settings.security.CORS_ALLOWED_ORIGINS:
        allow_origins = settings.security.CORS_ALLOWED_ORIGINS
    elif environment in ("local", "dev"):
        allow_origins = ["*"]
    else:
        allow_origins = []

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
