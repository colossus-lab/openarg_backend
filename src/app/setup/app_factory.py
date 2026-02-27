from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.infrastructure.persistence_sqla.mappings.all import map_tables
from app.presentation.http.errors.handlers import register_exception_handlers

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    map_tables()
    logger.info("OpenArg API started")
    yield
    if hasattr(app.state, "dishka_container"):
        await app.state.dishka_container.close()
    logger.info("OpenArg API shutdown")


limiter = Limiter(key_func=get_remote_address, storage_uri="memory://")


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
) -> None:
    app.include_router(root_router)
    register_exception_handlers(app)

    allow_origins = ["*"] if environment in ("local", "dev") else []
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
