from __future__ import annotations

import logging
import os

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request

from app.infrastructure.monitoring.health import HealthCheckService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])

_BACKEND_API_KEY = os.getenv("BACKEND_API_KEY", "")


@router.get("/health")
@inject
async def health_check(
    request: Request,
    health_service: FromDishka[HealthCheckService],
) -> dict:
    result = await health_service.check_all()
    # If API key is configured but not provided, return minimal info
    if _BACKEND_API_KEY and request.headers.get("X-API-Key") != _BACKEND_API_KEY:
        return {"status": result.get("status", "unknown")}
    return result


@router.get("/health/ready")
async def readiness_check():
    return {"status": "ready"}
