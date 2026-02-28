from __future__ import annotations

import logging

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter

from app.infrastructure.monitoring.health import HealthCheckService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
@inject
async def health_check(
    health_service: FromDishka[HealthCheckService],
) -> dict:
    result = await health_service.check_all()
    return result


@router.get("/health/ready")
async def readiness_check():
    return {"status": "ready"}
