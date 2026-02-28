from __future__ import annotations

from fastapi import APIRouter

from app.infrastructure.monitoring.metrics import MetricsCollector

router = APIRouter(prefix="/metrics", tags=["monitoring"])


@router.get("")
async def get_metrics() -> dict:
    return MetricsCollector().get_metrics()
