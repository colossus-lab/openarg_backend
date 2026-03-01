from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest

from app.infrastructure.monitoring.metrics import MetricsCollector

router = APIRouter(prefix="/metrics", tags=["monitoring"])


@router.get("")
async def get_metrics() -> dict:
    return MetricsCollector().get_metrics()


@router.get("/prometheus")
async def get_prometheus_metrics() -> PlainTextResponse:
    return PlainTextResponse(
        content=generate_latest().decode("utf-8"),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
