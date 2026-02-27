from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "openarg"}


@router.get("/health/ready")
async def readiness_check():
    return {"status": "ready"}
