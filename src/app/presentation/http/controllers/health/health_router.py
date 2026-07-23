from __future__ import annotations

import logging
import os
import secrets as _secrets
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request

from app.infrastructure.monitoring.health import HealthCheckService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get("/health")
@inject  # type: ignore[untyped-decorator]
async def health_check(
    request: Request,
    health_service: FromDishka[HealthCheckService],
) -> dict[str, Any]:
    result = await health_service.check_all()
    # M14 (round v46): read BACKEND_API_KEY at request time (not at
    # import time) so a key rotation in the env without a process
    # restart takes effect. Compare in constant time with
    # `secrets.compare_digest` — the previous `!=` made the check
    # technically timing-vulnerable. Realistic impact is small (the
    # info exposed is component status, not a credential), but the
    # fix is one line and we follow the same hygiene as the other
    # admin endpoints.
    expected = os.getenv("BACKEND_API_KEY", "")
    if expected:
        provided = request.headers.get("X-API-Key", "")
        if not provided or not _secrets.compare_digest(provided, expected):
            return {"status": result.get("status", "unknown")}
    return result  # type: ignore[no-any-return]


@router.get("/health/ready")
async def readiness_check() -> dict[str, str]:
    return {"status": "ready"}
