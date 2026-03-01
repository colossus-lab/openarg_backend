from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.resilience.circuit_breaker import circuit_breakers

logger = logging.getLogger(__name__)


class HealthCheckService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis_url: str,
        ddjj: DDJJAdapter,
    ) -> None:
        self._session_factory = session_factory
        self._redis_url = redis_url
        self._ddjj = ddjj

    async def check_all(self) -> dict[str, Any]:
        checks = await asyncio.gather(
            self._check_postgres(),
            self._check_redis(),
            self._check_ddjj(),
            self._check_sesion_chunks(),
            self._check_stuck_tasks(),
            return_exceptions=True,
        )

        components = {}
        names = ["postgres", "redis", "ddjj_loaded", "sesion_chunks", "pipeline_health"]
        for name, result in zip(names, checks):
            if isinstance(result, Exception):
                components[name] = {"status": "unhealthy", "error": str(result)}
            else:
                components[name] = result

        # Circuit breaker status
        cb_status = {}
        any_cb_open = False
        for name, cb in circuit_breakers.items():
            cb_status[name] = cb.to_dict()
            if cb.state.value == "open":
                any_cb_open = True
        components["circuit_breakers"] = cb_status

        all_healthy = all(
            c.get("status") in ("healthy", None)
            for k, c in components.items()
            if k != "circuit_breakers" and isinstance(c, dict) and "status" in c
        ) and not any_cb_open

        return {
            "status": "healthy" if all_healthy else "degraded",
            "components": components,
        }

    async def _check_postgres(self) -> dict:
        start = time.monotonic()
        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        latency_ms = round((time.monotonic() - start) * 1000, 1)
        return {"status": "healthy", "latency_ms": latency_ms}

    async def _check_redis(self) -> dict:
        start = time.monotonic()
        r = aioredis.from_url(self._redis_url, decode_responses=True)
        try:
            pong = await r.ping()
            latency_ms = round((time.monotonic() - start) * 1000, 1)
            return {
                "status": "healthy" if pong else "unhealthy",
                "latency_ms": latency_ms,
            }
        finally:
            await r.aclose()

    async def _check_ddjj(self) -> dict:
        count = self._ddjj.record_count
        return {
            "status": "healthy" if count > 0 else "unhealthy",
            "records": count,
        }

    async def _check_sesion_chunks(self) -> dict:
        start = time.monotonic()
        try:
            async with self._session_factory() as session:
                result = await session.execute(text("SELECT COUNT(*) FROM sesion_chunks"))
                count = result.scalar() or 0
            latency_ms = round((time.monotonic() - start) * 1000, 1)
            return {"status": "healthy", "count": count, "latency_ms": latency_ms}
        except Exception:
            return {"status": "unhealthy", "count": 0, "error": "table not found"}

    async def _check_stuck_tasks(self) -> dict:
        try:
            async with self._session_factory() as session:
                # Stuck downloads (>30 min in 'downloading')
                result = await session.execute(
                    text(
                        "SELECT COUNT(*) FROM cached_datasets "
                        "WHERE status = 'downloading' "
                        "AND updated_at < NOW() - INTERVAL '30 minutes'"
                    )
                )
                stuck_downloads = result.scalar() or 0

                # Stuck queries (>30 min in intermediate states)
                result = await session.execute(
                    text(
                        "SELECT COUNT(*) FROM user_queries "
                        "WHERE status IN ('planning', 'collecting', 'analyzing') "
                        "AND updated_at < NOW() - INTERVAL '30 minutes'"
                    )
                )
                stuck_queries = result.scalar() or 0

                # Recent errors (last 24h)
                result = await session.execute(
                    text(
                        "SELECT COUNT(*) FROM cached_datasets "
                        "WHERE status = 'error' "
                        "AND updated_at > NOW() - INTERVAL '24 hours'"
                    )
                )
                recent_errors_24h = result.scalar() or 0

            status = "healthy" if (stuck_downloads == 0 and stuck_queries == 0) else "degraded"
            return {
                "status": status,
                "stuck_downloads": stuck_downloads,
                "stuck_queries": stuck_queries,
                "recent_errors_24h": recent_errors_24h,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
