from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)


class StaffAdapter:
    """Async, DB-backed adapter for HCDN staff queries."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    # ── helpers ─────────────────────────────────────────────

    def _result(self, title: str, records: list[dict], metadata: dict | None = None) -> DataResult:
        return DataResult(
            source="staff:hcdn",
            portal_name="Nómina de Personal — Cámara de Diputados",
            portal_url="https://datos.hcdn.gob.ar/dataset/personal-702b6c52-79b9-4308-bfbe-e7e090e7f6ab",
            dataset_title=title,
            format="json",
            records=records,
            metadata={
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                **(metadata or {}),
            },
        )

    async def _latest_snapshot_date(self, session: AsyncSession) -> str | None:
        row = await session.execute(
            text("SELECT MAX(snapshot_date) FROM staff_snapshots")
        )
        val = row.scalar()
        return str(val) if val else None

    # ── public API ─────────────────────────────────────────

    async def get_by_legislator(self, name: str, limit: int = 50) -> DataResult:
        """Return staff members whose area_desempeno matches *name*."""
        async with self._session_factory() as session:
            snap = await self._latest_snapshot_date(session)
            if not snap:
                return self._result(f"Personal de {name}", [])

            rows = await session.execute(
                text(
                    "SELECT legajo, apellido, nombre, escalafon, area_desempeno, convenio "
                    "FROM staff_snapshots "
                    "WHERE snapshot_date = :snap AND area_desempeno ILIKE :pattern "
                    "ORDER BY apellido, nombre LIMIT :lim"
                ),
                {"snap": snap, "pattern": f"%{name}%", "lim": limit},
            )
            records = [dict(r._mapping) for r in rows]
            return self._result(f"Personal de {name}", records)

    async def count_by_legislator(self, name: str) -> DataResult:
        """Count staff members whose area_desempeno matches *name*."""
        async with self._session_factory() as session:
            snap = await self._latest_snapshot_date(session)
            if not snap:
                return self._result(f"Cantidad de personal de {name}", [])

            row = await session.execute(
                text(
                    "SELECT COUNT(*) AS total "
                    "FROM staff_snapshots "
                    "WHERE snapshot_date = :snap AND area_desempeno ILIKE :pattern"
                ),
                {"snap": snap, "pattern": f"%{name}%"},
            )
            total = row.scalar() or 0
            return self._result(
                f"Cantidad de personal de {name}",
                [{"legislador": name, "cantidad_asesores": total}],
            )

    async def get_changes(self, name: str | None = None, limit: int = 20) -> DataResult:
        """Return recent altas/bajas, optionally filtered by area name."""
        async with self._session_factory() as session:
            if name:
                rows = await session.execute(
                    text(
                        "SELECT legajo, apellido, nombre, area_desempeno, tipo, detected_at "
                        "FROM staff_changes "
                        "WHERE area_desempeno ILIKE :pattern "
                        "ORDER BY detected_at DESC LIMIT :lim"
                    ),
                    {"pattern": f"%{name}%", "lim": limit},
                )
            else:
                rows = await session.execute(
                    text(
                        "SELECT legajo, apellido, nombre, area_desempeno, tipo, detected_at "
                        "FROM staff_changes "
                        "ORDER BY detected_at DESC LIMIT :lim"
                    ),
                    {"lim": limit},
                )
            records = [
                {**dict(r._mapping), "detected_at": str(r.detected_at)}
                for r in rows
            ]
            title = f"Cambios de personal de {name}" if name else "Últimos cambios de personal"
            return self._result(title, records)

    async def search(self, query: str, limit: int = 20) -> DataResult:
        """Free-text search across apellido, nombre, area_desempeno."""
        async with self._session_factory() as session:
            snap = await self._latest_snapshot_date(session)
            if not snap:
                return self._result(f"Búsqueda: {query}", [])

            rows = await session.execute(
                text(
                    "SELECT legajo, apellido, nombre, escalafon, area_desempeno, convenio "
                    "FROM staff_snapshots "
                    "WHERE snapshot_date = :snap "
                    "  AND (apellido ILIKE :pattern OR nombre ILIKE :pattern "
                    "       OR area_desempeno ILIKE :pattern) "
                    "ORDER BY apellido, nombre LIMIT :lim"
                ),
                {"snap": snap, "pattern": f"%{query}%", "lim": limit},
            )
            records = [dict(r._mapping) for r in rows]
            return self._result(f"Búsqueda de personal: {query}", records)

    async def stats(self) -> DataResult:
        """Aggregate statistics about the latest snapshot."""
        async with self._session_factory() as session:
            snap = await self._latest_snapshot_date(session)
            if not snap:
                return self._result("Estadísticas de personal", [])

            row = await session.execute(
                text(
                    "SELECT COUNT(*) AS total, "
                    "  COUNT(DISTINCT area_desempeno) AS areas, "
                    "  COUNT(DISTINCT escalafon) AS escalafones "
                    "FROM staff_snapshots WHERE snapshot_date = :snap"
                ),
                {"snap": snap},
            )
            r = row.fetchone()
            records = [{
                "total_empleados": r.total if r else 0,
                "areas_distintas": r.areas if r else 0,
                "escalafones_distintos": r.escalafones if r else 0,
                "snapshot_date": snap,
            }]
            return self._result("Estadísticas de personal HCDN", records)
