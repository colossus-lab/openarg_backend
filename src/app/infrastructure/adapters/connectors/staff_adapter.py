from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.staff import IStaffConnector

logger = logging.getLogger(__name__)


def _escape_like(value: str) -> str:
    """Escape ILIKE metacharacters so they are treated as literals."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class StaffAdapter(IStaffConnector):
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

    def _like_pattern(self, value: str) -> str:
        return f"%{_escape_like(value)}%"

    def _smart_like_patterns(self, name: str) -> list[str]:
        """Full name first, then individual words longest-first (>=3 chars)."""
        patterns = [self._like_pattern(name)]
        words = [w for w in name.split() if len(w) >= 3]
        for word in sorted(words, key=len, reverse=True):
            p = self._like_pattern(word)
            if p not in patterns:
                patterns.append(p)
        return patterns

    async def _query_senado_staff(
        self, session: AsyncSession, name: str, limit: int,
    ) -> list[dict]:
        """Find staff via senado_staff table (scraped from senator profiles)."""
        for pattern in self._smart_like_patterns(name):
            rows = await session.execute(
                text(
                    "SELECT employee_name, categoria, senator_name, bloque, provincia, senator_id "
                    "FROM senado_staff WHERE senator_name ILIKE :pattern "
                    "ORDER BY employee_name LIMIT :lim"
                ),
                {"pattern": pattern, "lim": limit},
            )
            records = [dict(r._mapping) for r in rows]
            if records:
                return records
        return []

    async def _count_senado_staff(
        self, session: AsyncSession, name: str,
    ) -> int:
        """Count staff via senado_staff table."""
        for pattern in self._smart_like_patterns(name):
            row = await session.execute(
                text(
                    "SELECT COUNT(*) AS total "
                    "FROM senado_staff WHERE senator_name ILIKE :pattern"
                ),
                {"pattern": pattern},
            )
            total = row.scalar() or 0
            if total:
                return total
        return 0

    async def _suggest_similar_areas(
        self, session: AsyncSession, snap: str, name: str, limit: int = 5,
    ) -> list[dict]:
        """Return areas whose name partially matches any word in *name*."""
        words = [w for w in name.split() if len(w) >= 3]
        if not words:
            return []
        conditions = " OR ".join(f"area_desempeno ILIKE :w{i}" for i in range(len(words)))
        params: dict = {"snap": snap, "lim": limit}
        for i, w in enumerate(words):
            params[f"w{i}"] = self._like_pattern(w)
        rows = await session.execute(
            text(
                f"SELECT area_desempeno, COUNT(*) AS total "
                f"FROM staff_snapshots "
                f"WHERE snapshot_date = :snap AND ({conditions}) "
                f"GROUP BY area_desempeno ORDER BY total DESC LIMIT :lim"
            ),
            params,
        )
        return [{"area": r.area_desempeno, "cantidad": r.total} for r in rows]

    # ── public API ─────────────────────────────────────────

    async def get_by_legislator(self, name: str, limit: int = 50) -> DataResult:
        """Return staff members whose area_desempeno matches *name*.

        Tries the full name first, then falls back to individual words
        (longest first) so that ``"Martin Yeza"`` still matches areas
        containing just ``"YEZA"``.
        """
        name = name.strip()
        if not name:
            return self._result("Personal de (sin especificar)", [])
        limit = max(1, min(limit, 500))
        try:
            async with self._session_factory() as session:
                snap = await self._latest_snapshot_date(session)
                if not snap:
                    logger.info("No staff snapshot found — returning empty for '%s'", name)
                    return self._result(f"Personal de {name}", [])

                # 1. Try senado staff first
                senado_records = await self._query_senado_staff(session, name, limit)
                if senado_records:
                    logger.info("get_by_legislator('%s'): %d results (senado)", name, len(senado_records))
                    return self._result(
                        f"Personal de {name}", senado_records, {"source": "senado_perfiles"},
                    )

                # 2. Fallback to area_desempeno
                records: list[dict] = []
                matched_pattern: str | None = None
                for pattern in self._smart_like_patterns(name):
                    rows = await session.execute(
                        text(
                            "SELECT legajo, apellido, nombre, escalafon, area_desempeno, convenio "
                            "FROM staff_snapshots "
                            "WHERE snapshot_date = :snap AND area_desempeno ILIKE :pattern "
                            "ORDER BY apellido, nombre LIMIT :lim"
                        ),
                        {"snap": snap, "pattern": pattern, "lim": limit},
                    )
                    records = [dict(r._mapping) for r in rows]
                    if records:
                        matched_pattern = pattern
                        break

                metadata = {}
                if not records:
                    similar = await self._suggest_similar_areas(session, snap, name)
                    if similar:
                        metadata["areas_similares"] = similar
                else:
                    metadata["matched_pattern"] = matched_pattern

                logger.info("get_by_legislator('%s'): %d results", name, len(records))
                return self._result(f"Personal de {name}", records, metadata)
        except ConnectorError:
            raise
        except Exception as exc:
            logger.error("Staff query failed for get_by_legislator('%s')", name, exc_info=True)
            raise ConnectorError(
                error_code=ErrorCode.CN_STAFF_UNAVAILABLE,
                details={"action": "get_by_legislator", "name": name[:100]},
            ) from exc

    async def count_by_legislator(self, name: str) -> DataResult:
        """Count staff members whose area_desempeno matches *name*.

        Cascades through full-name → individual-word patterns until a
        non-zero count is found.  When all patterns yield 0, the result
        includes ``areas_similares`` so the LLM can suggest alternatives.
        """
        name = name.strip()
        if not name:
            return self._result("Cantidad de personal (sin especificar)", [])
        try:
            async with self._session_factory() as session:
                snap = await self._latest_snapshot_date(session)
                if not snap:
                    logger.info("No staff snapshot found — returning empty count for '%s'", name)
                    return self._result(f"Cantidad de personal de {name}", [])

                # 1. Try senado staff count first
                senado_total = await self._count_senado_staff(session, name)
                if senado_total:
                    logger.info("count_by_legislator('%s'): %d (senado)", name, senado_total)
                    return self._result(
                        f"Cantidad de personal de {name}",
                        [{"legislador": name, "cantidad_asesores": senado_total, "fuente": "senado_perfiles"}],
                    )

                # 2. Fallback to area_desempeno
                total = 0
                for pattern in self._smart_like_patterns(name):
                    row = await session.execute(
                        text(
                            "SELECT COUNT(*) AS total "
                            "FROM staff_snapshots "
                            "WHERE snapshot_date = :snap AND area_desempeno ILIKE :pattern"
                        ),
                        {"snap": snap, "pattern": pattern},
                    )
                    total = row.scalar() or 0
                    if total:
                        break

                record: dict = {"legislador": name, "cantidad_asesores": total}
                if not total:
                    similar = await self._suggest_similar_areas(session, snap, name)
                    if similar:
                        record["areas_similares"] = similar

                logger.info("count_by_legislator('%s'): %d", name, total)
                return self._result(
                    f"Cantidad de personal de {name}",
                    [record],
                )
        except ConnectorError:
            raise
        except Exception as exc:
            logger.error("Staff query failed for count_by_legislator('%s')", name, exc_info=True)
            raise ConnectorError(
                error_code=ErrorCode.CN_STAFF_UNAVAILABLE,
                details={"action": "count_by_legislator", "name": name[:100]},
            ) from exc

    async def get_changes(self, name: str | None = None, limit: int = 20) -> DataResult:
        """Return recent altas/bajas, optionally filtered by area name.

        When a *name* is given, cascades through smart LIKE patterns so
        that ``"Martin Yeza"`` still matches areas containing ``"YEZA"``.
        """
        limit = max(1, min(limit, 500))
        try:
            async with self._session_factory() as session:
                if name and name.strip():
                    clean = name.strip()
                    records: list[dict] = []
                    for pattern in self._smart_like_patterns(clean):
                        rows = await session.execute(
                            text(
                                "SELECT legajo, apellido, nombre, area_desempeno, tipo, detected_at "
                                "FROM staff_changes "
                                "WHERE area_desempeno ILIKE :pattern "
                                "ORDER BY detected_at DESC LIMIT :lim"
                            ),
                            {"pattern": pattern, "lim": limit},
                        )
                        records = [
                            {
                                **dict(r._mapping),
                                "detected_at": r.detected_at.isoformat() if r.detected_at else None,
                            }
                            for r in rows
                        ]
                        if records:
                            break
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
                        {
                            **dict(r._mapping),
                            "detected_at": r.detected_at.isoformat() if r.detected_at else None,
                        }
                        for r in rows
                    ]
                title = f"Cambios de personal de {name}" if name else "Últimos cambios de personal"
                logger.info("get_changes(name=%s): %d results", name, len(records))
                return self._result(title, records)
        except ConnectorError:
            raise
        except Exception as exc:
            logger.error("Staff query failed for get_changes(name=%s)", name, exc_info=True)
            raise ConnectorError(
                error_code=ErrorCode.CN_STAFF_UNAVAILABLE,
                details={"action": "get_changes", "name": (name or "")[:100]},
            ) from exc

    async def search(self, query: str, limit: int = 20) -> DataResult:
        """Free-text search across apellido, nombre, area_desempeno."""
        query = query.strip()
        if not query:
            return self._result("Búsqueda de personal (sin consulta)", [])
        limit = max(1, min(limit, 500))
        try:
            async with self._session_factory() as session:
                snap = await self._latest_snapshot_date(session)
                if not snap:
                    logger.info("No staff snapshot found — returning empty search for '%s'", query)
                    return self._result(f"Búsqueda: {query}", [])

                pattern = self._like_pattern(query)
                rows = await session.execute(
                    text(
                        "SELECT legajo, apellido, nombre, escalafon, area_desempeno, convenio "
                        "FROM staff_snapshots "
                        "WHERE snapshot_date = :snap "
                        "  AND (apellido ILIKE :pattern OR nombre ILIKE :pattern "
                        "       OR area_desempeno ILIKE :pattern) "
                        "ORDER BY apellido, nombre LIMIT :lim"
                    ),
                    {"snap": snap, "pattern": pattern, "lim": limit},
                )
                records = [dict(r._mapping) for r in rows]
                logger.info("search('%s'): %d results", query, len(records))
                return self._result(f"Búsqueda de personal: {query}", records)
        except ConnectorError:
            raise
        except Exception as exc:
            logger.error("Staff query failed for search('%s')", query, exc_info=True)
            raise ConnectorError(
                error_code=ErrorCode.CN_STAFF_UNAVAILABLE,
                details={"action": "search", "query": query[:100]},
            ) from exc

    async def stats(self) -> DataResult:
        """Aggregate statistics about the latest snapshot."""
        try:
            async with self._session_factory() as session:
                snap = await self._latest_snapshot_date(session)
                if not snap:
                    logger.info("No staff snapshot found — returning empty stats")
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
                logger.info("stats: %d employees", records[0]["total_empleados"])
                return self._result("Estadísticas de personal HCDN", records)
        except ConnectorError:
            raise
        except Exception as exc:
            logger.error("Staff query failed for stats", exc_info=True)
            raise ConnectorError(
                error_code=ErrorCode.CN_STAFF_UNAVAILABLE,
                details={"action": "stats"},
            ) from exc
