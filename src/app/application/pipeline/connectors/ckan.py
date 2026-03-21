"""Connector: CKAN search (portales de datos abiertos)."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
    from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox

logger = logging.getLogger(__name__)


def sanitize_ckan_query(raw: str) -> str:
    """Extract meaningful keywords from a potentially verbose LLM-generated query."""
    _STOPWORDS = {
        "buscar",
        "busca",
        "buscando",
        "datasets",
        "dataset",
        "datos",
        "relacionados",
        "relacionado",
        "sobre",
        "acerca",
        "portal",
        "nacional",
        "disponibles",
        "disponible",
        "listar",
        "mostrar",
        "obtener",
        "consultar",
        "encontrar",
        "abiertos",
        "abierto",
        "informacion",
        "información",
        "con",
        "del",
        "los",
        "las",
        "que",
        "hay",
        "tiene",
        "para",
        "una",
        "por",
        "como",
        "son",
        "mas",
        "más",
        "este",
        "esta",
        "estos",
        "estas",
        "datos.gob.ar",
        "datos.gob",
        "gob.ar",
    }
    clean = re.sub(r"[\"'()]", " ", raw)
    words = [w for w in clean.lower().split() if len(w) > 2 and w not in _STOPWORDS]
    if not words:
        return raw.strip()[:100]
    return " ".join(words[:5])


async def search_cached_tables(
    query: str,
    sandbox: ISQLSandbox | None,
    limit: int = 10,
) -> list[DataResult]:
    """Search locally cached tables (cache_*) for datasets matching *query*."""
    if not sandbox:
        return []

    try:
        all_tables = await sandbox.list_cached_tables()
    except Exception:
        logger.debug("Failed to list cached tables", exc_info=True)
        return []

    if not all_tables:
        return []

    keywords = [k.lower() for k in query.split() if len(k) > 2]
    if not keywords:
        return []

    matched = [t for t in all_tables if any(kw in t.table_name.lower() for kw in keywords)]
    if not matched:
        return []

    matched = matched[:limit]
    results: list[DataResult] = []
    now = datetime.now(UTC).isoformat()

    for table in matched:
        try:
            cols = ", ".join(f'"{c}"' for c in table.columns[:30]) if table.columns else "*"
            sql = f'SELECT {cols} FROM "{table.table_name}" LIMIT 50'
            sandbox_result = await sandbox.execute_readonly(sql, timeout_seconds=5)
            if sandbox_result.error or not sandbox_result.rows:
                continue

            results.append(
                DataResult(
                    source=f"cache:{table.table_name}",
                    portal_name="Base de datos local (cache)",
                    portal_url="",
                    dataset_title=table.table_name.replace("cache_", "").replace("_", " ").title(),
                    format="json",
                    records=sandbox_result.rows[:50],
                    metadata={
                        "total_records": table.row_count or len(sandbox_result.rows),
                        "columns": table.columns,
                        "fetched_at": now,
                        "source": "local_cache",
                    },
                )
            )
        except Exception:
            logger.debug("Failed to query cached table %s", table.table_name, exc_info=True)
            continue

    return results


async def execute_ckan_step(
    step: PlanStep,
    ckan: ICKANSearchConnector,
    sandbox: ISQLSandbox | None,
) -> list[DataResult]:
    params = step.params
    raw_query = params.get("query", step.description)
    query = sanitize_ckan_query(raw_query) if raw_query not in ("*", "*:*") else raw_query
    portal_id = params.get("portalId")
    rows = params.get("rows", 10)

    resource_id = params.get("resourceId") or params.get("resource_id")
    if resource_id and portal_id:
        q = params.get("q")
        records = await ckan.query_datastore(portal_id, resource_id, q=q)
        if records:
            return [
                DataResult(
                    source=f"ckan:{portal_id}",
                    portal_name=f"CKAN {portal_id}",
                    portal_url="",
                    dataset_title=f"Datastore query: {q or 'all'}",
                    format="json",
                    records=records,
                    metadata={
                        "total_records": len(records),
                        "fetched_at": datetime.now(UTC).isoformat(),
                    },
                )
            ]

    # --- Try local cached tables first ---
    local_results = await search_cached_tables(query, sandbox, limit=rows)
    if local_results:
        logger.info("CKAN step resolved from %d local cached table(s)", len(local_results))
        return local_results

    # --- Fallback: live CKAN search ---
    return await ckan.search_datasets(query, portal_id=portal_id, rows=rows)
