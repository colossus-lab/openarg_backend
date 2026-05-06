"""Connector: NL2SQL sandbox (query_sandbox action).

Handles table discovery (fnmatch, vector search, catalog search),
SQL generation via LLM, self-correction loop, and INDEC live fallback.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from app.application.discovery import (
    catalog_discovery,
    catalog_only_mode,
    discovery_enabled,
)
from app.application.pipeline.connectors.cache_table_selection import (
    build_table_compat_notes,
    expand_table_hints_compat,
    prefer_consolidated_table,
)
from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider
    from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
    from app.domain.ports.search.vector_search import IVectorSearch
    from app.infrastructure.adapters.cache.semantic_cache import SemanticCache

logger = logging.getLogger(__name__)


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_text.lower().split())


def _is_dataset_discovery_query(query: str) -> bool:
    normalized = _normalize_text(query)
    if "dataset" not in normalized:
        return False
    padded = f" {normalized} "
    discovery_markers = (
        " que datasets ",
        " que dataset ",
        " cuales datasets ",
        " cuales dataset ",
        " lista de datasets ",
        " listado de datasets ",
        " mostrar datasets ",
        " mostrame datasets ",
        " datasets disponibles ",
        " hay en ",
        " hay de ",
    )
    return any(marker in padded for marker in discovery_markers)


def _build_dataset_discovery_results(
    *,
    nl_query: str,
    tables: list[Any],
    catalog_entries: dict[str, dict[str, Any]],
) -> list[DataResult]:
    records: list[dict[str, Any]] = []
    for table in tables[:20]:
        entry = catalog_entries.get(table.table_name, {})
        records.append(
            {
                "table_name": table.table_name,
                "display_name": entry.get("display_name") or table.table_name,
                "description": entry.get("description") or "",
                "domain": entry.get("domain") or "",
                "subdomain": entry.get("subdomain") or "",
                "row_count": table.row_count or 0,
                "columns": list(table.columns or []),
            }
        )

    return [
        DataResult(
            source="sandbox:dataset_discovery",
            portal_name="Cache Local (descubrimiento de datasets)",
            portal_url="",
            dataset_title=f"Datasets relacionados: {nl_query[:100]}",
            format="json",
            records=records,
            metadata={
                "dataset_discovery": True,
                "total_matches": len(tables),
                "returned_matches": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
            },
        )
    ]


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------


async def get_catalog_entries(
    table_names: list[str],
    sandbox: ISQLSandbox | None,
) -> dict[str, dict[str, Any]]:
    """Fetch table_catalog metadata for given table names.

    Uses the sandbox's sync engine via run_in_executor to avoid
    needing an async session.
    """
    if not table_names or not sandbox:
        return {}
    try:
        loop = asyncio.get_running_loop()

        def _fetch() -> dict[str, dict[str, Any]]:
            engine = sandbox._get_engine()  # type: ignore[union-attr]
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT table_name, display_name, description, domain, subdomain, "
                        "key_columns, column_types, sample_queries, tags "
                        "FROM table_catalog WHERE table_name = ANY(:names)"
                    ),
                    {"names": table_names},
                )
                rows = result.fetchall()
                conn.rollback()
                return {
                    r.table_name: {
                        "display_name": r.display_name,
                        "description": r.description,
                        "domain": r.domain,
                        "subdomain": r.subdomain,
                        "key_columns": r.key_columns,
                        "column_types": r.column_types,
                        "sample_queries": r.sample_queries,
                        "tags": r.tags,
                    }
                    for r in rows
                }

        return await loop.run_in_executor(None, _fetch)
    except Exception:
        logger.debug(
            "table_catalog lookup failed",
            exc_info=True,
        )
        return {}


async def discover_tables_by_catalog_search(
    query: str,
    sandbox: ISQLSandbox | None,
    embedding: IEmbeddingProvider,
    limit: int = 5,
    min_score: float = 0.45,
) -> list[tuple[str, float]]:
    """Search table_catalog embeddings for relevant tables.

    Returns a list of (table_name, similarity_score) tuples filtered by
    *min_score* and ordered by descending similarity.
    """
    if not sandbox:
        return []
    try:
        q_embedding = await embedding.embed(query)
        embedding_str = "[" + ",".join(str(x) for x in q_embedding) + "]"
        loop = asyncio.get_running_loop()

        def _search() -> list[tuple[str, float]]:
            engine = sandbox._get_engine()  # type: ignore[union-attr]
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT table_name, "
                        "1 - (catalog_embedding <=> CAST(:emb AS vector)) AS score "
                        "FROM table_catalog "
                        "WHERE catalog_embedding IS NOT NULL "
                        "AND 1 - (catalog_embedding <=> CAST(:emb AS vector)) >= :min_score "
                        "ORDER BY catalog_embedding <=> CAST(:emb AS vector) "
                        "LIMIT :lim"
                    ),
                    {"emb": embedding_str, "lim": limit, "min_score": min_score},
                )
                rows = [(r.table_name, round(r.score, 3)) for r in result.fetchall()]
                conn.rollback()
                return rows

        return await loop.run_in_executor(None, _search)
    except Exception:
        logger.debug("catalog vector search failed", exc_info=True)
        return []


async def discover_catalog_hints_for_planner(
    query: str,
    sandbox: ISQLSandbox | None,
    embedding: IEmbeddingProvider,
    limit: int = 5,
    *,
    serving_port: Any | None = None,
) -> str:
    """Search table_catalog for relevant tables and format as planner hints.

    This runs BEFORE the planner LLM so it knows which cached tables exist
    for the user's question. Without this, the planner only knows about
    tables matched by keyword routing in dataset_index.py.

    MASTERPLAN Fase 4.5: when `serving_port` is provided AND
    `OPENARG_PIPELINE_USE_SERVING_PORT=1`, mart/staging hits from the
    Serving Port are prepended to the legacy block. Marts are the curated
    surface — the planner should prefer them over raw `cache_*` tables.
    """
    serving_block = ""
    if serving_port is not None:
        try:
            serving_block = await _serving_port_planner_hints(
                query, serving_port, limit=limit
            )
        except Exception:
            logger.debug("serving port planner hints failed", exc_info=True)
            serving_block = ""

    if not sandbox:
        return serving_block
    try:
        q_embedding = await embedding.embed(query)
        # In `OPENARG_CATALOG_ONLY` cutover mode, skip the legacy
        # `table_catalog` query entirely and let `_hybrid_logical_hints`
        # drive discovery from `catalog_resources` alone.
        if catalog_only_mode():
            legacy = await _hybrid_logical_hints(query, q_embedding, limit=limit)
            return _join_hint_blocks(serving_block, legacy)
        embedding_str = "[" + ",".join(str(x) for x in q_embedding) + "]"
        loop = asyncio.get_running_loop()

        def _search() -> list[tuple[str, str, str, int, float]]:
            engine = sandbox._get_engine()  # type: ignore[union-attr]
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT tc.table_name, tc.display_name, tc.description, "
                        "COALESCE(tc.row_count, 0) AS row_count, "
                        "1 - (tc.catalog_embedding <=> CAST(:emb AS vector)) AS score "
                        "FROM table_catalog tc "
                        "WHERE tc.catalog_embedding IS NOT NULL "
                        "AND 1 - (tc.catalog_embedding <=> CAST(:emb AS vector)) > 0.55 "
                        "ORDER BY tc.catalog_embedding <=> CAST(:emb AS vector) "
                        "LIMIT :lim"
                    ),
                    {"emb": embedding_str, "lim": limit},
                )
                rows = [
                    (
                        r.table_name,
                        r.display_name or "",
                        r.description or "",
                        r.row_count,
                        round(r.score, 2),
                    )
                    for r in result.fetchall()
                ]
                conn.rollback()
                return rows

        rows = await loop.run_in_executor(None, _search)
        if not rows:
            # WS3 hybrid discovery — when no materialized table matches, try
            # the logical catalog so the planner can still see resources that
            # exist conceptually (or live-API connectors).
            legacy = await _hybrid_logical_hints(query, q_embedding, limit=limit)
            return _join_hint_blocks(serving_block, legacy)

        lines = ["TABLAS CACHEADAS RELEVANTES (datos reales descargados, usar query_sandbox):"]
        for table_name, display_name, description, row_count, score in rows:
            line = f"  - {table_name}"
            if display_name:
                line += f" ({display_name})"
            line += f" — {row_count} filas"
            if description:
                line += f" — {description[:120]}"
            line += f" [relevancia: {score}]"
            lines.append(line)

        lines.append(
            "\nSi alguna de estas tablas es relevante para la pregunta, "
            "usá la acción query_sandbox con table_hints incluyendo el nombre exacto de la tabla."
        )
        # Append logical catalog hints when the feature flag is on. Optional,
        # additive — doesn't break the existing planner path.
        logical_hints = await _hybrid_logical_hints(query, q_embedding, limit=limit)
        if logical_hints:
            lines.append("")
            lines.append(logical_hints)
        legacy_block = "\n".join(lines)
        return _join_hint_blocks(serving_block, legacy_block)
    except Exception:
        logger.debug("catalog hints for planner failed", exc_info=True)
        return serving_block


def _join_hint_blocks(*blocks: str) -> str:
    """Concatenate non-empty hint blocks with a blank line separator."""
    return "\n\n".join(b for b in blocks if b and b.strip())


# Match `FROM <schema>.` and `JOIN <schema>.`. The previous regex only
# caught FROM, so a query like `WITH x AS (SELECT ... FROM cache_legacy)
# SELECT ... FROM x JOIN mart.foo` was tagged `cache_legacy` even though
# it joined a mart. We now tag it `mart` (the highest layer is what
# matters for the served_from coverage metric). A second pass over JOIN
# keeps the simple-query path unchanged.
_MART_SQL_RE = re.compile(r"\b(?:from|join)\s+mart\.", re.IGNORECASE)
_STAGING_SQL_RE = re.compile(r"\b(?:from|join)\s+staging\.", re.IGNORECASE)
_RAW_SQL_RE = re.compile(r"\b(?:from|join)\s+raw\.", re.IGNORECASE)


def detect_serving_layer_in_sql(sql: str) -> str:
    """Inspect a SQL statement and return the medallion layer it touches.

    Used by the executor (Fase 4.5c) to emit a `served_from_layer` metric
    so we can track mart-coverage over time without doing log scrapes.
    Lookups are heuristic — multi-CTE queries that touch several layers
    return the highest layer (mart > staging > raw > legacy). Both `FROM`
    and `JOIN` are inspected so a CTE that joins a mart at the outer
    SELECT is correctly tagged.
    """
    if _MART_SQL_RE.search(sql):
        return "mart"
    if _STAGING_SQL_RE.search(sql):
        return "staging"
    if _RAW_SQL_RE.search(sql):
        return "raw"
    return "cache_legacy"


async def _mart_semantics_block(serving_port: Any, table_names: list[str]) -> str:
    """MASTERPLAN Fase 4.5b — append per-column semantics for mart tables.

    Looks up each table_name through the Serving Port; if the table is in
    schema `mart` AND the schema carries a non-empty `semantics` map (built
    from `COMMENT ON COLUMN`), formats it as an NL2SQL hint block.

    Failures and non-mart tables are silently dropped — the legacy hints
    remain intact.
    """
    from app.application.pipeline.connectors.serving_resolver import (
        ServingResolver,
        serving_port_enabled,
    )
    from app.domain.entities.serving import ServingLayer

    if not serving_port_enabled():
        return ""

    resolver = ServingResolver(serving_port)
    blocks: list[str] = []
    for tn in table_names:
        # Mart resources are addressed as `mart::<mart_id>` from
        # `_discover_marts` (mart_id == mart_view_name in current YAMLs,
        # but they are conceptually distinct: mart_id is the logical
        # identifier, mart_view_name is the physical Postgres relation).
        # Planners may reference marts in three forms:
        #   - bare mart_id:        "demo_energia_pozos"
        #   - canonical resource:  "mart::demo_energia_pozos"
        #   - qualified relation:  "mart.demo_energia_pozos"
        # We build candidates that cover all three so the resolver finds
        # the schema regardless of which form the planner emitted.
        candidates: list[str] = []
        if tn.startswith("mart::"):
            candidates.append(tn)
        elif tn.startswith("mart."):
            # Strip the schema prefix to recover the bare view_name and
            # try it as a mart_id resource. Today mart_id == mart_view_name
            # so this works; if they ever diverge, this still resolves
            # via mart_view_name lookup downstream.
            candidates.append(f"mart::{tn[len('mart.'):]}")
        else:
            candidates.append(f"mart::{tn}")
        for resource_id in candidates:
            try:
                schema = await resolver.get_schema(resource_id)
            except Exception:
                continue
            if schema.layer != ServingLayer.MART or not schema.semantics:
                continue
            lines = [f"SEMÁNTICA DE COLUMNAS — {tn}:"]
            for col, desc in schema.semantics.items():
                lines.append(f"  - {col}: {desc}")
            blocks.append("\n".join(lines))
            break
    return "\n\n".join(blocks)


async def _serving_port_planner_hints(
    query: str,
    serving_port: Any,
    *,
    limit: int,
) -> str:
    """MASTERPLAN Fase 4.5 — surface mart/staging resources to the planner.

    Calls `IServingPort.discover()` and formats the top hits as a planner
    block. Marts come first; staging next; cache_legacy is suppressed here
    because the legacy `table_catalog` block already covers it.
    """
    from app.application.pipeline.connectors.serving_resolver import (
        ServingResolver,
        serving_port_enabled,
    )
    from app.domain.entities.serving import ServingLayer

    if not serving_port_enabled():
        return ""

    resolver = ServingResolver(serving_port)
    try:
        resources, layer_counts = await resolver.discover_for_planner(
            query, limit=limit
        )
    except Exception:
        logger.debug("ServingResolver.discover_for_planner failed", exc_info=True)
        return ""

    # Filter out the cache_legacy hits — those are already covered by the
    # legacy table_catalog block downstream. We surface marts and staging
    # explicitly so the planner sees them first.
    preferred_layers = (ServingLayer.MART, ServingLayer.STAGING, ServingLayer.RAW)
    preferred = [r for r in resources if r.layer in preferred_layers]
    if not preferred:
        return ""

    lines: list[str] = []
    by_layer: dict[ServingLayer, list] = {}
    for r in preferred:
        by_layer.setdefault(r.layer, []).append(r)

    if ServingLayer.MART in by_layer:
        lines.append("MARTS DISPONIBLES (vistas semánticas curadas, preferí estas):")
        for r in by_layer[ServingLayer.MART]:
            lines.append(f"  - {r.title}" + (f" — {r.domain}" if r.domain else ""))
        lines.append(
            "Para una mart, usá query_sandbox con el nombre canónico de la vista."
        )

    if ServingLayer.STAGING in by_layer:
        lines.append("")
        lines.append("STAGING (datasets validados por contracts):")
        for r in by_layer[ServingLayer.STAGING]:
            lines.append(f"  - {r.title}")

    return "\n".join(lines)


async def _hybrid_logical_hints(
    query: str, q_embedding: list[float] | None, *, limit: int
) -> str:
    """WS3 — surface `catalog_resources` to the planner when the flag is on.

    Returns a planner-facing block describing logical resources that match
    the query (whether materialized, pending or connector_endpoint).
    """
    if not discovery_enabled():
        return ""
    try:
        loop = asyncio.get_running_loop()
        discovery = catalog_discovery()

        def _search() -> list:
            if q_embedding:
                return discovery.find_by_embedding(q_embedding, k=limit)
            return discovery.find_by_text(query, k=limit)

        results = await loop.run_in_executor(None, _search)
        if not results:
            return ""
        lines = [
            "RECURSOS LÓGICOS RELEVANTES (catalog_resources, pueden no estar materializados):"
        ]
        for r in results:
            tag = r.materialization_status.upper()
            line = f"  - [{tag}] {r.display_name or r.canonical_title} (kind={r.resource_kind})"
            if r.materialized_table_name:
                line += f" → tabla `{r.materialized_table_name}`"
            if getattr(r, "score", 0):
                line += f" [score: {round(r.score, 2)}]"
            lines.append(line)
        lines.append(
            "Para LIVE_API/PENDING usá el conector apropiado (BCRA, series_tiempo, etc.). "
            "Para READY usá query_sandbox con la tabla indicada."
        )
        return "\n".join(lines)
    except Exception:
        logger.debug("hybrid logical hints failed", exc_info=True)
        return ""


async def discover_tables_by_vector_search(
    query: str,
    cached_tables: list[Any],
    embedding: IEmbeddingProvider,
    vector_search: IVectorSearch,
) -> list[str]:
    """Use vector search to find relevant cached table names for a query.

    Embeds the query, searches for similar datasets, and returns the
    table names of any that are cached.  *cached_tables* is the
    already-fetched list of ``CachedTableInfo`` to avoid a redundant DB
    round-trip.
    """
    try:
        q_embedding = await embedding.embed(query)
        vector_results = await vector_search.search_datasets(
            q_embedding,
            limit=10,
            min_similarity=0.65,
        )
        if not vector_results:
            return []

        # Collect dataset_ids from vector search results
        hit_dataset_ids = {vr.dataset_id for vr in vector_results}

        # Match against cached tables by dataset_id
        direct_matches = [
            t.table_name for t in cached_tables if t.dataset_id and t.dataset_id in hit_dataset_ids
        ]
        available_names = [t.table_name for t in cached_tables]
        matched_names = [
            prefer_consolidated_table(name, available_names) for name in direct_matches
        ]
        matched_names = list(dict.fromkeys(matched_names))

        if matched_names:
            logger.info(
                "Vector search discovered %d cached table(s) for sandbox: %s",
                len(matched_names),
                matched_names[:5],
            )
        return matched_names
    except Exception:
        logger.warning("Vector-based table discovery failed, falling back", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# INDEC live fallback
# ---------------------------------------------------------------------------


async def indec_live_fallback(nl_query: str) -> list[DataResult]:
    """Plan B: download INDEC XLS on-the-fly when cache tables don't exist."""
    import asyncio as _asyncio

    from app.infrastructure.celery.tasks.indec_tasks import INDEC_DATASETS, _download_and_parse

    query_lower = nl_query.lower()
    keyword_map = {
        # IDs deben coincidir con INDEC_DATASETS en indec_tasks.py
        "ipc": ["ipc", "inflacion", "precios"],
        "emae": ["emae", "actividad economica", "actividad económica"],
        "pib": ["pib", "producto bruto", "producto interno"],
        "comercio_exterior": [
            "exportacion",
            "importacion",
            "comercio exterior",
            "balanza comercial",
        ],
        "eph_tasas": ["empleo", "eph", "desempleo", "trabajo", "mercado laboral"],
        "canasta_basica": ["canasta basica", "canasta básica", "cbt", "cba"],
        "salarios_indice": ["salario", "salarios", "sueldo"],
        "pobreza_informe": ["pobreza", "indigencia"],
        "pobreza_historica": ["pobreza histor", "indigencia histor"],
        "isac": ["construccion", "construcción", "isac"],
        "ipi_manufacturero": ["industria", "ipi", "manufacturero", "produccion industrial"],
        "supermercados": ["supermercado"],
        "turismo_receptivo": ["turismo"],
        "distribucion_ingreso": [
            "distribucion del ingreso",
            "distribución del ingreso",
            "gini",
            "decil",
        ],
        "balance_pagos": ["balance de pagos", "balanza de pagos", "cuenta corriente"],
    }

    matched_ids = []
    for ds_id, keywords in keyword_map.items():
        if any(kw in query_lower for kw in keywords):
            matched_ids.append(ds_id)

    if not matched_ids:
        matched_ids = ["ipc", "emae", "pib"]

    async def _fetch_one(ds_id: str) -> DataResult | None:
        ds_info = next((d for d in INDEC_DATASETS if d["id"] == ds_id), None)
        if not ds_info:
            return None
        try:
            sheets = await _asyncio.to_thread(_download_and_parse, ds_info["url"])
            if not sheets:
                return None
            df = next(iter(sheets.values()))
            if df is None or df.empty:
                return None
            if len(df) > 500:
                df = df.tail(500)
            records = df.to_dict(orient="records")
            return DataResult(
                source="indec:live",
                portal_name="INDEC (descarga en vivo)",
                portal_url="https://www.indec.gob.ar",
                dataset_title=f"INDEC - {ds_info['name']} (live)",
                format="json",
                records=records[:200],
                metadata={
                    "total_records": len(records),
                    "columns": list(df.columns),
                    "source_url": ds_info["url"],
                    "fallback": True,
                    "fetched_at": datetime.now(UTC).isoformat(),
                },
            )
        except Exception:
            logger.warning("INDEC live fallback failed for %s", ds_id, exc_info=True)
            return None

    fetched = await _asyncio.gather(*[_fetch_one(ds_id) for ds_id in matched_ids[:3]])
    return [r for r in fetched if r is not None]


# ---------------------------------------------------------------------------
# Main sandbox step
# ---------------------------------------------------------------------------


async def execute_sandbox_step(
    step: PlanStep,
    sandbox: ISQLSandbox | None,
    llm: ILLMProvider,
    embedding: IEmbeddingProvider,
    vector_search: IVectorSearch,
    semantic_cache: SemanticCache,
    user_query: str = "",
    *,
    serving_port: Any | None = None,
) -> list[DataResult]:
    if not sandbox:
        logger.warning("ISQLSandbox not configured, skipping step %s", step.id)
        return []
    params = step.params
    # Use the original user question for NL2SQL so specific filters
    # (e.g. "gobernador de jujuy") are not lost to the planner's generic query.
    nl_query = user_query or params.get("query", step.description)

    # Few-shot helper used to build the subgraph's initial state.
    # INDEC_PATTERN and save_successful_query are now owned by the
    # NL2SQL subgraph itself (FIX-004).
    from app.application.pipeline.history import get_few_shot_examples

    try:
        tables = await sandbox.list_cached_tables()
        # MASTERPLAN Fase 4.5d — surface marts to the executor's table
        # universe so a planner that suggested a mart (via the hints
        # block built by `_serving_port_planner_hints`) actually finds
        # it after fnmatch filtering. Without this, table_hints like
        # `mart.staff_estado` never intersect `tables` and the filter
        # silently falls back to listing every cache_* table → the
        # NL2SQL subgraph never sees the mart and emits a query against
        # legacy cache instead.
        # `PgSandboxAdapter._engine` is a SYNCHRONOUS Engine, so the
        # query has to run in a worker thread via `asyncio.to_thread`.
        # The earlier version used `async with engine.connect()` directly,
        # which AttributeError'd on `__aenter__` and silently dropped the
        # mart enrichment. Same bug shape that hid marts from /data/search
        # and /data/tables until Sprint 0.5/0.7 — this is the executor-side
        # variant. Fixed here so the planner's mart hints actually match
        # rows in the executor's `tables` universe.
        def _query_marts_sync() -> list:
            from sqlalchemy import text as _sql_text

            engine_for_marts = getattr(sandbox, "_engine", None)
            if engine_for_marts is None:
                getter = getattr(sandbox, "_get_engine", None)
                if callable(getter):
                    engine_for_marts = getter()
            if engine_for_marts is None:
                return []
            with engine_for_marts.connect() as _mart_conn:
                return (
                    _mart_conn.execute(
                        _sql_text(
                            "SELECT mart_id, mart_schema, mart_view_name, "
                            "       last_row_count, canonical_columns_json "
                            "FROM mart_definitions "
                            "WHERE COALESCE(last_row_count, 0) > 0"
                        )
                    )
                ).fetchall()

        try:
            import asyncio as _asyncio

            from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo

            mart_rows = await _asyncio.to_thread(_query_marts_sync)
            for _mr in mart_rows:
                _columns: list[str] = []
                try:
                    _cc = _mr.canonical_columns_json
                    if isinstance(_cc, list):
                        _columns = [str(c.get("name", "")) for c in _cc if c.get("name")]
                except Exception:
                    _columns = []
                _schema = str(_mr.mart_schema or "mart")
                _view = str(_mr.mart_view_name or _mr.mart_id)
                tables.append(
                    CachedTableInfo(
                        table_name=f"{_schema}.{_view}",
                        dataset_id="",
                        row_count=int(_mr.last_row_count or 0),
                        columns=_columns,
                    )
                )
        except Exception:
            # Mart enrichment is best-effort. Failure here just means the
            # planner can't pick a mart; cache_legacy path keeps working.
            logger.warning("Could not enrich tables with marts", exc_info=True)
        table_hints = params.get("tables", [])
        # Resolve table_notes and table_hints from routing when planner didn't provide them
        table_notes = params.get("table_notes", "")
        if not table_notes or not table_hints:
            from app.infrastructure.adapters.connectors.dataset_index import resolve_hints as _rh

            for _hint in _rh(nl_query):
                if _hint.action == "query_sandbox":
                    if not table_hints and _hint.params.get("tables"):
                        table_hints = _hint.params["tables"]
                    if not table_notes and _hint.params.get("table_notes"):
                        table_notes = _hint.params["table_notes"]
                    if table_hints and table_notes:
                        break
        if table_hints:
            table_hints = expand_table_hints_compat(table_hints)

        logger.info(
            "Sandbox step %s: %d cached tables, hints=%s, query=%s",
            step.id,
            len(tables),
            table_hints,
            nl_query[:80],
        )
        if not tables:
            if table_hints and any("indec" in h for h in table_hints):
                logger.info("No cached tables at all, attempting INDEC live fallback")
                return await indec_live_fallback(nl_query)
            return []

        # When no table_hints from planner, try catalog search first, then vector search
        _from_catalog_or_vector = False
        if not table_hints:
            catalog_results = await discover_tables_by_catalog_search(
                nl_query, sandbox, embedding, limit=10, min_score=0.45
            )
            if catalog_results:
                table_hints = [name for name, _score in catalog_results]
                _from_catalog_or_vector = True
                logger.info(
                    "Catalog search discovered %d table(s): %s",
                    len(catalog_results),
                    [(name, score) for name, score in catalog_results[:5]],
                )
            else:
                logger.info(
                    "Catalog search found no tables (min_score=0.45), falling back to vector search"
                )
                discovered = await discover_tables_by_vector_search(
                    nl_query, tables, embedding, vector_search
                )
                if discovered:
                    table_hints = discovered
                    _from_catalog_or_vector = True
                    logger.info(
                        "Vector search fallback discovered %d table(s): %s",
                        len(discovered),
                        discovered[:5],
                    )

        if table_hints:
            if _from_catalog_or_vector:
                # Catalog/vector search returns exact table names — use set lookup
                hint_set = set(table_hints)
                filtered = [t for t in tables if t.table_name in hint_set]
            else:
                # Planner returns glob patterns — use fnmatch
                import fnmatch

                filtered = []
                for t in tables:
                    for pattern in table_hints:
                        if fnmatch.fnmatch(t.table_name, pattern):
                            filtered.append(t)
                            break
            if filtered:
                # If query mentions a specific year, filter out tables that don't cover it
                import re as _re

                year_match = _re.search(r"\b(19\d{2}|20[0-2]\d)\b", nl_query)
                if year_match and len(filtered) > 1 and table_notes:
                    asked_year = int(year_match.group(1))
                    # Parse "Cubre YYYY-YYYY" from table_notes
                    narrowed = []
                    for t in filtered:
                        note_match = _re.search(
                            rf"{_re.escape(t.table_name)}.*?[Cc]ubre\s+(\d{{4}})-(\d{{4}})",
                            table_notes,
                        )
                        if note_match:
                            start_y, end_y = int(note_match.group(1)), int(note_match.group(2))
                            if start_y <= asked_year <= end_y:
                                narrowed.append(t)
                        else:
                            narrowed.append(t)  # No note = keep
                    if narrowed:
                        filtered = narrowed
                        logger.info(
                            "Year filter %d narrowed tables from %d to %d: %s",
                            asked_year,
                            len(tables),
                            len(filtered),
                            [t.table_name for t in filtered],
                        )
                tables = filtered
            elif any("indec" in h for h in table_hints):
                indec_tables = [t.table_name for t in tables if "indec" in t.table_name]
                logger.info(
                    "INDEC fnmatch miss: hints=%s, indec_tables_in_cache=%d (sample: %s)",
                    table_hints,
                    len(indec_tables),
                    indec_tables[:3],
                )
                logger.info("No cached INDEC tables, attempting live fallback")
                return await indec_live_fallback(nl_query)
            else:
                # Planner specified tables but none matched via fnmatch.
                # Try vector search as last resort before giving up.
                logger.info(
                    "Sandbox: fnmatch miss for hints %s, trying vector search fallback",
                    table_hints,
                )
                vector_discovered = await discover_tables_by_vector_search(
                    nl_query,
                    tables,
                    embedding,
                    vector_search,
                )
                if vector_discovered:
                    hint_set = set(vector_discovered)
                    filtered = [t for t in tables if t.table_name in hint_set]
                    if filtered:
                        tables = filtered
                        logger.info(
                            "Sandbox: vector search fallback found %d table(s): %s",
                            len(filtered),
                            [t.table_name for t in filtered[:5]],
                        )
                    else:
                        return []
                else:
                    logger.warning(
                        "Sandbox: none of the hinted tables %s found in cache, skipping",
                        table_hints,
                    )
                    return []

        # Smart table ordering: when the query asks for rates/indices,
        # put pre-aggregated tables (with "tasa", "indice", "porcentaje" in name)
        # first so the NL2SQL LLM sees them at the top of the context.
        _query_lower = nl_query.lower()
        if any(kw in _query_lower for kw in ("tasa", "indice", "índice", "porcentaje")):
            _rate_keywords = ("tasa_", "indice_", "porcentaje_")
            tables = sorted(
                tables,
                key=lambda t: (
                    0 if any(kw in t.table_name for kw in _rate_keywords) else 1,
                    t.row_count or 999_999_999,
                ),
            )

        # Enrich tables with semantic catalog metadata if available
        catalog_entries = await get_catalog_entries([t.table_name for t in tables[:50]], sandbox)

        if _is_dataset_discovery_query(nl_query):
            logger.info(
                "Sandbox step %s resolved as dataset discovery query; returning %d table candidates without NL2SQL",
                step.id,
                len(tables),
            )
            return _build_dataset_discovery_results(
                nl_query=nl_query,
                tables=tables,
                catalog_entries=catalog_entries,
            )

        tables_context_parts = []
        for t in tables[:50]:
            cols = ", ".join(t.columns) if t.columns else "(no column info)"
            entry = catalog_entries.get(t.table_name)
            if entry:
                name = entry.get("display_name") or t.table_name
                desc = entry.get("description") or ""
                domain = entry.get("domain") or ""
                col_types = entry.get("column_types") or {}
                col_desc = (
                    ", ".join(
                        f"{c} ({col_types[c]})" if c in col_types else c for c in (t.columns or [])
                    )
                    or cols
                )
                part = f"Table: {t.table_name} — {name}  (rows: {t.row_count or '?'})"
                if desc:
                    part += f"\n  Descripción: {desc}"
                if domain:
                    part += f"\n  Dominio: {domain}"
                part += f"\n  Columns: {col_desc}"
            else:
                part = f"Table: {t.table_name}  (rows: {t.row_count or '?'})\n  Columns: {cols}"
            tables_context_parts.append(part)
        tables_context = "\n\n".join(tables_context_parts)

        compat_notes = build_table_compat_notes([t.table_name for t in tables[:50]])
        all_table_notes = "\n".join(note for note in [table_notes, compat_notes] if note)

        # Inject table_notes (resolved early at top of function) into NL2SQL context
        if all_table_notes:
            tables_context += f"\n\nNOTAS SOBRE LAS TABLAS:\n{all_table_notes}"

        # MASTERPLAN Fase 4.5b — when the planner picked a mart, append its
        # canonical-column semantics (sourced from `pg_description`, which
        # `build_mart` populates via `COMMENT ON COLUMN`) to the NL2SQL
        # context. The LLM uses these to write SQL with the right semantic
        # intent instead of guessing from column names alone.
        if serving_port is not None:
            try:
                semantics_block = await _mart_semantics_block(
                    serving_port, [t.table_name for t in tables[:10]]
                )
                if semantics_block:
                    tables_context += f"\n\n{semantics_block}"
            except Exception:
                logger.debug("mart semantics block failed", exc_info=True)

        # Retrieve dynamic few-shot examples from successful past queries
        few_shot_block = await get_few_shot_examples(nl_query, embedding, semantic_cache)

        # Build display descriptions from catalog entries for the queried
        # table(s). These land in DataResult.metadata.table_descriptions
        # via the subgraph's format_result_node.
        table_descriptions: list[str] = []
        for t in tables[:5]:
            entry = catalog_entries.get(t.table_name)
            if entry:
                desc_parts = []
                if entry.get("display_name"):
                    desc_parts.append(entry["display_name"])
                if entry.get("description"):
                    desc_parts.append(entry["description"])
                if desc_parts:
                    table_descriptions.append(f"{t.table_name}: {' — '.join(desc_parts)}")

        # Hand off to the NL2SQL subgraph. It owns the generate → execute
        # → fix → last_resort → indec_fallback → save_success → format
        # state machine. See specs/010-sandbox-sql/010b-nl2sql/ for the
        # contract. FIX-004 (2026-04-11).
        from app.application.pipeline.subgraphs.nl2sql import (
            get_compiled_nl2sql_subgraph,
            nl2sql_runtime,
        )

        compiled_subgraph = await get_compiled_nl2sql_subgraph()
        initial_state: dict[str, Any] = {
            "nl_query": nl_query,
            "tables": tables,
            "tables_context": tables_context,
            "table_notes": all_table_notes,
            "catalog_entries": catalog_entries,
            "table_descriptions": table_descriptions,
            "few_shot_block": few_shot_block or "",
            "max_attempts": 2,
        }
        with nl2sql_runtime(
            llm=llm,
            sandbox=sandbox,
            embedding=embedding,
            semantic_cache=semantic_cache,
            serving_port=serving_port,
        ):
            final_state = await compiled_subgraph.ainvoke(initial_state)
        results = final_state.get("data_results", []) or []

        # MASTERPLAN Fase 4.5c — annotate every result with the medallion
        # layer the SQL touched. The metric is a side-effect of the SQL
        # already produced; nothing else changes about the execution path.
        executed_sql = final_state.get("generated_sql", "")
        if executed_sql:
            served_layer = detect_serving_layer_in_sql(executed_sql)
            for r in results:
                if r.metadata is None:
                    r.metadata = {}
                r.metadata.setdefault("served_from_layer", served_layer)
            try:
                from app.infrastructure.monitoring.metrics import MetricsCollector

                MetricsCollector().record_connector_call(
                    f"sandbox:served_from:{served_layer}",
                    latency_ms=0,
                    error=False,
                )
            except Exception:
                logger.debug("Could not record served_from_layer metric", exc_info=True)

        return results
    except Exception:
        logger.warning("Sandbox step %s failed", step.id, exc_info=True)
        return []
