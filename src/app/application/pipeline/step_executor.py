"""Step dispatch, retry, and parallel execution for the smart query pipeline."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.application.pipeline.connectors.argentina_datos import execute_argentina_datos_step
from app.application.pipeline.connectors.bcra import execute_bcra_step
from app.application.pipeline.connectors.ckan import execute_ckan_step
from app.application.pipeline.connectors.ddjj import execute_ddjj_step
from app.application.pipeline.connectors.georef import execute_georef_step
from app.application.pipeline.connectors.sandbox import execute_sandbox_step
from app.application.pipeline.connectors.series import execute_series_step
from app.application.pipeline.connectors.sesiones import execute_sesiones_step
from app.application.pipeline.connectors.staff import execute_staff_step
from app.application.pipeline.connectors.vector_search import execute_search_datasets_step
from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError

if TYPE_CHECKING:
    from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
    from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
    from app.domain.ports.connectors.georef import IGeorefConnector
    from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
    from app.domain.ports.connectors.sesiones import ISesionesConnector
    from app.domain.ports.connectors.staff import IStaffConnector
    from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider
    from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
    from app.domain.ports.search.vector_search import IVectorSearch
    from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
    from app.infrastructure.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


# ── Connector dependencies container ──────────────────────────


@dataclass
class ConnectorDeps:
    """All connector dependencies needed by the step dispatcher."""

    series: ISeriesTiempoConnector
    arg_datos: IArgentinaDatosConnector
    georef: IGeorefConnector
    ckan: ICKANSearchConnector
    sesiones: ISesionesConnector
    ddjj: DDJJAdapter
    staff: IStaffConnector | None
    bcra: BCRAAdapter | None
    sandbox: ISQLSandbox | None
    vector_search: IVectorSearch
    llm: ILLMProvider
    embedding: IEmbeddingProvider
    semantic_cache: SemanticCache
    # MASTERPLAN Fase 4.5b/c — Serving Port for nodes that benefit from
    # schemas + semantics or read marts directly. Optional; legacy paths
    # still work when this is None.
    serving_port: object | None = None
    # H4 (round v46) — the authenticated caller's identifier. Propagated
    # to `execute_sandbox_step` so `get_few_shot_examples` can scope
    # the cross-tenant prompt-poisoning store (`successful_queries`) by
    # owner. None for unauthenticated or legacy paths.
    user_id: str | None = None


# ── Retryable error detection ─────────────────────────────────

_RETRYABLE_PATTERNS = ("timeout", "503", "502", "504", "connection", "reset", "refused")


def is_retryable(exc: Exception) -> bool:
    """Return *True* if *exc* looks like a transient/network error."""
    exc_str = str(exc).lower()
    if any(p in exc_str for p in _RETRYABLE_PATTERNS):
        return True
    if "Timeout" in type(exc).__name__:
        return True
    return False


# ── No-op actions (handled by the analyst, not a connector) ───

_NOOP_ACTIONS = frozenset(("analyze", "compare", "synthesize"))


# ── Live-connector wall-clock cap (BUG-002) ───────────────────
# A `search_ckan` step that scrapes a large dataset live has been
# observed at 99s, and a 124s step killed the WebSocket mid-stream.
# Cap live-connector steps so a pathologically slow fetch fails fast and
# gracefully (returns no rows → the analyst answers "sin datos") instead
# of hanging the request. Mart/sandbox steps are NOT capped here — a big
# mart query legitimately takes 20-45s.
_LIVE_CONNECTOR_ACTIONS = frozenset(("search_ckan",))
_LIVE_CONNECTOR_TIMEOUT_S = 60.0

# ── Mart redirect safety net (BUG-001) ────────────────────────
# Last line of defense: if the planner still emits a `search_ckan` step
# but a curated mart confidently covers the question, the executor
# redirects to the sandbox/mart path. This ONLY fires when a strong mart
# exists — a topic with NO mart (or a weak match) yields a low score and
# is left to `search_ckan` untouched, so live-only topics keep working.
# Round v4.4 verification (R002 peajes) showed mart sim 0.630 passing
# easily, but other questions with weaker matches needed coverage. Lowered
# from 0.50 → 0.45 to capture more cases. Tests on v4.3 baseline confirmed
# no-mart questions (e.g. "receta de asado") stay at sim 0.43-0.44 → still
# don't get redirected.
_MART_REDIRECT_THRESHOLD = 0.45


async def _top_mart_similarity(nl_query: str, deps: ConnectorDeps) -> float:
    """Cosine similarity of the best-matching mart for ``nl_query``.

    Returns 0.0 on any failure, when there is no sandbox, or when no mart
    exists — guaranteeing a no-mart topic is never redirected.
    """
    sandbox = deps.sandbox
    if sandbox is None or not nl_query.strip():
        return 0.0
    try:
        from sqlalchemy import text as _text

        q_emb = await deps.embedding.embed(nl_query)
        emb_str = "[" + ",".join(str(x) for x in q_emb) + "]"

        def _query() -> float:
            engine = sandbox._get_engine()  # type: ignore[union-attr]
            with engine.connect() as conn:
                row = conn.execute(
                    _text(
                        "SELECT 1 - (embedding <=> CAST(:e AS vector)) AS sim "
                        "FROM mart_definitions "
                        "WHERE embedding IS NOT NULL "
                        "  AND COALESCE(last_row_count, 0) > 0 "
                        "  AND NOT COALESCE(serving_blocked, FALSE) "
                        "ORDER BY embedding <=> CAST(:e AS vector) LIMIT 1"
                    ),
                    {"e": emb_str},
                ).fetchone()
                conn.rollback()
            return float(row.sim) if row else 0.0

        return await asyncio.to_thread(_query)
    except Exception:
        logger.debug("top mart similarity check failed", exc_info=True)
        return 0.0


# ── Step dispatch ─────────────────────────────────────────────


async def dispatch_step(
    step: PlanStep,
    deps: ConnectorDeps,
    nl_query: str = "",
) -> list[DataResult]:
    """Route a single plan step to the appropriate connector function.

    Uses a dispatch table for O(1) action lookup instead of if/elif chain.
    """
    # BUG-001 safety net: the planner should already route mart-backed
    # questions to `query_sandbox` (hint suppression + planner rule), but
    # if a `search_ckan` step slips through AND a strong mart covers the
    # question, redirect to the sandbox/mart path. No mart → low score →
    # no redirect → `search_ckan` runs normally (live-only topics intact).
    if step.action == "search_ckan" and nl_query.strip() and deps.sandbox is not None:
        mart_sim = await _top_mart_similarity(nl_query, deps)
        if mart_sim >= _MART_REDIRECT_THRESHOLD:
            logger.info(
                "BUG-001: redirecting search_ckan step → sandbox/mart (top mart sim=%.3f >= %.2f)",
                mart_sim,
                _MART_REDIRECT_THRESHOLD,
            )
            return await execute_sandbox_step(
                step,
                deps.sandbox,
                deps.llm,
                deps.embedding,
                deps.vector_search,
                deps.semantic_cache,
                user_query=nl_query,
                serving_port=deps.serving_port,
                user_id=deps.user_id,
            )

    handler = _DISPATCH_TABLE.get(step.action)
    if handler is not None:
        result = handler(step, deps, nl_query)
        if not asyncio.iscoroutine(result):
            return result
        # BUG-002: cap live-connector steps so a slow fetch fails fast
        # instead of escalating to a 124s hang that kills the WebSocket.
        if step.action in _LIVE_CONNECTOR_ACTIONS:
            try:
                return await asyncio.wait_for(result, timeout=_LIVE_CONNECTOR_TIMEOUT_S)
            except TimeoutError:
                logger.warning(
                    "BUG-002: live connector step '%s' exceeded %.0fs cap — "
                    "aborting to keep the request responsive",
                    step.action,
                    _LIVE_CONNECTOR_TIMEOUT_S,
                )
                return []
        return await result

    if step.action in _NOOP_ACTIONS:
        return []
    logger.info("Unknown step action '%s', skipping", step.action)
    return []


# ── Dispatch table — O(1) action→handler mapping ────────────────

_DISPATCH_TABLE: dict[str, Callable[..., Any]] = {
    "query_ddjj": lambda s, d, q: execute_ddjj_step(s, d.ddjj),
    "query_series": lambda s, d, q: execute_series_step(s, d.series, user_query=q),
    "query_argentina_datos": lambda s, d, q: execute_argentina_datos_step(s, d.arg_datos),
    "query_georef": lambda s, d, q: execute_georef_step(s, d.georef),
    "search_ckan": lambda s, d, q: execute_ckan_step(s, d.ckan, d.sandbox),
    "query_sesiones": lambda s, d, q: execute_sesiones_step(s, d.sesiones),
    "query_staff": lambda s, d, q: execute_staff_step(s, d.staff),
    "query_bcra": lambda s, d, q: execute_bcra_step(s, d.bcra),
    "search_datasets": lambda s, d, q: execute_search_datasets_step(
        s, d.embedding, d.vector_search
    ),
    "query_sandbox": lambda s, d, q: execute_sandbox_step(
        s,
        d.sandbox,
        d.llm,
        d.embedding,
        d.vector_search,
        d.semantic_cache,
        user_query=q,
        serving_port=d.serving_port,
        user_id=d.user_id,
    ),
}


# ── Retry wrapper ─────────────────────────────────────────────


async def dispatch_step_with_retry(
    step: PlanStep,
    deps: ConnectorDeps,
    nl_query: str = "",
    max_retries: int = 2,
) -> list[DataResult]:
    """Call ``dispatch_step`` with up to *max_retries* retries on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return await dispatch_step(step, deps, nl_query=nl_query)
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries and is_retryable(exc):
                delay = 1.5 * (attempt + 1)
                logger.warning(
                    "Step %s attempt %d/%d failed (%s), retrying in %.1fs …",
                    step.id,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                raise
    # Unreachable in practice, but keeps mypy happy.
    raise last_exc  # type: ignore[misc]


# ── Friendly connector labels (Spanish UX) ────────────────────

_CONNECTOR_LABELS: dict[str, str] = {
    "query_series": "Series de Tiempo",
    "query_argentina_datos": "Argentina Datos",
    "query_georef": "Georef",
    "search_ckan": "Portales CKAN",
    "query_sesiones": "Sesiones del Congreso",
    "query_ddjj": "Declaraciones Juradas",
    "query_staff": "Personal legislativo",
    "query_bcra": "BCRA",
    "search_datasets": "Búsqueda semántica",
    "query_sandbox": "Sandbox SQL",
}


# ── Parallel step execution ───────────────────────────────────


async def execute_steps(
    plan: ExecutionPlan,
    deps: ConnectorDeps,
    metrics: MetricsCollector,
    nl_query: str = "",
    on_step_start: Callable[[PlanStep], None] | None = None,
) -> tuple[list[DataResult], list[str]]:
    """Execute plan steps, catching ConnectorError per step.

    Steps are grouped by dependency level and executed in parallel
    within each level using ``asyncio.gather()``.
    """
    results: list[DataResult] = []
    warnings: list[str] = []
    steps = plan.steps[:5]

    if not steps:
        return results, warnings

    # BUG-001 Capa 1: strip redundant search_ckan when a mart covers the
    # question. This runs at every execute_steps invocation — including
    # replan-emitted plans — because `inject_fallbacks_node` only sees the
    # *initial* plan, not the one produced by `coordinator → replan`.
    # Round v4.4 verification revealed the planner ignores the rule mainly
    # on replan: a first-step SQL failure triggers replan, which emits
    # search_ckan "by trying broader sources". The redirect at dispatch_step
    # still catches it, but pays an extra NL2SQL round trip. Stripping
    # here cuts that loop short.
    has_sandbox = any(s.action == "query_sandbox" for s in steps)
    has_ckan = any(s.action == "search_ckan" for s in steps)
    if has_sandbox and has_ckan and nl_query.strip():
        try:
            sim = await _top_mart_similarity(nl_query, deps)
            if sim >= _MART_REDIRECT_THRESHOLD:
                dropped = [s for s in steps if s.action == "search_ckan"]
                steps = [s for s in steps if s.action != "search_ckan"]
                logger.info(
                    "BUG-001 Capa 1: dropped %d redundant search_ckan step(s) "
                    "in execute_steps — mart sim=%.3f >= %.2f",
                    len(dropped),
                    sim,
                    _MART_REDIRECT_THRESHOLD,
                )
        except Exception:
            logger.debug("BUG-001 Capa 1 (execute_steps) check failed", exc_info=True)

    if not steps:
        return results, warnings

    # --- build a lookup and group steps by dependency level ---
    step_by_id: dict[str, PlanStep] = {s.id: s for s in steps}
    step_ids = set(step_by_id)
    remaining = list(steps)
    levels: list[list[PlanStep]] = []

    completed_ids: set[str] = set()
    while remaining:
        # Steps whose dependencies are all completed (or absent)
        ready = [
            s
            for s in remaining
            if all(dep in completed_ids or dep not in step_ids for dep in s.depends_on)
        ]
        if not ready:
            # Avoid infinite loop: treat remaining steps as ready
            ready = list(remaining)
        levels.append(ready)
        completed_ids.update(s.id for s in ready)
        remaining = [s for s in remaining if s.id not in completed_ids]

    # --- wrapper that runs a single step with error handling ---
    async def _run_step(step: PlanStep) -> tuple[list[DataResult], str | None]:
        if on_step_start is not None:
            on_step_start(step)
        step_start = time.monotonic()
        connector_name = step.action
        try:
            data = await dispatch_step_with_retry(step, deps, nl_query=nl_query)
            step_ms = round((time.monotonic() - step_start) * 1000, 1)
            metrics.record_connector_call(connector_name, step_ms)
            return data, None
        except ConnectorError as exc:
            step_ms = round((time.monotonic() - step_start) * 1000, 1)
            metrics.record_connector_call(connector_name, step_ms, error=True)
            logger.warning("Step %s failed (ConnectorError)", step.id, exc_info=True)
            return [], f"Conector '{connector_name}' no disponible: {exc}"
        except Exception as exc:
            step_ms = round((time.monotonic() - step_start) * 1000, 1)
            metrics.record_connector_call(connector_name, step_ms, error=True)
            logger.warning("Step %s failed", step.id, exc_info=True)
            return [], f"Conector '{connector_name}' falló: {type(exc).__name__}"

    # --- execute level by level, parallelising within each level ---
    for level in levels:
        outcomes = await asyncio.gather(*[_run_step(s) for s in level])
        for data, warning in outcomes:
            results.extend(data)
            if warning:
                warnings.append(warning)

    return results, warnings
