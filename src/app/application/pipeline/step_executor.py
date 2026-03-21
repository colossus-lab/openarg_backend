"""Step dispatch, retry, and parallel execution for the smart query pipeline."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

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


# ── Step dispatch ─────────────────────────────────────────────


async def dispatch_step(
    step: PlanStep,
    deps: ConnectorDeps,
    nl_query: str = "",
) -> list[DataResult]:
    """Route a single plan step to the appropriate connector function."""
    # Sync handler (ddjj is not async)
    if step.action == "query_ddjj":
        return execute_ddjj_step(step, deps.ddjj)

    if step.action == "query_series":
        return await execute_series_step(step, deps.series)
    if step.action == "query_argentina_datos":
        return await execute_argentina_datos_step(step, deps.arg_datos)
    if step.action == "query_georef":
        return await execute_georef_step(step, deps.georef)
    if step.action == "search_ckan":
        return await execute_ckan_step(step, deps.ckan, deps.sandbox)
    if step.action == "query_sesiones":
        return await execute_sesiones_step(step, deps.sesiones)
    if step.action == "query_staff":
        return await execute_staff_step(step, deps.staff)
    if step.action == "query_bcra":
        return await execute_bcra_step(step, deps.bcra)
    if step.action == "search_datasets":
        return await execute_search_datasets_step(step, deps.embedding, deps.vector_search)
    if step.action == "query_sandbox":
        return await execute_sandbox_step(
            step,
            deps.sandbox,
            deps.llm,
            deps.embedding,
            deps.vector_search,
            deps.semantic_cache,
            user_query=nl_query,
        )
    if step.action in _NOOP_ACTIONS:
        return []
    logger.info("Unknown step action '%s', skipping", step.action)
    return []


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
