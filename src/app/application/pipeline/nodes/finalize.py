"""LangGraph node: finalize — build sources/documents, write cache, update memory."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline._background_tasks import spawn_background
from app.application.pipeline.cache_manager import write_cache
from app.application.pipeline.history import record_terminal_analytics
from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.connectors.memory_agent import (
    save_memory,
    update_memory,
)
from app.infrastructure.audit.audit_logger import audit_query

logger = logging.getLogger(__name__)


def _extract_sources(results: list) -> list[dict[str, Any]]:
    """Build the sources list from data results."""
    return [
        {
            "name": r.dataset_title,
            "url": r.portal_url,
            "portal": r.portal_name,
            "accessed_at": r.metadata.get("fetched_at", ""),
        }
        for r in results
        if r.records
    ]


def _extract_documents(results: list) -> list[dict[str, Any]] | None:
    """Extract structured documents for frontend card rendering."""
    documents: list[dict[str, Any]] = []
    for r in results:
        if r.source.startswith("ddjj:"):
            for rec in r.records:
                if rec.get("nombre") and rec.get("patrimonio_cierre") is not None:
                    documents.append({**rec, "doc_type": "ddjj"})
    return documents if documents else None


async def finalize_node(state: OpenArgState) -> dict:
    """Build sources, documents, write cache, audit, and update memory (fire-and-forget).

    This is the final node before the graph terminates for a normal
    data-retrieval flow.
    """
    deps = nodes_pkg.get_deps()

    results = state.get("data_results", [])
    question = state["question"]
    user_id = state["user_id"]
    plan = state.get("plan")
    clean_answer = state.get("clean_answer", "")
    chart_data = state.get("chart_data")
    map_data = state.get("map_data")
    tokens_used = state.get("tokens_used", 0)
    last_embedding = state.get("last_embedding")
    all_warnings = list(state.get("step_warnings", []))

    # Build sources and documents
    sources = _extract_sources(results)
    documents = _extract_documents(results)

    # Record token usage
    if tokens_used:
        deps.metrics.record_tokens_used(tokens_used)

    # Audit
    plan_intent = state.get("plan_intent", plan.intent if plan else "unknown")
    audit_query(
        user=user_id,
        question=question,
        intent=plan_intent,
        duration_ms=int((time.monotonic() - state.get("_start_time", time.monotonic())) * 1000),
    )

    # Cache write (fire-and-forget)
    result_dict = {
        "answer": clean_answer,
        "sources": sources,
        "chart_data": chart_data,
        "map_data": map_data,
        "tokens_used": tokens_used,
        "documents": documents,
        "confidence": state.get("confidence", 1.0),
        "citations": state.get("citations", []),
        "warnings": all_warnings,
    }
    # No-data deflections must never be cached: a cached deflection gets
    # re-served verbatim to reformulations of the same question, locking
    # the user in a suggestion loop even after the data becomes reachable.
    no_data_deflection = bool(state.get("no_data_deflection"))
    if no_data_deflection:
        logger.debug("finalize_node: no-data deflection, skipping cache write")
    else:
        try:
            await write_cache(
                question,
                result_dict,
                plan_intent,
                deps.cache,
                deps.embedding,
                deps.semantic_cache,
                last_embedding=last_embedding,
            )
        except Exception:
            logger.debug("Cache write failed in finalize_node", exc_info=True)

    # Memory update (fire-and-forget background task)
    conversation_id = state.get("conversation_id", "")
    session_id = conversation_id or ""
    memory = state.get("memory")
    if memory and plan:
        spawn_background(
            _update_memory_bg(deps, session_id, memory, plan, results, clean_answer),
            name="finalize.memory_update",
        )

    duration_ms = int((time.monotonic() - state.get("_start_time", time.monotonic())) * 1000)
    # BUG-010: empty answers reached the client silently. Log them as ERROR
    # so monitoring can surface them; the response still ships so the user
    # gets some signal instead of a hang.
    answer_ok = bool(clean_answer and clean_answer.strip())
    if not answer_ok:
        logger.error(
            "finalize_node: empty answer for question=%r (results=%d, duration_ms=%d)",
            question[:120],
            len(results),
            duration_ms,
        )

    # BUG-016/017: finalize is the single terminal node for every data
    # flow (sandbox, connectors, marts), so it logs query_analytics
    # exactly once per query. The NL2SQL subgraph no longer logs here
    # (that double-counted sandbox queries and missed connector/mart
    # flows); the served table comes from the DataResult metadata.
    served_table = next(
        (r.metadata.get("served_table") or r.source for r in results if r.records),
        None,
    )
    # A no-data deflection has a non-empty answer but served no data —
    # log it as a failure with a distinct marker so /admin/analytics
    # surfaces it instead of counting it as a success.
    analytics_error: str | None
    if no_data_deflection:
        analytics_success = False
        analytics_error = "no_data_deflection"
    else:
        analytics_success = answer_ok
        analytics_error = None if answer_ok else "empty_answer"
    # A metric without a consumer is decoration. This is the consumer — the
    # one place where a person actually reads the number.
    #
    # 78.5 % of the resources we serve were last read more than 90 days ago and
    # the reader has no way to know it. The answer is not wrong; it is the best
    # reading of what we hold. Presenting it undated is what lets someone assume
    # a currency nobody promised.
    #
    # In a thread and wrapped, because a freshness lookup must never cost the
    # user their answer: worst case the line is absent, which is today's state.
    try:
        import asyncio as _asyncio

        from app.application.quality.data_age import staleness_warning
        from app.infrastructure.celery.tasks._db import get_sync_engine

        stale_line = await _asyncio.to_thread(
            staleness_warning, get_sync_engine(), served_table
        )
        if stale_line and stale_line not in all_warnings:
            all_warnings.append(stale_line)
    except Exception:
        logger.debug("finalize_node: freshness notice skipped", exc_info=True)

    try:
        await record_terminal_analytics(
            question=question,
            served_table=served_table,
            row_count=sum(len(r.records) for r in results),
            success=analytics_success,
            duration_ms=duration_ms,
            error_message=analytics_error,
            semantic_cache=deps.semantic_cache,
        )
    except Exception:
        logger.debug("finalize_node: analytics logging skipped", exc_info=True)
    # FR-036a: LangGraph's ``updates`` stream forwards ONLY what this node
    # returns; fields left out are silently dropped from the ``complete``
    # event even if they were populated earlier in the pipeline. Keep
    # chart_data, map_data, confidence and citations in lockstep here.
    return {
        "clean_answer": clean_answer,
        "sources": sources,
        "documents": documents,
        "chart_data": chart_data,
        "map_data": map_data,
        "confidence": state.get("confidence", 1.0),
        "citations": state.get("citations", []),
        "warnings": all_warnings,
        "duration_ms": duration_ms,
    }


async def _update_memory_bg(
    deps: Any,
    session_id: str,
    memory: Any,
    plan: Any,
    results: list,
    answer: str,
) -> None:
    """Fire-and-forget memory update — runs after the response is sent."""
    max_retries = 2
    backoff_base = 0.5
    last_exc: Exception | None = None

    for attempt in range(1 + max_retries):
        try:
            updated = await update_memory(deps.llm, memory, plan, results, answer)
            await save_memory(deps.cache, session_id, updated)
            return
        except asyncio.CancelledError:
            return
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = backoff_base * (2**attempt)
                logger.debug(
                    "Memory update attempt %d/%d failed, retrying in %.1fs",
                    attempt + 1,
                    1 + max_retries,
                    delay,
                    exc_info=True,
                )
                await asyncio.sleep(delay)

    logger.warning(
        "Background memory update failed after %d attempts: %s",
        1 + max_retries,
        last_exc,
    )
