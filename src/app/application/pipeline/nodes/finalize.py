"""LangGraph node: finalize — build sources/documents, write cache, update memory."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.cache_manager import write_cache
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
        "tokens_used": tokens_used,
        "documents": documents,
    }
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
        asyncio.create_task(
            _update_memory_bg(deps, session_id, memory, plan, results, clean_answer)
        )

    duration_ms = int((time.monotonic() - state.get("_start_time", time.monotonic())) * 1000)
    return {
        "clean_answer": clean_answer,
        "sources": sources,
        "documents": documents,
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
