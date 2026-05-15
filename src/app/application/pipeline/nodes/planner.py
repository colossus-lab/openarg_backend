"""LangGraph nodes: planner (plan generation) and clarification reply."""

from __future__ import annotations

import asyncio
import logging
import os
import time

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.connectors.sandbox import discover_catalog_hints_for_planner
from app.application.pipeline.history import record_terminal_analytics
from app.application.pipeline.state import OpenArgState
from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.infrastructure.adapters.connectors.query_planner import (
    _classify_ambiguity,
    generate_plan,
)

logger = logging.getLogger(__name__)


def _parallel_classifier_enabled() -> bool:
    """Feature flag for the parallel classifier optimization.

    Default ON. Set `OPENARG_PARALLEL_AMBIGUITY_CLASSIFIER=0` to disable
    and revert to the legacy sequential flow.
    """
    raw = os.getenv("OPENARG_PARALLEL_AMBIGUITY_CLASSIFIER", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _high_confidence_skip_threshold() -> float:
    """Threshold for skipping the ambiguity classifier when a curated
    `mart_sample_queries` row matches the user's question with very high
    cosine similarity. Above this number we treat the query as clearly
    answerable by the matching mart and bypass the 1-2s classifier LLM
    call. Disable by setting to a value > 1.0 (impossible to reach).
    """
    raw = os.getenv("OPENARG_SKIP_CLASSIFIER_MART_SIM", "0.80").strip()
    try:
        return float(raw)
    except ValueError:
        return 0.80


def _plan_cache_enabled() -> bool:
    """Feature flag for the plan cache (default ON).

    When ON, similar queries (cosine sim >= threshold to a cached plan)
    skip the planner LLM call (~3-4s) and reuse the cached
    `ExecutionPlan`. Set `OPENARG_PLAN_CACHE_ENABLED=0` to disable.
    """
    raw = os.getenv("OPENARG_PLAN_CACHE_ENABLED", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _plan_cache_threshold() -> float:
    raw = os.getenv("OPENARG_PLAN_CACHE_HIT_THRESHOLD", "0.95").strip()
    try:
        return float(raw)
    except ValueError:
        return 0.95


def _plan_cache_ttl_seconds() -> int:
    raw = os.getenv("OPENARG_PLAN_CACHE_TTL_SECONDS", "604800").strip()
    try:
        return int(raw)
    except ValueError:
        return 604800  # 7 days


async def _try_plan_cache_hit(
    deps,
    q_embedding: list[float] | None,
) -> dict | None:
    """Look up a cached `ExecutionPlan` by embedding similarity.

    Returns the deserialized plan dict on hit (caller wraps it in
    `ExecutionPlan`), or None on miss / disabled / error.
    """
    if not q_embedding or deps.sandbox is None:
        return None
    if not _plan_cache_enabled():
        return None
    import asyncio

    from sqlalchemy import text

    threshold = _plan_cache_threshold()
    embedding_str = "[" + ",".join(str(x) for x in q_embedding) + "]"
    loop = asyncio.get_running_loop()

    def _query() -> dict | None:
        try:
            engine = deps.sandbox._get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT plan_json, "
                        "1 - (embedding <=> CAST(:e AS vector)) AS sim, "
                        "question "
                        "FROM query_plan_cache "
                        "WHERE embedding IS NOT NULL "
                        "  AND expires_at > now() "
                        "ORDER BY embedding <=> CAST(:e AS vector) "
                        "LIMIT 1"
                    ),
                    {"e": embedding_str},
                ).fetchone()
                conn.rollback()
                if row is None:
                    return None
                sim = float(row[1] or 0)
                if sim < threshold:
                    return None
                logger.info(
                    "plan_cache HIT sim=%.4f cached_q=%r",
                    sim,
                    str(row[2])[:80],
                )
                return dict(row[0])
        except Exception:
            logger.debug("plan_cache lookup failed", exc_info=True)
            return None

    return await loop.run_in_executor(None, _query)


async def _store_plan_cache(
    deps,
    question: str,
    q_embedding: list[float] | None,
    plan,
) -> None:
    """Persist `plan` for future similarity lookups. Best-effort; errors
    swallowed because the planner already produced the answer.
    """
    if not q_embedding or deps.sandbox is None or plan is None:
        return
    if not _plan_cache_enabled():
        return
    if getattr(plan, "intent", None) == "clarification":
        # Don't cache clarification stubs — those are query-specific
        # follow-ups and lose meaning when applied to a different query.
        return
    import asyncio
    import hashlib
    import json
    from dataclasses import asdict, is_dataclass

    from sqlalchemy import text

    embedding_str = "[" + ",".join(str(x) for x in q_embedding) + "]"
    ttl = _plan_cache_ttl_seconds()
    qhash = hashlib.sha256(question.encode("utf-8")).hexdigest()
    try:
        if is_dataclass(plan) and not isinstance(plan, type):
            plan_dict = asdict(plan)
        elif hasattr(plan, "model_dump"):
            plan_dict = plan.model_dump()
        else:
            plan_dict = dict(plan.__dict__)
        plan_json = json.dumps(plan_dict, default=str)
    except Exception:
        logger.debug("plan_cache serialization failed", exc_info=True)
        return
    loop = asyncio.get_running_loop()

    def _insert() -> None:
        try:
            engine = deps.sandbox._get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO query_plan_cache "
                        "(question_hash, question, embedding, plan_json, "
                        " ttl_seconds, expires_at) "
                        "VALUES (:h, :q, CAST(:e AS vector), CAST(:p AS jsonb), "
                        "        :ttl, now() + (:ttl || ' seconds')::interval) "
                        "ON CONFLICT (question_hash) DO UPDATE SET "
                        "  plan_json = EXCLUDED.plan_json, "
                        "  embedding = EXCLUDED.embedding, "
                        "  expires_at = EXCLUDED.expires_at"
                    ),
                    {
                        "h": qhash,
                        "q": question,
                        "e": embedding_str,
                        "p": plan_json,
                        "ttl": ttl,
                    },
                )
        except Exception:
            logger.debug("plan_cache insert failed", exc_info=True)

    await loop.run_in_executor(None, _insert)


def _plan_from_cached_dict(plan_dict: dict):
    """Rehydrate an `ExecutionPlan` from a cached JSON dict.

    The dataclass shape is `ExecutionPlan(query, intent, steps=[PlanStep,...])`
    and is plain-dataclass so we can reconstruct directly.
    """
    try:
        steps_in = plan_dict.get("steps") or []
        steps = []
        for s in steps_in:
            if isinstance(s, dict):
                steps.append(PlanStep(
                    id=str(s.get("id", "")),
                    action=str(s.get("action", "")),
                    description=str(s.get("description", "")),
                    params=s.get("params") or {},
                ))
        return ExecutionPlan(
            query=str(plan_dict.get("query", "")),
            intent=str(plan_dict.get("intent", "")),
            steps=steps,
        )
    except Exception:
        logger.debug("plan rehydration failed", exc_info=True)
        return None


async def _top_mart_sample_sim(deps, q_embedding: list[float]) -> float:
    """Return the highest cosine similarity between the user's query
    embedding and any curated `mart_sample_queries` row.

    Cheap single-row vector query (~50ms with HNSW). Used as a confidence
    signal: a high value means the query is clearly answerable by an
    existing mart, so we can skip the ambiguity classifier.
    """
    if not q_embedding or deps.sandbox is None:
        return 0.0
    import asyncio

    from sqlalchemy import text

    embedding_str = "[" + ",".join(str(x) for x in q_embedding) + "]"
    loop = asyncio.get_running_loop()

    def _query() -> float:
        try:
            engine = deps.sandbox._get_engine()
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT MAX(1 - (embedding <=> CAST(:e AS vector))) AS top_sim "
                        "FROM mart_sample_queries WHERE embedding IS NOT NULL"
                    ),
                    {"e": embedding_str},
                ).fetchone()
                conn.rollback()
                if row is None or row[0] is None:
                    return 0.0
                return float(row[0])
        except Exception:
            logger.debug("top_mart_sample_sim query failed", exc_info=True)
            return 0.0

    return await loop.run_in_executor(None, _query)


async def planner_node(state: OpenArgState) -> dict:
    """Discover relevant cached tables and generate an execution plan via LLM.

    Writes *catalog_hints*, *plan*, and *plan_intent* to state.

    **Parallel optimization (`OPENARG_PARALLEL_AMBIGUITY_CLASSIFIER=1`,
    default):** the ambiguity classifier LLM call (~1-2s) is fired in
    parallel with `discover_catalog_hints` (~3-5s, includes reranker LLM).
    Without history this saves the classifier latency entirely; the
    classifier output gates discover via task cancellation when it
    triggers a clarification. Disable by setting the env var to 0.
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "planning", "detail": "Planificando estrategia..."})
    deps = nodes_pkg.get_deps()

    try:
        preprocessed_q = state.get("preprocessed_query", state["question"])
        planner_ctx = state.get("planner_ctx", "")

        # Inject skill context if a skill was detected
        skill_ctx = state.get("skill_context")
        if skill_ctx and skill_ctx.get("planner"):
            planner_ctx = (planner_ctx or "") + "\n\n--- SKILL ACTIVA ---\n" + skill_ctx["planner"]

        has_history = bool(planner_ctx and planner_ctx.strip())

        # Compute query embedding once and reuse across the plan cache
        # lookup, fast-path confidence check, and discover. Without
        # history we also try the plan cache (skips planner LLM ~3-4s
        # on a similar query hit) and the high-mart-sim classifier
        # bypass.
        q_embedding: list[float] | None = None
        skip_classifier_high_conf = False
        cached_plan_dict: dict | None = None
        if not has_history and deps.embedding is not None:
            try:
                q_embedding = await deps.embedding.embed(preprocessed_q)
                # Plan cache lookup BEFORE running the planner LLM. On
                # hit we still run discover (cheap, ~100ms vector search)
                # for the catalog_hints field that downstream nodes may
                # consume for explanations, but skip the planner LLM.
                cached_plan_dict = await _try_plan_cache_hit(deps, q_embedding)
                if cached_plan_dict is None:
                    top_sim = await _top_mart_sample_sim(deps, q_embedding)
                    if top_sim >= _high_confidence_skip_threshold():
                        skip_classifier_high_conf = True
                        logger.info(
                            "ambiguity_classifier skipped query=%r reason=high_mart_sim sim=%.3f",
                            preprocessed_q[:100],
                            top_sim,
                        )
            except Exception:
                logger.debug("pre-planner embedding/sim check failed", exc_info=True)
                q_embedding = None

        use_parallel = (
            _parallel_classifier_enabled()
            and not has_history
            and not skip_classifier_high_conf
        )

        async def _discover() -> str:
            return await discover_catalog_hints_for_planner(
                preprocessed_q,
                deps.sandbox,
                deps.embedding,
                serving_port=deps.serving_port,
                llm=deps.llm,
                precomputed_embedding=q_embedding,
            )

        catalog_hints = ""
        clar_result: dict | None = None

        # Plan cache hit fast path: rehydrate the cached plan and skip
        # the planner LLM call. Discover still runs (cheap) so downstream
        # nodes have catalog_hints for prompt enrichment.
        if cached_plan_dict is not None:
            cached_plan = _plan_from_cached_dict(cached_plan_dict)
            if cached_plan is not None:
                try:
                    catalog_hints = await _discover()
                except Exception:
                    logger.exception("discover failed on plan-cache-hit path")
                    catalog_hints = ""
                return {
                    "catalog_hints": catalog_hints,
                    "plan": cached_plan,
                    "plan_intent": cached_plan.intent,
                }

        if skip_classifier_high_conf:
            # Direct path: skip both the parallel classifier and the
            # in-planner classifier. Just discover hints and run the
            # planner LLM straight.
            try:
                catalog_hints = await _discover()
            except Exception:
                logger.exception("discover_catalog_hints failed (high-confidence path)")
                catalog_hints = ""
        elif use_parallel:
            # Run classifier and discover in parallel. The classifier is
            # typically faster (~1-2s) than discover (~3-5s including
            # reranker LLM); when it returns a clarification we cancel
            # discover and short-circuit. When it returns None we wait
            # for discover and proceed with the planner LLM (with
            # `skip_classifier=True` to avoid running it twice).
            discover_task = asyncio.create_task(_discover())
            clar_task = asyncio.create_task(_classify_ambiguity(deps.llm, preprocessed_q))
            try:
                clar_result = await clar_task
            except Exception:
                logger.debug("parallel classifier failed; treating as not-ambiguous", exc_info=True)
                clar_result = None
            if clar_result:
                discover_task.cancel()
                try:
                    await discover_task
                except (asyncio.CancelledError, Exception):
                    pass
                # Build clarification plan directly (avoids planner LLM call).
                question_text = clar_result.get("question", "¿Podés ser más específico?")
                plan = ExecutionPlan(
                    query=preprocessed_q,
                    intent="clarification",
                    steps=[
                        PlanStep(
                            id="clarification",
                            action="clarification",
                            description=question_text,
                            params={
                                "question": question_text,
                                "options": clar_result.get("options", []),
                            },
                        )
                    ],
                )
                return {
                    "catalog_hints": "",
                    "plan": plan,
                    "plan_intent": plan.intent,
                }
            try:
                catalog_hints = await discover_task
            except Exception:
                logger.exception("discover_catalog_hints failed (parallel path)")
                catalog_hints = ""
        else:
            catalog_hints = await _discover()

        # Generate the plan. `skip_classifier=True` when we already ran
        # the classifier (parallel path) or decided to bypass it
        # (high-confidence mart match path). Sequential fallback +
        # history path keep the legacy behaviour.
        plan = await generate_plan(
            deps.llm,
            preprocessed_q,
            memory_context=planner_ctx,
            catalog_hints=catalog_hints,
            skip_classifier=use_parallel or skip_classifier_high_conf,
        )

        # Persist plan to cache for similarity reuse on future queries.
        # Best-effort (errors swallowed). Skipped for clarifications +
        # history-bound queries (handled inside _store_plan_cache).
        if not has_history and q_embedding is not None and plan is not None:
            try:
                await _store_plan_cache(deps, preprocessed_q, q_embedding, plan)
            except Exception:
                logger.debug("plan cache store failed", exc_info=True)

        return {
            "catalog_hints": catalog_hints,
            "plan": plan,
            "plan_intent": plan.intent,
        }
    except Exception:
        logger.exception("planner_node failed")
        return {
            "catalog_hints": "",
            "plan": None,
            "plan_intent": "error",
            "error": "Planner failed",
        }


async def clarify_reply_node(state: OpenArgState) -> dict:
    """Build a clarification answer from the plan (terminal node).

    When the planner determines the query is ambiguous, it returns a
    plan with ``intent="clarification"`` and a clarification step
    containing a question and clickable options.
    """
    # BUG-016/017: log every terminal exit to query_analytics (best-effort —
    # telemetry must never break the response path).
    async def _log_analytics(ok: bool) -> None:
        try:
            deps = nodes_pkg.get_deps()
            duration_ms = int(
                (time.monotonic() - state.get("_start_time", time.monotonic())) * 1000
            )
            await record_terminal_analytics(
                question=state.get("question", ""),
                served_table="clarification",
                row_count=0,
                success=ok,
                duration_ms=duration_ms,
                error_message=None if ok else "planner_failed",
                semantic_cache=deps.semantic_cache,
            )
        except Exception:
            logger.debug("clarify_reply_node: analytics logging skipped", exc_info=True)

    plan = state.get("plan")
    if not plan:
        await _log_analytics(ok=False)
        return {
            "clean_answer": "No pude procesar tu consulta. Probá reformulándola.",
            "sources": [],
            "chart_data": None,
            "confidence": 1.0,
            "citations": [],
            "documents": None,
            "tokens_used": 0,
            "warnings": [],
        }

    from langgraph.config import get_stream_writer

    writer = get_stream_writer()

    clar_step = next((s for s in plan.steps if s.action == "clarification"), None)
    if clar_step:
        clar_q = clar_step.params.get("question", "¿Podés ser más específico?")
        clar_opts = clar_step.params.get("options", [])
    else:
        clar_q = "¿Podés ser más específico?"
        clar_opts = []

    # Emit clarification event so the frontend renders clickable chips
    writer(
        {
            "type": "clarification",
            "question": clar_q,
            "options": clar_opts,
        }
    )

    opts_text = "\n".join(f"- {o}" for o in clar_opts) if clar_opts else ""
    answer = f"**{clar_q}**\n\n{opts_text}" if opts_text else f"**{clar_q}**"

    await _log_analytics(ok=True)
    return {
        "clean_answer": answer,
        "sources": [],
        "plan_intent": "clarification",
        "chart_data": None,
        "confidence": 1.0,
        "citations": [],
        "documents": None,
        "tokens_used": 0,
        "warnings": [],
    }
