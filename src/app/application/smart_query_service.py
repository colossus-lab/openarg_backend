"""Application service for the smart query pipeline.

Thin orchestrator that delegates to pipeline modules:
  - pipeline.classifiers     — casual/meta/injection/educational detection
  - pipeline.cache_manager   — Redis + semantic cache read/write
  - pipeline.history         — chat history load/save
  - pipeline.step_executor   — connector dispatch with retry + parallelism
  - pipeline.context_builder — LLM data context + capabilities block
  - pipeline.chart_builder   — deterministic charts, LLM chart extraction, META parsing
  - pipeline.connectors.sandbox — catalog hints for planner
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from app.application.pipeline.cache_manager import (
    check_cache,
    get_cached_dict,
    write_cache,
)
from app.application.pipeline.chart_builder import (
    build_deterministic_charts,
    extract_llm_charts,
    extract_meta,
)
from app.application.pipeline.classifiers import (
    _CASUAL_RESPONSES,  # noqa: F401 — re-export for tests
    _FAREWELL_PATTERN,  # noqa: F401 — re-export for tests
    _GREETING_PATTERN,  # noqa: F401 — re-export for tests
    _META_PATTERNS,  # noqa: F401 — re-export for tests
    _META_RESPONSE,  # noqa: F401 — re-export for tests
    _THANKS_PATTERN,  # noqa: F401 — re-export for tests
    DATA_ACTIONS,
    classify_request,
)
from app.application.pipeline.connectors.sandbox import (
    discover_catalog_hints_for_planner,
)
from app.application.pipeline.context_builder import (
    build_capabilities_block,
    build_data_context,
)
from app.application.pipeline.history import (
    load_chat_history,
    save_history,
)
from app.application.pipeline.step_executor import (
    ConnectorDeps,
    execute_steps,
)
from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.domain.ports.llm.llm_provider import LLMMessage
from app.infrastructure.adapters.connectors.memory_agent import (
    build_memory_context_prompt,
    load_memory,
    save_memory,
    update_memory,
)
from app.infrastructure.adapters.connectors.policy_agent import analyze_policy
from app.infrastructure.adapters.connectors.query_planner import generate_plan
from app.infrastructure.adapters.search.query_preprocessor import (
    expand_acronyms,
    expand_synonyms,
    normalize_provinces,
    normalize_temporal,
)
from app.infrastructure.audit.audit_logger import audit_query
from app.infrastructure.monitoring.metrics import MetricsCollector
from app.prompts import load_prompt

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.domain.ports.cache.cache_port import ICacheService
    from app.domain.ports.chat.chat_repository import IChatRepository
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

logger = logging.getLogger(__name__)

# Keep module-level alias so old `from smart_query_service import _DATA_ACTIONS` works
_DATA_ACTIONS = DATA_ACTIONS


# ── Result dataclass ────────────────────────────────────────


@dataclass
class SmartQueryResult:
    answer: str
    sources: list[dict[str, Any]]
    chart_data: list[dict[str, Any]] | None = None
    tokens_used: int = 0
    intent: str = ""
    cached: bool = False
    casual: bool = False
    educational: bool = False
    duration_ms: int = 0
    confidence: float = 1.0
    citations: list[dict[str, Any]] = field(default_factory=list)
    documents: list[dict[str, Any]] | None = None
    warnings: list[str] = field(default_factory=list)


def _dict_to_result(d: dict[str, Any]) -> SmartQueryResult:
    """Convert a cached dict to a SmartQueryResult."""
    return SmartQueryResult(
        answer=d.get("answer", ""),
        sources=d.get("sources", []),
        chart_data=d.get("chart_data"),
        tokens_used=d.get("tokens_used", 0),
        documents=d.get("documents"),
        cached=True,
    )


# ── Service ─────────────────────────────────────────────────


class SmartQueryService:
    """Orchestrates the full smart-query pipeline: plan -> dispatch -> analyze."""

    def __init__(
        self,
        llm: ILLMProvider,
        embedding: IEmbeddingProvider,
        vector_search: IVectorSearch,
        cache: ICacheService,
        series: ISeriesTiempoConnector,
        arg_datos: IArgentinaDatosConnector,
        georef: IGeorefConnector,
        ckan: ICKANSearchConnector,
        sesiones: ISesionesConnector,
        ddjj: DDJJAdapter,
        semantic_cache: SemanticCache,
        staff: IStaffConnector | None = None,
        bcra: BCRAAdapter | None = None,
        sandbox: ISQLSandbox | None = None,
        chat_repo: IChatRepository | None = None,
    ) -> None:
        self._llm = llm
        self._embedding = embedding
        self._vector_search = vector_search
        self._cache = cache
        self._series = series
        self._arg_datos = arg_datos
        self._georef = georef
        self._ckan = ckan
        self._sesiones = sesiones
        self._ddjj = ddjj
        self._semantic_cache = semantic_cache
        self._staff = staff
        self._bcra = bcra
        self._sandbox = sandbox
        self._chat_repo = chat_repo
        self._metrics = MetricsCollector()

    # ── Helper to build ConnectorDeps ──

    def _build_deps(self) -> ConnectorDeps:
        return ConnectorDeps(
            series=self._series,
            arg_datos=self._arg_datos,
            georef=self._georef,
            ckan=self._ckan,
            sesiones=self._sesiones,
            ddjj=self._ddjj,
            staff=self._staff,
            bcra=self._bcra,
            sandbox=self._sandbox,
            vector_search=self._vector_search,
            llm=self._llm,
            embedding=self._embedding,
            semantic_cache=self._semantic_cache,
        )

    # ── Public API ──────────────────────────────────────────

    async def execute(
        self,
        question: str,
        user_id: str,
        conversation_id: str = "",
        session: AsyncSession | None = None,
        policy_mode: bool = False,
    ) -> SmartQueryResult:
        """Run the full pipeline synchronously and return a result."""
        start_time = time.monotonic()

        # 0. Casual/meta/injection/educational -- instant responses (0 LLM calls)
        cls_type, cls_text = classify_request(question, user_id)
        if cls_type == "casual":
            return SmartQueryResult(answer=cls_text, sources=[], casual=True)
        if cls_type == "meta":
            return SmartQueryResult(answer=cls_text, sources=[])
        if cls_type == "injection":
            return SmartQueryResult(
                answer=cls_text,
                sources=[],
                intent="injection_blocked",
                duration_ms=int((time.monotonic() - start_time) * 1000),
            )
        if cls_type == "off_topic":
            return SmartQueryResult(
                answer=cls_text,
                sources=[],
                intent="off_topic",
                duration_ms=int((time.monotonic() - start_time) * 1000),
            )
        if cls_type == "educational":
            return SmartQueryResult(answer=cls_text, sources=[], educational=True)

        # 1. Cache lookup (skip in policy mode -- always fetch fresh data)
        last_embedding: list[float] | None = None
        if not policy_mode:
            cached_dict, last_embedding = await check_cache(
                question,
                user_id,
                self._cache,
                self._embedding,
                self._semantic_cache,
                self._metrics,
            )
            if cached_dict is not None:
                return _dict_to_result(cached_dict)

        # 2. Memory (Redis summaries + DB chat history)
        session_id = conversation_id or ""
        memory = await load_memory(self._cache, session_id)
        memory_ctx = build_memory_context_prompt(memory)
        memory_ctx_analyst = build_memory_context_prompt(memory, for_analyst=True)
        chat_history = await load_chat_history(conversation_id, self._chat_repo)
        planner_ctx = chat_history or memory_ctx

        # 3. Preprocess query
        _q = expand_acronyms(question)
        _q, _ = normalize_temporal(_q)
        _q = normalize_provinces(_q)
        preprocessed_q = expand_synonyms(_q)

        # 4. Discover relevant cached tables for the planner
        catalog_hints = await discover_catalog_hints_for_planner(
            preprocessed_q, self._sandbox, self._embedding
        )

        # 5. Plan (1 LLM call)
        plan = await generate_plan(
            self._llm,
            preprocessed_q,
            memory_context=planner_ctx,
            catalog_hints=catalog_hints,
        )

        # Handle clarification from planner
        clar_step = _get_clarification_step(plan)
        if clar_step is not None:
            clar_q = clar_step.params.get("question", "¿Podés ser más específico?")
            clar_opts = clar_step.params.get("options", [])
            opts_text = "\n".join(f"- {o}" for o in clar_opts) if clar_opts else ""
            answer = f"**{clar_q}**\n\n{opts_text}" if opts_text else f"**{clar_q}**"
            return SmartQueryResult(
                answer=answer,
                sources=[],
                intent="clarification",
                duration_ms=int((time.monotonic() - start_time) * 1000),
            )

        # Inject search_datasets fallback if plan has no vector/data step
        _inject_vector_fallback(plan, question, preprocessed_q)

        logger.info(
            "Plan for '%s': intent=%s, steps=%d",
            question[:60],
            plan.intent,
            len(plan.steps),
        )

        # 6. Dispatch steps (0 LLM calls -- just connector calls)
        deps = self._build_deps()
        all_warnings: list[str] = []
        results, step_warnings = await execute_steps(plan, deps, self._metrics, nl_query=question)
        all_warnings.extend(step_warnings)

        # 7. LLM analysis (1 LLM call)
        result_payloads = _collect_result_payloads(results)
        analysis_prompt = _build_analysis_prompt(
            question,
            plan,
            results,
            memory_ctx_analyst,
            all_warnings,
            has_records=result_payloads.has_records,
        )

        response = await self._llm.chat(
            messages=[
                LLMMessage(role="system", content=load_prompt("analyst")),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.4,
            max_tokens=8192,
        )

        # 8. Charts
        det_charts = build_deterministic_charts(results)
        llm_charts = extract_llm_charts(response.content)
        charts = det_charts if det_charts else llm_charts

        clean_answer = _strip_tags(response.content)

        # 8a. Parse META confidence/citations
        confidence, citations = extract_meta(clean_answer)
        clean_answer = _strip_meta(clean_answer)

        # Add disclaimer for low confidence
        if confidence < 0.5:
            clean_answer = (
                "**Nota:** La información disponible es limitada. "
                "Los datos presentados podrían ser parciales o requerir "
                "verificación adicional.\n\n" + clean_answer
            )

        # 8b. Policy analysis (optional, 1 LLM call)
        if policy_mode:
            try:
                policy_text = await analyze_policy(
                    self._llm, plan, results, clean_answer, memory_ctx
                )
                clean_answer += "\n\n---\n\n" + policy_text
            except Exception:
                logger.warning("Policy agent failed", exc_info=True)

        sources = result_payloads.sources
        documents = result_payloads.documents

        tokens_used = response.tokens_used or 0
        self._metrics.record_tokens_used(tokens_used)
        duration_ms = int((time.monotonic() - start_time) * 1000)

        result = SmartQueryResult(
            answer=clean_answer,
            sources=sources,
            chart_data=charts if charts else None,
            tokens_used=tokens_used,
            intent=plan.intent,
            duration_ms=duration_ms,
            confidence=confidence,
            citations=citations,
            documents=documents if documents else None,
            warnings=all_warnings,
        )

        # 9. Audit
        audit_query(user=user_id, question=question, intent=plan.intent, duration_ms=duration_ms)

        # 10. Memory update (fire-and-forget)
        asyncio.create_task(self._update_memory_bg(session_id, memory, plan, results, clean_answer))

        # 11. Save conversation history
        if session:
            await save_history(
                session,
                question,
                user_id,
                clean_answer,
                sources,
                tokens_used,
                duration_ms,
                plan_json=_serialize_plan(plan),
            )

        # 12. Cache write
        result_dict = {
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
            "tokens_used": tokens_used,
            "documents": documents if documents else None,
        }
        await write_cache(
            question,
            result_dict,
            plan.intent,
            self._cache,
            self._embedding,
            self._semantic_cache,
            last_embedding=last_embedding,
        )

        return result

    async def execute_streaming(
        self,
        question: str,
        user_id: str,
        conversation_id: str = "",
        policy_mode: bool = False,
    ) -> AsyncIterator[dict[str, Any]]:
        """Run the pipeline yielding status/chunk/complete/error events for WebSocket."""
        yield {"type": "status", "step": "classifying"}

        # Casual/meta/injection/educational -- instant responses
        cls_type, cls_text = classify_request(question, user_id)
        if cls_type is not None:
            yield {"type": "chunk", "content": cls_text}
            complete_evt: dict[str, Any] = {
                "type": "complete",
                "answer": cls_text,
                "sources": [],
                "chart_data": None,
            }
            if cls_type == "casual":
                complete_evt["casual"] = True
            yield complete_evt
            return

        # Cache check (skip in policy mode)
        session_id = conversation_id or ""
        if not policy_mode:
            cached_result, _ = await get_cached_dict(
                question,
                self._cache,
                self._embedding,
                self._semantic_cache,
            )
            if cached_result:
                yield {"type": "status", "step": "cache_hit"}
                answer = cached_result.get("answer", "")
                yield {"type": "chunk", "content": answer}
                yield {
                    "type": "complete",
                    "answer": answer,
                    "sources": cached_result.get("sources", []),
                    "chart_data": cached_result.get("chart_data"),
                    "cached": True,
                }
                return

        # Memory
        memory = await load_memory(self._cache, session_id)
        memory_ctx = build_memory_context_prompt(memory)
        memory_ctx_analyst = build_memory_context_prompt(memory, for_analyst=True)

        chat_history = await load_chat_history(conversation_id, self._chat_repo)
        planner_ctx = chat_history or memory_ctx

        # Preprocess query
        _q = expand_acronyms(question)
        _q, _ = normalize_temporal(_q)
        _q = normalize_provinces(_q)
        preprocessed_q = expand_synonyms(_q)

        # Discover relevant cached tables for the planner
        catalog_hints = await discover_catalog_hints_for_planner(
            preprocessed_q, self._sandbox, self._embedding
        )

        # Plan (1 LLM call)
        yield {"type": "status", "step": "planning"}
        plan = await generate_plan(
            self._llm,
            preprocessed_q,
            memory_context=planner_ctx,
            catalog_hints=catalog_hints,
        )

        # Handle clarification
        clar_step = _get_clarification_step(plan)
        if clar_step is not None:
            yield {
                "type": "clarification",
                "question": clar_step.params.get("question", "¿Podés ser más específico?"),
                "options": clar_step.params.get("options", []),
            }
            return

        # Inject search_datasets fallback
        _inject_vector_fallback(plan, question, preprocessed_q)

        yield {
            "type": "status",
            "step": "planned",
            "intent": plan.intent,
            "steps_count": len(plan.steps),
        }

        # Dispatch steps (0 LLM calls)
        yield {"type": "status", "step": "searching"}
        deps = self._build_deps()
        all_warnings: list[str] = []
        results, step_warnings = await execute_steps(plan, deps, self._metrics, nl_query=question)
        all_warnings.extend(step_warnings)

        # Analysis (1 LLM call, streamed)
        yield {"type": "status", "step": "generating"}

        result_payloads = _collect_result_payloads(results)
        analysis_prompt = _build_analysis_prompt(
            question,
            plan,
            results,
            memory_ctx_analyst,
            all_warnings,
            has_records=result_payloads.has_records,
        )

        full_parts: list[str] = []
        stream_buf = ""
        async for chunk in self._llm.chat_stream(
            messages=[
                LLMMessage(role="system", content=load_prompt("analyst")),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.4,
            max_tokens=8192,
        ):
            full_parts.append(chunk)
            stream_buf += chunk

            # If we're inside a tag, keep buffering
            if "<!--" in stream_buf:
                tag_start = stream_buf.index("<!--")
                if tag_start > 0:
                    yield {"type": "chunk", "content": stream_buf[:tag_start]}
                    stream_buf = stream_buf[tag_start:]
                if "-->" in stream_buf:
                    tag_end = stream_buf.index("-->") + 3
                    stream_buf = stream_buf[tag_end:]
                    if stream_buf and "<!--" not in stream_buf:
                        yield {"type": "chunk", "content": stream_buf}
                        stream_buf = ""
            else:
                yield {"type": "chunk", "content": stream_buf}
                stream_buf = ""

        # Flush remaining buffer
        if stream_buf:
            cleaned = _RE_ANY_TAG.sub("", stream_buf)
            cleaned = _RE_ANY_TAG_TRUNC.sub("", cleaned)
            if cleaned.strip():
                yield {"type": "chunk", "content": cleaned}

        full_text = "".join(full_parts)

        # Complete
        det_charts = build_deterministic_charts(results)
        llm_charts = extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts
        clean_answer = _strip_tags(full_text)

        confidence, citations = extract_meta(clean_answer)
        clean_answer = _strip_meta(clean_answer)

        # Policy analysis (optional, streamed)
        if policy_mode:
            try:
                yield {"type": "status", "step": "policy_analysis"}
                policy_text = await analyze_policy(
                    self._llm, plan, results, clean_answer, memory_ctx
                )
                separator = "\n\n---\n\n"
                yield {"type": "chunk", "content": separator + policy_text}
                clean_answer += separator + policy_text
            except Exception:
                logger.warning("Policy agent failed in streaming", exc_info=True)

        sources = result_payloads.sources
        documents = result_payloads.documents

        yield {
            "type": "complete",
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
            "confidence": confidence,
            "citations": citations,
            "documents": documents if documents else None,
            **({"warnings": all_warnings} if all_warnings else {}),
        }

        # Audit
        audit_query(user=user_id, question=question, intent=plan.intent, duration_ms=0)

        # Memory update (fire-and-forget)
        asyncio.create_task(self._update_memory_bg(session_id, memory, plan, results, clean_answer))

        # Cache write
        try:
            result_dict = {
                "answer": clean_answer,
                "sources": sources,
                "chart_data": charts if charts else None,
                "tokens_used": 0,
                "documents": documents if documents else None,
            }
            await write_cache(
                question,
                result_dict,
                plan.intent,
                self._cache,
                self._embedding,
                self._semantic_cache,
            )
        except Exception:
            logger.debug("Cache write failed in streaming", exc_info=True)

    # ── Background helpers ─────────────────────────────────

    async def _update_memory_bg(
        self,
        session_id: str,
        memory: Any,
        plan: ExecutionPlan,
        results: list,
        answer: str,
    ) -> None:
        """Fire-and-forget memory update -- runs after the response is sent."""
        max_retries = 2
        backoff_base = 0.5
        last_exc: Exception | None = None

        for attempt in range(1 + max_retries):
            try:
                updated = await update_memory(self._llm, memory, plan, results, answer)
                await save_memory(self._cache, session_id, updated)
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


# ── Module-level helpers ────────────────────────────────────


def _inject_vector_fallback(
    plan: ExecutionPlan,
    question: str,
    preprocessed_q: str,
) -> None:
    """Add a search_datasets step if the plan has no vector/data step."""
    if not _has_data_or_vector_step(plan):
        plan.steps.insert(
            0,
            PlanStep(
                id="step_vector_fallback",
                action="search_datasets",
                description=(
                    f"Buscar datasets relevantes por similitud semántica: {question[:100]}"
                ),
                params={"query": preprocessed_q, "limit": 5},
                depends_on=[],
            ),
        )


def _has_data_or_vector_step(plan: ExecutionPlan) -> bool:
    """Return whether the plan already contains a data-producing search step."""
    return any(
        step.action == "search_datasets" or step.action in DATA_ACTIONS for step in plan.steps
    )


def _serialize_plan(plan: ExecutionPlan) -> str:
    """Serialize a plan once for history persistence."""
    return json.dumps(
        {
            "intent": plan.intent,
            "steps": [{"action": step.action, "params": step.params} for step in plan.steps],
        },
        ensure_ascii=False,
    )


def _build_errors_block(all_warnings: list[str]) -> str:
    """Format connector warnings for the analyst prompt."""
    if not all_warnings:
        return ""
    return "\nERRORES EN LA RECOLECCIÓN:\n" + "\n".join(f"- {warning}" for warning in all_warnings)


def _today_iso_utc() -> str:
    """Return today's UTC date formatted for analyst prompts."""
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _has_result_records(results: list[Any]) -> bool:
    """Return whether any connector result contains records."""
    return any(result.records for result in results)


def _get_clarification_step(plan: ExecutionPlan) -> PlanStep | None:
    """Return the clarification step when the planner produced one."""
    if plan.intent != "clarification":
        return None
    return next((step for step in plan.steps if step.action == "clarification"), None)


def _build_analysis_prompt(
    question: str,
    plan: ExecutionPlan,
    results: list,
    memory_ctx_analyst: str,
    all_warnings: list[str],
    has_records: bool | None = None,
) -> str:
    """Build the analyst LLM prompt from data context and warnings."""
    today = _today_iso_utc()
    errors_block = _build_errors_block(all_warnings)

    if has_records is None:
        has_records = _has_result_records(results)

    no_data_fallback = not results or not has_records

    if no_data_fallback:
        caps = build_capabilities_block()
        return load_prompt(
            "analyst_no_data",
            question=question,
            today=today,
            memory_ctx_analyst=memory_ctx_analyst,
            caps=caps,
        )

    data_context = build_data_context(results)

    return (
        f'PREGUNTA DEL USUARIO: "{question}"\n'
        f"FECHA ACTUAL: {today}\n"
        f"INTENCIÓN: {plan.intent}\n\n"
        f"DATOS RECOLECTADOS:\n{data_context}\n"
        f"{errors_block}"
        f"{memory_ctx_analyst}\n\n"
        "Si hay historial de conversación, tené en cuenta lo que ya se discutió "
        "para no repetir contexto innecesariamente y para mantener coherencia "
        "con respuestas anteriores.\n\n"
        "Respondé de forma breve y conversacional. Destacá el dato más importante, "
        "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
        "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
    )


_RE_CHART_TAG = re.compile(r"<!--CHART:.*?-->", re.DOTALL)
_RE_CHART_TRUNC = re.compile(r"<!--CHART:.*", re.DOTALL)
_RE_META_TAG = re.compile(r"<!--META:.*?-->", re.DOTALL)
_RE_META_TRUNC = re.compile(r"<!--META:.*", re.DOTALL)
_RE_ANY_TAG = re.compile(r"<!--.*?-->", re.DOTALL)
_RE_ANY_TAG_TRUNC = re.compile(r"<!--.*", re.DOTALL)


def _strip_tags(text: str) -> str:
    """Strip <!--CHART:...--> tags (complete and truncated) from text."""
    text = _RE_CHART_TAG.sub("", text)
    return _RE_CHART_TRUNC.sub("", text).strip()


def _strip_meta(text: str) -> str:
    """Strip <!--META:...--> tags (complete and truncated) from text."""
    text = _RE_META_TAG.sub("", text)
    return _RE_META_TRUNC.sub("", text).strip()


def _extract_sources(results: list) -> list[dict[str, Any]]:
    """Build the sources list from data results, deduplicating by (name, url)."""
    seen: set[tuple[str, str]] = set()
    sources: list[dict[str, Any]] = []
    for r in results:
        if not r.records:
            continue
        key = (r.dataset_title, r.portal_url)
        if key in seen:
            continue
        seen.add(key)
        sources.append(
            {
                "name": r.dataset_title,
                "url": r.portal_url,
                "portal": r.portal_name,
                "accessed_at": r.metadata.get("fetched_at", ""),
            }
        )
    return sources


def _extract_documents(results: list) -> list[dict[str, Any]] | None:
    """Extract structured documents for frontend card rendering."""
    documents: list[dict[str, Any]] = []
    for r in results:
        if r.source.startswith("ddjj:"):
            for rec in r.records:
                if rec.get("nombre") and rec.get("patrimonio_cierre") is not None:
                    documents.append({**rec, "doc_type": "ddjj"})
    return documents if documents else None


@dataclass(slots=True)
class ResultPayloads:
    has_records: bool
    sources: list[dict[str, Any]]
    documents: list[dict[str, Any]] | None


def _collect_result_payloads(results: list) -> ResultPayloads:
    """Collect structured payloads from connector results in a single pass."""
    seen_sources: set[tuple[str, str]] = set()
    sources: list[dict[str, Any]] = []
    documents: list[dict[str, Any]] = []
    has_records = False

    for result in results:
        if not result.records:
            continue

        has_records = True

        source_key = (result.dataset_title, result.portal_url)
        if source_key not in seen_sources:
            seen_sources.add(source_key)
            sources.append(
                {
                    "name": result.dataset_title,
                    "url": result.portal_url,
                    "portal": result.portal_name,
                    "accessed_at": result.metadata.get("fetched_at", ""),
                }
            )

        if result.source.startswith("ddjj:"):
            for rec in result.records:
                if rec.get("nombre") and rec.get("patrimonio_cierre") is not None:
                    documents.append({**rec, "doc_type": "ddjj"})

    return ResultPayloads(
        has_records=has_records,
        sources=sources,
        documents=documents if documents else None,
    )
