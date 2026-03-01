"""Application service for the smart query pipeline.

Extracts all business logic from smart_query_router.py into a testable,
reusable service used by both POST and WebSocket endpoints.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.entities.connectors.data_result import ChartData, DataResult, ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError, EmbeddingError, LLMProviderError
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider, LLMMessage
from app.domain.ports.search.vector_search import IVectorSearch
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.adapters.connectors.memory_agent import (
    build_memory_context_prompt,
    load_memory,
    save_memory,
    update_memory,
)
from app.infrastructure.adapters.connectors.policy_agent import analyze_policy
from app.infrastructure.adapters.connectors.query_planner import (
    ANALYSIS_SYSTEM_PROMPT,
    generate_plan,
)
from app.infrastructure.adapters.connectors.series_tiempo_adapter import find_catalog_match
from app.infrastructure.adapters.search.prompt_injection_detector import is_suspicious
from app.infrastructure.adapters.search.query_preprocessor import QueryPreprocessor
from app.infrastructure.audit.audit_logger import (
    audit_cache_hit,
    audit_injection_blocked,
    audit_query,
)
from app.infrastructure.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


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


# ── Casual / meta / educational patterns ────────────────────

_GREETING_PATTERN = re.compile(
    r"^("
    r"hola|buenas|buen día|buenos días|buenas tardes|buenas noches"
    r"|hey|qué tal|que tal|qué onda|que onda|cómo estás|como estas"
    r"|cómo andás|como andas|qué hacés|que haces"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_THANKS_PATTERN = re.compile(
    r"^("
    r"gracias|muchas gracias|genial|perfecto|dale|ok|de una|buenísimo|buenisimo"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_FAREWELL_PATTERN = re.compile(
    r"^("
    r"chau|adiós|adios|hasta luego|nos vemos|hasta pronto"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_CASUAL_PATTERNS = re.compile(
    r"^("
    r"hola|buenas|buen día|buenos días|buenas tardes|buenas noches"
    r"|hey|qué tal|que tal|qué onda|que onda|cómo estás|como estas"
    r"|cómo andás|como andas|qué hacés|que haces"
    r"|gracias|muchas gracias|genial|perfecto|dale|ok|de una|buenísimo|buenisimo"
    r"|chau|adiós|adios|hasta luego|nos vemos|hasta pronto"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_META_PATTERNS = re.compile(
    r"(qué pod[eé]s hacer|qu[eé] sab[eé]s|cu[aá]les son tus funciones"
    r"|c[oó]mo funcion[aá]s|para qu[eé] serv[ií]s|ayuda"
    r"|qu[eé] sos|qui[eé]n sos|qu[eé] es openarg)",
    re.IGNORECASE,
)

_CASUAL_RESPONSES: dict[str, list[str]] = {
    "greeting": [
        "¡Hola! ¿Qué querés saber sobre datos abiertos de Argentina?",
        "¡Buenas! Estoy listo para ayudarte con datos públicos argentinos.",
        "¡Hola! Preguntame sobre economía, presupuesto, educación o cualquier dato público.",
    ],
    "thanks": [
        "¡De nada! Si tenés más consultas sobre datos públicos, acá estoy.",
        "¡Con gusto! ¿Necesitás analizar algo más?",
        "¡No hay de qué! Preguntame lo que necesites.",
    ],
    "farewell": [
        "¡Hasta luego! Volvé cuando necesites datos.",
        "¡Chau! Que andes bien.",
        "¡Nos vemos! Estoy acá para cuando necesites.",
    ],
    "generic": [
        "¡Hola! ¿Qué querés saber sobre datos abiertos de Argentina?",
        "¿En qué puedo ayudarte hoy?",
    ],
}

_META_RESPONSE = (
    "Soy **OpenArg**, un asistente de inteligencia artificial especializado "
    "en datos abiertos de Argentina.\n\n"
    "Puedo ayudarte con:\n"
    "- **Series de tiempo** — inflación, tipo de cambio, PBI, reservas del BCRA\n"
    "- **Economía** — dólar, riesgo país, cotizaciones\n"
    "- **Datos gubernamentales** — datasets de datos.gob.ar y portales provinciales\n"
    "- **Declaraciones juradas** — patrimonio de diputados nacionales\n"
    "- **Sesiones legislativas** — transcripciones del Congreso\n"
    "- **Georeferenciación** — normalización de direcciones y localidades\n\n"
    "Probá preguntándome algo como: *¿Cómo viene la inflación en los últimos meses?*"
)

_EDUCATIONAL_PATTERNS: dict[re.Pattern[str], str] = {
    re.compile(r"qu[eé]\s+(es|son)\s+(los\s+)?datos?\s+abiertos", re.IGNORECASE): (
        "Los **datos abiertos** son datos que cualquier persona puede acceder, usar y compartir libremente. "
        "En Argentina, el portal datos.gob.ar publica miles de datasets de gobierno nacional, provincias y municipios.\n\n"
        "Probá preguntarme: *¿Qué datasets hay sobre educación?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?pbi", re.IGNORECASE): (
        "El **PBI (Producto Bruto Interno)** es el valor total de todos los bienes y servicios producidos "
        "en un país durante un período determinado. Es el principal indicador de la actividad económica.\n\n"
        "Probá preguntarme: *¿Cómo evolucionó el PBI en los últimos años?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(la\s+)?inflaci[oó]n", re.IGNORECASE): (
        "La **inflación** es el aumento generalizado y sostenido de los precios de bienes y servicios "
        "en una economía. En Argentina, el INDEC la mide mensualmente a través del IPC.\n\n"
        "Probá preguntarme: *¿Cuál fue la inflación del último mes?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?riesgo\s+pa[ií]s", re.IGNORECASE): (
        "El **riesgo país** mide la diferencia de tasa de interés que pagan los bonos de un país "
        "respecto de los bonos del Tesoro de EE.UU. Un valor alto indica mayor percepción de riesgo.\n\n"
        "Probá preguntarme: *¿Cuánto está el riesgo país hoy?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?ddjj", re.IGNORECASE): (
        "Las **DDJJ (Declaraciones Juradas)** son documentos donde los funcionarios públicos declaran "
        "su patrimonio. En OpenArg tenemos 195 declaraciones juradas de diputados nacionales.\n\n"
        "Probá preguntarme: *¿Quién es el diputado con mayor patrimonio?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?tipo\s+de\s+cambio", re.IGNORECASE): (
        "El **tipo de cambio** es el precio de una moneda en términos de otra. En Argentina se habla "
        "del dólar oficial, blue, MEP, CCL, entre otros.\n\n"
        "Probá preguntarme: *¿A cuánto está el dólar hoy?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?reservas\s+(del\s+)?bcra", re.IGNORECASE): (
        "Las **reservas del BCRA** son los activos en moneda extranjera que posee el Banco Central "
        "de la República Argentina. Se usan para respaldar la moneda y las obligaciones de deuda.\n\n"
        "Probá preguntarme: *¿Cómo vienen las reservas del BCRA?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?presupuesto\s+(nacional|p[uú]blico)", re.IGNORECASE): (
        "El **presupuesto nacional** es la ley que estima los ingresos y autoriza los gastos del Estado "
        "para cada año fiscal. Define las prioridades de gasto público.\n\n"
        "Probá preguntarme: *¿Cuánto se destina a educación en el presupuesto?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?sesiones\s+(del\s+)?congreso", re.IGNORECASE): (
        "Las **sesiones del Congreso** son las reuniones de la Cámara de Diputados y el Senado "
        "donde se debaten y votan proyectos de ley. Tenemos transcripciones de sesiones disponibles.\n\n"
        "Probá preguntarme: *¿Qué se debatió sobre educación en el Congreso?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?[ií]ndice\s+de\s+pobreza", re.IGNORECASE): (
        "El **índice de pobreza** mide el porcentaje de personas u hogares cuyos ingresos no alcanzan "
        "para cubrir una canasta básica. El INDEC lo publica semestralmente en Argentina.\n\n"
        "Probá preguntarme: *¿Cuál es el porcentaje de pobreza actual?*"
    ),
}


# ── Service ─────────────────────────────────────────────────


class SmartQueryService:
    """Orchestrates the full smart-query pipeline."""

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
        self._metrics = MetricsCollector()

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

        # 0. Classify — casual/meta/educational get instant responses
        casual = self._get_casual_response(question)
        if casual:
            return SmartQueryResult(answer=casual, sources=[], casual=True)

        meta = self._get_meta_response(question)
        if meta:
            return SmartQueryResult(answer=meta, sources=[])

        # 0b. Prompt injection check
        suspicious, score = is_suspicious(question)
        if suspicious:
            audit_injection_blocked(user=user_id, question=question, score=score)
            return SmartQueryResult(
                answer="",
                sources=[],
                intent="injection_blocked",
                duration_ms=int((time.monotonic() - start_time) * 1000),
            )

        # 0c. Educational
        edu = self._get_educational_response(question)
        if edu:
            return SmartQueryResult(answer=edu, sources=[], educational=True)

        # 1. Cache lookup
        cached = await self._check_cache(question, user_id)
        if cached:
            return cached

        # 2. Memory
        session_id = conversation_id or ""
        memory = await load_memory(self._cache, session_id)

        # 2b. Query preprocessing
        preprocessor = QueryPreprocessor(self._llm)
        enhanced_query = await preprocessor.reformulate(question)

        # 3. Plan
        plan = await generate_plan(
            self._llm, enhanced_query,
            memory_context=build_memory_context_prompt(memory),
        )
        logger.info(
            "Plan for '%s': intent=%s, steps=%d",
            question[:60], plan.intent, len(plan.steps),
        )

        # 4. Execute connectors
        results = await self._execute_steps(plan)

        # 5. Hybrid search complement
        results = await self._enrich_with_hybrid_search(results, enhanced_query, plan.intent)

        # 6. LLM analysis
        data_context = self._build_data_context(results)
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        memory_ctx = build_memory_context_prompt(memory)

        analysis_prompt = (
            f'PREGUNTA DEL USUARIO: "{question}"\n'
            f"FECHA ACTUAL: {today}\n"
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS:\n{data_context}\n"
            f"{memory_ctx}\n"
            "Respondé de forma breve y conversacional. Destacá el dato más importante, "
            "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
            "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
        )

        response = await self._llm.chat(
            messages=[
                LLMMessage(role="system", content=ANALYSIS_SYSTEM_PROMPT),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.7,
            max_tokens=8192,
        )

        # 7. Charts
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(response.content)
        charts = det_charts if det_charts else llm_charts

        clean_answer = re.sub(r"<!--CHART:.*?-->", "", response.content, flags=re.DOTALL).strip()

        # 7b. Policy analysis (optional)
        if policy_mode:
            try:
                policy_text = await analyze_policy(
                    self._llm, plan, results, clean_answer, memory_ctx,
                )
                clean_answer += "\n\n---\n\n" + policy_text
            except Exception:
                logger.warning("Policy agent failed", exc_info=True)

        sources = [
            {
                "name": r.dataset_title,
                "url": r.portal_url,
                "portal": r.portal_name,
                "accessed_at": r.metadata.get("fetched_at", ""),
            }
            for r in results
            if r.records
        ]

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
        )

        # 8. Audit
        audit_query(user=user_id, question=question, intent=plan.intent, duration_ms=duration_ms)

        # 9. Memory update (non-blocking)
        try:
            updated_memory = await update_memory(self._llm, memory, plan, results, clean_answer)
            await save_memory(self._cache, session_id, updated_memory)
        except Exception:
            logger.debug("Memory update failed", exc_info=True)

        # 10. Save conversation history
        if session:
            await self._save_history(session, question, user_id, clean_answer, sources, tokens_used, duration_ms)

        # 11. Cache write
        result_dict = {
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
            "tokens_used": tokens_used,
        }
        await self._write_cache(question, result_dict)

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

        # Injection check
        suspicious, score = is_suspicious(question)
        if suspicious:
            audit_injection_blocked(user=user_id, question=question, score=score)
            yield {
                "type": "error",
                "message": "Potential prompt injection detected",
                "code": "SEC_001",
            }
            return

        # Casual
        casual = self._get_casual_response(question)
        if casual:
            yield {"type": "chunk", "content": casual}
            yield {"type": "complete", "answer": casual, "sources": [], "chart_data": None, "casual": True}
            return

        # Meta
        meta = self._get_meta_response(question)
        if meta:
            yield {"type": "chunk", "content": meta}
            yield {"type": "complete", "answer": meta, "sources": [], "chart_data": None}
            return

        # Educational
        edu = self._get_educational_response(question)
        if edu:
            yield {"type": "chunk", "content": edu}
            yield {"type": "complete", "answer": edu, "sources": [], "chart_data": None}
            return

        # Cache check
        session_id = conversation_id or ""
        cached_result = await self._get_cached_dict(question)
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

        # Planning
        yield {"type": "status", "step": "planning"}

        preprocessor = QueryPreprocessor(self._llm)
        enhanced_query = await preprocessor.reformulate(question)
        memory = await load_memory(self._cache, session_id)
        plan = await generate_plan(
            self._llm, enhanced_query,
            memory_context=build_memory_context_prompt(memory),
        )

        yield {
            "type": "status",
            "step": "planned",
            "intent": plan.intent,
            "steps_count": len(plan.steps),
        }

        # Searching
        yield {"type": "status", "step": "searching"}
        results = await self._execute_steps_streaming(plan)

        # Hybrid search
        results = await self._enrich_with_hybrid_search(results, enhanced_query, plan.intent)

        # Generating
        yield {"type": "status", "step": "generating"}

        data_context = self._build_data_context(results)
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        memory_ctx = build_memory_context_prompt(memory)

        analysis_prompt = (
            f'PREGUNTA DEL USUARIO: "{question}"\n'
            f"FECHA ACTUAL: {today}\n"
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS:\n{data_context}\n"
            f"{memory_ctx}\n"
            "Respondé de forma breve y conversacional. Destacá el dato más importante, "
            "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
            "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
        )

        full_text = ""
        async for chunk in self._llm.chat_stream(
            messages=[
                LLMMessage(role="system", content=ANALYSIS_SYSTEM_PROMPT),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.7,
            max_tokens=8192,
        ):
            full_text += chunk
            yield {"type": "chunk", "content": chunk}

        # Complete
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts
        clean_answer = re.sub(r"<!--CHART:.*?-->", "", full_text, flags=re.DOTALL).strip()

        # Policy analysis (optional, streamed)
        if policy_mode:
            try:
                yield {"type": "status", "step": "policy_analysis"}
                policy_text = await analyze_policy(
                    self._llm, plan, results, clean_answer, memory_ctx,
                )
                separator = "\n\n---\n\n"
                yield {"type": "chunk", "content": separator + policy_text}
                clean_answer += separator + policy_text
            except Exception:
                logger.warning("Policy agent failed in streaming", exc_info=True)

        sources = [
            {
                "name": r.dataset_title,
                "url": r.portal_url,
                "portal": r.portal_name,
                "accessed_at": r.metadata.get("fetched_at", ""),
            }
            for r in results
            if r.records
        ]

        yield {
            "type": "complete",
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
        }

    # ── Classification helpers ──────────────────────────────

    @staticmethod
    def _get_casual_response(question: str) -> str | None:
        t = question.strip()
        if not _CASUAL_PATTERNS.match(t):
            return None
        if _GREETING_PATTERN.match(t):
            subtype = "greeting"
        elif _THANKS_PATTERN.match(t):
            subtype = "thanks"
        elif _FAREWELL_PATTERN.match(t):
            subtype = "farewell"
        else:
            subtype = "generic"
        responses = _CASUAL_RESPONSES.get(subtype, _CASUAL_RESPONSES["generic"])
        return random.choice(responses)  # noqa: S311

    @staticmethod
    def _get_meta_response(question: str) -> str | None:
        if _META_PATTERNS.search(question.strip()):
            return _META_RESPONSE
        return None

    @staticmethod
    def _get_educational_response(text: str) -> str | None:
        for pattern, response in _EDUCATIONAL_PATTERNS.items():
            if pattern.search(text.strip()):
                return response
        return None

    # ── Cache helpers ───────────────────────────────────────

    @staticmethod
    def _cache_key(question: str) -> str:
        normalized = question.strip().lower()
        h = hashlib.sha256(normalized.encode()).hexdigest()[:16]
        return f"openarg:smart:{h}"

    async def _check_cache(self, question: str, user_id: str) -> SmartQueryResult | None:
        # Semantic cache
        try:
            sem_cached = await self._semantic_cache.get(question)
            if sem_cached:
                self._metrics.record_cache_hit()
                audit_cache_hit(user=user_id, question=question)
                return SmartQueryResult(
                    answer=sem_cached.get("answer", ""),
                    sources=sem_cached.get("sources", []),
                    chart_data=sem_cached.get("chart_data"),
                    tokens_used=sem_cached.get("tokens_used", 0),
                    cached=True,
                )
        except Exception:
            logger.debug("Semantic cache read failed")

        # Redis cache
        try:
            cache_key = self._cache_key(question)
            cached = await self._cache.get(cache_key)
            if cached and isinstance(cached, dict):
                self._metrics.record_cache_hit()
                audit_cache_hit(user=user_id, question=question)
                return SmartQueryResult(
                    answer=cached.get("answer", ""),
                    sources=cached.get("sources", []),
                    chart_data=cached.get("chart_data"),
                    tokens_used=cached.get("tokens_used", 0),
                    cached=True,
                )
            self._metrics.record_cache_miss()
        except Exception:
            logger.debug("Cache read failed, proceeding without cache")

        return None

    async def _get_cached_dict(self, question: str) -> dict[str, Any] | None:
        """Return raw cached dict for streaming endpoint."""
        try:
            sem = await self._semantic_cache.get(question)
            if sem and isinstance(sem, dict):
                return sem
        except Exception:
            logger.debug("WS semantic cache read failed")
        try:
            cached = await self._cache.get(self._cache_key(question))
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            logger.debug("WS Redis cache read failed")
        return None

    async def _write_cache(self, question: str, result: dict[str, Any]) -> None:
        try:
            await self._cache.set(self._cache_key(question), result, ttl_seconds=1800)
        except Exception:
            logger.warning("Failed to cache smart query result", exc_info=True)
        try:
            q_embedding = await self._embedding.embed(question)
            await self._semantic_cache.set(question, q_embedding, result)
        except Exception:
            logger.debug("Semantic cache write failed", exc_info=True)

    # ── Step executors ──────────────────────────────────────

    async def _execute_series_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        series_ids = params.get("seriesIds", [])
        collapse = params.get("collapse")
        representation = params.get("representation")

        if not series_ids:
            query = params.get("query", step.description)
            match = find_catalog_match(query)
            if match:
                series_ids = match["ids"]
                if not collapse and "default_collapse" in match:
                    collapse = match["default_collapse"]
                if not representation and "default_representation" in match:
                    representation = match["default_representation"]

        if not series_ids:
            search_results = await self._series.search(params.get("query", step.description))
            if search_results:
                series_ids = [r["id"] for r in search_results[:3]]

        if not series_ids:
            return []

        start_date = params.get("startDate")
        end_date = params.get("endDate")
        if not end_date:
            end_date = datetime.now(UTC).strftime("%Y-%m-%d")

        result = await self._series.fetch(
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
            collapse=collapse,
            representation=representation,
        )
        return [result] if result else []

    async def _execute_argentina_datos_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        data_type = params.get("type", "dolar")

        if data_type == "dolar":
            result = await self._arg_datos.fetch_dolar(casa=params.get("casa"))
            return [result] if result else []
        elif data_type == "riesgo_pais":
            result = await self._arg_datos.fetch_riesgo_pais(ultimo=params.get("ultimo", False))
            return [result] if result else []
        elif data_type == "inflacion":
            result = await self._arg_datos.fetch_inflacion()
            return [result] if result else []
        return []

    async def _execute_georef_step(self, step: PlanStep) -> list[DataResult]:
        query = step.params.get("query", step.description)
        result = await self._georef.normalize_location(query)
        return [result] if result else []

    def _execute_ddjj_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        action = params.get("action", "search")

        if action == "ranking":
            result = self._ddjj.ranking(
                sort_by=params.get("sortBy", "patrimonio"),
                top=params.get("top", 10),
                order=params.get("order", "desc"),
            )
        elif action == "stats":
            result = self._ddjj.stats()
        elif action == "detail" or params.get("nombre"):
            result = self._ddjj.get_by_name(params.get("nombre", ""))
        else:
            result = self._ddjj.search(params.get("query", params.get("nombre", "")))

        return [result] if result.records else []

    async def _execute_ckan_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        query = params.get("query", step.description)
        portal_id = params.get("portalId")
        rows = params.get("rows", 10)

        resource_id = params.get("resourceId") or params.get("resource_id")
        if resource_id and portal_id:
            q = params.get("q")
            records = await self._ckan.query_datastore(portal_id, resource_id, q=q)
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

        return await self._ckan.search_datasets(query, portal_id=portal_id, rows=rows)

    async def _execute_sesiones_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        result = await self._sesiones.search(
            query=params.get("query", step.description),
            periodo=params.get("periodo"),
            orador=params.get("orador"),
            limit=params.get("limit", 15),
        )
        return [result] if result else []

    async def _execute_steps(self, plan: ExecutionPlan) -> list[DataResult]:
        """Execute plan steps, catching ConnectorError per step."""
        results: list[DataResult] = []
        for step in plan.steps[:5]:
            step_start = time.monotonic()
            connector_name = step.action
            try:
                data = await self._dispatch_step(step)
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms)
                results.extend(data)
            except ConnectorError:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
                logger.warning("Step %s failed (ConnectorError)", step.id, exc_info=True)
            except Exception:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
                logger.warning("Step %s failed", step.id, exc_info=True)
        return results

    async def _execute_steps_streaming(self, plan: ExecutionPlan) -> list[DataResult]:
        """Same as _execute_steps but used by streaming endpoint."""
        return await self._execute_steps(plan)

    async def _dispatch_step(self, step: PlanStep) -> list[DataResult]:
        if step.action == "query_series":
            return await self._execute_series_step(step)
        elif step.action == "query_argentina_datos":
            return await self._execute_argentina_datos_step(step)
        elif step.action == "query_georef":
            return await self._execute_georef_step(step)
        elif step.action == "query_ddjj":
            return self._execute_ddjj_step(step)
        elif step.action == "search_ckan":
            return await self._execute_ckan_step(step)
        elif step.action == "query_sesiones":
            return await self._execute_sesiones_step(step)
        elif step.action in ("analyze", "compare", "synthesize"):
            return []
        else:
            logger.info("Unknown step action '%s', skipping", step.action)
            return []

    # ── Hybrid search enrichment ────────────────────────────

    async def _enrich_with_hybrid_search(
        self, results: list[DataResult], enhanced_query: str, intent: str,
    ) -> list[DataResult]:
        if results and intent != "busqueda_general":
            return results
        try:
            query_embedding = await self._embedding.embed(enhanced_query)
            vector_results = await self._vector_search.search_datasets_hybrid(
                query_embedding, query_text=enhanced_query, limit=5,
            )
            for vr in vector_results:
                if vr.score < 0.005:
                    continue
                results.append(
                    DataResult(
                        source=f"pgvector:{vr.portal}",
                        portal_name=vr.portal,
                        portal_url="",
                        dataset_title=vr.title,
                        format="json",
                        records=[],
                        metadata={
                            "total_records": 0,
                            "description": vr.description,
                            "columns": vr.columns,
                            "score": round(vr.score, 3),
                        },
                    )
                )
        except Exception:
            logger.warning("Hybrid search enrichment failed", exc_info=True)
        return results

    # ── Data context builder ────────────────────────────────

    @staticmethod
    def _build_data_context(results: list[DataResult]) -> str:
        if not results:
            return (
                "No se obtuvieron resultados directos en esta búsqueda. Sin embargo, TENÉS acceso en tiempo real a estos portales de datos abiertos:\n"
                "- **Portal Nacional** (datos.gob.ar): 1200+ datasets de economía, salud, educación, transporte, energía, gobierno\n"
                "- **CABA** (data.buenosaires.gob.ar): movilidad, presupuesto, educación\n"
                "- **Buenos Aires Provincia** (catalogo.datos.gba.gob.ar): salud, género, estadísticas\n"
                "- **Córdoba, Santa Fe, Mendoza, Entre Ríos, Neuquén** y más\n"
                "- **Cámara de Diputados** (datos.hcdn.gob.ar): legisladores, proyectos, leyes\n"
                "- **Series de Tiempo**: inflación, tipo de cambio, PBI, presupuesto\n"
                "- **DDJJ**: 195 declaraciones juradas patrimoniales de diputados\n\n"
                "INSTRUCCIÓN: NO digas que 'no pudiste acceder' o 'no tenés datos'. En cambio, explicale al usuario qué fuentes están disponibles y sugerí búsquedas concretas. Ofrecé 3-4 opciones temáticas."
            )

        parts = []
        for i, result in enumerate(results):
            valid_records = [r for r in result.records if isinstance(r, dict)]
            if not valid_records and result.records:
                continue

            is_metadata_only = (
                valid_records
                and valid_records[0].get("_type") == "resource_metadata"
            )

            if is_metadata_only:
                preview = valid_records[:20]
                records_text = json.dumps(preview, ensure_ascii=False, indent=2)
                part = (
                    f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                    f"Fuente: {result.portal_name} ({result.source})\n"
                    f"URL: {result.portal_url}\n"
                    "NOTA: Este dataset no tiene Datastore habilitado. Solo metadatos de los recursos.\n"
                )
                if result.metadata.get("description"):
                    part += f"Descripción: {result.metadata['description']}\n"
                part += f"Recursos disponibles:\n{records_text}\n\nExplicale al usuario qué datos contiene y proporcioná el link."
            else:
                columns = list(valid_records[0].keys()) if valid_records else []
                total_rows = len(valid_records)

                if total_rows > 50:
                    records_to_send = valid_records[:25] + valid_records[-25:]
                    truncation_note = f", primeros 25 + últimos 25 de {total_rows} totales"
                else:
                    records_to_send = valid_records
                    truncation_note = ""

                records_text = json.dumps(records_to_send, ensure_ascii=False, indent=2)

                part = (
                    f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                    f"Fuente: {result.portal_name} ({result.source})\n"
                    f"URL: {result.portal_url}\n"
                    f"Formato: {result.format}\n"
                    f"Total de registros: {result.metadata.get('total_records', total_rows)}\n"
                    f"Columnas: {', '.join(columns)}\n"
                )
                if result.metadata.get("description"):
                    part += f"Descripción: {result.metadata['description']}\n"
                part += (
                    f"Datos ({len(records_to_send)} registros{truncation_note}):\n"
                    f"{records_text}\n\n"
                    "IMPORTANTE: Si hay una columna temporal (año, fecha, mes), generá un gráfico de línea temporal con <!--CHART:{}-->."
                )

            parts.append(part)

        return "\n\n".join(parts)

    # ── Chart builders ──────────────────────────────────────

    @staticmethod
    def _build_deterministic_charts(results: list[DataResult]) -> list[dict[str, Any]]:
        charts: list[dict[str, Any]] = []
        for result in results:
            if not result.records or len(result.records) < 2:
                continue
            first = result.records[0]
            if not isinstance(first, dict):
                continue
            if first.get("_type") == "resource_metadata":
                continue

            keys = list(first.keys())
            time_key = None
            for k in keys:
                kl = k.lower()
                if k == "fecha" or "date" in kl or kl in ("año", "year", "mes"):
                    time_key = k
                    break

            if not time_key:
                continue

            numeric_keys = [
                k
                for k in keys
                if k != time_key and not k.startswith("_") and isinstance(first.get(k), (int, float))
            ]
            if not numeric_keys:
                continue

            clean = [
                row for row in result.records
                if any(row.get(k) is not None for k in numeric_keys)
            ]
            if len(clean) < 2:
                continue

            chart_type = "line_chart" if result.format == "time_series" or time_key == "fecha" else "bar_chart"
            title = result.dataset_title
            units = result.metadata.get("units")
            if units:
                title += f" ({units})"

            charts.append({
                "type": chart_type,
                "title": title,
                "data": [
                    {time_key: row[time_key], **{k: row.get(k) for k in numeric_keys}}
                    for row in clean
                ],
                "xKey": time_key,
                "yKeys": numeric_keys,
            })
        return charts

    @staticmethod
    def _extract_llm_charts(text: str) -> list[dict[str, Any]]:
        charts: list[dict[str, Any]] = []
        for match in re.finditer(r"<!--CHART:(.*?)-->", text, re.DOTALL):
            try:
                chart = json.loads(match.group(1))
                if chart.get("type") and chart.get("data") and chart.get("xKey") and chart.get("yKeys"):
                    charts.append(chart)
            except (json.JSONDecodeError, KeyError):
                pass
        return charts

    # ── History persistence ─────────────────────────────────

    @staticmethod
    async def _save_history(
        session: AsyncSession,
        question: str,
        user_id: str,
        answer: str,
        sources: list[dict[str, Any]],
        tokens_used: int,
        duration_ms: int,
    ) -> None:
        try:
            query_id = str(uuid4())
            await session.execute(
                text("""
                    INSERT INTO user_queries (id, question, user_id, status, analysis_result, sources_json, tokens_used, duration_ms)
                    VALUES (CAST(:id AS uuid), :question, :user_id, 'completed', :result, :sources, :tokens, :duration_ms)
                """),
                {
                    "id": query_id,
                    "question": question,
                    "user_id": user_id,
                    "result": answer,
                    "sources": json.dumps(sources, ensure_ascii=False),
                    "tokens": tokens_used,
                    "duration_ms": duration_ms,
                },
            )
            await session.commit()
        except Exception:
            logger.warning("Failed to save conversation history", exc_info=True)
            try:
                await session.rollback()
            except Exception:
                pass
