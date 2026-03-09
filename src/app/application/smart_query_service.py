"""Application service for the smart query pipeline.

Simplified pipeline: planner (1 LLM) → dispatch steps (0 LLM) → analysis (1 LLM) + optional policy (1 LLM).
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import text

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider, LLMMessage
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache, ttl_for_intent
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
from app.prompts import load_prompt
from app.infrastructure.adapters.connectors.series_tiempo_adapter import find_catalog_match
from app.infrastructure.adapters.search.prompt_injection_detector import is_suspicious
from app.infrastructure.audit.audit_logger import (
    audit_cache_hit,
    audit_injection_blocked,
    audit_query,
)
from app.infrastructure.celery.tasks.indec_tasks import INDEC_DATASETS, _download_and_parse
from app.infrastructure.monitoring.metrics import MetricsCollector

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.domain.ports.cache.cache_port import ICacheService
    from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
    from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
    from app.domain.ports.connectors.georef import IGeorefConnector
    from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
    from app.domain.ports.connectors.sesiones import ISesionesConnector
    from app.domain.ports.connectors.staff import IStaffConnector
    from app.domain.ports.chat.chat_repository import IChatRepository
    from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
    from app.domain.ports.search.vector_search import IVectorSearch
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter

logger = logging.getLogger(__name__)


def _build_capabilities_block() -> str:
    """Build a concise list of system capabilities from the taxonomy."""
    try:
        from app.infrastructure.adapters.connectors.dataset_index import TAXONOMY

        lines = ["CAPACIDADES DEL SISTEMA — OpenArg tiene datos sobre:"]
        for cat in TAXONOMY.values():
            children = cat.get("children", {})
            child_labels = [c["label"] for c in children.values()]
            lines.append(f"• {cat['label']}: {', '.join(child_labels)}")
        lines.append("• Georeferenciación: provincias, departamentos, municipios y localidades")
        return "\n".join(lines)
    except Exception:
        logger.debug("Failed to build capabilities from taxonomy", exc_info=True)
        return (
            "CAPACIDADES DEL SISTEMA — OpenArg tiene datos sobre:\n"
            "• Economía (inflación, dólar, empleo, actividad económica)\n"
            "• Gobierno (presupuesto, autoridades, gobernadores, DDJJ)\n"
            "• Congreso (legisladores, sesiones, personal, comisiones)\n"
            "• Datos sociales (educación, salud, seguridad, género)\n"
            "• Infraestructura (transporte, energía, telecomunicaciones)\n"
            "• Georeferenciación (provincias, municipios, localidades)"
        )


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

_DATA_ACTIONS = frozenset((
    "query_sandbox", "query_series", "query_argentina_datos", "query_bcra",
    "query_ddjj", "query_sesiones", "query_staff", "search_ckan", "query_georef",
))

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
    re.compile(
        r"qu[eé]\s+(es|son)\s+(los\s+)?datos?\s+abiertos", re.IGNORECASE,
    ): (
        "Los **datos abiertos** son datos que cualquier persona puede "
        "acceder, usar y compartir libremente. En Argentina, el portal "
        "datos.gob.ar publica miles de datasets de gobierno nacional, "
        "provincias y municipios.\n\n"
        "Probá preguntarme: *¿Qué datasets hay sobre educación?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?pbi", re.IGNORECASE,
    ): (
        "El **PBI (Producto Bruto Interno)** es el valor total de todos "
        "los bienes y servicios producidos en un país durante un período "
        "determinado. Es el principal indicador de la actividad "
        "económica.\n\n"
        "Probá preguntarme: *¿Cómo evolucionó el PBI en los últimos "
        "años?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(la\s+)?inflaci[oó]n", re.IGNORECASE,
    ): (
        "La **inflación** es el aumento generalizado y sostenido de "
        "los precios de bienes y servicios en una economía. En "
        "Argentina, el INDEC la mide mensualmente a través del IPC.\n\n"
        "Probá preguntarme: *¿Cuál fue la inflación del último mes?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?riesgo\s+pa[ií]s",
        re.IGNORECASE,
    ): (
        "El **riesgo país** mide la diferencia de tasa de interés que "
        "pagan los bonos de un país respecto de los bonos del Tesoro "
        "de EE.UU. Un valor alto indica mayor percepción de riesgo.\n\n"
        "Probá preguntarme: *¿Cuánto está el riesgo país hoy?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|son)\s+(las\s+)?ddjj", re.IGNORECASE,
    ): (
        "Las **DDJJ (Declaraciones Juradas)** son documentos donde "
        "los funcionarios públicos declaran su patrimonio. En OpenArg "
        "tenemos 195 declaraciones juradas de diputados nacionales.\n\n"
        "Probá preguntarme: *¿Quién es el diputado con mayor "
        "patrimonio?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?tipo\s+de\s+cambio",
        re.IGNORECASE,
    ): (
        "El **tipo de cambio** es el precio de una moneda en términos "
        "de otra. En Argentina se habla del dólar oficial, blue, MEP, "
        "CCL, entre otros.\n\n"
        "Probá preguntarme: *¿A cuánto está el dólar hoy?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|son)\s+(las\s+)?reservas\s+(del\s+)?bcra",
        re.IGNORECASE,
    ): (
        "Las **reservas del BCRA** son los activos en moneda "
        "extranjera que posee el Banco Central de la República "
        "Argentina. Se usan para respaldar la moneda y las "
        "obligaciones de deuda.\n\n"
        "Probá preguntarme: *¿Cómo vienen las reservas del BCRA?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?presupuesto"
        r"\s+(nacional|p[uú]blico)",
        re.IGNORECASE,
    ): (
        "El **presupuesto nacional** es la ley que estima los "
        "ingresos y autoriza los gastos del Estado para cada año "
        "fiscal. Define las prioridades de gasto público.\n\n"
        "Probá preguntarme: *¿Cuánto se destina a educación en el "
        "presupuesto?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|son)\s+(las\s+)?sesiones\s+(del\s+)?congreso",
        re.IGNORECASE,
    ): (
        "Las **sesiones del Congreso** son las reuniones de la "
        "Cámara de Diputados y el Senado donde se debaten y votan "
        "proyectos de ley. Tenemos transcripciones de sesiones "
        "disponibles.\n\n"
        "Probá preguntarme: *¿Qué se debatió sobre educación en el "
        "Congreso?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?[ií]ndice\s+de\s+pobreza",
        re.IGNORECASE,
    ): (
        "El **índice de pobreza** mide el porcentaje de personas u "
        "hogares cuyos ingresos no alcanzan para cubrir una canasta "
        "básica. El INDEC lo publica semestralmente en Argentina.\n\n"
        "Probá preguntarme: *¿Cuál es el porcentaje de pobreza "
        "actual?*"
    ),
}

# ── INDEC fallback detection pattern ─────────────────────────
_INDEC_PATTERN = re.compile(
    r"(indec|ipc|inflaci[oó]n|emae|actividad\s+econ[oó]mica"
    r"|pib|producto\s+bruto|comercio\s+exterior|exportaci[oó]n|importaci[oó]n|balanza\s+comercial"
    r"|empleo|desempleo|eph|mercado\s+laboral|trabajo"
    r"|canasta\s+b[aá]sica|cbt|cba"
    r"|salario|sueldo"
    r"|pobreza|indigencia"
    r"|construcci[oó]n|isac|industria|ipi|producci[oó]n\s+industrial"
    r"|supermercado"
    r"|turismo"
    r"|distribuci[oó]n\s+del\s+ingreso|gini|decil"
    r"|balance\s+de\s+pagos|cuenta\s+corriente)",
    re.IGNORECASE,
)


# ── Service ─────────────────────────────────────────────────


class SmartQueryService:
    """Orchestrates the full smart-query pipeline: plan → dispatch → analyze."""

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

    async def _load_chat_history(self, conversation_id: str) -> str:
        """Load recent messages from the DB to build conversation context.

        Only called when there is a conversation_id and a chat_repo.
        Returns a formatted string or empty if no history.
        """
        if not conversation_id or not self._chat_repo:
            return ""
        try:
            from uuid import UUID
            messages = await self._chat_repo.get_messages(UUID(conversation_id), limit=7)
            if len(messages) <= 1:
                return ""
            # Skip the last message (it's the current question the frontend just saved)
            recent = messages[:-1][-6:]
            if not recent:
                return ""
            parts = ["\nHISTORIAL DE CONVERSACIÓN:"]
            for m in recent:
                label = "Usuario" if m.role == "user" else "Asistente"
                content = m.content[:300].replace("\n", " ")
                parts.append(f"  - {label}: {content}")
            parts.append(
                "INSTRUCCIÓN: Usá este historial solo si el usuario hace referencia "
                "a algo previo. Priorizá siempre la pregunta actual.\n"
            )
            return "\n".join(parts)
        except Exception:
            logger.debug("Failed to load chat history for %s", conversation_id, exc_info=True)
            return ""

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

        # 0. Casual/meta/educational — instant responses (0 LLM calls)
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
                answer=(
                    "No pude procesar esa consulta. "
                    "Probá reformulándola con una pregunta sobre datos públicos argentinos, "
                    "por ejemplo: *¿Cuál fue la inflación del último mes?*"
                ),
                sources=[],
                intent="injection_blocked",
                duration_ms=int((time.monotonic() - start_time) * 1000),
            )

        # 0c. Educational
        edu = self._get_educational_response(question)
        if edu:
            return SmartQueryResult(answer=edu, sources=[], educational=True)

        # 1. Cache lookup (skip in policy mode — always fetch fresh data)
        if not policy_mode:
            cached = await self._check_cache(question, user_id)
            if cached:
                return cached

        # 2. Memory (Redis summaries + DB chat history)
        session_id = conversation_id or ""
        memory = await load_memory(self._cache, session_id)
        memory_ctx = build_memory_context_prompt(memory)
        memory_ctx_analyst = build_memory_context_prompt(memory, for_analyst=True)
        chat_history = await self._load_chat_history(conversation_id)
        # Planner gets chat history (real messages) for follow-up understanding
        planner_ctx = chat_history or memory_ctx

        # 3. Preprocess query (expand synonyms, acronyms, normalize)
        _q = expand_acronyms(question)
        _q, _ = normalize_temporal(_q)
        _q = normalize_provinces(_q)
        preprocessed_q = expand_synonyms(_q)

        # 4. Plan (1 LLM call)
        plan = await generate_plan(self._llm, preprocessed_q, memory_context=planner_ctx)

        # Inject search_datasets fallback if plan has no vector search step
        # and no high-confidence data-fetching step
        _has_vector_step = any(s.action == "search_datasets" for s in plan.steps)
        _has_data_step = any(s.action in _DATA_ACTIONS for s in plan.steps)
        if not _has_vector_step and not _has_data_step:
            plan.steps.insert(0, PlanStep(
                id="step_vector_fallback",
                action="search_datasets",
                description=f"Buscar datasets relevantes por similitud semántica: {question[:100]}",
                params={"query": preprocessed_q, "limit": 5},
                depends_on=[],
            ))

        logger.info(
            "Plan for '%s': intent=%s, steps=%d",
            question[:60], plan.intent, len(plan.steps),
        )

        # 4. Dispatch steps (0 LLM calls — just connector calls)
        all_warnings: list[str] = []
        results, step_warnings = await self._execute_steps(plan, nl_query=question)
        all_warnings.extend(step_warnings)

        # 5. LLM analysis (1 LLM call)
        data_context = self._build_data_context(results)
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        errors_block = ""
        if all_warnings:
            errors_block = (
                "\nERRORES EN LA RECOLECCIÓN:\n"
                + "\n".join(f"- {w}" for w in all_warnings)
            )

        # If no data was collected, fall back to LLM general knowledge
        no_data_fallback = not results or not any(
            r.records for r in results
        )

        if no_data_fallback:
            caps = _build_capabilities_block()
            analysis_prompt = (
                f'PREGUNTA DEL USUARIO: "{question}"\n'
                f"FECHA ACTUAL: {today}\n\n"
                f"{memory_ctx_analyst}\n\n"
                "No se encontraron datos en las fuentes de datos abiertos para esta consulta. "
                "Respondé usando tu conocimiento general, pero aclaralo brevemente "
                "al final (ej: 'Esta respuesta se basa en conocimiento general, "
                "no en datos abiertos del sistema.').\n"
                "Respondé de forma breve y conversacional en español argentino.\n\n"
                f"{caps}\n\n"
                "Al final de tu respuesta, sugerí 2-3 preguntas concretas de seguimiento "
                "que SÍ podamos responder con datos del sistema, eligiendo de las categorías listadas arriba. "
                "Formulalas como preguntas naturales, no como categorías."
            )
        else:
            analysis_prompt = (
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

        response = await self._llm.chat(
            messages=[
                LLMMessage(role="system", content=load_prompt("analyst")),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.4,
            max_tokens=8192,
        )

        # 6. Charts
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(response.content)
        charts = det_charts if det_charts else llm_charts

        clean_answer = re.sub(r"<!--CHART:.*?-->", "", response.content, flags=re.DOTALL)
        # Also strip truncated/unclosed chart tags (LLM ran out of tokens)
        clean_answer = re.sub(r"<!--CHART:.*", "", clean_answer, flags=re.DOTALL).strip()

        # 6a. Parse META confidence/citations
        confidence, citations = self._extract_meta(clean_answer)
        clean_answer = re.sub(r"<!--META:.*?-->", "", clean_answer, flags=re.DOTALL)
        clean_answer = re.sub(r"<!--META:.*", "", clean_answer, flags=re.DOTALL).strip()

        # Add disclaimer for low confidence
        if confidence < 0.5:
            clean_answer = (
                "**Nota:** La información disponible es limitada. "
                "Los datos presentados podrían ser parciales o requerir verificación adicional.\n\n"
                + clean_answer
            )

        # 6b. Policy analysis (optional, 1 LLM call)
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

        # Extract structured documents for frontend card rendering
        documents: list[dict[str, Any]] = []
        for r in results:
            if r.source.startswith("ddjj:"):
                for rec in r.records:
                    if rec.get("nombre") and rec.get("patrimonio_cierre") is not None:
                        documents.append({**rec, "doc_type": "ddjj"})

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

        # 7. Audit
        audit_query(user=user_id, question=question, intent=plan.intent, duration_ms=duration_ms)

        # 8. Memory update (fire-and-forget — don't block the response)
        asyncio.create_task(
            self._update_memory_bg(session_id, memory, plan, results, clean_answer)
        )

        # 9. Save conversation history
        if session:
            plan_data = {
                "intent": plan.intent,
                "steps": [
                    {"action": s.action, "params": s.params}
                    for s in plan.steps
                ],
            }
            await self._save_history(
                session, question, user_id, clean_answer,
                sources, tokens_used, duration_ms,
                plan_json=json.dumps(plan_data, ensure_ascii=False),
            )

        # 10. Cache write
        result_dict = {
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
            "tokens_used": tokens_used,
            "documents": documents if documents else None,
        }
        await self._write_cache(question, result_dict, intent=plan.intent)

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
            friendly_msg = (
                "No pude procesar esa consulta. "
                "Probá reformulándola con una pregunta sobre datos públicos argentinos, "
                "por ejemplo: *¿Cuál fue la inflación del último mes?*"
            )
            yield {"type": "chunk", "content": friendly_msg}
            yield {
                "type": "complete", "answer": friendly_msg,
                "sources": [], "chart_data": None,
            }
            return

        # Casual
        casual = self._get_casual_response(question)
        if casual:
            yield {"type": "chunk", "content": casual}
            yield {
                "type": "complete", "answer": casual,
                "sources": [], "chart_data": None, "casual": True,
            }
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

        # Cache check (skip in policy mode)
        session_id = conversation_id or ""
        if not policy_mode:
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

        # Memory
        memory = await load_memory(self._cache, session_id)
        memory_ctx = build_memory_context_prompt(memory)
        memory_ctx_analyst = build_memory_context_prompt(memory, for_analyst=True)

        # Chat history from DB (preferred over Redis memory for planner)
        chat_history = await self._load_chat_history(conversation_id)
        planner_ctx = chat_history or memory_ctx

        # Preprocess query (expand synonyms, acronyms, normalize)
        _q = expand_acronyms(question)
        _q, _ = normalize_temporal(_q)
        _q = normalize_provinces(_q)
        preprocessed_q = expand_synonyms(_q)

        # Plan (1 LLM call)
        yield {"type": "status", "step": "planning"}
        plan = await generate_plan(self._llm, preprocessed_q, memory_context=planner_ctx)

        # Inject search_datasets fallback if plan has no vector search step
        # and no high-confidence data-fetching step
        _has_vector_step = any(s.action == "search_datasets" for s in plan.steps)
        _has_data_step = any(s.action in _DATA_ACTIONS for s in plan.steps)
        if not _has_vector_step and not _has_data_step:
            plan.steps.insert(0, PlanStep(
                id="step_vector_fallback",
                action="search_datasets",
                description=f"Buscar datasets relevantes por similitud semántica: {question[:100]}",
                params={"query": preprocessed_q, "limit": 5},
                depends_on=[],
            ))

        yield {
            "type": "status",
            "step": "planned",
            "intent": plan.intent,
            "steps_count": len(plan.steps),
        }

        # Dispatch steps (0 LLM calls)
        yield {"type": "status", "step": "searching"}
        all_warnings: list[str] = []
        results, step_warnings = await self._execute_steps(plan, nl_query=question)
        all_warnings.extend(step_warnings)

        # Analysis (1 LLM call, streamed)
        yield {"type": "status", "step": "generating"}

        data_context = self._build_data_context(results)
        today = datetime.now(UTC).strftime("%Y-%m-%d")

        errors_block = ""
        if all_warnings:
            errors_block = (
                "\nERRORES EN LA RECOLECCIÓN:\n"
                + "\n".join(f"- {w}" for w in all_warnings)
            )

        no_data_fallback = not results or not any(
            r.records for r in results
        )

        if no_data_fallback:
            caps = _build_capabilities_block()
            analysis_prompt = (
                f'PREGUNTA DEL USUARIO: "{question}"\n'
                f"FECHA ACTUAL: {today}\n\n"
                f"{memory_ctx_analyst}\n\n"
                "No se encontraron datos en las fuentes de datos abiertos para esta consulta. "
                "Respondé usando tu conocimiento general, pero aclaralo brevemente "
                "al final (ej: 'Esta respuesta se basa en conocimiento general, "
                "no en datos abiertos del sistema.').\n"
                "Respondé de forma breve y conversacional en español argentino.\n\n"
                f"{caps}\n\n"
                "Al final de tu respuesta, sugerí 2-3 preguntas concretas de seguimiento "
                "que SÍ podamos responder con datos del sistema, eligiendo de las categorías listadas arriba. "
                "Formulalas como preguntas naturales, no como categorías."
            )
        else:
            analysis_prompt = (
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

        full_text = ""
        stream_buf = ""  # buffer for filtering <!--CHART/META--> tags
        async for chunk in self._llm.chat_stream(
            messages=[
                LLMMessage(role="system", content=load_prompt("analyst")),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.4,
            max_tokens=8192,
        ):
            full_text += chunk
            stream_buf += chunk

            # If we're inside a tag, keep buffering
            if "<!--" in stream_buf:
                tag_start = stream_buf.index("<!--")
                # Flush everything before the tag
                if tag_start > 0:
                    yield {"type": "chunk", "content": stream_buf[:tag_start]}
                    stream_buf = stream_buf[tag_start:]
                # Check if the tag is complete
                if "-->" in stream_buf:
                    # Drop the tag entirely, keep anything after it
                    tag_end = stream_buf.index("-->") + 3
                    stream_buf = stream_buf[tag_end:]
                    # Flush remaining if no new tag is starting
                    if stream_buf and "<!--" not in stream_buf:
                        yield {"type": "chunk", "content": stream_buf}
                        stream_buf = ""
                # else: tag not yet complete, keep buffering
            else:
                yield {"type": "chunk", "content": stream_buf}
                stream_buf = ""

        # Flush any remaining buffer (strip complete + truncated tags)
        if stream_buf:
            cleaned = re.sub(r"<!--.*?-->", "", stream_buf, flags=re.DOTALL)
            cleaned = re.sub(r"<!--.*", "", cleaned, flags=re.DOTALL)
            if cleaned.strip():
                yield {"type": "chunk", "content": cleaned}

        # Complete
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts
        clean_answer = re.sub(r"<!--CHART:.*?-->", "", full_text, flags=re.DOTALL)
        # Also strip truncated/unclosed chart tags (LLM ran out of tokens)
        clean_answer = re.sub(r"<!--CHART:.*", "", clean_answer, flags=re.DOTALL).strip()

        # Parse META from streaming output
        confidence, citations = self._extract_meta(clean_answer)
        clean_answer = re.sub(r"<!--META:.*?-->", "", clean_answer, flags=re.DOTALL)
        clean_answer = re.sub(r"<!--META:.*", "", clean_answer, flags=re.DOTALL).strip()

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

        # Extract structured documents for frontend card rendering
        documents: list[dict[str, Any]] = []
        for r in results:
            if r.source.startswith("ddjj:"):
                for rec in r.records:
                    if rec.get("nombre") and rec.get("patrimonio_cierre") is not None:
                        documents.append({**rec, "doc_type": "ddjj"})

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

        # Audit (streaming was missing this)
        audit_query(user=user_id, question=question, intent=plan.intent, duration_ms=0)

        # Memory update (fire-and-forget)
        asyncio.create_task(
            self._update_memory_bg(session_id, memory, plan, results, clean_answer)
        )

        # Cache write
        try:
            result_dict = {
                "answer": clean_answer,
                "sources": sources,
                "chart_data": charts if charts else None,
                "tokens_used": 0,
                "documents": documents if documents else None,
            }
            await self._write_cache(question, result_dict, intent=plan.intent)
        except Exception:
            logger.debug("Cache write failed in streaming", exc_info=True)

    # ── Background helpers ─────────────────────────────────

    async def _update_memory_bg(
        self,
        session_id: str,
        memory: Any,
        plan: ExecutionPlan,
        results: list[DataResult],
        answer: str,
    ) -> None:
        """Fire-and-forget memory update — runs after the response is sent."""
        try:
            updated = await update_memory(self._llm, memory, plan, results, answer)
            await save_memory(self._cache, session_id, updated)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("Background memory update failed", exc_info=True)

    # ── Classification helpers ──────────────────────────────

    @staticmethod
    def _get_casual_response(question: str) -> str | None:
        t = question.strip()
        if _GREETING_PATTERN.match(t):
            subtype = "greeting"
        elif _THANKS_PATTERN.match(t):
            subtype = "thanks"
        elif _FAREWELL_PATTERN.match(t):
            subtype = "farewell"
        else:
            return None
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

    async def _get_embedding(self, question: str) -> list[float] | None:
        try:
            emb = await self._embedding.embed(question)
            self._last_embedding = emb
            return emb
        except Exception:
            logger.debug("Embedding generation failed for cache", exc_info=True)
            return None

    async def _check_cache(self, question: str, user_id: str) -> SmartQueryResult | None:
        # 1. Redis cache first (cheap, ~1ms)
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
                    documents=cached.get("documents"),
                    cached=True,
                )
        except Exception:
            logger.debug("Redis cache read failed", exc_info=True)

        # 2. Semantic cache (requires embedding, ~100-200ms)
        try:
            q_embedding = await self._get_embedding(question)
            if q_embedding:
                sem_cached = await self._semantic_cache.get(question, embedding=q_embedding)
                if sem_cached:
                    self._metrics.record_cache_hit()
                    audit_cache_hit(user=user_id, question=question)
                    return SmartQueryResult(
                        answer=sem_cached.get("answer", ""),
                        sources=sem_cached.get("sources", []),
                        chart_data=sem_cached.get("chart_data"),
                        tokens_used=sem_cached.get("tokens_used", 0),
                        documents=sem_cached.get("documents"),
                        cached=True,
                    )
        except Exception:
            logger.debug("Semantic cache read failed", exc_info=True)

        self._metrics.record_cache_miss()
        return None

    async def _get_cached_dict(self, question: str) -> dict[str, Any] | None:
        """Return raw cached dict for streaming endpoint."""
        try:
            cached = await self._cache.get(self._cache_key(question))
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            logger.debug("WS Redis cache read failed", exc_info=True)
        try:
            q_embedding = await self._get_embedding(question)
            if q_embedding:
                sem = await self._semantic_cache.get(question, embedding=q_embedding)
                if sem and isinstance(sem, dict):
                    return sem
        except Exception:
            logger.debug("WS semantic cache read failed", exc_info=True)
        return None

    async def _write_cache(self, question: str, result: dict[str, Any], intent: str = "") -> None:
        ttl = ttl_for_intent(intent)
        try:
            await self._cache.set(self._cache_key(question), result, ttl_seconds=ttl)
        except Exception:
            logger.warning("Failed to cache smart query result", exc_info=True)
        try:
            # Reuse embedding already computed by _check_cache / _get_cached_dict
            q_embedding = getattr(self, "_last_embedding", None)
            if not q_embedding:
                q_embedding = await self._get_embedding(question)
            if q_embedding:
                await self._semantic_cache.set(question, q_embedding, result, ttl=ttl)
        except Exception:
            logger.debug("Semantic cache write failed", exc_info=True)

    # ── Step executors ──────────────────────────────────────

    async def _execute_series_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        series_ids = params.get("seriesIds", [])
        collapse = params.get("collapse")
        representation = params.get("representation")

        query_text = params.get("query", step.description)

        # Always check catalog for defaults (collapse, representation)
        # even when the planner already provided seriesIds, because
        # the planner often omits representation=percent_change.
        match = find_catalog_match(query_text)
        if match:
            if not series_ids:
                series_ids = match["ids"]
            if not collapse and "default_collapse" in match:
                collapse = match["default_collapse"]
            if not representation and "default_representation" in match:
                representation = match["default_representation"]
            logger.info("Series catalog match for '%s': %s", query_text[:60], series_ids)

        if not series_ids:
            search_results = await self._series.search(query_text)
            if search_results:
                series_ids = [r["id"] for r in search_results[:3]]
                logger.info("Series dynamic search for '%s': %s", query_text[:60], series_ids)

        if not series_ids:
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details={"query": query_text[:100], "reason": "No se encontraron series matching"},
            )

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
        if not result:
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details={"series_ids": series_ids, "reason": "API respondió sin datos"},
            )
        return [result]

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
            position = params.get("position")
            if position and result.records and len(result.records) >= position:
                result = DataResult(
                    source=result.source,
                    portal_name=result.portal_name,
                    portal_url=result.portal_url,
                    dataset_title=result.dataset_title,
                    format=result.format,
                    records=[result.records[position - 1]],
                    metadata={**result.metadata, "position": position},
                )
        elif action == "stats":
            result = self._ddjj.stats()
        elif action == "detail" or params.get("nombre"):
            result = self._ddjj.get_by_name(params.get("nombre", ""))
        else:
            result = self._ddjj.search(params.get("query", params.get("nombre", "")))

        return [result] if result.records else []

    async def _execute_staff_step(self, step: PlanStep) -> list[DataResult]:
        if not self._staff:
            logger.warning("Staff adapter not configured, skipping step %s", step.id)
            return []
        params = step.params
        action = params.get("action", "search")
        name = params.get("name", "")

        if (action in ("get_by_legislator", "count") and name) or name:
            # Always fetch full list — it includes the count and the names.
            # Using get_by_legislator ensures the LLM gets the detail it
            # needs regardless of whether the user asked "cuántos" or "quiénes".
            result = await self._staff.get_by_legislator(name)
        elif action == "changes":
            result = await self._staff.get_changes(name=name or None)
        elif action == "stats":
            result = await self._staff.stats()
        else:
            result = await self._staff.search(params.get("query", name or step.description))

        return [result] if result.records else []

    @staticmethod
    def _sanitize_ckan_query(raw: str) -> str:
        """Extract meaningful keywords from a potentially verbose LLM-generated query.

        CKAN's Solr search works best with short keyword queries, not full
        sentences like "Buscar datasets relacionados con educación en el portal".
        """
        # Common filler words the LLM likes to include
        _STOPWORDS = {
            "buscar", "busca", "buscando", "datasets", "dataset", "datos",
            "relacionados", "relacionado", "sobre", "acerca", "portal",
            "nacional", "disponibles", "disponible", "listar", "mostrar",
            "obtener", "consultar", "encontrar", "abiertos", "abierto",
            "informacion", "información", "con", "del", "los", "las",
            "que", "hay", "tiene", "para", "una", "por", "como", "son",
            "mas", "más", "este", "esta", "estos", "estas",
            "datos.gob.ar", "datos.gob", "gob.ar",
        }
        # Strip quotes and parenthetical text
        clean = re.sub(r"[\"'()]", " ", raw)
        # Extract words, keep only meaningful ones
        words = [w for w in clean.lower().split() if len(w) > 2 and w not in _STOPWORDS]
        if not words:
            # If everything was filtered, use the original minus obvious filler
            return raw.strip()[:100]
        return " ".join(words[:5])

    async def _execute_ckan_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        raw_query = params.get("query", step.description)
        query = self._sanitize_ckan_query(raw_query) if raw_query not in ("*", "*:*") else raw_query
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

        # --- Try local cached tables first ---
        local_results = await self._search_cached_tables(query, limit=rows)
        if local_results:
            logger.info("CKAN step resolved from %d local cached table(s)", len(local_results))
            return local_results

        # --- Fallback: live CKAN search ---
        return await self._ckan.search_datasets(query, portal_id=portal_id, rows=rows)

    async def _search_cached_tables(
        self, query: str, limit: int = 10,
    ) -> list[DataResult]:
        """Search locally cached tables (cache_*) for datasets matching *query*.

        Uses a simple keyword match against table names.  If matches are found,
        fetches a sample of rows via the sandbox so we avoid hitting the
        internet entirely.
        """
        if not self._sandbox:
            return []

        try:
            all_tables = await self._sandbox.list_cached_tables()
        except Exception:
            logger.debug("Failed to list cached tables", exc_info=True)
            return []

        if not all_tables:
            return []

        # Normalise query keywords for matching
        keywords = [k.lower() for k in query.split() if len(k) > 2]
        if not keywords:
            return []

        matched = [
            t for t in all_tables
            if any(kw in t.table_name.lower() for kw in keywords)
        ]
        if not matched:
            return []

        matched = matched[:limit]
        results: list[DataResult] = []
        now = datetime.now(UTC).isoformat()

        for table in matched:
            try:
                cols = ", ".join(f'"{c}"' for c in table.columns[:30]) if table.columns else "*"
                sql = f'SELECT {cols} FROM "{table.table_name}" LIMIT 50'
                sandbox_result = await self._sandbox.execute_readonly(sql, timeout_seconds=5)
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

    async def _execute_sesiones_step(self, step: PlanStep) -> list[DataResult]:
        params = step.params
        result = await self._sesiones.search(
            query=params.get("query", step.description),
            periodo=params.get("periodo"),
            orador=params.get("orador"),
            limit=params.get("limit", 15),
        )
        return [result] if result else []

    async def _execute_bcra_step(self, step: PlanStep) -> list[DataResult]:
        if not self._bcra:
            logger.warning("BCRAAdapter not configured, skipping step %s", step.id)
            return []
        params = step.params
        tipo = params.get("tipo", "cotizaciones")
        try:
            if tipo == "cotizaciones":
                result = await self._bcra.get_cotizaciones(
                    moneda=params.get("moneda", "USD"),
                    fecha_desde=params.get("fecha_desde"),
                    fecha_hasta=params.get("fecha_hasta"),
                )
            elif tipo == "variables":
                result = await self._bcra.get_principales_variables()
            elif tipo == "historica":
                id_variable = params.get("id_variable")
                if not id_variable:
                    result = await self._bcra.get_principales_variables()
                else:
                    result = await self._bcra.get_variable_historica(
                        id_variable=int(id_variable),
                        fecha_desde=params.get("fecha_desde", "2024-01-01"),
                        fecha_hasta=params.get("fecha_hasta", datetime.now(UTC).strftime("%Y-%m-%d")),
                    )
            else:
                result = await self._bcra.get_cotizaciones()
            return [result] if result else []
        except Exception:
            logger.warning("BCRA step %s failed", step.id, exc_info=True)
            return []

    async def _discover_tables_by_vector_search(
        self,
        query: str,
        cached_tables: list[Any],
    ) -> list[str]:
        """Use vector search to find relevant cached table names for a query.

        Embeds the query, searches for similar datasets, and returns the
        table names of any that are cached.  *cached_tables* is the
        already-fetched list of ``CachedTableInfo`` to avoid a redundant DB
        round-trip.
        """
        try:
            q_embedding = await self._embedding.embed(query)
            vector_results = await self._vector_search.search_datasets(
                q_embedding, limit=10,
            )
            if not vector_results:
                return []

            # Collect dataset_ids from vector search results
            hit_dataset_ids = {vr.dataset_id for vr in vector_results}

            # Match against cached tables by dataset_id
            matched_names = [
                t.table_name
                for t in cached_tables
                if t.dataset_id and t.dataset_id in hit_dataset_ids
            ]

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

    async def _execute_sandbox_step(self, step: PlanStep, user_query: str = "") -> list[DataResult]:
        if not self._sandbox:
            logger.warning("ISQLSandbox not configured, skipping step %s", step.id)
            return []
        params = step.params
        # Use the original user question for NL2SQL so specific filters
        # (e.g. "gobernador de jujuy") are not lost to the planner's generic query.
        nl_query = user_query or params.get("query", step.description)

        try:
            tables = await self._sandbox.list_cached_tables()
            table_hints = params.get("tables", [])
            logger.info(
                "Sandbox step %s: %d cached tables, hints=%s, query=%s",
                step.id, len(tables), table_hints, nl_query[:80],
            )
            if not tables:
                if table_hints and any("indec" in h for h in table_hints):
                    logger.info("No cached tables at all, attempting INDEC live fallback")
                    return await self._indec_live_fallback(nl_query)
                return []

            # When no table_hints from planner, use vector search to discover relevant tables
            _from_vector_search = False
            if not table_hints:
                discovered = await self._discover_tables_by_vector_search(nl_query, tables)
                if discovered:
                    table_hints = discovered
                    _from_vector_search = True

            if table_hints:
                if _from_vector_search:
                    # Vector search returns exact table names — use set lookup
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
                    tables = filtered
                elif any("indec" in h for h in table_hints):
                    indec_tables = [t.table_name for t in tables if "indec" in t.table_name]
                    logger.info(
                        "INDEC fnmatch miss: hints=%s, indec_tables_in_cache=%d (sample: %s)",
                        table_hints, len(indec_tables), indec_tables[:3],
                    )
                    logger.info("No cached INDEC tables, attempting live fallback")
                    return await self._indec_live_fallback(nl_query)
                else:
                    # Planner specified tables but none matched — return empty
                    # so the fallback LLM can answer with general knowledge.
                    logger.warning(
                        "Sandbox: none of the hinted tables %s found in cache, skipping",
                        table_hints,
                    )
                    return []

            tables_context_parts = []
            for t in tables[:50]:
                cols = ", ".join(t.columns) if t.columns else "(no column info)"
                tables_context_parts.append(
                    f"Table: {t.table_name}  (rows: {t.row_count or '?'})\n  Columns: {cols}"
                )
            tables_context = "\n\n".join(tables_context_parts)

            nl2sql_prompt = (
                "You are a SQL assistant for Argentine public datasets stored in PostgreSQL.\n"
                "You MUST generate ONLY a single SELECT query. Never use INSERT, UPDATE, DELETE, "
                "DROP, or any DDL/DML statement.\n\n"
                f"Available tables and their columns:\n\n{tables_context}\n\n"
                "Rules:\n"
                "- Only reference the tables and columns listed above.\n"
                '- Always use double-quoted identifiers for column names that contain spaces or special characters.\n'
                "- Limit results to 1000 rows max (add LIMIT 1000 if appropriate).\n"
                "- Return ONLY the SQL query, nothing else. No markdown, no explanation.\n"
                "- The query must be valid PostgreSQL syntax.\n"
                "- NEVER use parameter placeholders like $1, $2, :param, or ?. Always use literal values in the query.\n"
                "- Use ILIKE for case-insensitive text matching.\n"
            )

            messages = [
                LLMMessage(role="system", content=nl2sql_prompt),
                LLMMessage(role="user", content=nl_query),
            ]

            llm_response = await self._llm.chat(
                messages=messages,
                temperature=0.0,
                max_tokens=1024,
            )

            generated_sql = llm_response.content.strip()
            if generated_sql.startswith("```"):
                lines = generated_sql.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                generated_sql = "\n".join(lines).strip()

            result = await self._sandbox.execute_readonly(generated_sql)

            # Self-correction loop: retry up to 2 times with a minimal prompt
            # to avoid re-sending the full system prompt + history on each retry
            for _attempt in range(2):
                if not result.error:
                    break
                logger.warning("Sandbox query failed (attempt %d): %s", _attempt + 1, result.error)
                retry_messages = [
                    LLMMessage(
                        role="system",
                        content="Fix this PostgreSQL query. Return ONLY the corrected SQL, no markdown.",
                    ),
                    LLMMessage(
                        role="user",
                        content=(
                            f"SQL:\n{generated_sql}\n\n"
                            f"Error: {result.error}\n\n"
                            f"Tables:\n{tables_context[:2000]}"
                        ),
                    ),
                ]
                llm_response = await self._llm.chat(
                    messages=retry_messages, temperature=0.0, max_tokens=1024,
                )
                generated_sql = llm_response.content.strip()
                if generated_sql.startswith("```"):
                    lines = generated_sql.split("\n")
                    lines = [l for l in lines if not l.strip().startswith("```")]
                    generated_sql = "\n".join(lines).strip()
                result = await self._sandbox.execute_readonly(generated_sql)

            if result.error:
                logger.warning("Sandbox query failed after retries: %s", result.error)
                return []

            if not result.rows and _INDEC_PATTERN.search(nl_query):
                logger.info("Sandbox returned empty for INDEC query, trying live fallback")
                fallback = await self._indec_live_fallback(nl_query)
                if fallback:
                    return fallback

            return [
                DataResult(
                    source="sandbox:nl2sql",
                    portal_name="Cache Local (NL2SQL)",
                    portal_url="",
                    dataset_title=f"Consulta SQL: {nl_query[:100]}",
                    format="json",
                    records=result.rows[:200],
                    metadata={
                        "total_records": result.row_count,
                        "generated_sql": generated_sql,
                        "truncated": result.truncated,
                        "columns": result.columns,
                        "fetched_at": datetime.now(UTC).isoformat(),
                    },
                )
            ]
        except Exception:
            logger.warning("Sandbox step %s failed", step.id, exc_info=True)
            return []

    async def _execute_search_datasets_step(self, step: PlanStep) -> list[DataResult]:
        """Vector search over cached dataset chunks in pgvector."""
        params = step.params
        query = params.get("query", step.description)
        try:
            q_embedding = await self._embedding.embed(query)
            vector_results = await self._vector_search.search_datasets(
                q_embedding, limit=params.get("limit", 10),
            )
            if not vector_results:
                return []
            return [
                DataResult(
                    source=f"pgvector:{vr.portal}",
                    portal_name=vr.portal,
                    portal_url=vr.download_url or "",
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
                for vr in vector_results
            ]
        except Exception:
            logger.warning("search_datasets step failed", exc_info=True)
            return []

    async def _indec_live_fallback(self, nl_query: str) -> list[DataResult]:
        """Plan B: download INDEC XLS on-the-fly when cache tables don't exist."""
        import asyncio as _asyncio

        query_lower = nl_query.lower()
        keyword_map = {
            # IDs deben coincidir con INDEC_DATASETS en indec_tasks.py
            "ipc": ["ipc", "inflacion", "precios"],
            "emae": ["emae", "actividad economica", "actividad económica"],
            "pib": ["pib", "producto bruto", "producto interno"],
            "comercio_exterior": ["exportacion", "importacion", "comercio exterior", "balanza comercial"],
            "eph_tasas": ["empleo", "eph", "desempleo", "trabajo", "mercado laboral"],
            "canasta_basica": ["canasta basica", "canasta básica", "cbt", "cba"],
            "salarios_indice": ["salario", "salarios", "sueldo"],
            "pobreza_informe": ["pobreza", "indigencia"],
            "pobreza_historica": ["pobreza histor", "indigencia histor"],
            "isac": ["construccion", "construcción", "isac"],
            "ipi_manufacturero": ["industria", "ipi", "manufacturero", "produccion industrial"],
            "supermercados": ["supermercado"],
            "turismo_receptivo": ["turismo"],
            "distribucion_ingreso": ["distribucion del ingreso", "distribución del ingreso", "gini", "decil"],
            "balance_pagos": ["balance de pagos", "balanza de pagos", "cuenta corriente"],
        }

        matched_ids = []
        for ds_id, keywords in keyword_map.items():
            if any(kw in query_lower for kw in keywords):
                matched_ids.append(ds_id)

        if not matched_ids:
            matched_ids = ["ipc", "emae", "pib"]

        results: list[DataResult] = []
        for ds_id in matched_ids[:3]:
            ds_info = next((d for d in INDEC_DATASETS if d["id"] == ds_id), None)
            if not ds_info:
                continue

            try:
                sheets = await _asyncio.to_thread(_download_and_parse, ds_info["url"])
                if not sheets:
                    continue

                # Tomar la primera hoja con datos
                df = next(iter(sheets.values()))
                if df is None or df.empty:
                    continue

                if len(df) > 500:
                    df = df.tail(500)

                records = df.to_dict(orient="records")
                results.append(DataResult(
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
                ))
            except Exception:
                logger.warning("INDEC live fallback failed for %s", ds_id, exc_info=True)

        return results

    async def _execute_steps(
        self, plan: ExecutionPlan, nl_query: str = "",
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
                s for s in remaining
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
            step_start = time.monotonic()
            connector_name = step.action
            try:
                data = await self._dispatch_step_with_retry(step, nl_query=nl_query)
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms)
                return data, None
            except ConnectorError as exc:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
                logger.warning("Step %s failed (ConnectorError)", step.id, exc_info=True)
                return [], f"Conector '{connector_name}' no disponible: {exc}"
            except Exception as exc:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
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

    # ── Retry logic for transient step failures ───────────
    _RETRYABLE_PATTERNS = ("timeout", "503", "502", "504", "connection", "reset", "refused")

    def _is_retryable(self, exc: Exception) -> bool:
        """Return *True* if *exc* looks like a transient/network error."""
        exc_str = str(exc).lower()
        if any(p in exc_str for p in self._RETRYABLE_PATTERNS):
            return True
        if "Timeout" in type(exc).__name__:
            return True
        return False

    async def _dispatch_step_with_retry(
        self,
        step: PlanStep,
        nl_query: str = "",
        max_retries: int = 2,
    ) -> list[DataResult]:
        """Call ``_dispatch_step`` with up to *max_retries* retries on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                return await self._dispatch_step(step, nl_query=nl_query)
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries and self._is_retryable(exc):
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

    _NOOP_ACTIONS = frozenset(("analyze", "compare", "synthesize"))

    async def _dispatch_step(self, step: PlanStep, nl_query: str = "") -> list[DataResult]:
        # Sync handler (ddjj is not async)
        if step.action == "query_ddjj":
            return self._execute_ddjj_step(step)

        async_handlers = {
            "query_series": self._execute_series_step,
            "query_argentina_datos": self._execute_argentina_datos_step,
            "query_georef": self._execute_georef_step,
            "search_ckan": self._execute_ckan_step,
            "query_sesiones": self._execute_sesiones_step,
            "query_staff": self._execute_staff_step,
            "query_bcra": self._execute_bcra_step,
            "search_datasets": self._execute_search_datasets_step,
        }
        handler = async_handlers.get(step.action)
        if handler:
            return await handler(step)
        if step.action == "query_sandbox":
            return await self._execute_sandbox_step(step, user_query=nl_query)
        if step.action in self._NOOP_ACTIONS:
            return []
        logger.info("Unknown step action '%s', skipping", step.action)
        return []

    # ── Data context builder ────────────────────────────────

    @staticmethod
    def _build_data_context(results: list[DataResult]) -> str:
        if not results:
            return (
                "No se obtuvieron resultados directos en esta búsqueda. "
                "Sin embargo, TENÉS acceso en tiempo real a estos "
                "portales de datos abiertos:\n"
                "- **Portal Nacional** (datos.gob.ar): 1200+ datasets "
                "de economía, salud, educación, transporte, energía\n"
                "- **CABA** (data.buenosaires.gob.ar): movilidad, "
                "presupuesto, educación\n"
                "- **Buenos Aires Provincia** "
                "(catalogo.datos.gba.gob.ar): salud, estadísticas\n"
                "- **Córdoba, Santa Fe, Mendoza, Entre Ríos, "
                "Neuquén** y más\n"
                "- **Cámara de Diputados** (datos.hcdn.gob.ar): "
                "legisladores, proyectos, leyes\n"
                "- **Series de Tiempo**: inflación, tipo de cambio, "
                "PBI, presupuesto\n"
                "- **DDJJ**: 195 declaraciones juradas patrimoniales "
                "de diputados\n\n"
                "INSTRUCCIÓN: NO digas que 'no pudiste acceder' o "
                "'no tenés datos'. En cambio, explicale al usuario "
                "qué fuentes están disponibles y sugerí búsquedas "
                "concretas. Ofrecé 3-4 opciones temáticas."
            )

        max_per_result = 8_000
        max_results = 10
        max_total = 80_000

        parts = []
        for i, result in enumerate(results[:max_results]):
            valid_records = [r for r in result.records if isinstance(r, dict)]
            if not valid_records and result.records:
                continue

            # Vector search results: no records but have metadata
            is_vector_result = (
                not valid_records
                and result.source.startswith("pgvector:")
            )

            is_metadata_only = (
                valid_records
                and valid_records[0].get("_type") == "resource_metadata"
            )

            if is_vector_result:
                part = (
                    f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                    f"Portal: {result.portal_name}\n"
                    f"URL: {result.portal_url}\n"
                )
                if result.metadata.get("description"):
                    part += f"Descripción: {result.metadata['description']}\n"
                if result.metadata.get("columns"):
                    part += f"Columnas: {result.metadata['columns']}\n"
                if result.metadata.get("score"):
                    part += f"Relevancia: {result.metadata['score']}\n"
                part += (
                    "\nEste dataset está indexado en la base de datos. "
                    "Listalo al usuario con su título, descripción y URL."
                )
            elif is_metadata_only:
                preview = valid_records[:20]
                records_text = json.dumps(preview, ensure_ascii=False, separators=(",", ":"), default=str)
                part = (
                    f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                    f"Fuente: {result.portal_name} ({result.source})\n"
                    f"URL: {result.portal_url}\n"
                    "NOTA: Este dataset no tiene Datastore "
                    "habilitado. Solo metadatos de los recursos.\n"
                )
                if result.metadata.get("description"):
                    part += f"Descripción: {result.metadata['description']}\n"
                part += (
                    f"Recursos disponibles:\n{records_text}\n\n"
                    "Explicale al usuario qué datos contiene "
                    "y proporcioná el link."
                )
            else:
                columns = list(valid_records[0].keys()) if valid_records else []
                # Humanize column names for LLM display (replace _ with spaces)
                display_columns = [c.replace("_", " ") for c in columns]
                total_rows = len(valid_records)

                if total_rows > 50:
                    records_to_send = valid_records[:25] + valid_records[-25:]
                    truncation_note = f", primeros 25 + últimos 25 de {total_rows} totales"
                else:
                    records_to_send = valid_records
                    truncation_note = ""

                # Humanize keys in records for LLM context
                display_records = [
                    {k.replace("_", " "): v for k, v in rec.items()}
                    for rec in records_to_send
                ]
                records_text = json.dumps(display_records, ensure_ascii=False, separators=(",", ":"), default=str)

                part = (
                    f"--- Dataset {i + 1}: {result.dataset_title} ---\n"
                    f"Fuente: {result.portal_name} ({result.source})\n"
                    f"URL: {result.portal_url}\n"
                    f"Formato: {result.format}\n"
                    f"Total de registros: {result.metadata.get('total_records', total_rows)}\n"
                    f"Columnas: {', '.join(display_columns)}\n"
                )
                if result.metadata.get("description"):
                    part += f"Descripción: {result.metadata['description']}\n"
                part += (
                    f"Datos ({len(records_to_send)} registros{truncation_note}):\n"
                    f"{records_text}\n\n"
                    "IMPORTANTE: Si hay una columna temporal "
                    "(año, fecha, mes), generá un gráfico de línea "
                    "temporal con <!--CHART:{}--> usando TODOS los "
                    "datos proporcionados."
                )

            # Per-result truncation: prevent a single large result
            # from consuming the entire context budget
            if len(part) > max_per_result:
                part = part[:max_per_result] + "\n... [truncado, datos parciales]\n"

            parts.append(part)

        joined = "\n\n".join(parts)
        if len(joined) > max_total:
            joined = joined[:max_total] + "\n\n[... datos truncados por límite de contexto ...]"
        return joined

    # ── META parser ─────────────────────────────────────────

    @staticmethod
    def _extract_meta(text: str) -> tuple[float, list[dict[str, Any]]]:
        match = re.search(r"<!--META:(.*?)-->", text, re.DOTALL)
        if not match:
            return 1.0, []
        try:
            meta = json.loads(match.group(1))
            confidence = max(0.0, min(1.0, float(meta.get("confidence", 1.0))))
            citations = meta.get("citations", [])
            if not isinstance(citations, list):
                citations = []
            return confidence, citations
        except (json.JSONDecodeError, ValueError, TypeError):
            return 1.0, []

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

            # Detect temporal key
            time_key = None
            for k in keys:
                kl = k.lower()
                if k == "fecha" or "date" in kl or kl in ("año", "year", "mes"):
                    time_key = k
                    break

            # Detect label key for categorical data (e.g., "nombre")
            label_key = None
            if not time_key:
                for k in keys:
                    kl = k.lower()
                    if kl in ("nombre", "name", "titulo", "title", "label", "categoria", "category"):
                        label_key = k
                        break

            x_key = time_key or label_key
            if not x_key:
                continue

            # Columns that are numeric but should never be charted
            _SKIP_NUMERIC = {"centroide_lat", "centroide_lon", "lat", "lon", "latitud", "longitud",
                             "latitude", "longitude", "id", "provincia_id", "departamento_id",
                             "municipio_id", "localidad_censal_id"}
            numeric_keys = [
                k
                for k in keys
                if k != x_key
                and not k.startswith("_")
                and k.lower() not in _SKIP_NUMERIC
                and isinstance(first.get(k), int | float)
            ]
            if not numeric_keys:
                continue

            # For categorical charts, pick the most relevant numeric column
            if label_key and not time_key:
                # Prefer patrimonio/value columns for rankings
                preferred = [
                    k for k in numeric_keys
                    if any(t in k.lower() for t in ("patrimonio", "total", "monto", "valor", "cantidad", "importe"))
                ]
                if preferred:
                    numeric_keys = preferred[:1]
                else:
                    numeric_keys = numeric_keys[:1]

            clean = [
                row for row in result.records
                if any(row.get(k) is not None for k in numeric_keys)
            ]
            if len(clean) < 2:
                continue

            is_time = result.format == "time_series" or time_key == "fecha"
            chart_type = "line_chart" if is_time else "bar_chart"
            title = result.dataset_title
            units = result.metadata.get("units")
            if units:
                title += f" ({units})"

            charts.append({
                "type": chart_type,
                "title": title,
                "data": [
                    {x_key: row[x_key], **{k: row.get(k) for k in numeric_keys}}
                    for row in clean
                ],
                "xKey": x_key,
                "yKeys": numeric_keys,
            })
        return charts

    @staticmethod
    def _extract_llm_charts(text: str) -> list[dict[str, Any]]:
        charts: list[dict[str, Any]] = []
        for match in re.finditer(r"<!--CHART:(.*?)-->", text, re.DOTALL):
            try:
                chart = json.loads(match.group(1))
                if not (
                    chart.get("type")
                    and chart.get("data")
                    and chart.get("xKey")
                    and chart.get("yKeys")
                ):
                    continue
                # Validate that data rows actually contain numeric values
                y_keys = chart["yKeys"]
                valid_rows = [
                    row for row in chart["data"]
                    if any(isinstance(row.get(k), int | float) for k in y_keys)
                ]
                if len(valid_rows) < 2:
                    continue
                chart["data"] = valid_rows
                charts.append(chart)
            except (json.JSONDecodeError, KeyError):
                logger.debug("Failed to parse LLM chart tag", exc_info=True)
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
        plan_json: str = "",
    ) -> None:
        try:
            query_id = str(uuid4())
            await session.execute(
                text(
                    "INSERT INTO user_queries "
                    "(id, question, user_id, status, "
                    "analysis_result, sources_json, "
                    "tokens_used, duration_ms, plan_json) "
                    "VALUES (CAST(:id AS uuid), :question, "
                    ":user_id, 'completed', :result, "
                    ":sources, :tokens, :duration_ms, :plan)"
                ),
                {
                    "id": query_id,
                    "question": question,
                    "user_id": user_id,
                    "result": answer,
                    "sources": json.dumps(sources, ensure_ascii=False),
                    "tokens": tokens_used,
                    "duration_ms": duration_ms,
                    "plan": plan_json,
                },
            )
            await session.commit()
        except Exception:
            logger.warning(
                "Failed to save conversation history", exc_info=True,
            )
            with contextlib.suppress(Exception):
                await session.rollback()
