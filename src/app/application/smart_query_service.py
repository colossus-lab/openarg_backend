"""Application service for the smart query pipeline.

Extracts all business logic from smart_query_router.py into a testable,
reusable service used by both POST and WebSocket endpoints.
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

from app.application.intent_classifier import (
    ClassifiedIntent,
    classify_intent,
    intent_to_plan,
)
from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider, LLMMessage
from app.domain.ports.search.retrieval_evaluator import IRetrievalEvaluator, RetrievalQuality
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache, ttl_for_intent
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
from app.infrastructure.adapters.search.query_decomposer import QueryDecomposer
from app.infrastructure.adapters.search.query_preprocessor import QueryPreprocessor
from app.infrastructure.adapters.search.reranker import LLMReranker
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
    from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
    from app.domain.ports.search.vector_search import IVectorSearch
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter

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

_BCRA_PATTERN = re.compile(
    r"(cotizaci[oó]n|tipo\s+de\s+cambio|d[oó]lar\s+oficial"
    r"|reservas?\s+(?:del\s+)?bcra|tasa\s+(?:de\s+)?(?:pol[ií]tica|referencia)"
    r"|base\s+monetaria|circulaci[oó]n\s+monetaria)",
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
    r"(indec|ipc|inflaci[oó]n\s+mensual|emae|actividad\s+econ[oó]mica"
    r"|pib\s+trimestral|comercio\s+exterior|exportacion|importacion"
    r"|empleo\s+eph|canasta\s+b[aá]sica|salarios?\s+indec"
    r"|pobreza|indigencia|construcci[oó]n\s+isac|industria\s+ipi"
    r"|supermercados?\s+indec|turismo\s+internacional)",
    re.IGNORECASE,
)

# ── Economic query detection (for federated fallback) ──────
_ECONOMIC_PATTERN = re.compile(
    r"(inflaci[oó]n|ipc|dolar|d[oó]lar|tipo\s+de\s+cambio|pbi|pib|emae"
    r"|desempleo|reservas|tasa|exportaci|importaci|salario|deuda)",
    re.IGNORECASE,
)


def _is_economic_query(q: str) -> bool:
    return bool(_ECONOMIC_PATTERN.search(q))


# ── DDJJ deterministic routing patterns ─────────────────────
_DDJJ_PATTERN = re.compile(
    r"(patrimonio|declaraci[oó]n(?:es)?\s+jurada|ddjj"
    r"|bienes\s+de\s+|riqueza\s+de\s+|cu[aá]nto\s+tiene"
    r"|diputado.{0,20}(?:m[aá]s|mayor|menor)\s+(?:rico|patrimonio|plata)"
    r"|ranking\s+(?:de\s+)?(?:diputados|patrimonio|ddjj)"
    r"|mayor\s+patrimonio|menor\s+patrimonio"
    r"|m[aá]s\s+ricos?|m[aá]s\s+pobres?"
    r"|patrimonio\s+(?:de\s+)?diputado)",
    re.IGNORECASE,
)

_DDJJ_NAME_PATTERN = re.compile(
    r"(?:patrimonio|bienes|riqueza|ddjj|declaraci[oó]n\s+jurada)"
    r"\s+(?:de(?:l)?)\s+(.+?)(?:\?|$)",
    re.IGNORECASE,
)

_DDJJ_RANKING_PATTERN = re.compile(
    r"(ranking|mayor\s+patrimonio|menor\s+patrimonio"
    r"|m[aá]s\s+ricos?|m[aá]s\s+pobres?"
    r"|top\s+\d+|diputados?\s+(?:con\s+)?(?:m[aá]s|mayor|menor))",
    re.IGNORECASE,
)

# Extracts a specific position number from follow-up queries like
# "cual es el 11", "numero 11", "puesto 11", "#11", "el 11°"
_DDJJ_POSITION_PATTERN = re.compile(
    r"(?:n[uú]mero|puesto|posici[oó]n|lugar|#|el)\s*(\d{1,3})(?:[°ºª]|\b)",
    re.IGNORECASE,
)

# ── Staff deterministic routing patterns ──────────────────
_STAFF_PATTERN = re.compile(
    r"(asesores?\s+de\s+|personal\s+de(?:l)?\s+|empleados?\s+de\s+"
    r"|personas?\s+(?:que\s+)?(?:trabaja|en\s+(?:la\s+)?comisi[oó]n)"
    r"|cu[aá]ntos?\s+(?:asesores?|personal|empleados?|personas?)"
    r"|equipo\s+de\s+"
    r"|trabaja(?:n|r)?\s+(?:con|para|en)\s+"
    r"|n[oó]mina\s+de(?:l)?\s+personal"
    r"|qui[eé]n(?:es)?\s+trabaja"
    r"|list(?:a|ado)\s+de\s+(?:asesores?|personal|empleados?)"
    r"|altas?\s+y\s+bajas?|bajas?\s+y\s+altas?"
    r"|cambios?\s+(?:de\s+)?personal"
    r"|rotaci[oó]n\s+de\s+personal"
    r"|comisi[oó]n\s+de\s+\w+.{0,30}(?:personal|trabaja|empleado|asesor|miembro)"
    r"|(?:qui[eé]n(?:es)?|cu[aá]les?)\s+(?:se\s+fue|se\s+fueron|lleg[oó]|llegaron|entr[oó]|entraron|sali[oó]|salieron))",
    re.IGNORECASE,
)

# Stop words that should NOT be part of extracted legislator names
_STAFF_NAME_STOP = re.compile(
    r"\s+(?:tiene|hay|son|est[aá]n?|tiene[ns]?|en\s+|del?\s+congreso|del?\s+la\s+c[aá]mara).*$",
    re.IGNORECASE,
)

# Titles/roles that precede a legislator name and should be stripped
_STAFF_TITLE_PREFIX = re.compile(
    r"^(?:(?:el|la|del?)\s+)?"
    r"(?:diputad[oa]|senad(?:or|ora)|legislad(?:or|ora)|ministro|ministra|"
    r"secretari[oa]|director[a]?|presidente|presidenta|jef[ea])\s+",
    re.IGNORECASE,
)

_STAFF_NAME_PATTERN = re.compile(
    r"(?:asesores?|personal|empleados?|equipo)"
    r"\s+(?:de(?:l)?)\s+(.+?)(?:\?|$)",
    re.IGNORECASE,
)

_STAFF_NAME_TIENE_PATTERN = re.compile(
    r"(?:asesores?|personal|empleados?)\s+tiene\s+(.+?)(?:\?|$)",
    re.IGNORECASE,
)

_STAFF_COUNT_PATTERN = re.compile(
    r"cu[aá]ntos?\s+(?:asesores?|personal|empleados?)",
    re.IGNORECASE,
)

_STAFF_COUNT_TIENE_PATTERN = re.compile(
    r"cu[aá]ntos?\s+(?:asesores?|personal|empleados?)\s+tiene\s+(.+?)(?:\?|$)",
    re.IGNORECASE,
)

_STAFF_COMMISSION_PATTERN = re.compile(
    r"comisi[oó]n\s+de\s+(.+?)(?:\s+(?:en|del?)\s+(?:el\s+)?(?:congreso|c[aá]mara|senado|diputados?))?(?:\?|$)",
    re.IGNORECASE,
)

_STAFF_CHANGES_PATTERN = re.compile(
    r"((?:qui[eé]n(?:es)?|cu[aá]les?)\s+(?:se\s+fue|se\s+fueron|lleg[oó]|llegaron|entr[oó]|entraron|sali[oó]|salieron)"
    r"|altas?\s+y\s+bajas?|bajas?\s+y\s+altas?"
    r"|cambios?\s+(?:de\s+)?personal"
    r"|rotaci[oó]n\s+de\s+personal)",
    re.IGNORECASE,
)

# ── Argentina Datos deterministic routing ──────────────────
_ARGDATOS_PATTERN = re.compile(
    r"(d[oó]lar\s+(?:blue|mep|ccl|cripto|turista|tarjeta|contado\s+con\s+liqui)"
    r"|riesgo\s+pa[ií]s"
    r"|(?:a\s+)?cu[aá]nto\s+(?:est[aá]|cotiza|sale)\s+(?:el\s+)?d[oó]lar"
    r"|precio\s+del\s+d[oó]lar(?!\s+oficial))",
    re.IGNORECASE,
)

# ── Series de Tiempo deterministic routing ─────────────────
_SERIES_PATTERN = re.compile(
    r"(inflaci[oó]n|ipc\b|pbi\b|pib\b|emae\b"
    r"|desempleo|desocupaci[oó]n"
    r"|exportaci[oó]n(?:es)?|importaci[oó]n(?:es)?"
    r"|balanza\s+comercial|deuda\s+p[uú]blica"
    r"|salario(?:s)?\s+(?:promedio|real)"
    r"|serie(?:s)?\s+de\s+tiempo"
    r"|indicador(?:es)?\s+econ[oó]mico)",
    re.IGNORECASE,
)

# ── Sesiones deterministic routing ─────────────────────────
_SESIONES_PATTERN = re.compile(
    r"(sesi[oó]n(?:es)?\s+(?:del?\s+)?(?:congreso|c[aá]mara|diputados|senado)"
    r"|(?:qu[eé]\s+(?:se\s+)?debati[oó]|debate(?:s)?|discurso(?:s)?)\s+(?:sobre|de|en)\s+"
    r"|transcripci[oó]n(?:es)?\s+(?:del?\s+)?(?:congreso|c[aá]mara)"
    r"|(?:diputad[oa]|senador[a]?)\s+\w+\s+(?:dijo|habl[oó])"
    r"|qu[eé]\s+(?:dijo|dijeron)\s+.{0,20}(?:diputad|senador)"
    r"|(?:se\s+)?trat[oó]\s+en\s+(?:el\s+)?congreso)",
    re.IGNORECASE,
)


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
        retrieval_evaluator: IRetrievalEvaluator | None = None,
        staff: IStaffConnector | None = None,
        bcra: BCRAAdapter | None = None,
        sandbox: ISQLSandbox | None = None,
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
        self._retrieval_evaluator = retrieval_evaluator
        self._staff = staff
        self._bcra = bcra
        self._sandbox = sandbox
        self._reranker = LLMReranker(llm)
        self._metrics = MetricsCollector()

    # ── LLM intent classification ────────────────────────────

    def _available_connectors(self) -> list[str]:
        connectors = ["ddjj", "argentina_datos", "series_tiempo", "sesiones", "ckan"]
        if self._staff:
            connectors.append("staff")
        if self._bcra:
            connectors.append("bcra")
        if self._sandbox:
            connectors.append("sandbox")
        return connectors

    async def _classify_intent(
        self, question: str,
    ) -> ExecutionPlan | None:
        """LLM-based intent classification with regex fallback."""
        try:
            classified = await asyncio.wait_for(
                classify_intent(
                    self._llm, question,
                    available_connectors=(
                        self._available_connectors()
                    ),
                ),
                timeout=10.0,
            )
            if classified and not classified.is_clarification:
                plan = intent_to_plan(question, classified)
                intents_str = ", ".join(
                    m.intent for m in classified.intents
                )
                logger.info(
                    "LLM classified '%s' as [%s] "
                    "(confidence=%.2f, steps=%d)",
                    question[:60],
                    intents_str,
                    classified.confidence,
                    len(plan.steps),
                )
                return plan
            if classified and classified.is_clarification:
                self._pending_clarification = (
                    self._format_clarification(classified)
                )
                return None
        except Exception:
            logger.warning(
                "LLM intent classification failed, "
                "falling back to regex",
                exc_info=True,
            )

        # Fallback: deterministic regex (existing code)
        return (
            self._detect_ddjj_intent(question)
            or (
                self._detect_staff_intent(question)
                if self._staff else None
            )
            or (
                self._detect_bcra_intent(question)
                if self._bcra else None
            )
            or self._detect_argentina_datos_intent(question)
            or self._detect_series_intent(question)
            or self._detect_sesiones_intent(question)
        )

    @staticmethod
    def _format_clarification(classified: ClassifiedIntent) -> str:
        """Build a user-friendly clarification message."""
        msg = classified.clarification_message
        if not msg:
            msg = (
                "Tu pregunta podría referirse a "
                "varios recursos. ¿Podés especificar?"
            )
        opts = classified.clarification_options
        if opts:
            bullets = "\n".join(f"- {o}" for o in opts)
            msg += f"\n\n{bullets}"
        return msg

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

        # 1. Cache lookup FIRST (skip in policy mode — always fetch fresh data)
        if not policy_mode:
            cached = await self._check_cache(question, user_id)
            if cached:
                return cached

        # 2. Memory (lightweight cache read, needed for analysis)
        session_id = conversation_id or ""
        memory = await load_memory(self._cache, session_id)

        # 3. Intent classification (LLM → regex fallback)
        deterministic_plan = await self._classify_intent(question)

        # Handle clarification request
        pending = getattr(self, "_pending_clarification", "")
        if not deterministic_plan and pending:
            self._pending_clarification = ""
            return SmartQueryResult(
                answer=pending, sources=[], intent="clarification",
            )

        all_warnings: list[str] = []

        if deterministic_plan:
            plan = deterministic_plan
            results, step_warnings = await self._execute_steps(plan, nl_query=question)
            all_warnings.extend(step_warnings)
            logger.info(
                "Deterministic plan for '%s': intent=%s, steps=%d",
                question[:60], plan.intent, len(plan.steps),
            )
        else:
            # Full LLM pipeline: preprocess → decompose → plan → execute → CRAG → hybrid
            preprocessor = QueryPreprocessor(self._llm)
            enhanced_query = await preprocessor.reformulate(question)

            decomposer = QueryDecomposer(self._llm)
            sub_queries = await decomposer.decompose(enhanced_query)

            if len(sub_queries) > 1:
                memory_ctx_prompt = build_memory_context_prompt(memory)

                async def _plan_and_execute(sq: str) -> tuple[ExecutionPlan, list[DataResult], list[str]]:
                    sq_plan = await generate_plan(self._llm, sq, memory_context=memory_ctx_prompt)
                    sq_results, sq_warnings = await self._execute_steps(sq_plan, nl_query=sq)
                    return sq_plan, sq_results, sq_warnings

                plan_results = await asyncio.gather(
                    *[_plan_and_execute(sq) for sq in sub_queries],
                    return_exceptions=True,
                )

                all_results: list[DataResult] = []
                plan = None
                for pr in plan_results:
                    if isinstance(pr, Exception):
                        logger.warning("Sub-query plan/execute failed: %s", pr)
                        continue
                    sq_plan, sq_results, sq_warnings = pr
                    if plan is None:
                        plan = sq_plan
                    all_results.extend(sq_results)
                    all_warnings.extend(sq_warnings)

                if plan is None:
                    plan = await generate_plan(
                        self._llm, enhanced_query,
                        memory_context=build_memory_context_prompt(memory),
                    )
                results = all_results
                logger.info(
                    "Decomposed '%s' into %d sub-queries, got %d results",
                    question[:60], len(sub_queries), len(results),
                )
            else:
                plan = await generate_plan(
                    self._llm, enhanced_query,
                    memory_context=build_memory_context_prompt(memory),
                )
                logger.info(
                    "Plan for '%s': intent=%s, steps=%d",
                    question[:60], plan.intent, len(plan.steps),
                )
                results, step_warnings = await self._execute_steps(plan, nl_query=question)
                all_warnings.extend(step_warnings)

            # CRAG: evaluate retrieval quality and retry if needed (skip for deterministic)
            if self._retrieval_evaluator:
                try:
                    verdict = await self._retrieval_evaluator.evaluate(question, results, plan)
                    logger.info(
                        "CRAG verdict: quality=%s, confidence=%.2f",
                        verdict.quality.value, verdict.confidence,
                    )
                    if (
                        verdict.quality == RetrievalQuality.INCORRECT
                        and verdict.confidence >= 0.7
                    ):
                        logger.info(
                            "CRAG: re-executing with broader query (actions=%s)",
                            verdict.suggested_actions,
                        )
                        retry_plan = await generate_plan(
                            self._llm,
                            f"{question} (ampliar búsqueda, datos insuficientes)",
                            memory_context=build_memory_context_prompt(memory),
                        )
                        retry_results, retry_warnings = await self._execute_steps(retry_plan, nl_query=question)
                        all_warnings.extend(retry_warnings)
                        if retry_results:
                            results = retry_results
                            plan = retry_plan
                except Exception:
                    logger.debug("CRAG evaluation failed", exc_info=True)

            # Hybrid search complement (skip for deterministic)
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
            temperature=0.4,
            max_tokens=8192,
        )

        # 7. Charts
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(response.content)
        charts = det_charts if det_charts else llm_charts

        clean_answer = re.sub(r"<!--CHART:.*?-->", "", response.content, flags=re.DOTALL).strip()

        # 7a. Parse META confidence/citations
        confidence, citations = self._extract_meta(clean_answer)
        clean_answer = re.sub(r"<!--META:.*?-->", "", clean_answer, flags=re.DOTALL).strip()

        # Add disclaimer for low confidence
        if confidence < 0.5:
            clean_answer = (
                "**Nota:** La información disponible es limitada. "
                "Los datos presentados podrían ser parciales o requerir verificación adicional.\n\n"
                + clean_answer
            )

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
            await self._save_history(
                session, question, user_id, clean_answer,
                sources, tokens_used, duration_ms,
            )

        # 11. Cache write
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

        # Cache check FIRST (skip in policy mode — always fetch fresh data)
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

        # Intent classification (LLM → regex fallback)
        deterministic_plan = await self._classify_intent(question)

        # Handle clarification request
        pending = getattr(self, "_pending_clarification", "")
        if not deterministic_plan and pending:
            self._pending_clarification = ""
            yield {"type": "chunk", "content": pending}
            yield {
                "type": "complete", "answer": pending,
                "sources": [], "chart_data": None,
                "intent": "clarification",
            }
            return

        memory = await load_memory(self._cache, session_id)

        all_warnings: list[str] = []

        if deterministic_plan:
            plan = deterministic_plan
            logger.info(
                "Deterministic plan (streaming) for '%s': intent=%s",
                question[:60], plan.intent,
            )
            yield {
                "type": "status",
                "step": "planned",
                "intent": plan.intent,
                "steps_count": len(plan.steps),
            }
            yield {"type": "status", "step": "searching"}
            results, step_warnings = await self._execute_steps_streaming(plan, nl_query=question)
            all_warnings.extend(step_warnings)
        else:
            # Full LLM pipeline: preprocess → decompose → plan → execute → CRAG → hybrid
            yield {"type": "status", "step": "planning"}

            preprocessor = QueryPreprocessor(self._llm)
            enhanced_query = await preprocessor.reformulate(question)

            decomposer = QueryDecomposer(self._llm)
            sub_queries = await decomposer.decompose(enhanced_query)

            if len(sub_queries) > 1:
                memory_ctx_prompt = build_memory_context_prompt(memory)

                async def _plan_and_execute(sq: str) -> tuple[ExecutionPlan, list[DataResult], list[str]]:
                    sq_plan = await generate_plan(self._llm, sq, memory_context=memory_ctx_prompt)
                    sq_results, sq_warnings = await self._execute_steps(sq_plan, nl_query=sq)
                    return sq_plan, sq_results, sq_warnings

                plan_results = await asyncio.gather(
                    *[_plan_and_execute(sq) for sq in sub_queries],
                    return_exceptions=True,
                )

                all_results: list[DataResult] = []
                plan = None
                for pr in plan_results:
                    if isinstance(pr, Exception):
                        logger.warning("Sub-query plan/execute failed (streaming): %s", pr)
                        continue
                    sq_plan, sq_results, sq_warnings = pr
                    if plan is None:
                        plan = sq_plan
                    all_results.extend(sq_results)
                    all_warnings.extend(sq_warnings)

                if plan is None:
                    plan = await generate_plan(
                        self._llm, enhanced_query,
                        memory_context=build_memory_context_prompt(memory),
                    )
                results = all_results
            else:
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

                yield {"type": "status", "step": "searching"}
                results, step_warnings = await self._execute_steps_streaming(plan, nl_query=question)
                all_warnings.extend(step_warnings)

            # CRAG evaluation (skip for deterministic routes)
            if self._retrieval_evaluator:
                try:
                    verdict = await self._retrieval_evaluator.evaluate(question, results, plan)
                    logger.info(
                        "CRAG (streaming) verdict: quality=%s, confidence=%.2f",
                        verdict.quality.value, verdict.confidence,
                    )
                    if (
                        verdict.quality == RetrievalQuality.INCORRECT
                        and verdict.confidence >= 0.7
                    ):
                        retry_plan = await generate_plan(
                            self._llm,
                            f"{question} (ampliar búsqueda, datos insuficientes)",
                            memory_context=build_memory_context_prompt(memory),
                        )
                        retry_results, retry_warnings = await self._execute_steps_streaming(retry_plan, nl_query=question)
                        all_warnings.extend(retry_warnings)
                        if retry_results:
                            results = retry_results
                            plan = retry_plan
                except Exception:
                    logger.debug("CRAG evaluation failed in streaming", exc_info=True)

            # Hybrid search (skip for deterministic routes)
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
            temperature=0.4,
            max_tokens=8192,
        ):
            full_text += chunk
            yield {"type": "chunk", "content": chunk}

        # Complete
        det_charts = self._build_deterministic_charts(results)
        llm_charts = self._extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts
        clean_answer = re.sub(r"<!--CHART:.*?-->", "", full_text, flags=re.DOTALL).strip()

        # Parse META from streaming output
        confidence, citations = self._extract_meta(clean_answer)
        clean_answer = re.sub(r"<!--META:.*?-->", "", clean_answer, flags=re.DOTALL).strip()

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

        # Cache write (fire-and-forget, don't block streaming)
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

    # ── DDJJ deterministic intent detection ─────────────────

    @staticmethod
    def _detect_ddjj_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for DDJJ queries, or None to fall back to the LLM planner."""
        text = question.strip()
        if not _DDJJ_PATTERN.search(text):
            return None

        # Extract just the new user question (avoid matching numbers in context history)
        user_question = text
        marker = "NUEVA PREGUNTA DEL USUARIO"
        marker_idx = text.find(marker)
        if marker_idx != -1:
            user_question = text[marker_idx:]

        # Specific position query (e.g. "cual es el numero 11", "puesto 15")
        pos_match = _DDJJ_POSITION_PATTERN.search(user_question)
        if pos_match and _DDJJ_RANKING_PATTERN.search(text):
            position = int(pos_match.group(1))
            order = "asc" if re.search(r"menor|pobres?", text, re.IGNORECASE) else "desc"
            return ExecutionPlan(
                query=text,
                intent="ddjj_ranking",
                steps=[
                    PlanStep(
                        id="ddjj_ranking",
                        action="query_ddjj",
                        description=f"Posición {position} del ranking de diputados por patrimonio ({order})",
                        params={"action": "ranking", "sortBy": "patrimonio", "order": order, "top": position, "position": position},
                    ),
                ],
            )

        # Ranking queries
        if _DDJJ_RANKING_PATTERN.search(text):
            # Extract custom top N if specified (e.g. "top 20", "los 15 diputados")
            top_match = re.search(r"(?:top|los|las)\s+(\d{1,3})", user_question, re.IGNORECASE)
            top = int(top_match.group(1)) if top_match else 10
            order = "asc" if re.search(r"menor|pobres?", text, re.IGNORECASE) else "desc"
            return ExecutionPlan(
                query=text,
                intent="ddjj_ranking",
                steps=[
                    PlanStep(
                        id="ddjj_ranking",
                        action="query_ddjj",
                        description=f"Ranking de diputados por patrimonio ({order})",
                        params={"action": "ranking", "sortBy": "patrimonio", "order": order, "top": top},
                    ),
                ],
            )

        # Name-based search
        name_match = _DDJJ_NAME_PATTERN.search(text)
        if name_match:
            nombre = name_match.group(1).strip()
            return ExecutionPlan(
                query=text,
                intent="ddjj_persona",
                steps=[
                    PlanStep(
                        id="ddjj_name",
                        action="query_ddjj",
                        description=f"Buscar DDJJ de {nombre}",
                        params={"action": "detail", "nombre": nombre},
                    ),
                ],
            )

        # Generic DDJJ search (fallback within DDJJ)
        return ExecutionPlan(
            query=text,
            intent="ddjj_busqueda",
            steps=[
                PlanStep(
                    id="ddjj_search",
                    action="query_ddjj",
                    description="Búsqueda general en DDJJ",
                    params={"action": "search", "query": text},
                ),
            ],
        )

    # ── Staff deterministic intent detection ─────────────────

    @staticmethod
    def _clean_staff_name(raw: str) -> str:
        """Strip title prefixes and trailing stop words from an extracted legislator name."""
        name = raw.strip()
        name = _STAFF_TITLE_PREFIX.sub("", name).strip()
        name = _STAFF_NAME_STOP.sub("", name).strip()
        return name

    @staticmethod
    def _extract_staff_name(txt: str) -> str:
        """Try multiple patterns to extract a legislator name from the query."""
        # "asesores/personal/empleados de X"
        m = _STAFF_NAME_PATTERN.search(txt)
        if m:
            return SmartQueryService._clean_staff_name(m.group(1))

        # "cuantos asesores tiene X"
        m = _STAFF_COUNT_TIENE_PATTERN.search(txt)
        if m:
            return SmartQueryService._clean_staff_name(m.group(1))

        # "asesores tiene X" (without cuantos)
        m = _STAFF_NAME_TIENE_PATTERN.search(txt)
        if m:
            return SmartQueryService._clean_staff_name(m.group(1))

        return ""

    @staticmethod
    def _detect_staff_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for staff queries, or None to fall back to the LLM planner."""
        txt = question.strip()
        if not _STAFF_PATTERN.search(txt):
            return None

        # Changes queries (altas/bajas)
        if _STAFF_CHANGES_PATTERN.search(txt):
            nombre = SmartQueryService._extract_staff_name(txt) or None
            return ExecutionPlan(
                query=txt,
                intent="staff_changes",
                steps=[
                    PlanStep(
                        id="staff_changes",
                        action="query_staff",
                        description=f"Cambios de personal{f' de {nombre}' if nombre else ''}",
                        params={"action": "changes", **({"name": nombre} if nombre else {})},
                    ),
                ],
            )

        # Count queries ("cuantos asesores de X" or "cuantos asesores tiene X")
        if _STAFF_COUNT_PATTERN.search(txt):
            nombre = SmartQueryService._extract_staff_name(txt)
            if nombre:
                return ExecutionPlan(
                    query=txt,
                    intent="staff_count",
                    steps=[
                        PlanStep(
                            id="staff_count",
                            action="query_staff",
                            description=f"Cantidad de asesores de {nombre}",
                            params={"action": "count", "name": nombre},
                        ),
                    ],
                )

        # Name-based list
        nombre = SmartQueryService._extract_staff_name(txt)
        if nombre:
            return ExecutionPlan(
                query=txt,
                intent="staff_legislator",
                steps=[
                    PlanStep(
                        id="staff_list",
                        action="query_staff",
                        description=f"Personal de {nombre}",
                        params={"action": "get_by_legislator", "name": nombre},
                    ),
                ],
            )

        # Commission-based query ("comisión de educación", "comisión de presupuesto")
        # Extract user question only to avoid matching commission names in context
        user_q = txt
        marker = "NUEVA PREGUNTA DEL USUARIO"
        idx = txt.find(marker)
        if idx != -1:
            user_q = txt[idx:]
        comm_match = _STAFF_COMMISSION_PATTERN.search(user_q)
        if comm_match:
            commission = comm_match.group(1).strip()
            return ExecutionPlan(
                query=txt,
                intent="staff_commission",
                steps=[
                    PlanStep(
                        id="staff_commission",
                        action="query_staff",
                        description=f"Personal de la comisión de {commission}",
                        params={"action": "get_by_legislator", "name": f"comision de {commission}"},
                    ),
                ],
            )

        # Generic staff query — extract keywords instead of passing full text
        keywords = re.sub(
            r"\b(que|cual|cuales|quienes?|como|donde|cuanto|personas?|trabajan?|en|el|la|los|las|del?|un[oa]?s?|por|con|para|es|son|hay)\b",
            " ", txt, flags=re.IGNORECASE,
        ).strip()
        keywords = re.sub(r"\s+", " ", keywords).strip()[:200]
        return ExecutionPlan(
            query=txt,
            intent="staff_busqueda",
            steps=[
                PlanStep(
                    id="staff_search",
                    action="query_staff",
                    description="Búsqueda general de personal",
                    params={"action": "search", "query": keywords or txt[:200]},
                ),
            ],
        )

    # ── BCRA deterministic intent detection ──────────────────

    @staticmethod
    def _detect_bcra_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for BCRA queries."""
        text = question.strip()
        if not _BCRA_PATTERN.search(text):
            return None
        lower = text.lower()
        if any(kw in lower for kw in ("reserva", "reservas")):
            return ExecutionPlan(
                query=text,
                intent="bcra_reservas",
                steps=[PlanStep(
                    id="bcra_variables",
                    action="query_bcra",
                    description="Reservas del BCRA",
                    params={"tipo": "variables"},
                )],
            )
        if any(kw in lower for kw in ("tasa", "política monetaria", "politica monetaria")):
            return ExecutionPlan(
                query=text,
                intent="bcra_tasa",
                steps=[PlanStep(
                    id="bcra_variables",
                    action="query_bcra",
                    description="Tasa de política monetaria BCRA",
                    params={"tipo": "variables"},
                )],
            )
        if any(kw in lower for kw in ("base monetaria", "circulación monetaria", "circulacion monetaria")):
            return ExecutionPlan(
                query=text,
                intent="bcra_monetaria",
                steps=[PlanStep(
                    id="bcra_variables",
                    action="query_bcra",
                    description="Variables monetarias BCRA",
                    params={"tipo": "variables"},
                )],
            )
        return ExecutionPlan(
            query=text,
            intent="bcra_cotizaciones",
            steps=[PlanStep(
                id="bcra_cotizaciones",
                action="query_bcra",
                description="Cotizaciones oficiales BCRA",
                params={"tipo": "cotizaciones"},
            )],
        )

    # ── Argentina Datos deterministic intent detection ───────

    @staticmethod
    def _detect_argentina_datos_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for ArgDatos queries (dólar blue, riesgo país)."""
        text = question.strip()
        if not _ARGDATOS_PATTERN.search(text):
            return None
        lower = text.lower()
        if "riesgo" in lower and "pa" in lower:
            return ExecutionPlan(
                query=text,
                intent="argdatos_riesgo_pais",
                steps=[PlanStep(
                    id="argdatos_riesgo",
                    action="query_argentina_datos",
                    description="Riesgo país actual",
                    params={"type": "riesgo_pais", "ultimo": True},
                )],
            )
        casa = None
        if "blue" in lower:
            casa = "blue"
        elif "mep" in lower or "bolsa" in lower:
            casa = "bolsa"
        elif "ccl" in lower or "contado con liqui" in lower:
            casa = "contadoconliqui"
        elif "cripto" in lower:
            casa = "cripto"
        elif "tarjeta" in lower or "turista" in lower:
            casa = "tarjeta"
        return ExecutionPlan(
            query=text,
            intent="argdatos_dolar",
            steps=[PlanStep(
                id="argdatos_dolar",
                action="query_argentina_datos",
                description=f"Cotización dólar{f' {casa}' if casa else ''}",
                params={"type": "dolar", **({"casa": casa} if casa else {})},
            )],
        )

    # ── Series de Tiempo deterministic intent detection ──────

    @staticmethod
    def _detect_series_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for economic time-series queries."""
        text = question.strip()
        if not _SERIES_PATTERN.search(text):
            return None
        return ExecutionPlan(
            query=text,
            intent="series_tiempo",
            steps=[PlanStep(
                id="series_query",
                action="query_series",
                description=f"Serie de tiempo: {text[:100]}",
                params={"query": text},
            )],
        )

    # ── Sesiones deterministic intent detection ──────────────

    @staticmethod
    def _detect_sesiones_intent(question: str) -> ExecutionPlan | None:
        """Return a forced ExecutionPlan for congressional session queries."""
        text = question.strip()
        if not _SESIONES_PATTERN.search(text):
            return None
        return ExecutionPlan(
            query=text,
            intent="sesiones_congreso",
            steps=[PlanStep(
                id="sesiones_search",
                action="query_sesiones",
                description=f"Buscar en sesiones: {text[:100]}",
                params={"query": text},
            )],
        )

    # ── Cache helpers ───────────────────────────────────────

    @staticmethod
    def _cache_key(question: str) -> str:
        normalized = question.strip().lower()
        h = hashlib.sha256(normalized.encode()).hexdigest()[:16]
        return f"openarg:smart:{h}"

    async def _get_embedding(self, question: str) -> list[float] | None:
        """Generate embedding for a question, returning None on failure."""
        try:
            return await self._embedding.embed(question)
        except Exception:
            logger.debug("Embedding generation failed for cache")
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
            logger.debug("Redis cache read failed")

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
            logger.debug("Semantic cache read failed")

        self._metrics.record_cache_miss()
        return None

    async def _get_cached_dict(self, question: str) -> dict[str, Any] | None:
        """Return raw cached dict for streaming endpoint."""
        # Redis first
        try:
            cached = await self._cache.get(self._cache_key(question))
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            logger.debug("WS Redis cache read failed")
        # Then semantic
        try:
            q_embedding = await self._get_embedding(question)
            if q_embedding:
                sem = await self._semantic_cache.get(question, embedding=q_embedding)
                if sem and isinstance(sem, dict):
                    return sem
        except Exception:
            logger.debug("WS semantic cache read failed")
        return None

    async def _write_cache(self, question: str, result: dict[str, Any], intent: str = "") -> None:
        ttl = ttl_for_intent(intent)
        try:
            await self._cache.set(self._cache_key(question), result, ttl_seconds=ttl)
        except Exception:
            logger.warning("Failed to cache smart query result", exc_info=True)
        try:
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
            # If a specific position was requested, only return that record
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

        if action == "get_by_legislator" and name:
            result = await self._staff.get_by_legislator(name)
        elif action == "count" and name:
            result = await self._staff.count_by_legislator(name)
        elif action == "changes":
            result = await self._staff.get_changes(name=name or None)
        elif action == "stats":
            result = await self._staff.stats()
        else:
            result = await self._staff.search(params.get("query", name or step.description))

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

    async def _indec_live_fallback(self, nl_query: str) -> list[DataResult]:
        """Plan B: download INDEC XLS on-the-fly when cache tables don't exist."""
        query_lower = nl_query.lower()
        keyword_map = {
            "ipc": ["ipc", "inflacion", "precios"],
            "emae": ["emae", "actividad economica", "actividad económica"],
            "pib": ["pib", "producto bruto", "producto interno"],
            "comercio_exterior": ["exportacion", "importacion", "comercio exterior"],
            "empleo": ["empleo", "eph", "desempleo", "trabajo"],
            "canasta_basica": ["canasta basica", "canasta básica", "cbt", "cba"],
            "salarios": ["salario", "salarios"],
            "pobreza": ["pobreza", "indigencia"],
            "construccion": ["construccion", "construcción", "isac"],
            "industria": ["industria", "ipi", "manufacturero"],
            "supermercados": ["supermercado"],
            "turismo": ["turismo"],
            "ipc_regiones": ["ipc region", "inflacion region"],
            "pib_provincial": ["pib provincial"],
            "uso_tiempo": ["uso del tiempo"],
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
                df = await asyncio.to_thread(_download_and_parse, ds_info["url"])
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

    async def _federated_fallback(
        self, nl_query: str, existing_results: list[DataResult],
    ) -> list[DataResult]:
        """Búsqueda federada: intenta CKAN + series cuando los conectores primarios fallaron."""
        real_results = [r for r in existing_results if r.records]
        if len(real_results) >= 2:
            return existing_results

        extra: list[DataResult] = []

        # 1. CKAN search (busca en todos los portales)
        if self._ckan:
            try:
                ckan_results = await self._ckan.search_datasets(nl_query, rows=5)
                for r in ckan_results:
                    if r.records:
                        r.metadata["fallback"] = True
                        extra.append(r)
            except Exception:
                logger.debug("Federated CKAN fallback failed", exc_info=True)

        # 2. Series de Tiempo (si la query parece económica)
        if self._series and _is_economic_query(nl_query):
            try:
                series_step = PlanStep(
                    id="fallback_series",
                    action="query_series",
                    description=nl_query,
                    params={"query": nl_query},
                )
                series_data = await self._execute_series_step(series_step)
                for r in series_data:
                    if r.records:
                        r.metadata["fallback"] = True
                        extra.append(r)
            except Exception:
                logger.debug("Federated series fallback failed", exc_info=True)

        # 3. INDEC live (si parece INDEC y no hay ya datos INDEC)
        has_indec = any("indec" in r.source for r in existing_results if r.records)
        if not has_indec and _INDEC_PATTERN.search(nl_query):
            try:
                indec_data = await self._indec_live_fallback(nl_query)
                extra.extend(indec_data)
            except Exception:
                logger.debug("Federated INDEC fallback failed", exc_info=True)

        return existing_results + extra[:3]

    async def _execute_sandbox_step(self, step: PlanStep) -> list[DataResult]:
        if not self._sandbox:
            logger.warning("ISQLSandbox not configured, skipping step %s", step.id)
            return []
        params = step.params
        nl_query = params.get("query", step.description)

        try:
            # Get available tables
            tables = await self._sandbox.list_cached_tables()
            table_hints = params.get("tables", [])
            if not tables:
                # If no tables at all but hints point to INDEC, try live fallback
                if table_hints and any("indec" in h for h in table_hints):
                    logger.info("No cached tables at all, attempting INDEC live fallback")
                    return await self._indec_live_fallback(nl_query)
                return []

            # Filter tables if hints provided
            if table_hints:
                import fnmatch
                filtered = []
                for t in tables:
                    for pattern in table_hints:
                        if fnmatch.fnmatch(t.table_name, pattern):
                            filtered.append(t)
                            break
                if filtered:
                    tables = filtered
                else:
                    # INDEC fallback: if hints match indec pattern and no cached tables found
                    if any("indec" in h for h in table_hints):
                        logger.info("No cached INDEC tables, attempting live fallback")
                        return await self._indec_live_fallback(nl_query)

            # Build context for NL2SQL
            tables_context_parts = []
            for t in tables[:50]:  # safety limit
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
            )

            llm_response = await self._llm.chat(
                messages=[
                    LLMMessage(role="system", content=nl2sql_prompt),
                    LLMMessage(role="user", content=nl_query),
                ],
                temperature=0.0,
                max_tokens=1024,
            )

            generated_sql = llm_response.content.strip()
            # Strip markdown code fences
            if generated_sql.startswith("```"):
                lines = generated_sql.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                generated_sql = "\n".join(lines).strip()

            result = await self._sandbox.execute_readonly(generated_sql)
            if result.error:
                logger.warning("Sandbox query failed: %s", result.error)
                return []

            # If sandbox returned empty and query is INDEC-related, try live fallback
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

    async def _execute_steps(
        self, plan: ExecutionPlan, nl_query: str = "",
    ) -> tuple[list[DataResult], list[str]]:
        """Execute plan steps, catching ConnectorError per step.

        Returns:
            Tuple of (results, warnings) where warnings lists connector failures.
        """
        results: list[DataResult] = []
        warnings: list[str] = []
        for step in plan.steps[:5]:
            step_start = time.monotonic()
            connector_name = step.action
            try:
                data = await self._dispatch_step(step)
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms)
                results.extend(data)
            except ConnectorError as exc:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
                logger.warning("Step %s failed (ConnectorError)", step.id, exc_info=True)
                warnings.append(f"Conector '{connector_name}' no disponible: {exc}")
            except Exception as exc:
                step_ms = round((time.monotonic() - step_start) * 1000, 1)
                self._metrics.record_connector_call(connector_name, step_ms, error=True)
                logger.warning("Step %s failed", step.id, exc_info=True)
                warnings.append(f"Conector '{connector_name}' falló: {type(exc).__name__}")

        # If no results with actual data, attempt federated fallback
        real = [r for r in results if r.records]
        if not real and nl_query:
            logger.info("No real data from plan steps, attempting federated fallback")
            results = await self._federated_fallback(nl_query, results)

        return results, warnings

    async def _execute_steps_streaming(
        self, plan: ExecutionPlan, nl_query: str = "",
    ) -> tuple[list[DataResult], list[str]]:
        """Same as _execute_steps but used by streaming endpoint."""
        return await self._execute_steps(plan, nl_query=nl_query)

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
        elif step.action == "query_staff":
            return await self._execute_staff_step(step)
        elif step.action == "query_bcra":
            return await self._execute_bcra_step(step)
        elif step.action == "query_sandbox":
            return await self._execute_sandbox_step(step)
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
                min_score=0.01,
            )

            # Apply LLM reranking
            if vector_results:
                vector_results = await self._reranker.rerank(
                    enhanced_query, vector_results, top_k=5,
                )

            for vr in vector_results:
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
                    "IMPORTANTE: Si hay una columna temporal "
                    "(año, fecha, mes), generá un gráfico de línea "
                    "temporal con <!--CHART:{}-->."
                )

            parts.append(part)

        joined = "\n\n".join(parts)
        # Truncate to ~60k chars (~15k tokens) to avoid hitting LLM context limits
        max_context_chars = 60_000
        if len(joined) > max_context_chars:
            joined = joined[:max_context_chars] + "\n\n[... datos truncados por límite de contexto ...]"
        return joined

    # ── META parser ─────────────────────────────────────────

    @staticmethod
    def _extract_meta(text: str) -> tuple[float, list[dict[str, Any]]]:
        """Extract confidence and citations from <!--META:{...}--> block.

        Returns (confidence, citations). Defaults to (1.0, []) if not found.
        """
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
                if k != time_key
                and not k.startswith("_")
                and isinstance(first.get(k), int | float)
            ]
            if not numeric_keys:
                continue

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
                if (
                    chart.get("type")
                    and chart.get("data")
                    and chart.get("xKey")
                    and chart.get("yKeys")
                ):
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
                text(
                    "INSERT INTO user_queries "
                    "(id, question, user_id, status, "
                    "analysis_result, sources_json, "
                    "tokens_used, duration_ms) "
                    "VALUES (CAST(:id AS uuid), :question, "
                    ":user_id, 'completed', :result, "
                    ":sources, :tokens, :duration_ms)"
                ),
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
            logger.warning(
                "Failed to save conversation history", exc_info=True,
            )
            with contextlib.suppress(Exception):
                await session.rollback()
