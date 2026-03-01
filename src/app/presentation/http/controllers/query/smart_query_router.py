from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
from datetime import UTC, datetime
from uuid import uuid4

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text

from app.domain.entities.connectors.data_result import ChartData, DataResult, ExecutionPlan, PlanStep
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider, LLMMessage
from app.domain.ports.search.vector_search import IVectorSearch
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.adapters.connectors.memory_agent import (
    build_memory_context_prompt,
    load_memory,
    save_memory,
    update_memory,
)
from app.infrastructure.adapters.connectors.query_planner import (
    ANALYSIS_SYSTEM_PROMPT,
    generate_plan,
)
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
from app.infrastructure.adapters.connectors.series_tiempo_adapter import find_catalog_match
from app.infrastructure.monitoring.metrics import MetricsCollector
from app.infrastructure.persistence_sqla.provider import MainAsyncSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/query", tags=["smart-query"])
limiter = Limiter(key_func=get_remote_address)


class SmartQueryRequest(BaseModel):
    question: str
    user_email: str | None = None
    conversation_id: str | None = None


class SmartQueryResponse(BaseModel):
    answer: str
    sources: list[dict]
    chart_data: list[dict] | None = None
    tokens_used: int = 0


def _cache_key(question: str) -> str:
    normalized = question.strip().lower()
    h = hashlib.sha256(normalized.encode()).hexdigest()[:16]
    return f"openarg:smart:{h}"


# ── Casual / meta message detection ─────────────────────────

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
    # Saludos
    r"hola|buenas|buen día|buenos días|buenas tardes|buenas noches"
    r"|hey|qué tal|que tal|qué onda|que onda|cómo estás|como estas"
    r"|cómo andás|como andas|qué hacés|que haces"
    # Agradecimientos
    r"|gracias|muchas gracias|genial|perfecto|dale|ok|de una|buenísimo|buenisimo"
    # Despedidas
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


def _classify_casual_subtype(text: str) -> str:
    """Return 'greeting', 'thanks', 'farewell', or 'generic'."""
    t = text.strip()
    if _GREETING_PATTERN.match(t):
        return "greeting"
    if _THANKS_PATTERN.match(t):
        return "thanks"
    if _FAREWELL_PATTERN.match(t):
        return "farewell"
    return "generic"


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


def _get_casual_response(question: str) -> str | None:
    """Return a static response if the question is casual, else None."""
    text = question.strip()
    if not _CASUAL_PATTERNS.match(text):
        return None
    subtype = _classify_casual_subtype(text)
    responses = _CASUAL_RESPONSES.get(subtype, _CASUAL_RESPONSES["generic"])
    return random.choice(responses)  # noqa: S311


def _get_meta_response(question: str) -> str | None:
    """Return the meta response if the question is about OpenArg, else None."""
    if _META_PATTERNS.search(question.strip()):
        return _META_RESPONSE
    return None


# ── Step executors ────────────────────────────────────────────


async def _execute_series_step(
    series: ISeriesTiempoConnector, step: PlanStep
) -> list[DataResult]:
    params = step.params
    series_ids = params.get("seriesIds", [])
    collapse = params.get("collapse")
    representation = params.get("representation")

    # If no explicit seriesIds, try catalog match
    if not series_ids:
        query = params.get("query", step.description)
        match = find_catalog_match(query)
        if match:
            series_ids = match["ids"]
            # Apply catalog defaults if not overridden
            if not collapse and "default_collapse" in match:
                collapse = match["default_collapse"]
            if not representation and "default_representation" in match:
                representation = match["default_representation"]

    if not series_ids:
        # Try search API
        search_results = await series.search(params.get("query", step.description))
        if search_results:
            series_ids = [r["id"] for r in search_results[:3]]

    if not series_ids:
        return []

    start_date = params.get("startDate")
    end_date = params.get("endDate")
    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    result = await series.fetch(
        series_ids=series_ids,
        start_date=start_date,
        end_date=end_date,
        collapse=collapse,
        representation=representation,
    )
    return [result] if result else []


async def _execute_argentina_datos_step(
    arg_datos: IArgentinaDatosConnector, step: PlanStep
) -> list[DataResult]:
    params = step.params
    data_type = params.get("type", "dolar")

    if data_type == "dolar":
        result = await arg_datos.fetch_dolar(casa=params.get("casa"))
        return [result] if result else []
    elif data_type == "riesgo_pais":
        result = await arg_datos.fetch_riesgo_pais(ultimo=params.get("ultimo", False))
        return [result] if result else []
    elif data_type == "inflacion":
        result = await arg_datos.fetch_inflacion()
        return [result] if result else []
    return []


async def _execute_georef_step(
    georef: IGeorefConnector, step: PlanStep
) -> list[DataResult]:
    query = step.params.get("query", step.description)
    result = await georef.normalize_location(query)
    return [result] if result else []


def _execute_ddjj_step(ddjj: DDJJAdapter, step: PlanStep) -> list[DataResult]:
    params = step.params
    action = params.get("action", "search")

    if action == "ranking":
        result = ddjj.ranking(
            sort_by=params.get("sortBy", "patrimonio"),
            top=params.get("top", 10),
            order=params.get("order", "desc"),
        )
    elif action == "stats":
        result = ddjj.stats()
    elif action == "detail" or params.get("nombre"):
        result = ddjj.get_by_name(params.get("nombre", ""))
    else:
        result = ddjj.search(params.get("query", params.get("nombre", "")))

    return [result] if result.records else []


async def _execute_ckan_step(
    ckan: ICKANSearchConnector, step: PlanStep
) -> list[DataResult]:
    params = step.params
    query = params.get("query", step.description)
    portal_id = params.get("portalId")
    rows = params.get("rows", 10)

    # Direct datastore search if resourceId is provided (camelCase or snake_case)
    resource_id = params.get("resourceId") or params.get("resource_id")
    if resource_id and portal_id:
        q = params.get("q")
        records = await ckan.query_datastore(portal_id, resource_id, q=q)
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

    return await ckan.search_datasets(query, portal_id=portal_id, rows=rows)


async def _execute_sesiones_step(
    sesiones: ISesionesConnector, step: PlanStep
) -> list[DataResult]:
    params = step.params
    result = await sesiones.search(
        query=params.get("query", step.description),
        periodo=params.get("periodo"),
        orador=params.get("orador"),
        limit=params.get("limit", 15),
    )
    return [result] if result else []


# ── Data context builder ──────────────────────────────────────


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
        # Filter out non-dict records defensively
        valid_records = [r for r in result.records if isinstance(r, dict)]
        if not valid_records and result.records:
            continue  # Skip results with unparseable records

        # Check if metadata-only
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
            # Real data
            columns = list(valid_records[0].keys()) if valid_records else []
            total_rows = len(valid_records)

            # For large datasets, send first + last rows
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


# ── Chart builders ────────────────────────────────────────────


def _build_deterministic_charts(results: list[DataResult]) -> list[dict]:
    """Build charts deterministically from real collected data."""
    charts: list[dict] = []

    for result in results:
        if not result.records or len(result.records) < 2:
            continue

        first = result.records[0]
        if not isinstance(first, dict):
            continue
        if first.get("_type") == "resource_metadata":
            continue

        keys = list(first.keys())

        # Find time/date key
        time_key = None
        for k in keys:
            kl = k.lower()
            if k == "fecha" or "date" in kl or kl in ("año", "year", "mes"):
                time_key = k
                break

        if not time_key:
            continue

        # Find numeric value keys
        numeric_keys = [
            k
            for k in keys
            if k != time_key and not k.startswith("_") and isinstance(first.get(k), (int, float))
        ]

        if not numeric_keys:
            continue

        # Filter rows with all-null numeric values
        clean = [
            row
            for row in result.records
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


def _extract_llm_charts(text: str) -> list[dict]:
    """Extract chart data from <!--CHART:{}-->  comments in LLM response."""
    charts: list[dict] = []
    for match in re.finditer(r"<!--CHART:(.*?)-->", text, re.DOTALL):
        try:
            chart = json.loads(match.group(1))
            if chart.get("type") and chart.get("data") and chart.get("xKey") and chart.get("yKeys"):
                charts.append(chart)
        except (json.JSONDecodeError, KeyError):
            pass
    return charts


# ── Main endpoint ─────────────────────────────────────────────


@router.post("/smart", response_model=SmartQueryResponse)
@limiter.limit("15/minute")
@inject
async def smart_query(
    request: Request,
    body: SmartQueryRequest,
    llm: FromDishka[ILLMProvider],
    embedding: FromDishka[IEmbeddingProvider],
    vector_search: FromDishka[IVectorSearch],
    cache: FromDishka[ICacheService],
    series: FromDishka[ISeriesTiempoConnector],
    arg_datos: FromDishka[IArgentinaDatosConnector],
    georef: FromDishka[IGeorefConnector],
    ckan: FromDishka[ICKANSearchConnector],
    sesiones: FromDishka[ISesionesConnector],
    ddjj: FromDishka[DDJJAdapter],
    semantic_cache: FromDishka[SemanticCache],
    session: FromDishka[MainAsyncSession],
) -> dict:
    """
    Smart query endpoint — classifies the query, routes to real-time connectors,
    enriches with pgvector, and analyzes with LLM.
    """
    start_time = time.monotonic()
    metrics = MetricsCollector()

    # 0. Classify — casual/meta messages get instant responses (no LLM)
    casual_answer = _get_casual_response(body.question)
    if casual_answer:
        return {"answer": casual_answer, "sources": [], "chart_data": None, "tokens_used": 0, "casual": True}

    meta_answer = _get_meta_response(body.question)
    if meta_answer:
        return {"answer": meta_answer, "sources": [], "chart_data": None, "tokens_used": 0}

    cache_key = _cache_key(body.question)

    # 1. Check cache — semantic cache first, then Redis hash
    try:
        sem_cached = await semantic_cache.get(body.question)
        if sem_cached:
            metrics.record_cache_hit()
            return {**sem_cached, "cached": True}
    except Exception:
        logger.debug("Semantic cache read failed")

    try:
        cached = await cache.get(cache_key)
        if cached and isinstance(cached, dict):
            metrics.record_cache_hit()
            return {**cached, "cached": True}
        metrics.record_cache_miss()
    except Exception:
        logger.debug("Cache read failed, proceeding without cache")

    # 2. Load memory context for this session
    session_id = body.user_email or body.conversation_id or ""
    memory = await load_memory(cache, session_id)

    # 3. Plan — LLM classifies and generates execution plan
    plan = await generate_plan(llm, body.question, memory_context=build_memory_context_prompt(memory))
    logger.info(
        "Plan for '%s': intent=%s, steps=%d, turn=%d",
        body.question[:60],
        plan.intent,
        len(plan.steps),
        memory.turn_number + 1,
    )

    # 4. Execute — dispatch each step to the appropriate connector (max 5 steps)
    results: list[DataResult] = []
    for step in plan.steps[:5]:
        step_start = time.monotonic()
        connector_name = step.action
        try:
            if step.action == "query_series":
                data = await _execute_series_step(series, step)
            elif step.action == "query_argentina_datos":
                data = await _execute_argentina_datos_step(arg_datos, step)
            elif step.action == "query_georef":
                data = await _execute_georef_step(georef, step)
            elif step.action == "query_ddjj":
                data = _execute_ddjj_step(ddjj, step)
            elif step.action == "search_ckan":
                data = await _execute_ckan_step(ckan, step)
            elif step.action == "query_sesiones":
                data = await _execute_sesiones_step(sesiones, step)
            elif step.action in ("analyze", "compare", "synthesize"):
                continue
            else:
                logger.info("Unknown step action '%s', skipping", step.action)
                continue
            step_ms = round((time.monotonic() - step_start) * 1000, 1)
            metrics.record_connector_call(connector_name, step_ms)
            results.extend(data)
        except Exception:
            step_ms = round((time.monotonic() - step_start) * 1000, 1)
            metrics.record_connector_call(connector_name, step_ms, error=True)
            logger.warning("Step %s failed", step.id, exc_info=True)

    # 5. Complement with pgvector if no results or generic search
    if not results or plan.intent == "busqueda_general":
        try:
            query_embedding = await embedding.embed(body.question)
            vector_results = await vector_search.search_datasets(query_embedding, limit=5)
            for vr in vector_results:
                if vr.score < 0.30:
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
            logger.warning("pgvector enrichment failed", exc_info=True)

    # 6. Analyze with LLM
    data_context = _build_data_context(results)
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    memory_ctx = build_memory_context_prompt(memory)

    analysis_prompt = (
        f'PREGUNTA DEL USUARIO: "{plan.query}"\n'
        f"FECHA ACTUAL: {today}\n"
        f"INTENCIÓN: {plan.intent}\n\n"
        f"DATOS RECOLECTADOS:\n{data_context}\n"
        f"{memory_ctx}\n"
        "Respondé de forma breve y conversacional. Destacá el dato más importante, "
        "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
        "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
    )

    response = await llm.chat(
        messages=[
            LLMMessage(role="system", content=ANALYSIS_SYSTEM_PROMPT),
            LLMMessage(role="user", content=analysis_prompt),
        ],
        temperature=0.7,
        max_tokens=8192,
    )

    # 7. Build charts (deterministic first, LLM fallback)
    det_charts = _build_deterministic_charts(results)
    llm_charts = _extract_llm_charts(response.content)
    charts = det_charts if det_charts else llm_charts

    # Clean markdown (remove chart comments, including multi-line)
    clean_answer = re.sub(r"<!--CHART:.*?-->", "", response.content, flags=re.DOTALL).strip()

    # Build sources
    sources = [
        {
            "name": r.dataset_title,
            "url": r.portal_url,
            "portal": r.portal_name,
            "accessed_at": r.metadata.get("fetched_at", ""),
        }
        for r in results
        if r.records  # Only include sources with actual data
    ]

    tokens_used = response.tokens_used or 0
    metrics.record_tokens_used(tokens_used)

    result = {
        "answer": clean_answer,
        "sources": sources,
        "chart_data": charts if charts else None,
        "tokens_used": tokens_used,
    }

    # 8. Update memory (non-blocking, don't fail request)
    try:
        updated_memory = await update_memory(llm, memory, plan, results, clean_answer)
        await save_memory(cache, session_id, updated_memory)
    except Exception:
        logger.debug("Memory update failed", exc_info=True)

    # 9. Save to conversation history
    duration_ms = int((time.monotonic() - start_time) * 1000)
    try:
        query_id = str(uuid4())
        await session.execute(
            text("""
                INSERT INTO user_queries (id, question, user_id, status, analysis_result, sources_json, tokens_used, duration_ms)
                VALUES (CAST(:id AS uuid), :question, :user_id, 'completed', :result, :sources, :tokens, :duration_ms)
            """),
            {
                "id": query_id,
                "question": body.question,
                "user_id": body.user_email,
                "result": clean_answer,
                "sources": json.dumps(sources, ensure_ascii=False),
                "tokens": response.tokens_used or 0,
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

    # 10. Cache result (non-critical — don't fail the request if cache is down)
    try:
        await cache.set(cache_key, result, ttl_seconds=1800)
    except Exception:
        logger.warning("Failed to cache smart query result", exc_info=True)

    # 11. Save to semantic cache (non-critical)
    try:
        q_embedding = await embedding.embed(body.question)
        await semantic_cache.set(body.question, q_embedding, result)
    except Exception:
        logger.debug("Semantic cache write failed", exc_info=True)

    return result


# ── WebSocket streaming endpoint ──────────────────────────────


def _validate_ws_api_key(ws: WebSocket) -> bool:
    """Check api_key query param against configured BACKEND_API_KEY."""
    import os
    expected = os.getenv("BACKEND_API_KEY", "")
    if not expected:
        return True
    provided = ws.query_params.get("api_key", "")
    return provided == expected


@router.websocket("/ws/smart")
@inject
async def ws_smart_query(
    ws: WebSocket,
    llm: FromDishka[ILLMProvider],
    embedding: FromDishka[IEmbeddingProvider],
    vector_search: FromDishka[IVectorSearch],
    cache: FromDishka[ICacheService],
    series: FromDishka[ISeriesTiempoConnector],
    arg_datos: FromDishka[IArgentinaDatosConnector],
    georef: FromDishka[IGeorefConnector],
    ckan: FromDishka[ICKANSearchConnector],
    sesiones: FromDishka[ISesionesConnector],
    ddjj: FromDishka[DDJJAdapter],
    semantic_cache: FromDishka[SemanticCache],
) -> None:
    if not _validate_ws_api_key(ws):
        await ws.close(code=4401, reason="Invalid or missing API key")
        return
    await ws.accept()

    try:
        raw = await ws.receive_json()
        question = raw.get("question", "")
        conversation_id = raw.get("conversation_id", "")

        if not question:
            await ws.send_json({"type": "error", "message": "question is required"})
            await ws.close()
            return

        # ── Phase 1: Classifying ──
        await ws.send_json({"type": "status", "step": "classifying"})

        # Casual message → instant response (no LLM)
        casual_answer = _get_casual_response(question)
        if casual_answer:
            await ws.send_json({"type": "chunk", "content": casual_answer})
            await ws.send_json({
                "type": "complete",
                "answer": casual_answer,
                "sources": [],
                "chart_data": None,
                "casual": True,
            })
            return

        # Meta message → instant response (no LLM)
        meta_answer = _get_meta_response(question)
        if meta_answer:
            await ws.send_json({"type": "chunk", "content": meta_answer})
            await ws.send_json({
                "type": "complete",
                "answer": meta_answer,
                "sources": [],
                "chart_data": None,
            })
            return

        # ── Cache check (before expensive pipeline) ──
        session_id = conversation_id or ""

        try:
            sem_cached = await semantic_cache.get(question)
            if sem_cached and isinstance(sem_cached, dict):
                await ws.send_json({"type": "status", "step": "cache_hit"})
                answer = sem_cached.get("answer", "")
                await ws.send_json({"type": "chunk", "content": answer})
                await ws.send_json({
                    "type": "complete",
                    "answer": answer,
                    "sources": sem_cached.get("sources", []),
                    "chart_data": sem_cached.get("chart_data"),
                    "cached": True,
                })
                return
        except Exception:
            logger.debug("WS semantic cache read failed")

        try:
            cache_key = _cache_key(question)
            redis_cached = await cache.get(cache_key)
            if redis_cached and isinstance(redis_cached, dict):
                await ws.send_json({"type": "status", "step": "cache_hit"})
                answer = redis_cached.get("answer", "")
                await ws.send_json({"type": "chunk", "content": answer})
                await ws.send_json({
                    "type": "complete",
                    "answer": answer,
                    "sources": redis_cached.get("sources", []),
                    "chart_data": redis_cached.get("chart_data"),
                    "cached": True,
                })
                return
        except Exception:
            logger.debug("WS Redis cache read failed")

        # ── Phase 2: Planning ──
        await ws.send_json({"type": "status", "step": "planning"})

        memory = await load_memory(cache, session_id)
        plan = await generate_plan(llm, question, memory_context=build_memory_context_prompt(memory))

        await ws.send_json({
            "type": "status",
            "step": "planned",
            "intent": plan.intent,
            "steps_count": len(plan.steps),
        })

        # ── Phase 3: Searching (execute connectors) ──
        await ws.send_json({"type": "status", "step": "searching"})

        results: list[DataResult] = []
        connectors_map = {
            "query_series": ("series_tiempo", lambda s: _execute_series_step(series, s)),
            "query_argentina_datos": ("argentina_datos", lambda s: _execute_argentina_datos_step(arg_datos, s)),
            "query_georef": ("georef", lambda s: _execute_georef_step(georef, s)),
            "search_ckan": ("ckan", lambda s: _execute_ckan_step(ckan, s)),
            "query_sesiones": ("sesiones", lambda s: _execute_sesiones_step(sesiones, s)),
        }

        for step in plan.steps[:5]:
            if step.action in ("analyze", "compare", "synthesize"):
                continue

            if step.action == "query_ddjj":
                await ws.send_json({"type": "status", "step": "searching", "connector": "ddjj"})
                try:
                    data = _execute_ddjj_step(ddjj, step)
                    results.extend(data)
                except Exception:
                    logger.warning("WS step ddjj failed", exc_info=True)
                continue

            entry = connectors_map.get(step.action)
            if not entry:
                continue

            connector_name, executor = entry
            await ws.send_json({"type": "status", "step": "searching", "connector": connector_name})
            try:
                data = await executor(step)
                results.extend(data)
            except Exception:
                logger.warning("WS step %s failed", step.action, exc_info=True)

        # Pgvector complement
        if not results or plan.intent == "busqueda_general":
            try:
                query_embedding = await embedding.embed(question)
                vector_results = await vector_search.search_datasets(query_embedding, limit=5)
                for vr in vector_results:
                    if vr.score < 0.30:
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
                logger.debug("pgvector complement failed", exc_info=True)

        # ── Phase 4: Generating (LLM analysis) ──
        await ws.send_json({"type": "status", "step": "generating"})

        data_context = _build_data_context(results)
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        memory_ctx = build_memory_context_prompt(memory)

        analysis_prompt = (
            f'PREGUNTA DEL USUARIO: "{plan.query}"\n'
            f"FECHA ACTUAL: {today}\n"
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS:\n{data_context}\n"
            f"{memory_ctx}\n"
            "Respondé de forma breve y conversacional. Destacá el dato más importante, "
            "dá contexto mínimo, y sugerí preguntas de seguimiento para profundizar. "
            "Si los datos permiten un gráfico claro, incluilo con <!--CHART:{}-->."
        )

        full_text = ""
        async for chunk in llm.chat_stream(
            messages=[
                LLMMessage(role="system", content=ANALYSIS_SYSTEM_PROMPT),
                LLMMessage(role="user", content=analysis_prompt),
            ],
            temperature=0.7,
            max_tokens=8192,
        ):
            full_text += chunk
            await ws.send_json({"type": "chunk", "content": chunk})

        # ── Phase 5: Complete ──
        det_charts = _build_deterministic_charts(results)
        llm_charts = _extract_llm_charts(full_text)
        charts = det_charts if det_charts else llm_charts
        clean_answer = re.sub(r"<!--CHART:.*?-->", "", full_text, flags=re.DOTALL).strip()

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

        await ws.send_json({
            "type": "complete",
            "answer": clean_answer,
            "sources": sources,
            "chart_data": charts if charts else None,
        })

    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected")
    except Exception:
        logger.exception("WebSocket error")
        try:
            await ws.send_json({"type": "error", "message": "Internal error"})
        except Exception:
            pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass
