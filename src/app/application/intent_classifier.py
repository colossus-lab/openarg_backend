"""LLM-based intent classifier for smart query routing.

Replaces fragile regex patterns with a single Gemini Flash call that
classifies the user query into one or more known intents and extracts
structured parameters. Supports multi-intent queries (e.g. "comparame
los asesores de X con su declaración jurada") and improved clarification
with concrete options. Regex patterns remain as fallback.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from app.domain.entities.connectors.data_result import (
    ExecutionPlan,
    PlanStep,
)
from app.domain.ports.llm.llm_provider import (
    ILLMProvider,
    LLMMessage,
)

logger = logging.getLogger(__name__)


@dataclass
class IntentMatch:
    """A single intent detected in the user query."""

    intent: str
    action: str
    params: dict


@dataclass
class ClassifiedIntent:
    """Full classification result — may contain 1+ intents."""

    intents: list[IntentMatch]
    confidence: float
    reasoning: str
    # Only set when the primary intent is clarification_needed
    clarification_message: str = ""
    clarification_options: list[str] = field(
        default_factory=list,
    )

    @property
    def is_clarification(self) -> bool:
        return (
            len(self.intents) == 1
            and self.intents[0].intent == "clarification_needed"
        )

    @property
    def primary_intent(self) -> str:
        return self.intents[0].intent if self.intents else ""


INTENT_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "intents": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string"},
                    "action": {"type": "string"},
                    "params": {"type": "object"},
                },
                "required": ["intent", "action", "params"],
            },
        },
        "confidence": {"type": "number"},
        "reasoning": {"type": "string"},
        "clarification_message": {"type": "string"},
        "clarification_options": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["intents", "confidence", "reasoning"],
}

# ── System prompt ────────────────────────────────────────────

_SYSTEM_PROMPT_PARTS: list[str] = [
    (
        "Sos un clasificador de intenciones para OpenArg, "
        "un asistente de datos abiertos de Argentina.\n"
        "Clasificá la pregunta del usuario en UNO O MÁS "
        "intents y extraé los parámetros necesarios."
    ),
    # -- intent table --
    "\n## Intents disponibles\n",
    "| Intent | Action | Descripción | Parámetros |",
    "|--------|--------|-------------|------------|",
    (
        "| ddjj_ranking | query_ddjj | Ranking diputados "
        "patrimonio | action='ranking', sortBy='patrimonio', "
        "order('asc'/'desc'), top(int), position(int, opc) |"
    ),
    (
        "| ddjj_persona | query_ddjj | DDJJ persona "
        "| action='detail', nombre(str) |"
    ),
    (
        "| ddjj_busqueda | query_ddjj | Búsqueda DDJJ "
        "| action='search', query(str) |"
    ),
    (
        "| staff_legislator | query_staff | Personal legislador "
        "| action='get_by_legislator', name(str) |"
    ),
    (
        "| staff_count | query_staff | Cantidad asesores "
        "| action='count', name(str) |"
    ),
    (
        "| staff_changes | query_staff | Altas/bajas personal "
        "| action='changes', name(str, opc) |"
    ),
    (
        "| staff_commission | query_staff | Personal comisión "
        "| action='get_by_legislator', name='comision de X' |"
    ),
    (
        "| staff_busqueda | query_staff | Búsqueda personal "
        "| action='search', query(str) |"
    ),
    (
        "| boletin_designaciones | query_staff "
        "| Designaciones de personal en Boletín Oficial "
        "(planta transitoria, asesores de bloque) "
        "| action='search', query(str) |"
    ),
    (
        "| bcra_reservas | query_bcra | Reservas BCRA "
        "| tipo='variables' |"
    ),
    (
        "| bcra_tasa | query_bcra | Tasa política monetaria "
        "| tipo='variables' |"
    ),
    (
        "| bcra_cotizaciones | query_bcra "
        "| Cotizaciones oficiales | tipo='cotizaciones' |"
    ),
    (
        "| bcra_monetaria | query_bcra | Base monetaria "
        "| tipo='variables' |"
    ),
    (
        "| argdatos_dolar | query_argentina_datos | Dólar "
        "| type='dolar', casa('blue'/'bolsa'/"
        "'contadoconliqui'/'cripto'/'tarjeta', opc) |"
    ),
    (
        "| argdatos_riesgo_pais | query_argentina_datos "
        "| Riesgo país | type='riesgo_pais', ultimo=true |"
    ),
    (
        "| series_tiempo | query_series | Indicadores "
        "económicos (inflación, PBI, EMAE, desempleo, etc.) "
        "| query(str) |"
    ),
    (
        "| sesiones_congreso | query_sesiones "
        "| Transcripciones congreso | query(str) |"
    ),
    (
        "| busqueda_general | search_ckan "
        "| Búsqueda datos abiertos | query(str) |"
    ),
    (
        "| clarification_needed | none | Query ambigua "
        "| (ver sección Clarificación) |"
    ),
    # -- rules --
    "\n## Reglas\n",
    "1. Clasificá en el intent más específico posible.",
    (
        "2. 'dólar blue/mep/ccl/cripto/tarjeta', "
        "'a cuánto está el dólar', 'precio del dólar' "
        "→ argdatos_dolar."
    ),
    (
        "3. 'dólar oficial', 'cotización oficial', "
        "'tipo de cambio oficial' → bcra_cotizaciones."
    ),
    (
        "4. Inflación, IPC, PBI, EMAE, desempleo, "
        "exportaciones, importaciones, balanza comercial, "
        "deuda pública, salarios → series_tiempo."
    ),
    (
        "5. Reservas BCRA → bcra_reservas. "
        "Tasa política monetaria → bcra_tasa. "
        "Base monetaria → bcra_monetaria."
    ),
    (
        "6. Patrimonio, declaraciones juradas, DDJJ, "
        "riqueza diputados → algún intent ddjj_*."
    ),
    (
        "7. Personal, asesores, empleados de legislador "
        "o comisión → algún intent staff_*."
    ),
    (
        "8. Sesiones congreso, debates, transcripciones, "
        "discursos → sesiones_congreso."
    ),
    (
        "8b. Boletín oficial, designaciones, resoluciones "
        "de personal, planta transitoria, nombramientos "
        "de diputados → boletin_designaciones. "
        "Solo cubre designaciones de personal de HCDN "
        "(Cámara de Diputados), no del Senado ni "
        "otros organismos."
    ),
    (
        "9. confidence: >0.8 alta, 0.5-0.8 media, "
        "<0.5 baja."
    ),
    (
        "10. Rankings DDJJ: 'top N' → top=N. "
        "'más pobres'/'menor patrimonio' → order='asc'. "
        "Default: order='desc', top=10."
    ),
    (
        "11. Staff: 'cuántos asesores tiene X' → staff_count. "
        "'asesores de X' → staff_legislator. "
        "'comisión de X' → staff_commission."
    ),
    (
        "12. DDJJ persona: extraé el nombre limpio "
        "sin títulos (diputado/a, senador/a, etc.)"
    ),
    # -- multi-intent --
    "\n## Multi-intent\n",
    (
        "Si la pregunta toca MÁS DE UN recurso, "
        "devolvé múltiples objetos en el array `intents`. "
        "Ejemplos:"
    ),
    (
        '- "Comparame los asesores de Milei con su '
        'declaración jurada" → staff_legislator + '
        "ddjj_persona (ambos con el nombre extraído)."
    ),
    (
        '- "Dame el dólar blue y la inflación" '
        "→ argdatos_dolar + series_tiempo."
    ),
    (
        '- "Cuántos asesores tiene Kicillof y cuánto '
        'declaró en su DDJJ?" → staff_count + ddjj_persona.'
    ),
    (
        "Máximo 3 intents por query. Si necesitás más, "
        "priorizá los más relevantes."
    ),
    # -- clarification --
    "\n## Clarificación\n",
    (
        "Si la query es ambigua y podría ir a más de "
        "un conector sin que sea claro que el usuario "
        "quiere ambos, usá clarification_needed."
    ),
    "Cuando uses clarification_needed:",
    (
        '- `clarification_message`: mensaje amigable '
        "explicando la ambigüedad."
    ),
    (
        '- `clarification_options`: lista de 2-4 opciones '
        "concretas que el usuario puede elegir. "
        "Cada opción es un string corto describiendo "
        "el camino posible."
    ),
    "Ejemplo:",
    (
        '  "datos de Milei" → clarification_needed con '
        'message="Tu pregunta podría referirse a '
        "varios recursos. ¿Qué te interesa?\", "
        "options=["
    ),
    '    "Declaración jurada (patrimonio)",',
    '    "Personal y asesores",',
    '    "Discursos en sesiones del Congreso",',
    '    "Designaciones en el Boletín Oficial"',
    "  ]",
    # -- output format --
    "\n## Formato de respuesta\n",
    "Respondé ÚNICAMENTE con un JSON válido:",
    (
        "- intents: array de objetos, cada uno con "
        "intent(str), action(str), params(object)"
    ),
    "- confidence: number 0-1",
    "- reasoning: string breve",
    (
        "- clarification_message: string (solo si "
        "intent=clarification_needed)"
    ),
    (
        "- clarification_options: array de strings "
        "(solo si intent=clarification_needed)"
    ),
]

_SYSTEM_PROMPT_BASE = "\n".join(_SYSTEM_PROMPT_PARTS)


def _build_system_prompt(
    available_connectors: list[str] | None = None,
) -> str:
    prompt = _SYSTEM_PROMPT_BASE

    if available_connectors:
        unavailable: list[str] = []
        connector_map = {
            "staff": [
                "staff_legislator", "staff_count",
                "staff_changes", "staff_commission",
                "staff_busqueda",
                "boletin_designaciones",
            ],
            "bcra": [
                "bcra_reservas", "bcra_tasa",
                "bcra_cotizaciones", "bcra_monetaria",
            ],
        }
        for connector, intents in connector_map.items():
            if connector not in available_connectors:
                unavailable.extend(intents)
        if unavailable:
            joined = ", ".join(unavailable)
            prompt += (
                "\n\nNOTA: Los siguientes intents NO están "
                f"disponibles: {joined}. No los uses.\n"
            )

    return prompt


async def classify_intent(
    llm: ILLMProvider,
    question: str,
    available_connectors: list[str] | None = None,
) -> ClassifiedIntent | None:
    """Classify a user question into one or more intents.

    Returns None if classification fails (timeout, parse error).
    """
    user_question = question
    marker = "NUEVA PREGUNTA DEL USUARIO"
    marker_idx = question.find(marker)
    if marker_idx != -1:
        after = question[marker_idx + len(marker):]
        user_question = after.strip().lstrip(":").strip()

    system_prompt = _build_system_prompt(available_connectors)
    messages = [
        LLMMessage(role="system", content=system_prompt),
        LLMMessage(role="user", content=user_question),
    ]

    response = await llm.chat_json(
        messages=messages,
        json_schema=INTENT_JSON_SCHEMA,
        temperature=0.0,
        max_tokens=512,
    )

    try:
        data = json.loads(response.content)
    except json.JSONDecodeError:
        logger.warning(
            "Intent classifier returned invalid JSON: %s",
            response.content[:200],
        )
        return None

    raw_intents = data.get("intents", [])
    if not raw_intents:
        logger.warning("Intent classifier returned empty intents")
        return None

    intent_matches: list[IntentMatch] = []
    for item in raw_intents:
        intent = item.get("intent", "")
        action = item.get("action", "")
        if not intent:
            continue
        intent_matches.append(IntentMatch(
            intent=intent,
            action=action,
            params=item.get("params", {}),
        ))

    if not intent_matches:
        logger.warning("Intent classifier: no valid intents parsed")
        return None

    confidence = float(data.get("confidence", 0.0))
    reasoning = data.get("reasoning", "")
    clarification_msg = data.get("clarification_message", "")
    clarification_opts = data.get("clarification_options", [])

    result = ClassifiedIntent(
        intents=intent_matches,
        confidence=confidence,
        reasoning=reasoning,
        clarification_message=clarification_msg,
        clarification_options=clarification_opts,
    )

    intent_names = [m.intent for m in intent_matches]
    logger.info(
        "Intent classified: %s (confidence=%.2f, reasoning=%s)",
        intent_names,
        confidence,
        reasoning,
    )

    return result


def intent_to_plan(
    question: str,
    classified: ClassifiedIntent,
) -> ExecutionPlan:
    """Convert a ClassifiedIntent into an ExecutionPlan.

    Supports multi-intent: each IntentMatch becomes a PlanStep.
    """
    if classified.is_clarification:
        msg = "No se pudo generar plan: clarification_needed."
        raise ValueError(msg)

    steps: list[PlanStep] = []
    for match in classified.intents:
        if match.intent == "clarification_needed":
            continue
        steps.append(PlanStep(
            id=match.intent,
            action=match.action,
            description=classified.reasoning or match.intent,
            params=match.params,
        ))

    primary = classified.primary_intent
    return ExecutionPlan(
        query=question,
        intent=primary,
        steps=steps,
    )
