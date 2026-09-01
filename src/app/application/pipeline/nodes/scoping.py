"""Preguntar antes de buscar, cuando el usuario pidió profundidad.

Uno de los objetivos centrales de OpenArg es encontrar evidencia en datos
abiertos, y con ~27.000 recursos ubicar un dato puntual es difícil justamente
porque hay mucho. Acotar antes de buscar reduce el espacio: saber que la
pregunta es sobre 2024 y sobre CABA descarta la enorme mayoría del catálogo
antes de que el planner mire nada.

**Reusa la mecánica que ya existe, no una nueva.** El planner ya devuelve un
plan con `intent="clarification"` cuando detecta ambigüedad, `clarify_reply_node`
lo cierra como turno, y el frontend pinta las opciones como chips clickeables
(`page.tsx:500`). Lo único que cambia acá es el disparador: hoy el scoping
ocurre sólo si la pregunta es ambigua; en modo profundo es el primer paso
siempre.

**Una pregunta por turno, no un formulario.** El evento `clarification` que ya
viaja lleva una lista plana de opciones y el chip manda su texto como mensaje
nuevo — o sea que un formulario de 2-4 preguntas no es reuso, es un esquema
nuevo y UI nueva. Se eligió la versión que funciona con lo que hay: una ronda,
la que más recorta. Si más adelante se quiere el formulario, este nodo es el
lugar donde entra.

**Corre con el modelo rápido.** Acotar es una tarea corta y barata; el modelo
capaz se reserva para el plan y la síntesis, que es donde el razonamiento pesa.
"""

from __future__ import annotations

import json
import logging

from app.application.pipeline import nodes as nodes_pkg
from app.application.pipeline.state import OpenArgState
from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep

logger = logging.getLogger(__name__)

_SYSTEM = """Sos un asistente que acota búsquedas sobre datos públicos argentinos.

Te dan una pregunta. Devolvé UNA pregunta corta que sirva para reducir el
espacio de búsqueda, con 3 a 5 opciones clickeables.

Elegí el eje que MÁS recorte para esa pregunta en particular:
- período (año, rango, "lo más reciente")
- jurisdicción (nación, una provincia, CABA, un municipio)
- fuente o tipo de dato
- nivel de detalle (un número puntual vs un panorama)

Reglas:
- Si la pregunta YA es específica en todos los ejes, devolvé
  {"needs_scoping": false} y nada más.
- Las opciones son textos cortos que el usuario va a clickear como respuesta.
- Incluí siempre una opción de escape tipo "Buscar en todo" o "Sin filtrar".
- Marcá `policy_relevant` en true sólo si la pregunta es sobre una política
  pública concreta y su evaluación, no sobre un dato suelto.

Respondé SOLO JSON:
{"needs_scoping": true, "question": "...", "options": ["...", "..."], "policy_relevant": false}"""


async def scoping_node(state: OpenArgState) -> dict:
    """Emitir la ronda de acotamiento, o dejar pasar.

    Devuelve siempre `scoping_done=True`: haya preguntado o no, este turno ya
    gastó su oportunidad. Sin eso, una pregunta que el modelo considera
    específica volvería a entrar acá en cada turno de la conversación.
    """
    if state.get("mode") != "deep" or state.get("scoping_done"):
        return {"scoping_done": True}

    # `scoping_done` viaja por el checkpointer, y el grafo se compila también
    # SIN checkpointer (`smart_query_v2_router._get_or_compile_graph` cachea las
    # dos variantes). En ese camino la bandera no sobrevive al turno, así que
    # clickear un chip volvería a caer acá y a repreguntar: un bucle de
    # repreguntas del que el usuario no puede salir sin apagar el modo.
    #
    # El historial es la señal que no depende del checkpointer — sale de la DB
    # en `load_memory`, que corre antes que este nodo. Misma lectura que usa
    # `generate_plan` para saltear su clasificador de ambigüedad.
    if (state.get("planner_ctx") or "").strip():
        return {"scoping_done": True}

    question = state.get("question", "")
    deps = nodes_pkg.get_deps()

    from app.domain.ports.llm.llm_provider import LLMMessage

    try:
        resp = await deps.llm.chat_json(  # el modelo rápido: acotar es corto y barato
            messages=[
                LLMMessage(role="system", content=_SYSTEM),
                LLMMessage(role="user", content=question),
            ],
            json_schema={
                "type": "object",
                "properties": {
                    "needs_scoping": {"type": "boolean"},
                    "question": {"type": "string"},
                    "options": {"type": "array", "items": {"type": "string"}},
                    "policy_relevant": {"type": "boolean"},
                },
                "required": ["needs_scoping"],
            },
            temperature=0.0,
            max_tokens=512,
        )
        raw = (resp.content or "").strip()
        i, j = raw.find("{"), raw.rfind("}")
        data = json.loads(raw[i : j + 1]) if i >= 0 and j > i else {}
    except Exception:
        # Acotar es una mejora, no un requisito. Si el modelo no contesta, la
        # búsqueda profunda corre igual — degradada a lo que hace hoy, que no
        # es poco.
        logger.warning("scoping: el modelo no contestó, sigo sin acotar", exc_info=True)
        return {"scoping_done": True}

    opciones = [str(o) for o in (data.get("options") or []) if str(o).strip()]
    if not data.get("needs_scoping") or len(opciones) < 2:
        # La pregunta ya venía acotada. Preguntar igual sería la fricción que
        # hace que un modo profundo se apague después de usarlo dos veces.
        return {
            "scoping_done": True,
            "policy_relevant": bool(data.get("policy_relevant", False)),
        }

    texto = str(data.get("question") or "¿Sobre qué querés que busque?")
    plan = ExecutionPlan(
        query=question,
        intent="clarification",
        steps=[
            PlanStep(
                id="scoping",
                action="clarification",
                description=texto,
                params={"question": texto, "options": opciones},
            )
        ],
    )
    logger.info("scoping: acotando con %d opción(es)", len(opciones))
    return {
        "scoping_done": True,
        "policy_relevant": bool(data.get("policy_relevant", False)),
        "plan": plan,
        "plan_intent": plan.intent,
    }
