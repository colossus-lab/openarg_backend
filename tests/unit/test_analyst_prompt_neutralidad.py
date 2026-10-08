"""El prompt del analista del motor legacy no pide causas ni evaluaciones.

El legacy (``ANSWERS_ENGINE=legacy``) sigue vivo como rollback. Su
``analyst.txt`` pedía una línea de "perspectiva de política pública" en toda
respuesta, con un ejemplo causal ("la caída sostenida de este indicador
sugiere que las medidas implementadas no alcanzaron la eficacia esperada") y,
para las DDJJ, "vincular con transparencia y accountability". Es la frase
causal que vio la auditoría del 04-oct (ítem 1.3). La regla que la reemplaza:
describir qué pasó con los datos; no atribuir causas ni evaluar políticas ni
personas; contexto causal sólo si lo dice la fuente oficial, citado.

Se mide con el cargador real (``load_prompt``) y con el mensaje de sistema que
``analyst_node`` le manda de verdad al modelo, no con el archivo en reposo.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.nodes.analyst import analyst_node
from app.prompts import _load_prompt_template, load_prompt

# Lo que pedía el prompt viejo: si vuelve cualquiera de estas, vuelve el
# pedido de interpretación causal o de evaluación.
_PEDIDOS_CAUSALES = (
    "perspectiva de política pública",
    "sugiere que",
    "Esto sugiere",
    "accountability",
    "vincular con transparencia",
    "analizar impacto",
    "Eficacia",
    "Eficiencia",
    "CRITERIOS DE EVALUACIÓN",
    "contexto/interpretación",
)


@pytest.fixture(autouse=True)
def _sin_cache_de_prompts() -> Any:
    _load_prompt_template.cache_clear()
    yield
    _load_prompt_template.cache_clear()


def _seccion_neutralidad(prompt: str) -> str:
    inicio = prompt.index("NEUTRALIDAD")
    fin = prompt.find("\n\n", inicio)
    return prompt[inicio : fin if fin != -1 else len(prompt)]


@pytest.mark.parametrize("pedido", _PEDIDOS_CAUSALES)
def test_el_prompt_no_pide_interpretaciones_causales(pedido: str) -> None:
    assert pedido.lower() not in load_prompt("analyst").lower()


def test_el_prompt_tiene_la_regla_de_neutralidad() -> None:
    regla = _seccion_neutralidad(load_prompt("analyst")).lower()
    # Describir, no explicar.
    assert "describí qué muestran los datos" in regla
    # Ni causas, ni políticas, ni personas.
    assert "no atribuyas causas" in regla
    assert "no evalúes políticas" in regla
    assert "no valores a personas" in regla
    assert "ddjj" in regla
    # Contexto causal sólo de la fuente oficial, atribuido.
    assert "sólo si lo dice la fuente oficial" in regla
    # Las preguntas de seguimiento tampoco empujan a la causalidad (la
    # respuesta de reservas de la auditoría terminaba con "¿Qué factores
    # explican el salto...?").
    assert "preguntas de seguimiento" in regla


class _ModeloQueAnota:
    """Un LLM que guarda los mensajes que recibe."""

    def __init__(self) -> None:
        self.messages: list[Any] = []

    async def chat_stream(self, *, messages: list[Any], **_kwargs: Any):
        self.messages = messages
        yield "Las reservas eran de USD 46.092 millones al 30/09/2026."


@pytest.mark.asyncio
async def test_analyst_node_le_manda_al_modelo_la_regla_de_neutralidad(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    modelo = _ModeloQueAnota()
    monkeypatch.setattr(
        nodes_pkg, "get_deps", lambda: SimpleNamespace(llm=modelo, sandbox=None), raising=False
    )
    monkeypatch.setattr(
        "app.application.pipeline.nodes.analyst.get_stream_writer",
        lambda: lambda _event: None,
    )
    resultado = SimpleNamespace(
        records=[{"fecha": "2026-09-30", "valor": 46092.0}],
        source="bcra",
        metadata={},
        dataset_title="Reservas internacionales del BCRA (saldo diario)",
        portal_name="Banco Central de la República Argentina",
        portal_url="https://www.bcra.gob.ar/principales-variables/",
        format="time_series",
    )
    state = {
        "question": "¿Cuántas reservas tiene el BCRA?",
        "plan": SimpleNamespace(intent="reservas"),
        "data_results": [resultado],
        "memory_ctx_analyst": "",
        "step_warnings": [],
        "replan_count": 0,
    }

    await analyst_node(state)  # type: ignore[arg-type]

    sistema = modelo.messages[0]
    assert sistema.role == "system"
    assert "NEUTRALIDAD" in sistema.content
    assert "no atribuyas causas" in sistema.content.lower()
    for pedido in _PEDIDOS_CAUSALES:
        assert pedido.lower() not in sistema.content.lower(), pedido
