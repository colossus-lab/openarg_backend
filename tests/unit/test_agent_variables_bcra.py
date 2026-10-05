"""La herramienta ``variables_bcra`` del agente, con el adaptador real contra
un doble de la API del BCRA.

El caso que la motiva (auditoría del 04-oct): "¿Cuántas reservas
internacionales tiene el BCRA actualmente?" se contestaba con Series de Tiempo
("abril de 2023: USD 35.001 M" por la truncación, o el promedio de agosto), y
el dólar oficial sólo con DolarApi, que es la pizarra del Banco Nación vía un
agregador. El BCRA publica 46.092 al 30-sep, y el minorista (1.543,18) y la
A3500 (1.523,09) al 02-oct.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers import agent_engine as agent_module
from app.application.answers.agent_engine import AgentEngine
from app.application.answers.engine import CompleteEvent, EngineRequest, StatusEvent
from app.application.answers.tools import bcra as bcra_tool
from app.application.answers.tools import build_tools
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.bcra import VARIABLES, VariablesBCRA
from app.application.answers.tools.conectores import Cotizaciones
from app.application.pipeline.chart_builder import build_deterministic_charts
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.ports.llm.agent_llm import AgentTurn, AgentUsage, TextDelta, ToolCall
from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
from app.infrastructure.resilience import retry as retry_module
from app.infrastructure.resilience.circuit_breaker import CircuitState, get_circuit_breaker
from tests.unit.bcra_fake_api import HOY, FakeBCRA

_HOY_AR_REAL = bcra_tool.hoy_ar


@pytest.fixture(autouse=True)
def _hoy_y_circuito(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bcra_tool, "hoy_ar", lambda: HOY)
    monkeypatch.setattr(agent_module, "_record_tokens", lambda *a: None)
    monkeypatch.setattr(retry_module, "_backoff_delay", lambda *a: 0.0)
    for name in ("bcra_api", "bcra_api_catalogo"):
        breaker = get_circuit_breaker(name)
        breaker.state = CircuitState.CLOSED
        breaker.failure_count = 0


def _ctx(fake: FakeBCRA) -> ToolContext:
    deps = MagicMock()
    deps.bcra = BCRAAdapter(client=fake.client())
    return ToolContext(deps=deps, req=EngineRequest(question="q", user_id="u"))


async def _run(fake: FakeBCRA, **args: Any):
    return await VariablesBCRA().run(args, _ctx(fake))


# ── la herramienta ─────────────────────────────────────────


async def test_reservas_hoy_da_el_dato_del_bcra_con_fecha() -> None:
    out = await _run(FakeBCRA(), variables=["reservas"])

    payload = json.loads(out.content)["variables"][0]
    assert payload["ultimo_dato"] == {"fecha": "2026-09-30", "valor": 46092.0}
    assert payload["unidades"] == "millones de dólares"
    assert payload["fuente"].startswith("Banco Central")
    assert len(payload["filas"]) == 10
    assert payload["filas"][-1]["fecha"] == "2026-09-30"
    # La evidencia: una sola fuente, del BCRA, sin "Cotizaciones".
    assert len(out.results) == 1
    result = out.results[0]
    assert result.source == "bcra"
    assert "Reservas internacionales" in result.dataset_title
    assert "Cotizaciones" not in result.dataset_title
    meta = result.metadata
    assert meta["oficial"] is True and meta["frecuencia"] == "diaria"
    assert meta["ultima_observacion"] == "2026-09-30"
    assert meta["fecha_fin_fuente"] == "2026-09-30"
    assert out.summary == (
        "Leyó «Reservas internacionales del BCRA (saldo diario)» del BCRA (último dato: 30/09/2026)"
    )


async def test_dolar_oficial_son_dos_fuentes_del_bcra_con_fecha() -> None:
    out = await _run(FakeBCRA(), variables=["dolar_minorista", "dolar_mayorista"])

    payloads = {p["variable"]: p for p in json.loads(out.content)["variables"]}
    assert payloads["dolar_minorista"]["ultimo_dato"] == {"fecha": "2026-10-02", "valor": 1543.18}
    assert payloads["dolar_mayorista"]["ultimo_dato"] == {
        "fecha": "2026-10-02",
        "valor": 1523.0868,
    }
    titles = [r.dataset_title for r in out.results]
    assert titles == [VARIABLES["dolar_minorista"].titulo, VARIABLES["dolar_mayorista"].titulo]
    assert "Comunicación A 3500" in titles[1]
    assert all(r.metadata["oficial"] for r in out.results)
    assert out.summary == "Leyó 2 variables del BCRA (último dato: 02/10/2026)"


async def test_los_ids_son_los_del_catalogo_v4() -> None:
    fake = FakeBCRA()
    await _run(fake, variables=["reservas", "dolar_minorista", "dolar_mayorista", "badlar"])
    pedidos = {p for p in fake.paths() if "/monetarias/" in p}
    assert pedidos == {
        "/estadisticas/v4.0/monetarias/1",
        "/estadisticas/v4.0/monetarias/4",
        "/estadisticas/v4.0/monetarias/5",
        "/estadisticas/v4.0/monetarias/7",
    }


def test_la_inflacion_del_bcra_queda_afuera() -> None:
    """27 y 28 son copia del IPC del INDEC: la fuente es el INDEC."""
    assert {v.id for v in VARIABLES.values()}.isdisjoint({27, 28})
    assert {VARIABLES[k].id for k in ("reservas", "dolar_minorista", "dolar_mayorista")} == {
        1,
        4,
        5,
    }
    assert VARIABLES["badlar"].id == 7 and VARIABLES["base_monetaria"].id == 15


async def test_la_uva_de_hoy_no_es_la_publicada_por_adelantado() -> None:
    out = await _run(FakeBCRA(), variables=["uva"])
    payload = json.loads(out.content)["variables"][0]
    assert payload["ultimo_dato"]["fecha"] == HOY.isoformat()
    assert "adelantado" in payload["nota"]
    assert "15/10/2026" in payload["nota"]
    assert "04/10/2026" in (out.summary or "")


async def test_lo_publicado_por_adelantado_no_entra_en_la_evidencia() -> None:
    """El gráfico de la UVA llegaba al 15-oct y ultima_observacion decía 15-oct
    un día 05-oct: el DataResult es evidencia, gráfico y aviso de atraso."""
    out = await _run(FakeBCRA(), variables=["uva"])
    result = out.results[0]
    assert result.records[-1]["fecha"] == HOY.isoformat()
    meta = result.metadata
    assert meta["ultima_observacion"] == HOY.isoformat()
    assert meta["fecha_fin_fuente"] == HOY.isoformat()
    assert meta["total_records"] == len(result.records)
    assert meta["publicado_hasta"] == "2026-10-15"
    adelantados = meta["publicados_por_adelantado"]
    assert adelantados[0]["fecha"] == "2026-10-05" and adelantados[-1]["fecha"] == "2026-10-15"
    # El modelo igual los ve, aparte.
    payload = json.loads(out.content)["variables"][0]
    assert payload["publicados_por_adelantado"][-1]["fecha"] == "2026-10-15"
    assert all(f["fecha"] <= HOY.isoformat() for f in payload["filas"])
    charts = json.dumps(build_deterministic_charts(out.results))
    assert "2026-10-04" in charts and "2026-10-15" not in charts


async def test_cinco_variables_con_adelantados_entran_sin_cortar() -> None:
    out = await _run(
        FakeBCRA(), variables=["uva", "cer", "icl", "reservas", "dolar_minorista"], ultimos=40
    )
    assert "cortado" not in out.content
    payloads = {p["variable"]: p for p in json.loads(out.content)["variables"]}
    assert set(payloads) == {"uva", "cer", "icl", "reservas", "dolar_minorista"}
    cer = payloads["cer"]["publicados_por_adelantado"]
    assert len(cer) == 10 and cer[-1]["fecha"] == "2026-11-03"


async def test_una_tasa_es_porcentaje_aunque_el_catalogo_no_responda() -> None:
    """La marca `unidad` del contrato salía sólo del catálogo."""
    out = await _run(FakeBCRA(catalog_down=True), variables=["badlar", "reservas"])
    metas = {r.metadata["id_variable"]: r.metadata for r in out.results}
    assert metas[7]["unidad"] == "porcentaje"
    assert "unidad" not in metas[1]
    assert {VARIABLES[k].porcentaje for k in ("badlar", "tamar", "tasa_plazo_fijo")} == {True}


async def test_con_desde_trae_la_historia() -> None:
    out = await _run(FakeBCRA(), variables=["reservas"], desde="2026-01-01", ultimos=5)
    payload = json.loads(out.content)["variables"][0]
    result = out.results[0]
    assert result.records[0]["fecha"] >= "2026-01-01"
    assert result.records[0]["fecha"] <= "2026-01-05"
    assert result.records[-1]["fecha"] == "2026-09-30"
    assert len(payload["filas"]) == 5
    assert payload["filas_totales"] == len(result.records)


async def test_una_variable_caida_no_tira_abajo_a_las_demas() -> None:
    out = await _run(FakeBCRA(fail_ids={5}), variables=["dolar_minorista", "dolar_mayorista"])
    payloads = {p["variable"]: p for p in json.loads(out.content)["variables"]}
    assert "error" in payloads["dolar_mayorista"]
    assert payloads["dolar_minorista"]["ultimo_dato"]["valor"] == 1543.18
    assert [r.metadata["id_variable"] for r in out.results] == [4]


async def test_si_no_responde_ninguna_es_error_de_la_fuente() -> None:
    with pytest.raises(ConnectorError):
        await _run(FakeBCRA(fail_ids={1}), variables=["reservas"])


@pytest.mark.parametrize(
    "args",
    [
        {"variables": ["inflacion"]},
        {"variables": []},
        {"variables": ["reservas"], "desde": "30/09/2026"},
        {"variables": ["reservas"], "desde": "2026-10-10"},
        {"variables": ["reservas"], "desde": "2026-09-30", "hasta": "2026-09-01"},
        {"variables": list(VARIABLES)[:6]},
    ],
)
async def test_un_pedido_invalido_vuelve_al_modelo(args: dict[str, Any]) -> None:
    fake = FakeBCRA()
    with pytest.raises(ToolInputError):
        await _run(fake, **args)
    assert fake.requests == []


def test_describe_dice_que_se_consulta() -> None:
    text = VariablesBCRA().describe({"variables": ["dolar_minorista", "dolar_mayorista"]})
    assert text == "Consultando al BCRA: dólar minorista, dólar mayorista (A 3500)"


def test_la_descripcion_la_presenta_como_la_fuente_oficial_y_fresca() -> None:
    desc = VariablesBCRA.spec.description
    for palabra in (
        "oficial",
        "más fresca",
        "reservas",
        "dólar oficial",
        "tasas",
        "base monetaria",
    ):
        assert palabra in desc
    assert set(VariablesBCRA.spec.input_schema["properties"]["variables"]["items"]["enum"]) == set(
        VARIABLES
    )


# ── el registro y la herramienta de cotizaciones ───────────


def _deps(bcra: Any) -> MagicMock:
    deps = MagicMock()
    deps.bcra = bcra
    deps.staff = None
    return deps


def test_build_tools_ofrece_variables_bcra_si_hay_adaptador() -> None:
    names = [t.spec.name for t in build_tools(_deps(MagicMock()))]
    assert "variables_bcra" in names
    assert names.index("variables_bcra") < names.index("cotizaciones")
    assert "variables_bcra" not in [t.spec.name for t in build_tools(_deps(None))]


def test_cotizaciones_se_presenta_como_agregador_no_oficial() -> None:
    desc = Cotizaciones.spec.description
    assert "no oficiales" in desc
    assert "variables_bcra" in desc
    assert "Banco Nación" in desc


# ── el agente entero ───────────────────────────────────────


class _ScriptedLLM:
    model = "us.anthropic.claude-sonnet-4-6"

    def __init__(self, turns: list[AgentTurn]) -> None:
        self.turns = list(turns)
        self.tools: list[Any] = []

    async def stream_turn(self, *, tools: list[Any], **kw: Any) -> AsyncGenerator[Any, None]:
        self.tools = tools
        turn = self.turns.pop(0)
        if turn.text:
            yield TextDelta(turn.text)
        yield turn


def _turn(text: str = "", calls: list[ToolCall] | None = None) -> AgentTurn:
    calls = calls or []
    return AgentTurn(
        text=text,
        tool_calls=calls,
        stop_reason="tool_use" if calls else "end_turn",
        usage=AgentUsage(input_tokens=100, output_tokens=10),
        content=[{"type": "tool_use", "id": c.id, "name": c.name, "input": c.input} for c in calls],
    )


async def test_el_agente_cita_al_bcra_para_las_reservas() -> None:
    fake = FakeBCRA()
    deps = _deps(BCRAAdapter(client=fake.client()))
    llm = _ScriptedLLM(
        [
            _turn(
                calls=[ToolCall(id="t1", name="variables_bcra", input={"variables": ["reservas"]})]
            ),
            _turn("Las reservas son **US$ 46.092 millones** al 30/09/2026 (BCRA)."),
        ]
    )
    events = [
        e async for e in AgentEngine(llm, deps).stream(EngineRequest(question="q", user_id="u"))
    ]

    result = next(e for e in events if isinstance(e, CompleteEvent)).result
    assert [s["name"] for s in result.sources] == [VARIABLES["reservas"].titulo]
    assert all("Cotizaciones" not in s["name"] for s in result.sources)
    assert result.sources[0]["url"].startswith("https://www.bcra.gob.ar")
    assert result.evidence[0].records[-1] == {"fecha": "2026-09-30", "valor": 46092.0}
    steps = [e.detail for e in events if isinstance(e, StatusEvent)]
    assert "Consultando al BCRA: reservas internacionales" in steps
    assert any("30/09/2026" in s for s in steps)
    assert "variables_bcra" in [t.name for t in llm.tools]


def test_hoy_ar_es_la_fecha_argentina(monkeypatch: pytest.MonkeyPatch) -> None:
    """A las 23 h de Argentina el servidor (UTC) ya está en el día siguiente."""

    class _Reloj(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            return datetime(2026, 10, 5, 2, 19, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(bcra_tool, "datetime", _Reloj)
    assert _HOY_AR_REAL() == date(2026, 10, 4)


async def test_si_los_ultimos_datos_no_coinciden_el_paso_no_inventa_una_fecha() -> None:
    out = await _run(FakeBCRA(), variables=["reservas", "dolar_minorista"])
    assert out.summary == "Leyó 2 variables del BCRA"


async def test_con_desde_el_resumen_cubre_todo_el_periodo() -> None:
    """Para contar la evolución de 2026 el modelo pedía la serie tres veces."""
    out = await _run(FakeBCRA(), variables=["reservas"], desde="2026-01-01")
    payload = json.loads(out.content)["variables"][0]
    resumen = payload["resumen_mensual"]
    assert [p["periodo"] for p in resumen] == [f"2026-{m:02d}" for m in range(1, 10)]
    septiembre = resumen[-1]
    assert septiembre["cierre"] == 46092.0 and septiembre["fecha_cierre"] == "2026-09-30"
    assert septiembre["minimo"] <= septiembre["cierre"] <= septiembre["maximo"]


async def test_una_historia_larga_se_resume_por_anio() -> None:
    out = await _run(FakeBCRA(), variables=["reservas"], desde="2020-01-01")
    payload = json.loads(out.content)["variables"][0]
    assert "resumen_mensual" not in payload
    assert [p["periodo"] for p in payload["resumen_anual"]] == [str(y) for y in range(2020, 2027)]
    assert len(out.content) < 12_000
