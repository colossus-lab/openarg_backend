"""El agente: el ciclo de herramientas, con un modelo guionado.

Lo que se prueba:

- el ciclo: pedir herramientas, ejecutarlas, devolver los resultados, contestar;
- que las fuentes, los gráficos y la evidencia salgan sólo de herramientas
  que leen datos (ubicar un lugar no respalda una cifra);
- el presupuesto: al agotarse, se pide la respuesta sin más herramientas;
- que un error de herramienta vuelva al modelo para que corrija, no al usuario;
- que `describir_tabla` avise del ponderador y de la falta de nivel geográfico;
- la aclaración, el texto previo a las herramientas, y los tokens y el costo.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.answers import agent_engine as agent_module
from app.application.answers.agent_engine import AgentEngine
from app.application.answers.engine import (
    ChunkEvent,
    ClarificationEvent,
    ClearAnswerEvent,
    CompleteEvent,
    EngineRequest,
    StatusEvent,
)
from app.application.answers.tools.base import ToolContext
from app.application.answers.tools.catalogo import Calcular, DescribirTabla
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.agent_llm import AgentTurn, AgentUsage, TextDelta, ToolCall
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo, SandboxResult, TableSource

ESTUDIO = "raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"

# ── dobles ─────────────────────────────────────────────────


def _turn(
    text: str = "", calls: list[ToolCall] | None = None, stop: str | None = None, tokens: int = 100
) -> AgentTurn:
    calls = calls or []
    return AgentTurn(
        text=text,
        tool_calls=calls,
        stop_reason=stop or ("tool_use" if calls else "end_turn"),
        usage=AgentUsage(input_tokens=tokens, output_tokens=tokens // 10),
        content=[{"type": "text", "text": text}]
        + [{"type": "tool_use", "id": c.id, "name": c.name, "input": c.input} for c in calls],
    )


class ScriptedLLM:
    """Un modelo que contesta lo que dice el guion, vuelta por vuelta."""

    model = "us.anthropic.claude-sonnet-4-6"

    def __init__(self, turns: list[AgentTurn]) -> None:
        self.turns = list(turns)
        self.calls: list[dict[str, Any]] = []

    async def stream_turn(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[Any],
        max_tokens: int = 4096,
        allow_tools: bool = True,
    ) -> AsyncGenerator[Any, None]:
        self.calls.append(
            {
                "messages": json.loads(json.dumps(messages)),
                "allow_tools": allow_tools,
                "tools": tools,
            }
        )
        turn = self.turns.pop(0)
        for word in turn.text.split(" "):
            yield TextDelta(word + " ")
        yield turn


class FakeSandbox:
    def __init__(self) -> None:
        self.sqls: list[str] = []

    async def find_tables(self, **kw: Any) -> list[CachedTableInfo]:
        return [CachedTableInfo(ESTUDIO, "ds-1", 82327, [])]

    async def describe_marts(self, names: list[str]) -> dict[str, Any]:
        return {}

    async def find_marts(self, emb: list[float], limit: int = 5) -> list[Any]:
        return []

    async def get_table_sources(self, names: list[str]) -> dict[str, TableSource]:
        return {
            ESTUDIO.split(".")[1]: TableSource(
                "Estudio Nacional sobre el Perfil de las Personas con Discapacidad",
                "datos_gob_ar",
                "https://datos.gob.ar/dataset/x",
            )
        }

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        return {
            ESTUDIO: [
                ("pondera", "text"),
                ("dificultad_total", "text"),
                ("edad_agrupada", "text"),
                ("_source_dataset_id", "text"),
            ]
        }

    async def execute_readonly(self, sql: str, timeout_seconds: int = 10) -> SandboxResult:
        self.sqls.append(sql)
        if "sum(" in sql:
            rows = [{"valor": 3675564}]
        else:
            rows = [{"pondera": "471", "dificultad_total": "1", "edad_agrupada": "15-64"}]
        return SandboxResult(columns=list(rows[0]), rows=rows, row_count=len(rows), truncated=False)


def _deps(**over: Any) -> MagicMock:
    deps = MagicMock()
    deps.sandbox = FakeSandbox()
    deps.staff = None
    deps.georef.normalize_location = AsyncMock(
        return_value=DataResult(
            source="georef",
            portal_name="API de Georef Argentina",
            portal_url="https://apis.datos.gob.ar/georef",
            dataset_title="Ubicación: Pinamar",
            format="json",
            records=[{"nombre": "Pinamar", "provincia": "Buenos Aires"}],
        )
    )
    for k, v in over.items():
        setattr(deps, k, v)
    return deps


@pytest.fixture(autouse=True)
def _sin_metricas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(agent_module, "_record_tokens", lambda *a: None)


async def _run(engine: AgentEngine, req: EngineRequest | None = None) -> list[Any]:
    return [e async for e in engine.stream(req or EngineRequest(question="q", user_id="u"))]


def _call(name: str, i: int = 1, **args: Any) -> ToolCall:
    return ToolCall(id=f"t{i}", name=name, input=args)


# ── el ciclo ───────────────────────────────────────────────


async def test_pinamar_cuenta_personas_con_el_ponderador_y_cita_el_estudio() -> None:
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("ubicar_lugar", 1, texto="Pinamar"),
                    _call("describir_tabla", 2, tabla=ESTUDIO),
                ]
            ),
            _turn(
                calls=[
                    _call(
                        "calcular",
                        3,
                        tabla=ESTUDIO,
                        operacion="conteo",
                        ponderar_por="pondera",
                        filtros=[{"columna": "dificultad_total", "operador": "=", "valor": "1"}],
                    )
                ]
            ),
            _turn(
                "El estudio no tiene datos por partido. A nivel nacional estima **3.675.564** personas."
            ),
        ]
    )
    events = await _run(AgentEngine(llm, _deps()))
    result = events[-1].result

    assert "3.675.564" in result.answer
    # Fuente: el estudio, no Georef.
    assert [s["name"] for s in result.sources] == [
        "Estudio Nacional sobre el Perfil de las Personas con Discapacidad — conteo ponderado por pondera"
    ]
    assert all("Georef" not in s["portal"] for s in result.sources)
    assert result.served_table == ESTUDIO
    assert result.success is True
    # La segunda vuelta vio el aviso del ponderador y el del nivel geográfico.
    second = json.dumps(llm.calls[1]["messages"], ensure_ascii=False)
    assert "aviso_ponderador" in second
    assert "aviso_geografico" in second


async def test_los_pasos_que_ve_el_usuario_dicen_que_hace_y_que_hizo() -> None:
    """Como "Pensando… / Buscando X / Encontró Y" de los asistentes conocidos.

    Los nombres de paso son los que el frontend ya conoce: el chat los
    muestra aunque no se haya actualizado.
    """
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("ubicar_lugar", 1, texto="Pinamar"),
                    _call("calcular", 2, tabla=ESTUDIO, operacion="conteo", ponderar_por="pondera"),
                ]
            ),
            _turn("Listo."),
        ]
    )
    events = await _run(AgentEngine(llm, _deps()))
    status = [(e.step, e.detail) for e in events if isinstance(e, StatusEvent)]
    assert status == [
        ("coordination", "Pensando…"),
        ("searching", "Ubicando «Pinamar»"),
        ("searching", "Contando con el factor de expansión de la encuesta"),
        ("searching", "Ubicó «Pinamar»"),
        (
            "searching",
            "Calculó conteo ponderado por pondera en «Estudio Nacional sobre el Perfil de "
            "las Personas con Discapacidad»",
        ),
        ("coordination", "Pensando…"),
        ("generating", "Escribiendo la respuesta…"),
    ]


def test_un_resultado_sin_resumen_propio_dice_que_leyo_y_cuanto() -> None:
    from app.application.answers.agent_engine import _summary
    from app.application.answers.tools.base import ToolOutcome

    result = DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="",
        dataset_title="IPC Nacional",
        format="time_series",
        records=[{"v": i} for i in range(1234)],
    )
    assert _summary(ToolOutcome("{}", results=[result])) == "Leyó «IPC Nacional» (1.234 filas)"
    assert _summary(ToolOutcome("error", is_error=True)) is None


async def test_sin_datos_leidos_no_hay_fuentes_ni_exito() -> None:
    llm = ScriptedLLM([_turn("No encontré ese dato en OpenArg.")])
    result = (await _run(AgentEngine(llm, _deps())))[-1].result
    assert result.sources == []
    assert result.success is False
    assert result.no_data is True


async def test_el_texto_antes_de_las_herramientas_se_borra() -> None:
    llm = ScriptedLLM(
        [
            _turn("Voy a buscar.", calls=[_call("ubicar_lugar", texto="x")]),
            _turn("Respuesta final."),
        ]
    )
    events = await _run(AgentEngine(llm, _deps()))
    kinds = [
        "Herramienta" if isinstance(e, StatusEvent) and e.connector else type(e).__name__
        for e in events
    ]
    assert kinds.index("ClearAnswerEvent") < kinds.index("Herramienta")
    assert events[-1].result.answer == "Respuesta final."


async def test_el_texto_sale_en_streaming_y_limpio() -> None:
    llm = ScriptedLLM([_turn("Según cache_diputados_v2 hay 257 bancas.")])
    events = await _run(AgentEngine(llm, _deps()))
    streamed = "".join(e.content for e in events if isinstance(e, ChunkEvent))
    assert "257 bancas" in streamed
    assert "cache_diputados" not in streamed


async def test_un_error_de_herramienta_vuelve_al_modelo() -> None:
    llm = ScriptedLLM(
        [
            _turn(calls=[_call("calcular", tabla=ESTUDIO, operacion="suma")]),
            _turn("No pude calcularlo."),
        ]
    )
    await _run(AgentEngine(llm, _deps()))
    block = llm.calls[1]["messages"][-1]["content"][0]
    assert block["is_error"] is True
    assert "no es una columna" in block["content"]


async def test_una_herramienta_desconocida_no_rompe_el_turno() -> None:
    llm = ScriptedLLM([_turn(calls=[_call("borrar_todo")]), _turn("Ok.")])
    result = (await _run(AgentEngine(llm, _deps())))[-1].result
    assert result.answer == "Ok."
    assert llm.calls[1]["messages"][-1]["content"][0]["is_error"] is True


async def test_al_agotar_las_herramientas_se_pide_la_respuesta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(agent_module.MAX_TOOL_CALLS, "normal", 2)
    llm = ScriptedLLM(
        [
            _turn(calls=[_call("ubicar_lugar", 1, texto="a"), _call("ubicar_lugar", 2, texto="b")]),
            _turn("Con lo que encontré: nada."),
        ]
    )
    await _run(AgentEngine(llm, _deps()))
    final = llm.calls[-1]
    assert final["allow_tools"] is False
    assert "Ya no podés usar más herramientas" in json.dumps(final["messages"], ensure_ascii=False)


async def test_si_el_modelo_insiste_sin_presupuesto_se_cierra_igual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(agent_module.MAX_TOOL_CALLS, "normal", 1)
    llm = ScriptedLLM(
        [
            _turn(calls=[_call("ubicar_lugar", texto="a")]),
            _turn("parcial", calls=[_call("ubicar_lugar", 2, texto="b")]),
        ]
    )
    events = await _run(AgentEngine(llm, _deps()))
    assert isinstance(events[-1], CompleteEvent)
    assert events[-1].result.answer == "parcial"


async def test_el_tope_de_tiempo_del_canal_achica_el_presupuesto() -> None:
    engine = AgentEngine(ScriptedLLM([]), _deps())
    assert engine._time_budget(EngineRequest("q", "u", deadline_s=30)) == 20
    assert engine._time_budget(EngineRequest("q", "u")) == agent_module.SOFT_TIME_BUDGET_S["normal"]


async def test_la_aclaracion_cierra_el_turno_sin_otra_vuelta() -> None:
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call(
                        "pedir_aclaracion",
                        pregunta="¿Qué BAC?",
                        opciones=["Banco", "Buenos Aires Compras"],
                    )
                ]
            )
        ]
    )
    events = await _run(AgentEngine(llm, _deps()))
    assert ClarificationEvent("¿Qué BAC?", ("Banco", "Buenos Aires Compras")) in events
    result = events[-1].result
    assert result.intent == "clarification"
    assert "Buenos Aires Compras" in result.answer
    assert len(llm.calls) == 1


async def test_tokens_y_costo_suman_todas_las_vueltas() -> None:
    llm = ScriptedLLM(
        [_turn(calls=[_call("ubicar_lugar", texto="x")], tokens=1000), _turn("Fin.", tokens=2000)]
    )
    result = (await _run(AgentEngine(llm, _deps())))[-1].result
    assert result.tokens_used == 1000 + 100 + 2000 + 200
    # Sonnet 4.6: 3 US$ el millón de entrada, 15 el de salida.
    assert result.cost_usd == pytest.approx((3000 * 3 + 300 * 15) / 1_000_000)
    assert result.model == llm.model


async def test_el_contexto_de_la_conversacion_llega_al_modelo() -> None:
    llm = ScriptedLLM([_turn("Ok.")])
    req = EngineRequest(
        question="¿y en 2023?",
        user_id="u",
        history="HISTORIAL: Usuario: inflación 2024",
        previous_sources=("IPC Nacional — API de Series de Tiempo",),
    )
    await _run(AgentEngine(llm, _deps()), req)
    first = llm.calls[0]["messages"][0]["content"]
    assert "inflación 2024" in first
    assert "IPC Nacional — API de Series de Tiempo" in first
    assert first.endswith("Pregunta: ¿y en 2023?")


async def test_sin_sandbox_no_se_ofrecen_las_herramientas_de_tablas() -> None:
    llm = ScriptedLLM([_turn("Ok.")])
    await _run(AgentEngine(llm, _deps(sandbox=None)))
    names = {t.name for t in llm.calls[0]["tools"]}
    assert "calcular" not in names
    assert "buscar_series" in names


# ── las herramientas de tablas ─────────────────────────────


async def test_describir_tabla_avisa_ponderador_y_falta_de_geografia() -> None:
    out = await DescribirTabla().run(
        {"tabla": ESTUDIO}, ToolContext(_deps(), EngineRequest("q", "u"))
    )
    payload = json.loads(out.content)
    assert payload["ponderador"] == "pondera"
    assert payload["columnas_geograficas"] == []
    assert "aviso_geografico" in payload
    # Describir orienta pero no respalda una cifra.
    assert out.results == []
    # Las columnas internas del colector no se muestran.
    assert all(c["nombre"] != "_source_dataset_id" for c in payload["columnas"])


async def test_calcular_devuelve_un_resultado_citable() -> None:
    deps = _deps()
    out = await Calcular().run(
        {"tabla": ESTUDIO, "operacion": "conteo", "ponderar_por": "pondera"},
        ToolContext(deps, EngineRequest("q", "u")),
    )
    [result] = out.results
    assert result.records == [{"valor": 3675564}]
    assert result.metadata["served_table"] == ESTUDIO
    assert result.portal_url == "https://datos.gob.ar/dataset/x"
    assert "sum(" in deps.sandbox.sqls[-1]


async def test_una_tabla_que_no_existe_no_se_consulta() -> None:
    deps = _deps()

    async def _none(**kw: Any) -> list[Any]:
        return []

    deps.sandbox.find_tables = _none
    out = await AgentEngine(ScriptedLLM([]), deps)._run_tool(
        Calcular(),
        _call("calcular", tabla="public.users", operacion="conteo"),
        ToolContext(deps, EngineRequest("q", "u")),
    )
    assert out.is_error
    assert deps.sandbox.sqls == []


def test_las_descripciones_de_herramientas_no_repiten_nombres() -> None:
    from app.application.answers.tools import build_tools

    names = [t.spec.name for t in build_tools(_deps())]
    assert len(names) == len(set(names))
    assert ClearAnswerEvent  # usado arriba


# ── lo que encontró la prueba contra staging (01-oct) ──────


@pytest.mark.parametrize(
    ("fechas", "esperado"),
    [
        # Fechadas por el primer día del período (lo habitual).
        (["2025-01-01", "2025-07-01"], ["2025-S1", "2025-S2"]),
        (["2025-04-01", "2025-07-01"], ["2025-T2", "2025-T3"]),
        (["2024-01-01", "2025-01-01"], ["2024", "2025"]),
        # PBI trimestral real: el 2° trimestre de 2026 es `2026-04-01`.
        (["2026-01-01", "2026-04-01"], ["2026-T1", "2026-T2"]),
        # Tasa de pobreza real del INDEC: fechada por el día siguiente al fin.
        # `2026-07-01` es el 1er semestre de 2026 (publicado en septiembre),
        # y el 52,9 % de `2024-07-01` es el 1er semestre de 2024.
        (
            ["2024-07-01", "2025-01-01", "2025-07-01", "2026-01-01", "2026-07-01"],
            ["2024-S1", "2024-S2", "2025-S1", "2025-S2", "2026-S1"],
        ),
    ],
)
def test_las_series_llevan_el_periodo_con_la_convencion_de_la_serie(
    fechas: list[str], esperado: list[str]
) -> None:
    from datetime import date

    from app.application.answers.tools.conectores import _tail_for_model

    result = DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="",
        dataset_title="Pobreza",
        format="time_series",
        records=[{"fecha": f, "pobreza": 30.0} for f in fechas],
    )
    filas = _tail_for_model(result, 10, today=date(2026, 10, 2))["filas"]
    assert [r["periodo"] for r in filas] == esperado


def test_una_serie_mensual_no_lleva_periodo() -> None:
    from app.application.answers.tools.conectores import _tail_for_model

    result = DataResult(
        source="series_tiempo",
        portal_name="x",
        portal_url="",
        dataset_title="IPC",
        format="time_series",
        records=[{"fecha": "2026-07-01", "v": 1}, {"fecha": "2026-08-01", "v": 2}],
    )
    assert "periodo" not in _tail_for_model(result, 10)["filas"][0]


async def test_dos_busquedas_en_paralelo_no_comparten_la_sesion() -> None:
    """Una sesión de SQLAlchemy no admite dos operaciones a la vez."""
    import asyncio

    active = 0

    async def _search(**kw: Any) -> list[Any]:
        nonlocal active
        active += 1
        if active > 1:
            raise RuntimeError("concurrent operations are not permitted")
        await asyncio.sleep(0.01)
        active -= 1
        return []

    deps = _deps()
    deps.vector_search.search_datasets_ann = _search
    deps.embedding.embed = AsyncMock(return_value=[0.1])
    llm = ScriptedLLM(
        [
            _turn(calls=[_call("buscar_datos", 1, texto="a"), _call("buscar_datos", 2, texto="b")]),
            _turn("Nada."),
        ]
    )
    await _run(AgentEngine(llm, deps))
    results = llm.calls[1]["messages"][-1]["content"]
    assert not any(r.get("is_error") for r in results)


async def test_una_busqueda_que_falla_deja_la_sesion_usable() -> None:
    """Sin el rollback, todas las búsquedas siguientes del turno fallaban."""
    deps = _deps()
    deps.embedding.embed = AsyncMock(return_value=[0.1])
    deps.vector_search.search_datasets_ann = AsyncMock(side_effect=TimeoutError)
    deps.vector_search.reset = AsyncMock()
    from app.application.answers.tools.catalogo import BuscarDatos

    with pytest.raises(TimeoutError):
        await BuscarDatos().run({"texto": "x"}, ToolContext(deps, EngineRequest("q", "u")))
    deps.vector_search.reset.assert_awaited_once()
