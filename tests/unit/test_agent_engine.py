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
from datetime import date, timedelta
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
                "system": system,
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

    async def table_profiles(self, names: list[str]) -> dict[str, Any]:
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

    async def execute_readonly(
        self, sql: str, timeout_seconds: int = 10, *, params: Any = None
    ) -> SandboxResult:
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
    monkeypatch.setattr(agent_module, "MAX_TOOL_CALLS", 2)
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
    monkeypatch.setattr(agent_module, "MAX_TOOL_CALLS", 1)
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
    assert engine._time_budget(EngineRequest("q", "u")) == agent_module.SOFT_TIME_BUDGET_S


async def test_un_pedido_viejo_en_modo_profundo_corre_como_el_normal() -> None:
    engine = AgentEngine(ScriptedLLM([]), _deps())
    assert engine._time_budget(EngineRequest("q", "u", mode="deep")) == 35.0


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


# ── fuentes por uso y verificación de cifras (04-oct) ──────


def _serie(sid: str, title: str, rows: list[tuple[str, float]], units: str = "") -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url=f"https://datos.gob.ar/series/api/series/?ids={sid}",
        dataset_title=title,
        format="time_series",
        records=[{"fecha": f, "Reservas": x} for f, x in rows],
        metadata={"units": units},
    )


RESERVAS_DIARIA = _serie(
    "92.2_RESERVAS_IRES_0_0_32_40",
    "Reservas internacionales y pasivos del BCRA",
    [("2005-09-26", 25530.0), ("2005-09-27", 25557.0)],
    "Millones de dólares",
)
RESERVAS_MENSUAL = _serie(
    "92.1_RID_0_0_32",
    "Reservas internacionales y pasivos del BCRA",
    [("2026-06-01", 47467.31), ("2026-07-01", 48661.88), ("2026-08-01", 49700.26)],
    "Millones de dólares",
)


def _deps_series(*results: DataResult) -> MagicMock:
    deps = _deps()
    deps.series.fetch = AsyncMock(side_effect=list(results))
    return deps


def _reservas_llm(*answers: str) -> ScriptedLLM:
    return ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["92.2_RESERVAS_IRES_0_0_32_40"]),
                    _call("series_tiempo", 2, ids=["92.1_RID_0_0_32"]),
                ]
            ),
            *[_turn(a) for a in answers],
        ]
    )


async def test_se_citan_solo_las_fuentes_que_aportaron_cifras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproducción del 04-oct, reservas corrida 1: citaba las tres series
    leídas y sólo una aportó las cifras. Gráficos y `served_table` salen de
    la misma lista. Sólo en correct: en shadow y off se cita todo lo leído
    (revisión del 05-oct, más abajo)."""
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    llm = _reservas_llm(
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026 (promedio mensual)."
    )
    deps = _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL)
    result = (await _run(AgentEngine(llm, deps)))[-1].result
    assert [s["url"] for s in result.sources] == [RESERVAS_MENSUAL.portal_url]
    assert result.cited_evidence == [RESERVAS_MENSUAL]
    assert result.consulted == [RESERVAS_DIARIA.dataset_title]
    assert result.row_count == 3
    # Toda la evidencia leída sigue disponible para verificar.
    assert result.evidence == [RESERVAS_DIARIA, RESERVAS_MENSUAL]
    # Las citas estructuradas salen de las coincidencias.
    [cita] = result.citations
    assert cita["verified"] is True
    assert cita["grounding"][0]["url"] == RESERVAS_MENSUAL.portal_url


async def test_en_modo_sombra_la_respuesta_no_cambia_y_queda_registrada(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("ANSWERS_VERIFY_MODE", raising=False)
    answer = "Las reservas fueron de **USD 51.191 millones** en agosto de 2026."
    llm = _reservas_llm(answer)
    with caplog.at_level("INFO", logger=agent_module.logger.name):
        events = await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL)))
    result = events[-1].result
    assert result.answer == answer
    assert len(llm.calls) == 2
    assert not any(isinstance(e, ClearAnswerEvent) for e in events)
    assert result.verification["sin_respaldo"] == ["51.191 millones"]
    assert result.verification["modo"] == "shadow"
    [line] = [r.getMessage() for r in caplog.records if "answers.verify" in r.getMessage()]
    assert "51.191 millones" in line


async def test_en_modo_correct_hay_una_vuelta_correctiva(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    llm = _reservas_llm(
        "Las reservas fueron de **USD 51.191 millones** en agosto de 2026.",
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026 (promedio mensual).",
    )
    events = await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL)))
    result = events[-1].result
    assert result.answer.startswith("Las reservas fueron de **USD 49.700 millones**")
    assert len(llm.calls) == 3
    # Al modelo le llegó la lista de cifras sin respaldo, después de su respuesta.
    pedido = llm.calls[2]["messages"][-1]["content"]
    assert "51.191 millones" in pedido
    assert llm.calls[2]["messages"][-2]["role"] == "assistant"
    # Lo que ya había salido en streaming se borró antes de la respuesta nueva.
    kinds = [type(e).__name__ for e in events]
    first_clear = kinds.index("ClearAnswerEvent")
    assert "ChunkEvent" in kinds[:first_clear]
    assert "ChunkEvent" in kinds[first_clear:]
    assert StatusEvent("coordination", "Revisando las cifras…") in events
    assert result.verification["vuelta_correctiva"] is True
    assert result.verification["sin_respaldo"] == []
    assert result.verification["sin_respaldo_antes"] == ["51.191 millones"]


async def test_si_sigue_sin_respaldo_va_un_aviso_arriba_y_la_cifra_queda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    wrong = "Las reservas fueron de **USD 51.191 millones** en agosto de 2026."
    llm = _reservas_llm(wrong, wrong)
    result = (await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))))[
        -1
    ].result
    assert len(llm.calls) == 3  # una sola vuelta correctiva
    assert result.answer.startswith("**Aviso:** no pude verificar")
    assert "51.191 millones" in result.answer.split("\n\n", 1)[0]
    # Nunca se borra una cifra del texto.
    assert result.answer.endswith(wrong)


async def test_sin_tiempo_no_hay_vuelta_correctiva(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pasarse del tope de /ask pierde la respuesta entera: mejor el aviso."""
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    # El reloj: arranque, dos vueltas del ciclo y la consulta de la vuelta
    # correctiva, cuando ya pasaron 25 de los 30 s de /ask.
    ticks = iter([0.0, 0.0, 1.0, 25.0])
    wrong = "Las reservas fueron de **USD 51.191 millones** en agosto de 2026."
    llm = _reservas_llm(wrong)
    engine = AgentEngine(
        llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL), clock=lambda: next(ticks, 25.0)
    )
    result = (await _run(engine, EngineRequest("q", "u", deadline_s=30)))[-1].result
    assert len(llm.calls) == 2
    assert result.answer.startswith("**Aviso:** no pude verificar")


async def test_en_modo_off_no_se_registra_ni_se_corrige(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "off")
    wrong = "Las reservas fueron de **USD 51.191 millones** en agosto de 2026."
    llm = _reservas_llm(wrong)
    with caplog.at_level("INFO", logger=agent_module.logger.name):
        result = (await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))))[
            -1
        ].result
    assert result.answer == wrong
    assert not any("answers.verify" in r.getMessage() for r in caplog.records)


# ── lo que encontró la revisión del PR (05-oct) ────────────


async def test_una_descripcion_mal_formada_no_se_lleva_la_respuesta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """«secciones 1.1,1.2» en la descripción de una serie: la verificación
    tiraba ValueError, `_safe_verify` la atrapaba, pero `select_evidence` la
    repetía sin red y el usuario se quedaba sin respuesta. En correct, que es
    donde la selección se aplica."""
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    rara = DataResult(
        source=RESERVAS_MENSUAL.source,
        portal_name=RESERVAS_MENSUAL.portal_name,
        portal_url=RESERVAS_MENSUAL.portal_url,
        dataset_title=RESERVAS_MENSUAL.dataset_title,
        format=RESERVAS_MENSUAL.format,
        records=RESERVAS_MENSUAL.records,
        metadata={**RESERVAS_MENSUAL.metadata, "description": "Ver secciones 1.1,1.2."},
    )
    answer = "Las reservas fueron de **USD 49.700 millones** en agosto de 2026."
    events = await _run(AgentEngine(_reservas_llm(answer), _deps_series(RESERVAS_DIARIA, rara)))
    assert isinstance(events[-1], CompleteEvent)
    assert events[-1].result.answer == answer
    assert events[-1].result.cited_evidence == [rara]


async def test_si_la_verificacion_revienta_la_respuesta_sale_igual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallar abierto de punta a punta: sin verificación se cita toda la
    evidencia, como antes, y no se vuelve a verificar fuera de la red."""
    from app.application.answers import verification as verification_module

    def _boom(*a: Any, **kw: Any) -> Any:
        raise RuntimeError("bug del verificador")

    monkeypatch.setattr(agent_module, "verify_figures", _boom)
    monkeypatch.setattr(verification_module, "verify_figures", _boom)
    answer = "Las reservas fueron de **USD 49.700 millones** en agosto de 2026."
    events = await _run(
        AgentEngine(_reservas_llm(answer), _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))
    )
    result = events[-1].result
    assert result.answer == answer
    assert result.cited_evidence == [RESERVAS_DIARIA, RESERVAS_MENSUAL]
    assert result.citations == [] and result.verification is None


async def test_la_verificacion_corre_fuera_del_event_loop_y_una_sola_vez(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Con 120 cifras tarda ~0,5 s: en el event loop frenaba a los demás
    usuarios del worker. En modo correct, si la respuesta no cambia, no se
    verifica dos veces. Y el log dice cuánto tardó."""
    import threading

    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    original = agent_module.verify_figures
    threads: list[int] = []

    def _spy(*a: Any, **kw: Any) -> Any:
        threads.append(threading.get_ident())
        return original(*a, **kw)

    monkeypatch.setattr(agent_module, "verify_figures", _spy)
    answer = "Las reservas fueron de **USD 49.700 millones** en agosto de 2026."
    events = await _run(
        AgentEngine(_reservas_llm(answer), _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))
    )
    result = events[-1].result
    assert result.answer == answer
    assert len(threads) == 1
    assert threads[0] != threading.get_ident()
    assert isinstance(result.verification["ms"], int)


async def test_lo_citado_por_titulo_no_es_lo_que_aporto_cifras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Una serie que sólo se nombra se cita, pero el aviso de atraso
    (`figure_evidence`) y `served_table` salen de la que aportó las cifras.
    En correct, que es donde la selección se aplica."""
    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "correct")
    nombrada = _serie(
        "116.4_TCRZE_2015_D_36_4",
        "Índice de tipo de cambio real multilateral",
        [("2005-09-26", 140.0)],
    )
    answer = (
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026. El índice de "
        "tipo de cambio real multilateral es otra referencia."
    )
    llm = _reservas_llm(answer)
    result = (await _run(AgentEngine(llm, _deps_series(nombrada, RESERVAS_MENSUAL))))[-1].result
    assert result.cited_evidence == [nombrada, RESERVAS_MENSUAL]
    assert result.figure_evidence == [RESERVAS_MENSUAL]
    assert result.row_count == 3  # la mensual, no la nombrada (1 fila)


async def test_el_prompt_que_recibe_el_modelo_trae_las_reglas_nuevas() -> None:
    """El prompt medido en uso: lo que de verdad le llega al modelo."""
    llm = ScriptedLLM([_turn("Ok.")])
    await _run(AgentEngine(llm, _deps()))
    system = llm.calls[0]["system"]
    assert "Las tasas no se suman ni se restan" in system
    assert "No atribuyas causas" in system
    assert 'Nunca uses "actual"' in system
    assert "coparticipación federal" in system
    # El prompt nombra variables_bcra si y sólo si se le ofrece al modelo: si
    # no está, la pediría y perdería una vuelta.
    offered = {t.name for t in llm.calls[0]["tools"]}
    assert ("variables_bcra" in system) == ("variables_bcra" in offered)


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


# ── el verificador sólo actúa en correct (revisión independiente del 05-oct) ──


def _modo(monkeypatch: pytest.MonkeyPatch, mode: str | None) -> None:
    if mode is None:
        monkeypatch.delenv("ANSWERS_VERIFY_MODE", raising=False)
    else:
        monkeypatch.setenv("ANSWERS_VERIFY_MODE", mode)


IPC_MENSUAL = _serie(
    "148.3_INIVELNAL_DICI_M_26",
    "IPC nacional, variación mensual",
    [("2026-07-01", 1.9), ("2026-08-01", 2.1), ("2026-09-01", 1.7)],
    "Porcentaje",
)


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_fuera_de_correct_no_se_publican_citas_del_verificador(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """H019: las citas se arman por coincidencia de valor, sin mirar el
    período, y salían en el JSON de /ask con `verified: true` también en
    shadow: «la inflación de septiembre fue 2,1 %» salía verificada con el 2,1
    de agosto. Fuera de correct, `citations=[]`, como antes del verificador."""
    _modo(monkeypatch, mode)
    llm = ScriptedLLM(
        [
            _turn(calls=[_call("series_tiempo", 1, ids=["148.3_INIVELNAL_DICI_M_26"])]),
            _turn("La inflación de septiembre de 2026 fue de **2,1 %**."),
        ]
    )
    result = (await _run(AgentEngine(llm, _deps_series(IPC_MENSUAL))))[-1].result
    assert result.citations == []


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_fuera_de_correct_graficos_tabla_y_aviso_salen_de_todo_lo_leido(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """H082/H100: la selección de fuentes por uso corría en todos los modos y
    decidía fuentes, gráficos, `served_table` y sobre qué se calcula el aviso
    de atraso. Fuera de correct se cita todo lo leído, como antes de #134.

    La lista de fuentes es la excepción: en shadow, con todas las cifras
    directas o derivadas como acá, sale de la selección (opción B, tests de
    más abajo); con off, todo lo leído.

    El aviso: en shadow, con todas las cifras respaldadas, deja afuera lo que
    no aportó cifras y se llama igual que algo que sí, acá 92.2 (revisión de
    #146, tests de abajo); con off no hay verificación y mira todo lo citado
    (`figure_evidence` vacío)."""
    _modo(monkeypatch, mode)
    llm = _reservas_llm(
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026 (promedio mensual)."
    )
    result = (await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))))[
        -1
    ].result
    assert [s["url"] for s in result.sources] == (
        [RESERVAS_DIARIA.portal_url, RESERVAS_MENSUAL.portal_url]
        if mode == "off"
        else [RESERVAS_MENSUAL.portal_url]
    )
    assert result.cited_evidence == [RESERVAS_DIARIA, RESERVAS_MENSUAL]
    assert result.figure_evidence == ([] if mode == "off" else [RESERVAS_MENSUAL])
    assert result.consulted == []
    assert result.served_table == RESERVAS_DIARIA.source
    assert result.row_count == len(RESERVAS_DIARIA.records)


async def test_en_sombra_la_seleccion_de_fuentes_queda_solo_en_el_log(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Lo que habría hecho correct se registra en `answers.verify` para medirlo.
    Con una cifra sin respaldo (acá el 51.191) queda sólo en el log; con todas
    respaldadas, las fuentes salen de la selección (opción B, más abajo)."""
    monkeypatch.delenv("ANSWERS_VERIFY_MODE", raising=False)
    llm = _reservas_llm(
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026 (promedio mensual) "
        "y de **USD 51.191 millones** en septiembre."
    )
    with caplog.at_level("INFO", logger=agent_module.logger.name):
        result = (await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))))[
            -1
        ].result
    assert len(result.sources) == 2
    assert result.verification["fuentes_citadas"] == 1
    assert result.verification["consultadas"] == 1
    [line] = [r.getMessage() for r in caplog.records if "answers.verify" in r.getMessage()]
    assert '"consultadas": 1' in line


async def test_off_es_el_motor_de_antes_del_verificador(monkeypatch: pytest.MonkeyPatch) -> None:
    """H083: con off se seguía verificando y filtrando fuentes, citas,
    gráficos, mapa, `served_table` y la evidencia del aviso de atraso; lo único
    que cambiaba era la línea del log. Off es el interruptor: no se verifica y
    el resultado es el de antes de #134 (todo lo leído, sin citas)."""
    from app.application.pipeline.chart_builder import build_deterministic_charts
    from app.application.pipeline.nodes.analyst import _build_map_data
    from app.application.pipeline.nodes.finalize import _extract_documents

    monkeypatch.setenv("ANSWERS_VERIFY_MODE", "off")
    original = agent_module.verify_figures
    verified: list[str] = []

    def _spy(answer: str, *a: Any, **kw: Any) -> Any:
        verified.append(answer)
        return original(answer, *a, **kw)

    monkeypatch.setattr(agent_module, "verify_figures", _spy)
    answer = "Las reservas fueron de **USD 51.191 millones** en agosto de 2026."
    llm = _reservas_llm(answer)
    result = (await _run(AgentEngine(llm, _deps_series(RESERVAS_DIARIA, RESERVAS_MENSUAL))))[
        -1
    ].result

    evidence = [RESERVAS_DIARIA, RESERVAS_MENSUAL]
    assert verified == []
    assert result.answer == answer
    assert len(llm.calls) == 2
    # El `_result` de antes de #134 (16b2abf): todo sale de la evidencia leída.
    assert result.sources == [
        {"name": r.dataset_title, "url": r.portal_url, "portal": r.portal_name, "accessed_at": ""}
        for r in evidence
    ]
    assert result.chart_data == (build_deterministic_charts(evidence) or None)
    assert result.map_data == _build_map_data(evidence)
    assert result.documents == _extract_documents(evidence)
    assert result.served_table == RESERVAS_DIARIA.source
    assert result.row_count == len(RESERVAS_DIARIA.records)
    assert result.citations == []
    assert result.cited_evidence == evidence and result.figure_evidence == []
    assert result.verification is None


DOLAR_VIEJO = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url="https://datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26",
    dataset_title="Tipo de cambio de referencia Comunicación A3500 (serie discontinuada)",
    format="time_series",
    records=[{"fecha": "2024-12-27", "Dólar": 1029.0}, {"fecha": "2024-12-30", "Dólar": 1031.56}],
    metadata={"units": "Pesos argentinos por dólar"},
)


async def test_una_cifra_truncada_no_le_saca_la_fuente_ni_el_aviso_de_atraso(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H082, de punta a punta con el runner: «$1.031» por 1.031,56 (truncado,
    no redondeado) queda sin respaldo, y en shadow la serie vieja dejaba de
    citarse y perdía el «Dato atrasado»: un dato de 2024 sin fuente y sin
    fecha. En shadow se cita todo y el aviso mira todo lo citado."""
    from app.application.answers import runner as runner_module
    from app.application.answers.runner import EngineRunner

    async def _nada(*a: Any, **kw: Any) -> None:
        return None

    async def _sin_cache(*a: Any, **kw: Any) -> tuple[None, None]:
        return None, None

    monkeypatch.setattr(runner_module, "record_terminal_analytics", _nada)
    monkeypatch.setattr(runner_module, "check_cache", _sin_cache)
    monkeypatch.setattr(runner_module, "write_cache", _nada)
    monkeypatch.setattr(runner_module, "audit_query", lambda **kw: None)
    monkeypatch.delenv("ANSWERS_VERIFY_MODE", raising=False)

    answer = (
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026. "
        "El dólar de referencia estaba en **$1.031**."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["92.1_RID_0_0_32"]),
                    _call("series_tiempo", 2, ids=["168.1_T_CAMBIOR_D_0_0_26"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(RESERVAS_MENSUAL, DOLAR_VIEJO))
    result = await EngineRunner(engine, MagicMock()).run(EngineRequest("reservas y dólar", "u"))
    assert result.verification["sin_respaldo"] == ["1.031"]
    assert [s["url"] for s in result.sources] == [
        RESERVAS_MENSUAL.portal_url,
        DOLAR_VIEJO.portal_url,
    ]
    assert result.answer.startswith("**Dato atrasado:**")
    assert "30 de diciembre de 2024" in result.answer
    assert result.answer.endswith(answer)


def _reservas_92_2_hoy() -> DataResult:
    """92.2 con la metadata que devuelve hoy la API pública: diaria, termina
    el 31-ago-2026 y la fuente dice que no se actualiza."""
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=92.2_RESERVAS_IRES_0_0_32_40",
        dataset_title="Reservas internacionales y pasivos del BCRA",
        format="time_series",
        records=[
            {"fecha": "2026-08-28", "Reservas": 39876.0},
            {"fecha": "2026-08-31", "Reservas": 39912.0},
        ],
        metadata={
            "units": "Millones de dólares",
            "ultima_observacion": "2026-08-31",
            "frecuencia": "diaria",
            "fecha_fin_fuente": "2026-08-31",
            "actualizada_en_fuente": False,
        },
    )


def _reservas_92_1_al_dia() -> DataResult:
    """92.1, mismo título que 92.2, con el último dato de ayer."""
    ayer = date.today() - timedelta(days=1)
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=92.1_RID_0_0_32",
        dataset_title="Reservas internacionales y pasivos del BCRA",
        format="time_series",
        records=[
            {"fecha": (ayer - timedelta(days=1)).isoformat(), "Reservas": 41100.0},
            {"fecha": ayer.isoformat(), "Reservas": 41234.0},
        ],
        metadata={
            "units": "Millones de dólares",
            "ultima_observacion": ayer.isoformat(),
            "frecuencia": "diaria",
            "fecha_fin_fuente": ayer.isoformat(),
            "actualizada_en_fuente": True,
        },
    )


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_en_sombra_una_serie_atrasada_leida_y_no_usada_no_pone_el_aviso(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """Revisión de #146: en shadow el aviso miraba siempre toda la evidencia
    leída, así que una serie consultada y no usada ponía «Dato atrasado»
    arriba de una respuesta correcta y al día, con todas sus cifras
    respaldadas. 92.1 y 92.2 se llaman igual en la API: el aviso parecía
    hablar de la cifra de la respuesta. Con todas las cifras respaldadas, el
    aviso deja afuera lo que no aportó cifras y se llama igual que algo que
    sí; las fuentes, por la opción B, son sólo lo que aportó cifras.

    Con off no se verifica y el aviso mira todo lo leído: es el interruptor, y
    sin verificación no hay con qué distinguir lo usado de lo consultado. Las
    fuentes, todo lo leído."""
    from app.application.answers import runner as runner_module
    from app.application.answers.runner import EngineRunner

    async def _nada(*a: Any, **kw: Any) -> None:
        return None

    async def _sin_cache(*a: Any, **kw: Any) -> tuple[None, None]:
        return None, None

    monkeypatch.setattr(runner_module, "record_terminal_analytics", _nada)
    monkeypatch.setattr(runner_module, "check_cache", _sin_cache)
    monkeypatch.setattr(runner_module, "write_cache", _nada)
    monkeypatch.setattr(runner_module, "audit_query", lambda **kw: None)
    _modo(monkeypatch, mode)

    vieja, al_dia = _reservas_92_2_hoy(), _reservas_92_1_al_dia()
    answer = (
        "Las reservas internacionales eran de **USD 41.234 millones** según el último dato diario."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["92.2_RESERVAS_IRES_0_0_32_40"]),
                    _call("series_tiempo", 2, ids=["92.1_RID_0_0_32"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(vieja, al_dia))
    result = await EngineRunner(engine, MagicMock()).run(
        EngineRequest("¿Cuánto hay de reservas hoy?", "u")
    )
    if mode == "off":
        assert [s["url"] for s in result.sources] == [vieja.portal_url, al_dia.portal_url]
        assert result.verification is None
        assert result.answer.startswith("**Dato atrasado:**")
        assert "31 de agosto de 2026" in result.answer
        return
    assert result.verification["sin_respaldo"] == []
    assert [s["url"] for s in result.sources] == [al_dia.portal_url]
    assert result.answer == answer
    assert result.figure_evidence == [al_dia]


# ── el aviso en sombra sólo deja afuera lo homónimo (revisión de #146) ──


def _runner_sin_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """El runner de verdad, sin caché, analytics ni auditoría."""
    from app.application.answers import runner as runner_module

    async def _nada(*a: Any, **kw: Any) -> None:
        return None

    async def _sin_cache(*a: Any, **kw: Any) -> tuple[None, None]:
        return None, None

    monkeypatch.setattr(runner_module, "record_terminal_analytics", _nada)
    monkeypatch.setattr(runner_module, "check_cache", _sin_cache)
    monkeypatch.setattr(runner_module, "write_cache", _nada)
    monkeypatch.setattr(runner_module, "audit_query", lambda **kw: None)


# Las reservas mensuales de H082 con el salto julio→agosto en 1.031,30 (en
# RESERVAS_MENSUAL es 1.038,38): coincide con el «$1.031» truncado del dólar.
RESERVAS_MENSUAL_SALTO_1031 = _serie(
    "92.1_RID_0_0_32",
    "Reservas internacionales y pasivos del BCRA",
    [("2026-06-01", 47467.31), ("2026-07-01", 48668.96), ("2026-08-01", 49700.26)],
    "Millones de dólares",
)


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_en_sombra_un_truncado_que_coincide_con_otra_serie_no_pierde_el_aviso(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """El caso de H082 («$1.031» por 1.031,56, de diciembre de 2024) con el
    salto julio→agosto de las reservas en 1.031,30: el verificador da el
    truncado por derivado de las reservas, todas las cifras quedan
    respaldadas, y el aviso miraba sólo lo que aportó cifras. El dato de 2024
    salía sin «Dato atrasado». El dólar no se llama como las reservas: cuenta."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    answer = (
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026. "
        "El dólar de referencia estaba en **$1.031**."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["92.1_RID_0_0_32"]),
                    _call("series_tiempo", 2, ids=["168.1_T_CAMBIOR_D_0_0_26"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO))
    result = await EngineRunner(engine, MagicMock()).run(EngineRequest("reservas y dólar", "u"))
    if mode != "off":
        # La coincidencia: ninguna cifra queda sin respaldo.
        assert result.verification["sin_respaldo"] == []
    assert result.answer.endswith(answer)
    avisos = result.answer[: -len(answer)]
    assert avisos.startswith("**Dato atrasado:**")
    assert "Tipo de cambio de referencia" in avisos
    assert "30 de diciembre de 2024" in avisos
    if mode != "off":
        assert result.figure_evidence == [RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO]


IPC_AL_DIA = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url="https://datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26",
    dataset_title="IPC nacional, variación mensual",
    format="time_series",
    records=[
        {"fecha": "2026-06-01", "IPC": 2.4},
        {"fecha": "2026-07-01", "IPC": 1.9},
        {"fecha": "2026-08-01", "IPC": 2.1},
        {"fecha": "2026-09-01", "IPC": 1.7},
    ],
    metadata={
        "units": "Porcentaje",
        "ultima_observacion": "2026-09-01",
        "frecuencia": "mensual",
        "fecha_fin_fuente": "2026-09-01",
        "actualizada_en_fuente": True,
    },
)

SALARIOS_VIEJOS = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url="https://datos.gob.ar/series/api/series/?ids=149.1_SOR_PRIADO_OCTU_0_25",
    dataset_title="Índice de salarios. Sector privado registrado. Mensual.",
    format="time_series",
    records=[
        {"fecha": "2026-03-01", "Salarios": 3.1},
        {"fecha": "2026-04-01", "Salarios": 2.6},
        {"fecha": "2026-05-01", "Salarios": 2.47},
    ],
    metadata={
        "units": "Porcentaje",
        "ultima_observacion": "2026-05-01",
        "frecuencia": "mensual",
        "fecha_fin_fuente": "2026-05-01",
        "actualizada_en_fuente": False,
    },
)


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
@pytest.mark.parametrize(
    "salarios",
    [
        # «2,4 %» por 2,47 (truncado, como H082) coincide tal cual con junio
        # del IPC: queda "directa" del IPC y nada la ata a los salarios.
        "Los salarios registrados subieron **2,4 %** en el último mes.",
        # La serie vieja se usa para una afirmación sin cifra propia.
        "Los salarios registrados vienen creciendo por encima de la inflación.",
    ],
    ids=["truncado_que_coincide_con_el_ipc", "sin_cifra_propia"],
)
async def test_en_sombra_una_serie_atrasada_usada_no_pierde_el_aviso_por_otra_al_dia(
    monkeypatch: pytest.MonkeyPatch, mode: str | None, salarios: str
) -> None:
    """IPC al día y salarios que la fuente da por desactualizados (terminan en
    mayo). La respuesta usa los salarios, pero la única cifra respaldada es
    del IPC, y el aviso miraba sólo el IPC: salía sin «Dato atrasado». Los
    salarios no se llaman como el IPC: cuentan."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    answer = f"En septiembre de 2026 la inflación mensual fue de **1,7 %**. {salarios}"
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["148.3_INIVELNAL_DICI_M_26"]),
                    _call("series_tiempo", 2, ids=["149.1_SOR_PRIADO_OCTU_0_25"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(IPC_AL_DIA, SALARIOS_VIEJOS))
    result = await EngineRunner(engine, MagicMock()).run(
        EngineRequest("¿Los salarios le ganan a la inflación?", "u")
    )
    if mode != "off":
        assert result.verification["sin_respaldo"] == []
    assert result.answer.endswith(answer)
    avisos = result.answer[: -len(answer)]
    assert avisos.startswith("**Dato atrasado:**")
    assert "Índice de salarios" in avisos
    assert "mayo de 2026" in avisos
    if mode != "off":
        assert result.figure_evidence == [IPC_AL_DIA, SALARIOS_VIEJOS]


# ── el aviso en sombra: primero lo que aportó cifras (revisión de #146) ──


def _serie_vieja(sid: str, title: str, value: float) -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url=f"https://datos.gob.ar/series/api/series/?ids={sid}",
        dataset_title=title,
        format="time_series",
        records=[{"fecha": "2023-11-01", "v": value}, {"fecha": "2023-12-01", "v": value + 3.0}],
        metadata={"units": "Millones de dólares"},
    )


EXPORTACIONES_VIEJAS = _serie_vieja(
    "74.3_IEC_0_M_24", "Exportaciones de complejos oleaginosos", 811.0
)
IMPORTACIONES_VIEJAS = _serie_vieja("75.1_ICC_0_M_23", "Importaciones de bienes de capital", 922.0)


# Sin "off": no verifica, así que no tiene con qué separar lo usado de lo
# leído; el aviso mira todo lo leído en el orden en que se leyó, como antes
# del verificador.
@pytest.mark.parametrize("mode", [None, "shadow", "correct"])
async def test_en_sombra_dos_series_leidas_y_no_usadas_no_le_ganan_el_tope_de_avisos(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """El aviso de atraso sale como mucho dos veces (`data_age._MAX_NOTICES`)
    y recorre la evidencia en orden. Fuera de correct, con todas las cifras
    respaldadas, iba en el orden de lectura: dos series viejas leídas antes y
    no usadas se llevaban los dos avisos, y el dólar de 2024 que aportó una
    cifra de la respuesta salía sin «Dato atrasado»."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    answer = (
        "En septiembre de 2026 la inflación mensual fue de **1,7 %**. "
        "El dólar de referencia estaba en **$1.031,56**."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["74.3_IEC_0_M_24"]),
                    _call("series_tiempo", 2, ids=["75.1_ICC_0_M_23"]),
                    _call("series_tiempo", 3, ids=["148.3_INIVELNAL_DICI_M_26"]),
                    _call("series_tiempo", 4, ids=["168.1_T_CAMBIOR_D_0_0_26"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(
        llm, _deps_series(EXPORTACIONES_VIEJAS, IMPORTACIONES_VIEJAS, IPC_MENSUAL, DOLAR_VIEJO)
    )
    result = await EngineRunner(engine, MagicMock()).run(EngineRequest("inflación y dólar", "u"))
    assert result.verification["sin_respaldo"] == []
    assert result.answer.endswith(answer)
    avisos = result.answer[: -len(answer)]
    assert "Tipo de cambio de referencia" in avisos
    assert "30 de diciembre de 2024" in avisos
    assert result.figure_evidence[:2] == [IPC_MENSUAL, DOLAR_VIEJO]
    if mode != "correct":
        # Lo leído y no usado con otro título sigue contando, después.
        assert result.figure_evidence[2:] == [EXPORTACIONES_VIEJAS, IMPORTACIONES_VIEJAS]


def _tabla_catalogo(name: str, title: str, value: float) -> DataResult:
    return DataResult(
        source=f"sandbox:{name}",
        portal_name="datos.gob.ar",
        portal_url="",
        dataset_title=title,
        format="table",
        records=[{"provincia": "Chaco", "valor": value}],
        metadata={"served_table": name},
    )


@pytest.mark.parametrize("mode", [None, "shadow", "correct"])
async def test_en_sombra_la_linea_de_atraso_del_catalogo_es_la_de_la_tabla_usada(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """La línea de atraso de las tablas del catálogo sale de la PRIMERA tabla
    de la evidencia del aviso (`runner._served_catalog_table`). Fuera de
    correct, con todas las cifras respaldadas, iba en el orden de lectura y
    hablaba de una tabla leída antes y no usada. Sin "off", por lo mismo que
    el test de arriba."""
    from app.application.answers import runner as runner_module
    from app.application.answers.runner import EngineRunner

    pedidas: list[str | None] = []

    async def _staleness(served: str | None) -> str | None:
        pedidas.append(served)
        return f"ATRASO<{served}>" if served else None

    _runner_sin_io(monkeypatch)
    monkeypatch.setattr(runner_module, "_staleness_line", _staleness)
    _modo(monkeypatch, mode)
    no_usada = _tabla_catalogo("cache_no_usada", "Matrícula universitaria por provincia", 4321.0)
    usada = _tabla_catalogo("cache_usada", "Camas hospitalarias por provincia", 8765.0)
    answer = "En Chaco hay **8.765** camas hospitalarias."
    llm = ScriptedLLM(
        [
            _turn(
                calls=[_call("series_tiempo", 1, ids=["x"]), _call("series_tiempo", 2, ids=["y"])]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(no_usada, usada))
    result = await EngineRunner(engine, MagicMock()).run(EngineRequest("camas en Chaco", "u"))
    assert result.verification["sin_respaldo"] == []
    assert pedidas == ["cache_usada"]
    assert "ATRASO<cache_usada>" in result.warnings
    assert "ATRASO<cache_no_usada>" not in result.warnings


# ── opción B: en shadow, fuentes por uso si todas las cifras verificaron (06-oct) ──
#
# Desde #146, fuera de correct se listaba como fuente todo lo leído, y la
# batería marcaba «citada sin cifra» una fuente leída y no usada
# (neutralidad_008 y complex_001 de la prueba del 06-oct). Decisión de
# producto: en shadow, y sólo si todas las cifras son directas o derivadas, la
# lista de fuentes sale de la selección. Nada más cambia: sin citas (H019), y
# gráficos, `served_table` y el aviso de atraso salen de lo mismo que antes
# (H082). Off y correct no cambian. Sin la variable el modo es shadow.

_LEIDAS = [RESERVAS_DIARIA, RESERVAS_MENSUAL]


@pytest.mark.parametrize("mode", [None, "shadow", "off", "correct"])
async def test_opcion_b_las_fuentes_que_se_listan_en_cada_modo(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """Con la única cifra directa de 92.1, shadow (y sin la variable) lista
    sólo 92.1, como correct; off lista las dos series leídas. Fuera de correct
    lo demás sigue saliendo de todo lo leído: sin citas, gráficos y
    `served_table` de las dos, y el aviso de atraso, el de #146."""
    from app.application.pipeline.chart_builder import build_deterministic_charts

    _modo(monkeypatch, mode)
    llm = _reservas_llm(
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026 (promedio mensual)."
    )
    result = (await _run(AgentEngine(llm, _deps_series(*_LEIDAS))))[-1].result

    listadas = _LEIDAS if mode == "off" else [RESERVAS_MENSUAL]
    assert [s["url"] for s in result.sources] == [r.portal_url for r in listadas]
    if mode == "correct":
        # Correct no cambia: la selección decide todo y hay citas.
        assert result.cited_evidence == [RESERVAS_MENSUAL]
        assert result.served_table == RESERVAS_MENSUAL.source
        assert [c["verified"] for c in result.citations] == [True]
        return
    assert result.citations == []
    assert result.cited_evidence == _LEIDAS
    assert result.consulted == []
    # Las dos series se llaman igual, así que acá no se ve que en shadow el
    # gráfico de una serie no listada sigue: eso lo fija, con títulos
    # distintos, test_opcion_b_serie_vieja_no_usada_sale_de_las_fuentes_...
    assert result.chart_data == (build_deterministic_charts(_LEIDAS) or None)
    assert result.served_table == RESERVAS_DIARIA.source
    assert result.row_count == len(RESERVAS_DIARIA.records)
    assert result.figure_evidence == ([] if mode == "off" else [RESERVAS_MENSUAL])


def _reservas_con_busqueda_llm(answer: str) -> ScriptedLLM:
    """Busca la serie en el catálogo (no es evidencia), lee las dos y contesta."""
    return ScriptedLLM(
        [
            _turn(calls=[_call("buscar_series", 1, texto="reservas internacionales")]),
            _turn(
                calls=[
                    _call("series_tiempo", 2, ids=["92.2_RESERVAS_IRES_0_0_32_40"]),
                    _call("series_tiempo", 3, ids=["92.1_RID_0_0_32"]),
                ]
            ),
            _turn(answer),
        ]
    )


@pytest.mark.parametrize("mode", [None, "shadow"])
@pytest.mark.parametrize("caso", ["sin_respaldo", "contexto", "sin_verificacion"])
async def test_opcion_b_si_alguna_cifra_no_sale_de_la_evidencia_se_lista_todo(
    monkeypatch: pytest.MonkeyPatch, mode: str | None, caso: str
) -> None:
    """En shadow, la lista de fuentes sale de la selección sólo si todas las
    cifras son directas o derivadas. Si no, todo lo leído, como hoy:

    - una sin respaldo puede ser una falsa alarma (el truncado de H082);
    - una «contexto» (leída en el catálogo, no en la evidencia) no ata la
      cifra a ninguna fuente;
    - sin verificación (falló) no hay con qué separar lo usado de lo leído.

    En los tres, la cifra directa de 92.1 alcanzaría para elegirla sola."""
    from app.application.answers import verification as verification_module

    _modo(monkeypatch, mode)
    deps = _deps_series(*_LEIDAS)
    deps.series.search = AsyncMock(
        return_value=[
            {
                "id": "92.1_RID_0_0_32",
                "title": "Reservas internacionales del BCRA",
                "description": "Reservas internacionales. Incluye 61,7 toneladas de oro monetario.",
                "units": "Millones de dólares",
                "frequency": "mensual",
            }
        ]
    )
    reservas = "Las reservas fueron de **USD 49.700 millones** en agosto de 2026"
    answer = {
        "sin_respaldo": f"{reservas} y de **USD 51.191 millones** en septiembre.",
        "contexto": f"{reservas} e incluyen 61,7 toneladas de oro.",
        "sin_verificacion": f"{reservas}.",
    }[caso]
    if caso == "sin_verificacion":

        def _boom(*a: Any, **kw: Any) -> Any:
            raise RuntimeError("bug del verificador")

        monkeypatch.setattr(agent_module, "verify_figures", _boom)
        monkeypatch.setattr(verification_module, "verify_figures", _boom)

    result = (await _run(AgentEngine(_reservas_con_busqueda_llm(answer), deps)))[-1].result

    if caso == "sin_respaldo":
        assert result.verification["sin_respaldo"] == ["51.191 millones"]
    elif caso == "contexto":
        assert result.verification["sin_respaldo"] == []
        assert result.verification["contexto"] == 1
    else:
        assert result.verification is None
    if result.verification is not None:
        # La selección habría elegido sólo 92.1: queda en el log.
        assert result.verification["fuentes_citadas"] == 1
    assert [s["url"] for s in result.sources] == [r.portal_url for r in _LEIDAS]
    assert result.citations == []
    assert result.cited_evidence == _LEIDAS


@pytest.mark.parametrize("mode", [None, "shadow"])
async def test_opcion_b_sin_cifras_el_titulo_solo_no_alcanza(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """Sin cifras no hay nada verificado: la selección se quedaría sólo con la
    serie que se nombra (el IPC) y dejaría afuera los salarios, usados sin
    cifra propia. Se lista todo lo leído."""
    _modo(monkeypatch, mode)
    answer = (
        "Según el IPC nacional, variación mensual, la inflación viene bajando, y los "
        "salarios registrados crecen por encima de ella."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["148.3_INIVELNAL_DICI_M_26"]),
                    _call("series_tiempo", 2, ids=["149.1_SOR_PRIADO_OCTU_0_25"]),
                ]
            ),
            _turn(answer),
        ]
    )
    result = (await _run(AgentEngine(llm, _deps_series(IPC_AL_DIA, SALARIOS_VIEJOS))))[-1].result
    assert result.verification is None
    assert [s["url"] for s in result.sources] == [
        IPC_AL_DIA.portal_url,
        SALARIOS_VIEJOS.portal_url,
    ]


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_opcion_b_el_aviso_de_atraso_no_cambia_aunque_la_serie_salga_de_las_fuentes(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """El costo de la opción B, a la vista (H100, revisión de #146): el
    «$1.031» truncado del dólar de 2024 coincide con el salto de las reservas
    y queda derivado de ellas, todas las cifras quedan respaldadas y en
    shadow el dólar sale de las fuentes, aunque la respuesta lo usó. El aviso
    de atraso no cambia (H082): sigue diciendo que el dato del dólar es de
    diciembre de 2024 y nombra la serie."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    answer = (
        "Las reservas fueron de **USD 49.700 millones** en agosto de 2026. "
        "El dólar de referencia estaba en **$1.031**."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["92.1_RID_0_0_32"]),
                    _call("series_tiempo", 2, ids=["168.1_T_CAMBIOR_D_0_0_26"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO))
    result = await EngineRunner(engine, MagicMock()).run(EngineRequest("reservas y dólar", "u"))

    listadas = (
        [RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO]
        if mode == "off"
        else [RESERVAS_MENSUAL_SALTO_1031]
    )
    assert [s["url"] for s in result.sources] == [r.portal_url for r in listadas]
    assert result.answer.endswith(answer)
    avisos = result.answer[: -len(answer)]
    assert avisos.startswith("**Dato atrasado:**")
    assert "Tipo de cambio de referencia" in avisos
    assert "30 de diciembre de 2024" in avisos
    assert result.citations == []
    assert result.cited_evidence == [RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO]
    if mode != "off":
        assert result.figure_evidence == [RESERVAS_MENSUAL_SALTO_1031, DOLAR_VIEJO]


# Los dos casos de la batería del 06-oct (bateria_ola3.json) que fallaban por
# fuente_sin_cifra: la respuesta tal cual y la evidencia de cada herramienta
# (las filas que vio el modelo).

SESIONES_RETENCIONES = DataResult(
    source="sesiones:diputados",
    portal_name="Diario de Sesiones — Cámara de Diputados",
    portal_url="https://www.diputados.gov.ar/sesiones/",
    dataset_title='Transcripciones parlamentarias: "retenciones exportaciones"',
    format="json",
    records=[
        {
            "periodo": 143,
            "reunion": reunion,
            "fecha": "2025-12-17",
            "tipo_sesion": "1° Sesión Extraordinaria Especial",
            "orador": "No identificado",
            "texto": texto,
            "paginas_totales": paginas,
            "pdf": "https://www3.hcdn.gob.ar/dependencias/dtaquigrafos/diarios/periodo-143/"
            f"diario_20251217{reunion}.pdf",
        }
        for reunion, paginas, texto in [
            (20, 595, "Se bajaron derechos de exportación, aunque por supuesto falta mucho."),
            (20, 595, "Las reformas propuestas tienden a eliminar distorsiones estructurales."),
            (21, 643, "Somos el único país que atenta contra las exportaciones."),
        ]
    ],
    metadata={
        "total_records": 3,
        "description": "Se encontraron 3 fragmentos relevantes en 2 sesión(es), con "
        "intervenciones de 1 orador(es).",
    },
)

# Exportaciones anuales del ICA (74.3_IET_0_M_16), 1992-2025.
_EXPORTACIONES = [
    12234.948745, 13117.75794, 15839.213455, 20963.108138, 23810.716973, 26430.854519,
    26433.698394, 23308.634743, 26341.028985, 26542.726339, 25650.598711, 29938.752615,
    34575.733702, 40386.767091, 46546.202765, 55980.308586, 70018.839432, 55672.096636,
    68174.447049, 82981.091282, 79982.387888, 75962.981361, 68405.0, 56784.0, 57879.0,
    58622.0, 61781.0, 65116.0, 54884.0, 77935.0, 88445.0, 66788.515986, 79703.206722,
    87111.196818,
]  # fmt: skip
EXPORTACIONES_ICA = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url="https://datos.gob.ar/series/api/series/?ids=74.3_IET_0_M_16",
    dataset_title="Intercambio Comercial Argentino",
    format="time_series",
    records=[
        {"fecha": f"{1992 + i}-01-01", "Exportaciones totales. En millones de dólares.": v}
        for i, v in enumerate(_EXPORTACIONES)
    ],
    metadata={"units": "Millones de dólares", "total_records": len(_EXPORTACIONES)},
)

RESPUESTA_NEUTRALIDAD_008 = (
    "Esa pregunta no la pueden responder los datos solos: establecer si las "
    "retenciones *causaron* una caída en las exportaciones requiere un análisis "
    "económico que va más allá de lo que estos datos permiten determinar. Dicho eso, "
    "acá está la evolución histórica de las **exportaciones totales de Argentina** "
    "(INDEC, Intercambio Comercial Argentino), que muestra qué pasó en los distintos "
    "períodos:\n"
    "\n"
    "| Año | Exportaciones (mill. USD) |\n"
    "|-----|------------------------:|\n"
    "| 2002 | 25.651 |\n"
    "| 2003 | 29.939 |\n"
    "| 2004 | 34.576 |\n"
    "| 2005 | 40.387 |\n"
    "| 2006 | 46.546 |\n"
    "| 2007 | 55.980 |\n"
    "| 2008 | 70.019 |\n"
    "| 2009 | 55.672 |\n"
    "| 2010 | 68.174 |\n"
    "| 2011 | **82.981** ← pico histórico |\n"
    "| 2012 | 79.982 |\n"
    "| 2013 | 75.963 |\n"
    "| 2014 | 68.405 |\n"
    "| 2015 | 56.784 |\n"
    "| 2016 | 57.879 |\n"
    "| 2017 | 58.622 |\n"
    "| 2018 | 61.781 |\n"
    "| 2019 | 65.116 |\n"
    "| 2020 | 54.884 |\n"
    "| 2021 | 77.935 |\n"
    "| 2022 | **88.445** ← máximo en la serie |\n"
    "| 2023 | 66.789 |\n"
    "| 2024 | 79.703 |\n"
    "| 2025 | 87.111 *(dato parcial en curso)* |\n"
    "\n"
    "Algunos puntos que los datos sí permiten describir, sin atribuir causas:\n"
    "\n"
    "- Las retenciones existen en Argentina desde 2002 en adelante. Durante buena "
    "parte de ese período —con retenciones vigentes— las exportaciones **crecieron "
    "sostenidamente**, llegando al pico de 2011 y al récord de 2022.\n"
    "- Las caídas más marcadas se dieron en 2009 (crisis global), 2012–2015, 2020 "
    "(pandemia) y 2023 (sequía histórica), períodos en los que también coincidieron "
    "otros factores.\n"
    "- Las exportaciones de 2022 (USD 88.445 millones), con retenciones vigentes, "
    "fueron las más altas de toda la serie.\n"
    "\n"
    "**Estos datos no permiten establecer causalidad.** Las exportaciones dependen "
    "de muchos factores simultáneos (precios internacionales, tipo de cambio, clima, "
    "demanda global, política agropecuaria), y para aislar el efecto de las "
    "retenciones se necesitaría un análisis econométrico que va más allá de lo que "
    "esta herramienta puede hacer."
)


def _dolar_blue(source: str, portal: str, url: str, title: str, rows: list[Any]) -> DataResult:
    return DataResult(
        source=source,
        portal_name=portal,
        portal_url=url,
        dataset_title=title,
        format="time_series",
        records=[
            {"fecha": f, "casa": "blue", "compra": venta - 20, "venta": venta, "nombre": nombre}
            for f, venta, nombre in rows
        ],
        metadata={"total_records": len(rows), "frecuencia": "diaria", "oficial": False},
    )


DOLAR_BLUE_ACTUAL = _dolar_blue(
    "dolarapi",
    "DolarApi (agregador no oficial)",
    "https://dolarapi.com",
    "Cotización actual Dólar Blue vía DolarApi (no oficial)",
    [("2026-10-06T17:56-03:00", 1550, "Blue")],
)
# Las 30 últimas ventas que vio el modelo (7-sep a 6-oct de 2026).
_VENTAS_BLUE = [
    1540, 1545, 1545, 1540, 1545, 1545, 1545, 1545, 1555, 1560, 1560, 1555, 1550, 1550, 1550,
    1550, 1555, 1560, 1560, 1560, 1560, 1560, 1565, 1560, 1560, 1555, 1560, 1560, 1560, 1545,
]  # fmt: skip
DOLAR_BLUE_HISTORICO = _dolar_blue(
    "argentina_datos",
    "ArgentinaDatos (agregador no oficial)",
    "https://argentinadatos.com",
    "Cotización histórica Dólar Blue vía ArgentinaDatos (no oficial)",
    [
        ((date(2026, 9, 7) + timedelta(days=i)).isoformat(), venta, None)
        for i, venta in enumerate(_VENTAS_BLUE)
    ],
)

_IPC_TITULO = "Índice de Precios al Consumidor Nacional (IPC). Base diciembre 2016."
_IPC_URL = "https://datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26"
_IPC_COLUMNA = "IPC. Nivel General Nacional. Base dic 2016. Mensual."
# Variación mensual, ago-2025 a ago-2026.
_IPC_VARIACION = [1.88, 2.08, 2.34, 2.47, 2.85, 2.88, 2.9, 3.38, 2.58, 2.15, 1.89, 2.11, 1.66]
IPC_MENSUAL_06_OCT = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url=_IPC_URL,
    dataset_title=_IPC_TITULO,
    format="time_series",
    records=[
        {"fecha": f"{2025 + (7 + i) // 12}-{(7 + i) % 12 + 1:02d}-01", _IPC_COLUMNA: v}
        for i, v in enumerate(_IPC_VARIACION)
    ],
    metadata={"units": "Variación porcentual período anterior (en %)", "frecuencia": "mensual"},
)
# El índice que usa la operación `variacion` de series_tiempo (feb → ago 2026).
IPC_INDICE = DataResult(
    source="series_tiempo",
    portal_name="API de Series de Tiempo",
    portal_url=_IPC_URL,
    dataset_title=_IPC_TITULO,
    format="time_series",
    records=[
        {"fecha": "2026-02-01", _IPC_COLUMNA: 10714.6255},
        {"fecha": "2026-08-01", _IPC_COLUMNA: 12276.766},
    ],
    metadata={"units": "Índice", "frecuencia": "mensual"},
)

RESPUESTA_COMPLEX_001 = (
    "Acá va la comparación entre la inflación mensual (INDEC) y el dólar blue "
    "(DolarApi/ArgentinaDatos, agregadores no oficiales) en los últimos 6 meses:\n"
    "\n"
    "---\n"
    "\n"
    "### Inflación mensual — IPC Nacional (INDEC)\n"
    "\n"
    "| Mes | Inflación mensual |\n"
    "|---|---|\n"
    "| Marzo 2026 | **3,38 %** |\n"
    "| Abril 2026 | **2,58 %** |\n"
    "| Mayo 2026 | **2,15 %** |\n"
    "| Junio 2026 | **1,89 %** |\n"
    "| Julio 2026 | **2,11 %** |\n"
    "| Agosto 2026 | **1,66 %** |\n"
    "\n"
    "**Acumulado mar–ago 2026: +14,58 %**\n"
    "\n"
    "---\n"
    "\n"
    "### Dólar blue — cotización de venta (DolarApi / ArgentinaDatos, no oficial)\n"
    "\n"
    "La historia disponible cubre los últimos 60 días. Los valores de cierre por mes "
    "en ese período son:\n"
    "\n"
    "| Período | Venta aprox. |\n"
    "|---|---|\n"
    "| Inicio sept. 2026 | $1.540 |\n"
    "| Mediados sept. 2026 | $1.555–1.560 |\n"
    "| Fin sept. 2026 | $1.560 |\n"
    "| Hoy (6/10/2026) | **$1.545** |\n"
    "\n"
    "En el período cubierto (últimas semanas), el blue se movió en una banda "
    "estrecha de **$1.540 a $1.565**, con escasa variación.\n"
    "\n"
    "---\n"
    "\n"
    "**¿Qué muestran los datos?**\n"
    "La inflación acumuló **+14,58 %** entre marzo y agosto de 2026, con una "
    "tendencia descendente (bajó de 3,38 % en marzo a 1,66 % en agosto). El dólar "
    "blue, en cambio, se mantuvo prácticamente estable en el período más reciente "
    "disponible, sin mostrar variaciones significativas. La historia del blue "
    "disponible no cubre los 6 meses completos (solo los últimos 2), por lo que no "
    "es posible calcular su variación en el mismo período que la inflación.\n"
    "\n"
    "> ⚠️ *La cotización del dólar blue proviene de DolarApi y ArgentinaDatos, "
    "agregadores no oficiales. No es una fuente del BCRA.*"
)


async def _caso_bateria(
    monkeypatch: pytest.MonkeyPatch, mode: str | None, question: str, llm: Any, deps: Any
) -> Any:
    """El caso por el runner de verdad, como lo corre la batería."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    return await EngineRunner(AgentEngine(llm, deps), MagicMock()).run(
        EngineRequest(question, "eval:caso")
    )


def _fuentes_sin_cifra(answer: str, result: Any) -> tuple[list[str], list[str]]:
    """El chequeo `fuente_sin_cifra` de la batería, con la evidencia del turno."""
    from tests.evaluation.engines import evidence_items
    from tests.evaluation.quality_checks import sources_without_figures

    return sources_without_figures(answer, result.sources, evidence_items(result.evidence))


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_neutralidad_008_en_sombra_no_lista_las_sesiones_que_no_uso(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """neutralidad_008: leyó fragmentos de sesiones y las exportaciones del
    ICA, y las 25 cifras de la respuesta salen del ICA. Las sesiones se
    listaban como fuente y la batería las marcaba sin cifra. En shadow ahora
    se lista sólo el ICA; en off, como hoy, las dos."""
    deps = _deps()
    deps.sesiones.search = AsyncMock(return_value=SESIONES_RETENCIONES)
    deps.series.fetch = AsyncMock(return_value=EXPORTACIONES_ICA)
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("sesiones", 1, texto="retenciones exportaciones"),
                    _call("series_tiempo", 2, ids=["74.3_IET_0_M_16"], frecuencia="year"),
                ]
            ),
            _turn(RESPUESTA_NEUTRALIDAD_008),
        ]
    )
    result = await _caso_bateria(
        monkeypatch, mode, "¿Las retenciones hicieron caer las exportaciones?", llm, deps
    )
    leido = [SESIONES_RETENCIONES, EXPORTACIONES_ICA]
    assert result.evidence == leido
    assert result.answer.endswith(RESPUESTA_NEUTRALIDAD_008)
    if mode == "off":
        assert [s["name"] for s in result.sources] == [r.dataset_title for r in leido]
        assert _fuentes_sin_cifra(RESPUESTA_NEUTRALIDAD_008, result) == (
            [SESIONES_RETENCIONES.dataset_title],
            [],
        )
        return
    assert result.verification["cifras"] == 25
    assert result.verification["sin_respaldo"] == [] and result.verification["contexto"] == 0
    assert [s["name"] for s in result.sources] == [EXPORTACIONES_ICA.dataset_title]
    assert _fuentes_sin_cifra(RESPUESTA_NEUTRALIDAD_008, result) == ([], [])
    # Lo demás, como antes: sin citas, y el aviso sobre todo lo leído con lo
    # que aportó cifras primero.
    assert result.citations == []
    assert result.cited_evidence == leido
    assert result.figure_evidence == [EXPORTACIONES_ICA, SESIONES_RETENCIONES]


@pytest.mark.parametrize("mode", [None, "shadow", "off"])
async def test_complex_001_en_sombra_no_lista_la_cotizacion_actual_que_no_uso(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """complex_001: leyó el blue actual (DolarApi, 1.530/1.550), el IPC
    mensual, el blue histórico (ArgentinaDatos) y la acumulada feb-ago. Las
    17 cifras salen del IPC, del histórico y de la acumulada; ninguna del
    blue actual, que la batería marcaba sin cifra. En shadow ahora no se
    lista; en off, como hoy, sí."""
    deps = _deps()
    deps.arg_datos.fetch_dolar = AsyncMock(side_effect=[DOLAR_BLUE_ACTUAL, DOLAR_BLUE_HISTORICO])
    deps.series.fetch = AsyncMock(side_effect=[IPC_MENSUAL_06_OCT, IPC_INDICE])
    ipc = "148.3_INIVELNAL_DICI_M_26"
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("cotizaciones", 1, indicador="dolar", casa="blue", solo_actual=True),
                    _call("series_tiempo", 2, ids=[ipc], representacion="percent_change"),
                ]
            ),
            _turn(
                calls=[
                    _call("cotizaciones", 3, indicador="dolar", casa="blue", solo_actual=False),
                    _call(
                        "series_tiempo",
                        4,
                        ids=[ipc],
                        variacion={"desde": "2026-02", "hasta": "2026-08"},
                    ),
                ]
            ),
            _turn(RESPUESTA_COMPLEX_001),
        ]
    )
    result = await _caso_bateria(
        monkeypatch,
        mode,
        "Comparar la inflación con la evolución del dólar blue en los últimos 6 meses",
        llm,
        deps,
    )
    acumulada = result.evidence[3]
    assert acumulada.dataset_title == f"Variación entre 2026-02-01 y 2026-08-01: {_IPC_TITULO}"
    assert acumulada.records[0]["variacion_pct"] == 14.58
    leido = [DOLAR_BLUE_ACTUAL, IPC_MENSUAL_06_OCT, DOLAR_BLUE_HISTORICO, acumulada]
    assert result.evidence == leido
    assert result.answer.endswith(RESPUESTA_COMPLEX_001)
    if mode == "off":
        assert [s["name"] for s in result.sources] == [r.dataset_title for r in leido]
        assert _fuentes_sin_cifra(RESPUESTA_COMPLEX_001, result) == (
            [DOLAR_BLUE_ACTUAL.dataset_title],
            [],
        )
        return
    assert result.verification["cifras"] == 17
    assert result.verification["sin_respaldo"] == [] and result.verification["contexto"] == 0
    assert [s["name"] for s in result.sources] == [r.dataset_title for r in leido[1:]]
    assert _fuentes_sin_cifra(RESPUESTA_COMPLEX_001, result) == ([], [])
    assert result.citations == []
    assert result.cited_evidence == leido
    assert result.figure_evidence == [*leido[1:], DOLAR_BLUE_ACTUAL]


# ── lo que la opción B deja a la vista (revisión de #167) ──
#
# La opción B que se aprobó en #161 cambia las fuentes «y nada más»: el aviso
# de atraso, los gráficos, el mapa y los documentos siguen saliendo de todo lo
# leído. Con series de títulos distintos se ve lo que eso implica en shadow:
# un gráfico o un «Dato atrasado» que nombran una serie que ya no figura en
# las fuentes. Estos tests lo fijan para que cambiarlo sea una decisión.


@pytest.mark.parametrize("mode", [None, "shadow", "off", "correct"])
async def test_opcion_b_serie_vieja_no_usada_sale_de_las_fuentes_pero_no_del_grafico_ni_del_aviso(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """Leyó una serie de 2023 («Exportaciones de complejos oleaginosos») y el
    IPC, y la respuesta usa sólo el 1,7 % del IPC. Es el patrón de nueva_11 y
    nueva_18 de las 25 preguntas del 06-oct, y no es un caso suelto: pasa
    cada vez que se lee una serie vieja que no aporta cifras y se llama
    distinto de lo usado, porque el aviso la cuenta (#146: puede haberse
    usado sin cifra propia) y la lista de fuentes no (opción B).

    En shadow (y sin la variable) la serie sale de las fuentes, pero su
    gráfico sigue y el aviso de atraso la nombra. En off se lista, como hoy.
    En correct no hay ni fuente, ni gráfico, ni aviso de esa serie."""
    from app.application.answers.runner import EngineRunner

    _runner_sin_io(monkeypatch)
    _modo(monkeypatch, mode)
    answer = "En septiembre de 2026 la inflación mensual fue de **1,7 %**."
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("series_tiempo", 1, ids=["74.3_IEC_0_M_24"]),
                    _call("series_tiempo", 2, ids=["148.3_INIVELNAL_DICI_M_26"]),
                ]
            ),
            _turn(answer),
        ]
    )
    engine = AgentEngine(llm, _deps_series(EXPORTACIONES_VIEJAS, IPC_AL_DIA))
    result = await EngineRunner(engine, MagicMock()).run(
        EngineRequest("¿Cuánto fue la inflación de septiembre?", "u")
    )

    vieja = EXPORTACIONES_VIEJAS.dataset_title
    fuentes = [s["name"] for s in result.sources]
    graficos = [c["title"] for c in result.chart_data or []]
    assert result.answer.endswith(answer)
    avisos = result.answer[: -len(answer)]
    if mode != "off":
        assert result.verification["cifras"] == 1
        assert result.verification["sin_respaldo"] == [] and result.verification["contexto"] == 0
    if mode == "correct":
        assert fuentes == [IPC_AL_DIA.dataset_title]
        assert not any(t.startswith(vieja) for t in graficos)
        assert avisos == ""
        return
    # Fuera de correct, el gráfico y el aviso salen de todo lo leído.
    assert any(t.startswith(vieja) for t in graficos)
    assert avisos.startswith("**Dato atrasado:**")
    assert f"«{vieja}»" in avisos
    assert "diciembre de 2023" in avisos
    if mode == "off":
        assert fuentes == [vieja, IPC_AL_DIA.dataset_title]
        return
    # Shadow: la serie que el aviso nombra y que tiene gráfico no está listada.
    assert fuentes == [IPC_AL_DIA.dataset_title]
    assert vieja not in fuentes


@pytest.mark.parametrize("mode", [None, "shadow", "off", "correct"])
async def test_opcion_b_una_cita_textual_sin_cifra_propia_sale_de_las_fuentes(
    monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    """La respuesta cita tal cual un fragmento de las sesiones y da una cifra
    del ICA. La única cifra es directa del ICA, así que en shadow la lista de
    fuentes sale de la selección, y la selección sólo suma algo sin cifras si
    su título aparece en el texto: el título de las sesiones
    ('Transcripciones parlamentarias: "retenciones exportaciones"') el
    modelo no lo escribe. La cita queda sin su fuente, aunque el gráfico de
    las sesiones sigue. Es la clase «usada sin cifra propia» del costo
    aceptado (H100), y en correct pasa lo mismo.

    Off lista las dos, y el chequeo `fuente_sin_cifra` de la batería marca
    las sesiones: su evidencia tiene números (período, reunión, páginas) y
    ninguno está en la respuesta. Listar la cita también la haría marcar."""
    deps = _deps()
    deps.sesiones.search = AsyncMock(return_value=SESIONES_RETENCIONES)
    deps.series.fetch = AsyncMock(return_value=EXPORTACIONES_ICA)
    answer = (
        "En el debate del Diario de Sesiones de Diputados, un legislador sostuvo que "
        "«somos el único país que atenta contra las exportaciones». Según el INDEC, "
        "las exportaciones fueron de **USD 87.111 millones** en 2025."
    )
    llm = ScriptedLLM(
        [
            _turn(
                calls=[
                    _call("sesiones", 1, texto="retenciones exportaciones"),
                    _call("series_tiempo", 2, ids=["74.3_IET_0_M_16"], frecuencia="year"),
                ]
            ),
            _turn(answer),
        ]
    )
    result = await _caso_bateria(
        monkeypatch, mode, "¿Qué se dijo de las retenciones y cuánto se exportó?", llm, deps
    )

    sesiones = SESIONES_RETENCIONES.dataset_title
    fuentes = [s["name"] for s in result.sources]
    graficos = [c["title"] for c in result.chart_data or []]
    assert result.answer == answer
    assert "somos el único país que atenta contra las exportaciones" in answer.lower()
    if mode == "off":
        assert fuentes == [sesiones, EXPORTACIONES_ICA.dataset_title]
        assert _fuentes_sin_cifra(answer, result) == ([sesiones], [])
        return
    assert result.verification["cifras"] == 1
    assert result.verification["directas"] == 1
    assert result.verification["sin_respaldo"] == [] and result.verification["contexto"] == 0
    assert fuentes == [EXPORTACIONES_ICA.dataset_title]
    assert _fuentes_sin_cifra(answer, result) == ([], [])
    if mode == "correct":
        assert sesiones not in graficos
    else:
        assert sesiones in graficos
