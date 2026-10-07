"""Qué evidencia ve el juez de alucinación de la batería.

Batería v3 del 07-oct-2026 en staging: sesiones_002 y sesiones_003 fallaron
dos veces seguidas por el juez de alucinación (0,7 y 0,6), que marcó como
inventados "Temu, Shein, Alibaba", la Ley de Compromiso Nacional para la
Estabilidad Fiscal, el dictamen de García Aresca, Massot y Rizzotti, la
oferta de tropas para Gaza o la ANDIS. Todo eso está en ``sesion_chunks``.

La búsqueda le devolvió al agente 12 fragmentos de ~3.400 caracteres. El
modelo lee los primeros que entran en ``MAX_CONTENT_CHARS`` (tres); el juez
leía el final del texto de los 12, cortado a 20.000 caracteres: cinco
fragmentos y medio del final. Nunca veía lo que leyó el modelo.

El fixture tiene las respuestas reales y los fragmentos de staging que dicen
lo marcado (únicos en las 1.030 filas), más los seis que el juez sí vio: con
la fórmula de antes reproduce byte por byte la evidencia guardada en el
reporte del 07-oct (``sha256_evidencia_juez``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.evaluation.engines import (
    AgentEngine,
    EngineOutput,
    LegacyGraphEngine,
    summarize_evidence,
)
from tests.evaluation.run_eval import _judge_run

FIXTURE = Path(__file__).parents[1] / "evaluation" / "fixtures" / "sesiones_juez_2026_10_07.json"
CASOS: dict[str, dict[str, Any]] = json.loads(FIXTURE.read_text(encoding="utf-8"))["casos"]
TOPE = 20_000


def _resultado(caso: dict[str, Any]) -> SimpleNamespace:
    """El ``DataResult`` de la búsqueda, con las filas que se conocen."""
    return SimpleNamespace(
        dataset_title=caso["titulo"],
        portal_name=caso["portal"],
        portal_url=caso["url"],
        metadata={"total_records": caso["filas_devueltas"]},
        records=caso["filas"],
    )


def _lo_que_vio_el_juez(caso: dict[str, Any]) -> str:
    """La evidencia como la armaba la batería hasta este arreglo, para una
    sola fuente: el final del texto de las filas, cortado a ciegas."""
    header = f"## {caso['titulo']} — {caso['portal']} ({caso['filas_devueltas']} filas)"
    body = "\n".join(str(r) for r in caso["filas"])
    room = TOPE - 2 - len(header) - 1
    return header + "\n…" + body[-room:]


@pytest.mark.parametrize("cid", sorted(CASOS))
def test_lo_marcado_como_inventado_estaba_en_lo_que_el_juez_no_veia(cid: str) -> None:
    caso = CASOS[cid]
    vio = _lo_que_vio_el_juez(caso)
    assert hashlib.sha256(vio.encode("utf-8")).hexdigest() == caso["sha256_evidencia_juez"]
    filas = "\n".join(str(r) for r in caso["filas"])
    for en_la_fuente, en_la_respuesta in caso["marcadas"]:
        assert en_la_respuesta in caso["respuesta"]
        assert en_la_fuente in filas
        assert en_la_fuente not in vio


@pytest.mark.parametrize("cid", sorted(CASOS))
def test_el_juez_ve_de_donde_salen_las_afirmaciones_de_la_respuesta(cid: str) -> None:
    caso = CASOS[cid]
    evidencia = summarize_evidence([_resultado(caso)], caso["respuesta"])
    assert len(evidencia) <= TOPE
    # Cada afirmación, con el fragmento entero de donde sale: el juez no lee
    # una cita cortada a la mitad.
    filas = [str(r) for r in caso["filas"]]
    for en_la_fuente, _ in caso["marcadas"]:
        assert any(en_la_fuente in fila and fila in evidencia for fila in filas)


# Cada afirmación que el juez marcó, cambiada por una que no está en ningún fragmento.
_INVENTADAS = {
    "sesiones_002": [
        ("Temu, Shein, Alibaba", "Netflix, Spotify y Disney"),
        ("Ley de Compromiso Nacional para la Estabilidad Fiscal", "Ley de Soberanía Tributaria"),
        ("3,4 %", "1,9 %"),
        ("García Aresca, Massot y Rizzotti", "Pérez Lindo, Ocampo y Suárez"),
        ("(27.838)", "(27.901)"),
        ("(27.839)", "(27.902)"),
        ("(27.840)", "(27.903)"),
    ],
    "sesiones_003": [
        ("oferta de tropas argentinas para Gaza", "compra de submarinos nucleares a Noruega"),
        ("escándalo en la ANDIS", "escándalo en la AFIP"),
        ("Karina Milei y Mario Lugones", "Federico Sturzenegger y Luis Petri"),
    ],
}


@pytest.mark.parametrize("cid", sorted(CASOS))
def test_una_afirmacion_inventada_sigue_sin_respaldo_en_la_evidencia(cid: str) -> None:
    """La misma respuesta con lo marcado cambiado por algo inventado: el juez
    no ve nada que lo respalde, igual que el 07-oct, cuando lo puntuó 0,6-0,7."""
    caso = CASOS[cid]
    adulterada = caso["respuesta"]
    for real, inventada in _INVENTADAS[cid]:
        assert real in adulterada
        adulterada = adulterada.replace(real, inventada)
    evidencia = summarize_evidence([_resultado(caso)], adulterada)
    assert len(evidencia) <= TOPE
    for _, inventada in _INVENTADAS[cid]:
        assert inventada not in evidencia


@pytest.mark.parametrize("cid", sorted(CASOS))
def test_una_respuesta_inventada_no_trae_evidencia(cid: str) -> None:
    """Lo que no está en ninguna fila no elige filas: el juez ve lo mismo que sin respuesta."""
    caso = CASOS[cid]
    inventada = "Netflix, Spotify y Disney: ley 27.901 de criptoactivos, 4,7."
    filas = [str(r).lower() for r in caso["filas"]]
    for palabra in ("netflix", "spotify", "disney", "27.901", "criptoactivos", "4,7"):
        assert not any(re.search(rf"\b{re.escape(palabra)}\b", f) for f in filas)
    resultado = _resultado(caso)
    assert summarize_evidence([resultado], inventada) == summarize_evidence([resultado])


def test_si_la_fuente_entra_entera_la_respuesta_no_cambia_nada() -> None:
    serie = SimpleNamespace(
        dataset_title="IPC",
        portal_url="u",
        records=[{"fecha": f"2026-{m:02d}-01", "v": 2.0 + m / 10} for m in range(1, 9)],
    )
    respuesta = "La inflación de agosto de 2026 fue 2,8 %."
    assert summarize_evidence([serie], respuesta) == summarize_evidence([serie])


def test_sin_coincidencias_entran_el_principio_y_el_final() -> None:
    """Una tabla corta de filas largas que no entra y una respuesta que no
    coincide con nada (la cifra escrita de otra forma): entran la primera
    fila (lo que leyó el modelo) y la última (lo que se cita de una serie)."""
    filas = [{"fecha": f"2023-{i:03d}", "nota": "x" * 950, "valor": 1000.0 + i} for i in range(40)]
    tabla = SimpleNamespace(dataset_title="t", portal_url="u", records=filas)
    evidencia = summarize_evidence([tabla], "El último valor fue 1.039.")
    assert len(evidencia) <= TOPE
    assert str(filas[0]) in evidencia and str(filas[-1]) in evidencia
    assert "filas sin mostrar" in evidencia


def test_con_varias_fuentes_cada_una_respeta_su_parte_del_tope() -> None:
    caso = CASOS["sesiones_002"]
    serie = SimpleNamespace(
        dataset_title="Dólar",
        portal_url="u2",
        records=[{"fecha": "2026-10-06", "dolar": 1475.0}],
    )
    otra = SimpleNamespace(
        dataset_title="Reservas",
        portal_url="u3",
        records=[{"fecha": "2026-10-06", "reservas": 41234.0}],
    )
    evidencia = summarize_evidence([_resultado(caso), serie, otra], caso["respuesta"])
    assert len(evidencia) <= TOPE
    assert "1475.0" in evidencia and "41234.0" in evidencia
    # De los fragmentos entra uno solo: el que más coincide con la respuesta.
    assert "Temu, Shein o Alibaba" in evidencia or "Jorge Rizzotti" in evidencia


def test_una_fila_que_no_entra_entera_muestra_el_pedazo_de_lo_afirmado() -> None:
    """Un fragmento de 30.000 caracteres con lo citado en el medio y otro
    corto que no dice nada de eso: el juez ve el pedazo de lo citado."""
    larga = "relleno " * 2000 + "las plataformas chinas como Temu, Shein o Alibaba " + "más " * 3000
    filas = [{"texto": larga}, {"texto": "otro fragmento corto sobre el presupuesto"}]
    fuente = SimpleNamespace(dataset_title="t", portal_url="u", records=filas)
    evidencia = summarize_evidence([fuente], "Un orador habló de Temu y Shein.")
    assert len(evidencia) <= TOPE
    assert "Temu, Shein o Alibaba" in evidencia
    # Sin respuesta que lo pida, no hay por qué mostrar ese pedazo.
    assert "Temu" not in summarize_evidence([fuente])


N005 = json.loads(
    (FIXTURE.parent / "neutralidad_005_juez_2026_10_07.json").read_text(encoding="utf-8")
)
_IPC = "IPC. Nivel General Nacional. Base dic 2016. Mensual."


def _fuente(f: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        dataset_title=f["titulo"],
        portal_name=f["portal"],
        portal_url=f["url"],
        metadata={"total_records": f["total_records"], "units": f["unidades"]},
        records=f["filas"],
    )


def test_neutralidad_005_el_juez_sigue_viendo_la_serie_que_cita_la_respuesta() -> None:
    """Revisión de #172. neutralidad_005 aprobó el 07-oct con alucinación 0,0;
    el juez citó el 211,41 % como verificable. Las cuatro series no entran
    enteras (4 fuentes, ~5.000 caracteres cada una) y la respuesta nombra cada
    año de 2018 a 2026: con la selección por palabras, los años coincidían con
    las 78 filas del IPC, entraban las primeras y se perdía de 2020-11 a
    2023-12. Una serie se muestra como antes, con respuesta o sin ella."""
    fuentes = [_fuente(f) for f in N005["fuentes"]]
    respuesta = N005["respuesta"]
    evidencia = summarize_evidence(fuentes, respuesta)
    assert hashlib.sha256(evidencia.encode("utf-8")).hexdigest() == N005["sha256_evidencia_juez"]
    assert evidencia == summarize_evidence(fuentes)
    # Lo que la respuesta cita del IPC 78 filas, fila entera en la evidencia.
    citadas = [
        ("2020-12-01", 36.14, "53% → 36%"),
        ("2021-01-01", 38.53, "39% → 51%"),
        ("2021-12-01", 50.94, "39% → 51%"),
        ("2022-01-01", 50.69, "51% → **95%**"),
        ("2022-12-01", 94.79, "51% → **95%**"),
        ("2023-01-01", 98.83, "99% → **211%** (dic.)"),
        ("2023-12-01", 211.41, "211% en diciembre de 2023"),
    ]
    for fecha, valor, en_la_respuesta in citadas:
        assert en_la_respuesta in respuesta
        assert str({"fecha": fecha, _IPC: valor}) in evidencia


def test_de_una_serie_que_no_entra_la_cola_entra_aunque_la_respuesta_nombre_cada_anio() -> None:
    """Una serie mensual de 80 filas entre cuatro fuentes y una respuesta de
    evolución que nombra cada año: el último dato, que es el que se cita, se
    ve (el 01-oct el juez marcó como inventada la última fila que no veía)."""
    filas: list[dict[str, Any]] = []
    for i in range(80):
        y, m = divmod(2020 * 12 + 4 + i, 12)
        filas.append({"fecha": f"{y}-{m + 1:02d}-01", _IPC: round(30 + i * 0.37, 2)})
    serie = SimpleNamespace(dataset_title="IPC", portal_name="p", portal_url="u1", records=filas)
    otras = [
        SimpleNamespace(dataset_title=f"Otra {k}", portal_url=f"u{k}", records=[{"v": k}])
        for k in (2, 3, 4)
    ]
    respuesta = (
        "La inflación interanual fue 30 % en 2020, bajó en 2021 y 2022, subió en 2023, "
        "siguió en 2024 y 2025, y el último dato, diciembre de 2026, es 59,23 %."
    )
    assert filas[-1] == {"fecha": "2026-12-01", _IPC: 59.23}
    evidencia = summarize_evidence([serie, *otras], respuesta)
    assert evidencia == summarize_evidence([serie, *otras])
    assert all(str(f) in evidencia for f in filas if f["fecha"].startswith("2026"))


def test_la_marca_de_filas_salteadas_se_cobra_por_salto_y_no_por_fila() -> None:
    """Revisión de #172: se descontaban 32 caracteres de marca por cada fila
    elegida, aunque entre filas seguidas no va ninguna. 20 fragmentos de 1.030
    caracteres no entran en los 19.979 que quedan (20.619); 19 y una marca,
    sí (19.611). Cobrando la marca por fila entraban 18."""
    filas = [{"texto": f"{i:02d} " + "x" * 1014} for i in range(20)]
    assert {len(str(f)) for f in filas} == {1030}
    fuente = SimpleNamespace(dataset_title="t", portal_url="u", records=filas)
    evidencia = summarize_evidence([fuente])
    assert len(evidencia) <= TOPE
    assert sum(str(f) in evidencia for f in filas) == 19
    assert evidencia.count("sin mostrar") == 1


def test_de_texto_cada_fuente_respeta_su_parte_del_tope_con_las_marcas() -> None:
    """Con las marcas cobradas por salto, ninguna fuente de texto se pasa de
    su parte: filas de largo y palabras al azar (semilla fija), con y sin
    coincidencias, de 1 a 4 fuentes."""
    rnd = random.Random(172)
    palabras = [f"pal{k:04d}" for k in range(3000)]
    for _ in range(150):
        fuentes = []
        for k in range(rnd.randint(1, 4)):
            filas = [
                {"texto": " ".join(rnd.choices(palabras, k=rnd.randint(130, 700)))}
                for _ in range(rnd.randint(2, 30))
            ]
            fuentes.append(
                SimpleNamespace(dataset_title=f"t{k}", portal_url=f"u{k}", records=filas)
            )
        respuesta = " ".join(rnd.choices(palabras, k=rnd.randint(0, 12)))
        parte = TOPE // len(fuentes) - 2
        for seccion in summarize_evidence(fuentes, respuesta).split("\n\n"):
            assert len(seccion) <= parte


class _JuezQueAnota:
    """Un juez que no llama a ningún modelo: anota lo que le llega."""

    def __init__(self) -> None:
        self.mensajes: list[list[Any]] = []

    async def chat(
        self, messages: list[Any], temperature: float = 0.0, max_tokens: int = 600
    ) -> Any:
        self.mensajes.append(messages)
        return SimpleNamespace(content="Sale de los datos.\nPUNTAJE: 0.0")


@pytest.mark.parametrize("cid", sorted(CASOS))
def test_el_prompt_del_juez_de_alucinacion_trae_los_fragmentos(cid: str) -> None:
    caso = CASOS[cid]
    out = EngineOutput(
        answer=caso["respuesta"],
        sources=[],
        latency_ms=0,
        usage={},
        evidence=summarize_evidence([_resultado(caso)], caso["respuesta"]),
    )
    juez = _JuezQueAnota()
    asyncio.run(_judge_run(juez, {"question": caso["pregunta"]}, out))
    [prompt] = [m[1].content for m in juez.mensajes if "salen de los datos" in m[0].content]
    for en_la_fuente, _ in caso["marcadas"]:
        assert en_la_fuente in prompt


def test_el_agente_arma_la_evidencia_con_su_respuesta(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.application.answers.agent_engine as agent_mod
    import app.application.answers.runner as runner_mod
    from app.application.answers.engine import CompleteEvent, EngineResult

    caso = CASOS["sesiones_002"]
    result = EngineResult(answer=caso["respuesta"], evidence=[_resultado(caso)])

    class _Runner:
        def __init__(self, engine: Any, deps: Any) -> None:
            pass

        async def stream(self, req: Any) -> Any:
            yield CompleteEvent(result)

    class _Scope:
        async def __aenter__(self) -> _Scope:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def get(self, _: Any) -> None:
            return None

    monkeypatch.setattr(runner_mod, "EngineRunner", _Runner)
    monkeypatch.setattr(agent_mod, "AgentEngine", lambda llm, deps: None)
    engine = AgentEngine("modelo", "agente")
    engine._container = lambda scope: _Scope()
    out = asyncio.run(
        engine.run(caso["pregunta"], case_id="sesiones_002", mode="normal", bypass_cache=True)
    )
    assert out.answer == caso["respuesta"]
    assert all(en_la_fuente in out.evidence for en_la_fuente, _ in caso["marcadas"])


def test_el_grafo_arma_la_evidencia_con_su_respuesta() -> None:
    caso = CASOS["sesiones_003"]

    async def ainvoke(state: dict[str, Any]) -> dict[str, Any]:
        return {"clean_answer": caso["respuesta"], "data_results": [_resultado(caso)]}

    engine = LegacyGraphEngine()
    engine._graph = SimpleNamespace(ainvoke=ainvoke)
    out = asyncio.run(
        engine.run(caso["pregunta"], case_id="sesiones_003", mode="normal", bypass_cache=True)
    )
    assert all(en_la_fuente in out.evidence for en_la_fuente, _ in caso["marcadas"])
