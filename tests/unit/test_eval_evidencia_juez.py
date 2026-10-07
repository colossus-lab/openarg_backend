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
