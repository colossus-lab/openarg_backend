"""El gold set de búsqueda y la parte pura de su runner (run_search_gold.py).

Lo que toca la base (embedding, ``search_datasets_ann``, ``BuscarDatos``)
corre dentro del contenedor en sólo lectura y se probó en staging el
04-oct-2026; acá se prueba cómo se decide un acierto.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests.evaluation import run_search_gold as gold

GOLD = json.loads(
    (Path(__file__).parents[1] / "evaluation" / "search_gold.json").read_text(encoding="utf-8")
)


def test_el_gold_set_tiene_unas_50_positivas_y_10_negativas_bien_formadas() -> None:
    casos = GOLD["casos"]
    positivas = [c for c in casos if not c.get("negativo")]
    negativas = [c for c in casos if c.get("negativo")]
    assert len(positivas) >= 50 and len(negativas) == 10
    assert len({c["id"] for c in casos}) == len(casos)
    for c in positivas:
        assert c["esperado"], c["id"]
        for spec in c["esperado"]:
            re.compile(spec["titulo"])
    assert all(c.get("nota") for c in negativas)
    assert 0.0 < GOLD["umbral_negativo"] < 1.0


def test_un_dataset_sin_tablas_no_cuenta_como_acierto() -> None:
    """El MCP muestra copias sin tabla del mismo dataset: no sirven para consultar."""
    spec = {"titulo": r"pauta publicitaria", "portal": ["caba"]}
    assert not gold.matches(spec, {"titulo": "Pauta Publicitaria", "portal": "caba", "tablas": []})
    assert gold.matches(spec, {"titulo": "Pauta Publicitaria", "portal": "caba", "tablas": ["t"]})


def test_un_esperado_que_cuenta_sin_filas_cuenta_por_titulo() -> None:
    """busq_053 mide que el recorrido no quede atrapado; en staging las tablas
    de "Votaciones Nominales" tienen 0 filas (revisión del #183)."""
    spec = {"titulo": r"^votaciones nominales$", "cuenta_sin_filas": True}
    assert gold.matches(spec, {"titulo": "Votaciones Nominales", "portal": "x", "tablas": []})
    assert not gold.matches(spec, {"titulo": "Votaciones", "portal": "x", "tablas": ["t"]})


def test_los_casos_de_la_revision_del_183_estan_en_el_gold_set() -> None:
    """Las dos consultas que el recorrido de 400 candidatos atrapaba en
    staging (08-oct), con lo que la exacta pone arriba."""
    casos = {c["q"]: c for c in GOLD["casos"]}
    votaciones = casos["votaciones nominales"]["esperado"][0]
    assert gold.matches(votaciones, {"titulo": "Votaciones Nominales", "tablas": []})
    assert not gold.matches(votaciones, {"titulo": "Legislativas provinciales 2017", "tablas": []})
    dolar = casos["dólar oficial"]["esperado"][0]
    assert gold.matches(dolar, {"titulo": "Cotizaciones Cambiarias BCRA", "tablas": ["t"]})
    assert not gold.matches(
        dolar, {"titulo": "Brasil Tipo de cambio nominal y real", "tablas": ["t"]}
    )


def test_las_repeticiones_corren_cada_caso_seguido() -> None:
    casos = [{"id": "a"}, {"id": "b"}]
    assert [c["id"] for c in gold.expand_cases(casos, 3)] == ["a", "a", "a", "b", "b", "b"]
    assert gold.expand_cases(casos, 0) == casos


def test_con_repeticiones_el_resumen_nombra_una_vez_cada_caso_que_falla() -> None:
    row = {"id": "p1", "negativo": False, "ms": 1, "top_score": 0.5}
    rows = [
        {**row, "mcp": {"ok": False, "rank": None}, "agente": {"ok": True, "rank": 0}},
        {**row, "mcp": {"ok": False, "rank": 4}, "agente": {"ok": False, "rank": None}},
        {**row, "mcp": {"ok": True, "rank": 0}, "agente": {"ok": True, "rank": 0}},
    ]
    s = gold.summarize(rows)
    assert s["mcp"]["fallan"] == ["p1"] and s["agente"]["fallan"] == ["p1"]
    assert s["mcp"]["hit@3"] == round(1 / 3, 3) and s["agente"]["hit@3"] == round(2 / 3, 3)


def test_el_tope_de_la_exacta_se_fuerza_sobre_el_adaptador_de_verdad() -> None:
    from app.infrastructure.adapters.search.pgvector_search_adapter import (
        PgVectorSearchAdapter,
    )

    class _Adapter:
        _EXACT_FALLBACK_TIMEOUT_MS = 1500

    gold.force_exact_cap(_Adapter, None)
    assert _Adapter._EXACT_FALLBACK_TIMEOUT_MS == 1500
    gold.force_exact_cap(_Adapter, 1)
    assert _Adapter._EXACT_FALLBACK_TIMEOUT_MS == 1
    # El nombre que el runner fuerza es el que el adaptador usa; si cambia,
    # el runner falla en vez de medir sin tope.
    assert hasattr(PgVectorSearchAdapter, "_EXACT_FALLBACK_TIMEOUT_MS")

    class _Renamed:
        pass

    with pytest.raises(SystemExit):
        gold.force_exact_cap(_Renamed, 1)


def test_el_portal_esperado_se_respeta() -> None:
    spec = {"titulo": r"deuda p[uú]blica", "portal": ["mendoza"]}
    assert not gold.matches(spec, {"titulo": "Deuda Pública", "portal": "caba", "tablas": ["t"]})


def test_el_rango_es_el_del_primer_resultado_esperado() -> None:
    case = {"esperado": [{"titulo": r"SUBE"}]}
    results = [
        {"titulo": "Otra cosa", "portal": "x", "tablas": ["a"]},
        {"titulo": "SUBE - usos por fecha", "portal": "x", "tablas": []},
        {"titulo": "SUBE - usos por fecha", "portal": "x", "tablas": ["b"]},
    ]
    assert gold.rank_of(case, results) == 2
    assert gold.evaluate_case(case, results, 0.62) == {"ok": True, "rank": 2}
    assert not gold.evaluate_case(case, [results[0]] * 3 + results, 0.62)["ok"]


def test_una_negativa_falla_si_algo_supera_el_umbral() -> None:
    case = {"negativo": True}
    assert gold.evaluate_case(case, [{"score": 0.55}], 0.62)["ok"]
    assert not gold.evaluate_case(case, [{"score": 0.674}], 0.62)["ok"]
    assert gold.evaluate_case(case, [], 0.62)["ok"]


def test_el_resumen_separa_los_dos_puntos_de_entrada() -> None:
    rows = [
        {
            "id": "p1",
            "negativo": False,
            "ms": 100,
            "top_score": 0.7,
            "mcp": {"ok": True, "rank": 0},
            "agente": {"ok": True, "rank": 0},
        },
        {
            "id": "p2",
            "negativo": False,
            "ms": 300,
            "top_score": 0.6,
            "mcp": {"ok": False, "rank": 5},
            "agente": {"ok": True, "rank": 1},
        },
        {
            "id": "n1",
            "negativo": True,
            "ms": 200,
            "top_score": 0.5,
            "mcp": {"ok": True, "rank": None},
            "agente": {"ok": False, "rank": None},
        },
    ]
    s = gold.summarize(rows)
    assert s["mcp"]["hit@3"] == 0.5 and s["mcp"]["hit@10"] == 1.0
    assert s["agente"]["hit@3"] == 1.0
    assert s["mcp"]["negativas_ok"] == 1.0 and s["agente"]["negativas_ok"] == 0.0
    assert s["mcp"]["fallan"] == ["p2"] and s["agente"]["fallan"] == ["n1"]


def test_el_bundle_lleva_el_gold_set_adentro() -> None:
    source = gold.bundle_source()
    namespace: dict[str, object] = {"__name__": "bundle"}
    exec(compile(source, "<bundle>", "exec"), namespace)  # noqa: S102 — el propio script
    loaded = namespace["load_gold"]()  # type: ignore[operator]
    assert loaded["casos"] == GOLD["casos"]


def test_el_runner_abre_todas_las_transacciones_en_solo_lectura() -> None:
    """La garantía de que se puede correr en prod: un INSERT fallaría."""
    from sqlalchemy import create_engine, text

    engine = create_engine("sqlite://")
    seen: list[str] = []

    from sqlalchemy import event

    @event.listens_for(engine, "before_cursor_execute")
    def _log(conn, cursor, statement, *args):  # type: ignore[no-untyped-def]
        seen.append(statement)

    # SQLite no entiende SET TRANSACTION READ ONLY: alcanza con ver que se
    # manda antes que cualquier otra cosa en cada transacción.
    @event.listens_for(engine, "handle_error")
    def _ignore(ctx):  # type: ignore[no-untyped-def]
        return None

    gold._read_only(engine)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001 — SQLite rechaza la sentencia; importa el orden
        pass
    assert seen and seen[0] == "SET TRANSACTION READ ONLY"
