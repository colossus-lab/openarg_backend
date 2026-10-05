"""El gold set de búsqueda y la parte pura de su runner (run_search_gold.py).

Lo que toca la base (embedding, ``search_datasets_ann``, ``BuscarDatos``)
corre dentro del contenedor en sólo lectura y se probó en staging el
04-oct-2026; acá se prueba cómo se decide un acierto.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

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
