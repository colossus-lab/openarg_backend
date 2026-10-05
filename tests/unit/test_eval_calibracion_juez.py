"""El procedimiento para calibrar los jueces a mano (calibracion_juez.py)."""

from __future__ import annotations

from typing import Any

import pytest

from tests.evaluation.calibracion_juez import (
    agreement,
    cohen_kappa,
    export_rows,
    judge_says_good,
    select_runs,
)


def _run(i: int, neutrality: float, pattern_ok: bool = True) -> dict[str, Any]:
    return {
        "run": i,
        "answer": f"respuesta {i}",
        "evidence": "datos",
        "judge": {"neutrality": neutrality, "reasons": {"neutrality": f"motivo {i}"}},
        "quality": {"checks": [{"name": "patron_prohibido:x", "ok": pattern_ok, "detail": ""}]},
    }


def _report() -> dict[str, Any]:
    results = []
    for c in range(5):
        runs = [_run(i, 1.0) for i in range(4)]
        if c == 0:
            # El juez dice neutral y el patrón encontró una atribución.
            runs[0] = _run(0, 0.9, pattern_ok=False)
        results.append({"id": f"neutralidad_{c:03d}", "question": f"q{c}", "runs": runs})
    return {"results": results}


def test_el_juez_de_neutralidad_aprueba_por_arriba_y_el_de_alucinacion_por_abajo() -> None:
    assert judge_says_good(0.8, "neutralidad", 0.5)
    assert not judge_says_good(0.8, "alucinacion", 0.5)


def test_la_muestra_trae_primero_los_desacuerdos_y_reparte_el_resto_por_caso() -> None:
    chosen = select_runs(_report(), "neutralidad", n=6)
    ids = {f"{r['id']}#{run['run']}" for r, run in chosen}
    assert "neutralidad_000#0" in ids
    assert len(chosen) == 6
    assert len({r["id"] for r, _ in chosen}) == 5


def test_la_exportacion_es_a_ciegas() -> None:
    rows = export_rows(select_runs(_report(), "neutralidad", n=3), "neutralidad")
    assert rows and all("juez" not in k and "neutrality" not in k for k in rows[0])
    assert all(r["etiqueta"] == "" for r in rows)


def test_kappa_de_cohen() -> None:
    assert cohen_kappa([1, 1, 0, 0], [1, 1, 0, 0]) == pytest.approx(1.0)
    assert cohen_kappa([1, 0, 1, 0], [0, 1, 0, 1]) == pytest.approx(-1.0)
    assert cohen_kappa([1, 1, 1], [1, 1, 1]) is None


def test_el_acuerdo_lista_cada_desacuerdo_con_el_motivo_del_juez() -> None:
    labels = {"neutralidad_000#0": 0, "neutralidad_001#1": 1, "neutralidad_002#2": 1}
    out = agreement(_report(), labels, "neutralidad")
    assert out["etiquetadas"] == 3
    assert out["matriz"]["humano_malo_juez_bueno"] == 1
    assert out["desacuerdos"][0]["id"] == "neutralidad_000#0"
    assert out["desacuerdos"][0]["motivo"] == "motivo 0"
    assert not out["aprobado"]
