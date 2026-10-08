"""Los oráculos de la batería: cifras esperadas calculadas desde la fuente oficial.

Corren contra respuestas grabadas de la API de Series de Tiempo y de la v4
del BCRA el 04-oct-2026 (``fixtures/oraculos_2026-10-04.json``), así que no
usan red. Las cifras son las que verificó el plan de arreglos ese día.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from tests.evaluation import oracles
from tests.evaluation.oracles import (
    OracleError,
    business_days_before,
    resolve_dataset,
    resolve_entry,
)
from tests.evaluation.quality_checks import assess
from tests.evaluation.run_eval import load_golden_dataset, rescore, validate_dataset

EVAL = Path(__file__).parents[1] / "evaluation"
RECORDED = json.loads((EVAL / "fixtures" / "oraculos_2026-10-04.json").read_text(encoding="utf-8"))
HOY = date(2026, 10, 4)
IPC = "148.3_INIVELNAL_DICI_M_26"


def recorded_fetch(calls: list[dict[str, str]] | None = None) -> Any:
    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        if calls is not None:
            calls.append({"url": url, **params})
        key = url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))
        return RECORDED["respuestas"][key]

    return fetch


def resolve(name: str, **args: Any) -> oracles.Resolucion:
    return oracles.ORACLES[name](recorded_fetch(), HOY, **args)


# ── IPC sobre el índice ────────────────────────────────────


def test_la_mensual_y_la_interanual_salen_del_indice() -> None:
    mensual = resolve("serie_variacion_mensual", serie=IPC)
    interanual = resolve("serie_variacion_interanual", serie=IPC)
    assert mensual.valores[0] == pytest.approx(1.6592, abs=1e-4)
    assert interanual.valores[0] == pytest.approx(33.5412, abs=1e-4)
    assert mensual.periodo == interanual.periodo == "2026-08-01"


def test_la_interanual_exige_doce_meses_justos() -> None:
    # Una serie con un hueco en t−12: sin el mes base no hay interanual,
    # y la batería no inventa una con el mes de al lado.
    rows = [["2026-08-01", 110.0], ["2026-07-01", 109.0], ["2025-07-01", 90.0]]

    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        return {"data": rows, "meta": [{"frequency": "month"}, {"field": {"frequency": "R/P1M"}}]}

    with pytest.raises(OracleError, match="2025-08"):
        oracles.serie_variacion_interanual(fetch, HOY, serie="x")


def test_la_acumulada_en_el_anio_es_desde_diciembre_y_desde_enero_queda_prohibida() -> None:
    """Definición del INDEC: contra diciembre del año anterior (21,30 %).

    ``percent_change_since_beginning_of_year`` de la API mide desde enero
    (17,90 %): si el motor la usa como acumulada, la batería lo tiene que ver.
    """
    res = resolve("serie_acumulada_anio", serie=IPC)
    assert res.valores[0] == pytest.approx(21.2955, abs=1e-3)
    assert res.prohibidos[0] == pytest.approx(17.8977, abs=1e-3)


def test_el_tramo_se_compone_y_la_suma_de_tasas_queda_prohibida() -> None:
    res = resolve("serie_acumulada_tramo", serie=IPC, desde="2026-03", hasta="2026-08")
    assert res.valores[0] == pytest.approx(14.5795, abs=1e-3)
    assert res.prohibidos[0] == pytest.approx(13.7746, abs=1e-3)


def test_las_exportaciones_del_anio_se_suman_y_el_promedio_queda_prohibido() -> None:
    res = resolve("serie_suma_anual", serie="74.3_IET_0_M_16", anio="ultimo_completo")
    assert res.periodo == "2025-01-01"
    assert res.frecuencia == "anual"
    assert res.valores[0] == pytest.approx(87111.2, abs=0.5)
    assert res.prohibidos[0] == pytest.approx(7259.27, abs=0.5)


def test_los_oraculos_de_series_nunca_piden_una_transformacion_a_la_api() -> None:
    """Con sort=desc + representation_mode la API pierde los últimos períodos."""
    calls: list[dict[str, str]] = []
    resolve_dataset(load_golden_dataset(EVAL / "golden_dataset.json"), recorded_fetch(calls), HOY)
    series_calls = [c for c in calls if "apis.datos.gob.ar" in c["url"]]
    assert series_calls
    assert not any("representation_mode" in c for c in series_calls)


# ── BCRA v4 ────────────────────────────────────────────────


def test_las_reservas_aceptan_los_datos_de_los_ultimos_cinco_dias_habiles() -> None:
    res = resolve("bcra_ultimo", variable=1, dias_habiles=5)
    assert res.periodo == "2026-09-30"
    assert res.aceptable_desde == "2026-09-28"
    assert res.valores == [47960.0, 47482.0, 46092.0]


def test_un_bcra_atrasado_igual_acepta_su_ultimo_dato() -> None:
    def fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
        return {"results": [{"detalle": [{"fecha": "2026-09-15", "valor": 50000.0}]}]}

    res = oracles.bcra_ultimo(fetch, HOY, variable=1, dias_habiles=5)
    assert res.aceptable_desde == "2026-09-15"
    assert res.valores == [50000.0]


def test_los_dias_habiles_saltean_el_fin_de_semana() -> None:
    assert business_days_before(date(2026, 10, 4), 5) == date(2026, 9, 28)
    assert business_days_before(date(2026, 10, 6), 1) == date(2026, 10, 5)
    assert business_days_before(date(2026, 10, 5), 1) == date(2026, 10, 2)


# ── el caso no evaluable ───────────────────────────────────


def test_un_oraculo_caido_deja_el_caso_no_evaluable_y_nunca_aprobado() -> None:
    def broken(url: str, params: dict[str, str]) -> dict[str, Any]:
        raise OracleError("timeout")

    entry = {
        "id": "x",
        "expected_values_from": [{"oracle": "bcra_ultimo", "args": {"variable": 1}}],
    }
    resolved = resolve_entry(entry, oracles._Resolver(broken, HOY))
    assert resolved["errores"] and resolved["expected_values"] == []
    q = assess(entry, "Las reservas son US$ 46.092 millones al 30/09/2026.", [], resolved=resolved)
    assert not q.evaluable
    assert not q.passed


def test_el_dataset_resuelve_todos_sus_oraculos_con_lo_grabado() -> None:
    resolved = resolve_dataset(
        load_golden_dataset(EVAL / "golden_dataset.json"), recorded_fetch(), HOY
    )
    assert {c: r["errores"] for c, r in resolved["casos"].items() if r["errores"]} == {}
    assert resolved["casos"]["series_004"]["expected_period"]["aceptable_desde"] == "2026-09-28"


def test_un_oraculo_desconocido_rompe_el_dataset() -> None:
    entry = {
        "id": "x",
        "category": "c",
        "question": "q",
        "expected_intent": "i",
        "expected_values_from": [{"oracle": "serie_que_no_existe", "args": {}}],
    }
    assert any("oráculo desconocido" in p for p in validate_dataset([entry]))


# ── el rescore usa lo congelado ────────────────────────────


def test_el_rescore_usa_las_cifras_congeladas_del_dia_de_la_corrida() -> None:
    """El IPC del mes siguiente no puede desaprobar una respuesta que era correcta."""
    entry = {
        "id": "ipc",
        "category": "series_tiempo",
        "question": "q",
        "expected_intent": "inflacion",
        "expected_values_from": [{"oracle": "serie_variacion_mensual", "args": {"serie": IPC}}],
    }
    run: dict[str, Any] = {
        "run": 0,
        "error": None,
        "latency_ms": 1000,
        "answer": "La inflación de agosto de 2026 fue 1,66 %.",
        "sources": [],
        "usage": {"cost_usd": 0.01, "tokens": {"input": 1}, "llm_calls": 1},
        "tokens_reported": 0,
        "diagnostics": {},
        "quality": {"passed": True, "failures": [], "checks": [], "warnings": []},
    }
    congelado = {"casos": {"ipc": {"expected_values": [{"value": [1.6592], "tolerance": 0.05}]}}}
    otro_mes = {"casos": {"ipc": {"expected_values": [{"value": [2.4], "tolerance": 0.05}]}}}
    report = {"results": [{"id": "ipc", "runs": [run]}], "expectativas": congelado}
    out = rescore(report, [entry], expectativas=otro_mes)
    assert out["results"][0]["pass_rate"] == 1.0
    assert out["expectativas"]["casos"]["ipc"] == congelado["casos"]["ipc"]
