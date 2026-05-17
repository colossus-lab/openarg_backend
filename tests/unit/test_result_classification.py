"""Tests for NL2SQL result classification (LLM-001 / LLM-002).

`_classify_result_kind` and `_has_nonadditive_aggregate` are pure SQL
heuristics that feed two metadata signals the analyst relies on:
- `result_kind=sample` → the rows are a LIMIT-capped sample, not a count.
- `nonadditive_warning` → a rate/index column was SUM/AVG'd (inflated).
"""

from __future__ import annotations

import pytest

from app.application.pipeline.subgraphs.nl2sql import (
    _classify_result_kind,
    _has_nonadditive_aggregate,
)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT COUNT(*) FROM mart.delitos_caba WHERE anio = '2023'",
        "SELECT tipo, COUNT(*) AS n FROM mart.delitos_caba GROUP BY tipo",
        "SELECT provincia, SUM(CAST(monto AS NUMERIC)) FROM t GROUP BY provincia",
        "select avg(x) from t",
        "SELECT barrio FROM mart.delitos_caba GROUP BY barrio",
    ],
)
def test_classify_aggregate(sql: str) -> None:
    assert _classify_result_kind(sql) == "aggregate"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM mart.delitos_caba WHERE anio = '2023' LIMIT 50",
        "SELECT tipo, barrio FROM mart.delitos_caba WHERE tipo ILIKE '%robo%'",
        "",
    ],
)
def test_classify_sample(sql: str) -> None:
    assert _classify_result_kind(sql) == "sample"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT provincia, SUM(tasa_hechos) FROM mart.delitos_argentina_snic GROUP BY provincia",
        "SELECT SUM(CAST(tasa_victimas AS NUMERIC)) FROM t",
        "SELECT AVG(indice_precios) FROM t",
        "SELECT sum(porcentaje_ocupacion) FROM t",
    ],
)
def test_nonadditive_detected(sql: str) -> None:
    assert _has_nonadditive_aggregate(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT provincia, SUM(cantidad_hechos) FROM mart.delitos_argentina_snic GROUP BY provincia",
        "SELECT COUNT(*) FROM mart.delitos_caba",
        "SELECT tasa_hechos FROM mart.delitos_argentina_snic WHERE anio = '2023'",
        "",
    ],
)
def test_nonadditive_not_detected(sql: str) -> None:
    assert _has_nonadditive_aggregate(sql) is False
