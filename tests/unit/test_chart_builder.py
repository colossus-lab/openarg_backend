"""Tests for chart builder functions."""

from __future__ import annotations

from app.application.pipeline.chart_builder import (
    adopt_llm_titles,
    build_deterministic_charts,
    extract_llm_charts,
)
from app.domain.entities.connectors.data_result import DataResult

# Bind static methods for convenience
_build_deterministic_charts = build_deterministic_charts
_extract_llm_charts = extract_llm_charts


def _make_result(records, format="time_series", title="Test") -> DataResult:
    return DataResult(
        source="test",
        portal_name="Test",
        portal_url="",
        dataset_title=title,
        format=format,
        records=records,
        metadata={"total_records": len(records)},
    )


class TestBuildDeterministicCharts:
    def test_builds_line_chart_from_time_series(self):
        records = [
            {"fecha": "2024-01", "valor": 10.5},
            {"fecha": "2024-02", "valor": 11.2},
            {"fecha": "2024-03", "valor": 12.0},
        ]
        charts = _build_deterministic_charts([_make_result(records)])
        assert len(charts) == 1
        assert charts[0]["type"] == "line_chart"
        assert charts[0]["xKey"] == "fecha"
        assert "valor" in charts[0]["yKeys"]
        assert len(charts[0]["data"]) == 3

    def test_skips_short_datasets(self):
        records = [{"fecha": "2024-01", "valor": 10}]
        charts = _build_deterministic_charts([_make_result(records)])
        assert len(charts) == 0

    def test_skips_metadata_only(self):
        records = [
            {"_type": "resource_metadata", "name": "file.csv"},
            {"_type": "resource_metadata", "name": "file2.csv"},
        ]
        charts = _build_deterministic_charts([_make_result(records)])
        assert len(charts) == 0

    def test_skips_without_time_or_label_key(self):
        records = [
            {"codigo": "A", "valor": 10},
            {"codigo": "B", "valor": 20},
        ]
        charts = _build_deterministic_charts([_make_result(records)])
        assert len(charts) == 0

    def test_includes_units_in_title(self):
        records = [
            {"fecha": "2024-01", "valor": 10},
            {"fecha": "2024-02", "valor": 20},
        ]
        result = _make_result(records, title="IPC")
        result.metadata["units"] = "porcentaje"
        charts = _build_deterministic_charts([result])
        assert "porcentaje" in charts[0]["title"]

    def test_bar_chart_for_non_time_series(self):
        records = [
            {"año": "2023", "valor": 100},
            {"año": "2024", "valor": 200},
        ]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert len(charts) == 1
        assert charts[0]["type"] == "bar_chart"

    def test_sorts_time_series_rows_by_fecha(self):
        records = [
            {"fecha": "2024-03", "valor": 12.0},
            {"fecha": "2024-01", "valor": 10.5},
            {"fecha": "2024-02", "valor": 11.2},
        ]
        charts = _build_deterministic_charts([_make_result(records)])
        assert [row["fecha"] for row in charts[0]["data"]] == ["2024-01", "2024-02", "2024-03"]

    def test_skips_misleading_mixed_dollar_house_snapshot_chart(self):
        result = _make_result(
            [
                {"fecha": "2026-04-14T17:00:00.000Z", "compra": 1335, "venta": 1385},
                {"fecha": "2026-04-14T20:59:00.000Z", "compra": 1390, "venta": 1410},
                {"fecha": "2026-04-14T20:59:00.000Z", "compra": 1414.9, "venta": 1409.3},
                {"fecha": "2026-04-14T16:07:00.000Z", "compra": 1355, "venta": 1364},
            ],
            title="Cotización actual Dólar todas las casas",
        )
        charts = _build_deterministic_charts([result])
        assert charts == []


def _bcra_daily(days: int = 200) -> list[dict]:
    """Serie diaria como la de principales_tasas_de_interes (datos.gob.ar)."""
    from datetime import date, timedelta

    start = date(2025, 12, 1)
    return [
        {
            "indice_tiempo": (start + timedelta(days=i)).isoformat(),
            "tasas_interes_call": 20.0 + i * 0.05,
            "tasas_interes_badlar": 26.0 - i * 0.02,
        }
        for i in range(days)
    ]


class TestIndiceTiempo:
    """`indice_tiempo` es la columna de fecha de todas las series de datos.gob.ar.

    Sin reconocerla, el gráfico determinístico (todas las filas) no se armaba y
    quedaba el del modelo, que sólo ve 50 filas: el 29-sep un "último año" del
    BCRA salió con diciembre pegado a junio.
    """

    def test_nl2sql_series_charts_every_row_as_a_line(self):
        records = list(reversed(_bcra_daily()))  # la consulta vino ORDER BY DESC
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert len(charts) == 1
        chart = charts[0]
        assert chart["type"] == "line_chart"
        assert chart["xKey"] == "indice_tiempo"
        assert len(chart["data"]) == 200
        dates = [row["indice_tiempo"] for row in chart["data"]]
        assert dates == sorted(dates)
        assert dates[0] == "2025-12-01" and dates[-1] == "2026-06-18"

    def test_periodo_is_a_date_column(self):
        records = [{"periodo": "2024-02", "valor": 2}, {"periodo": "2024-01", "valor": 1}]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert charts[0]["type"] == "line_chart"
        assert [r["periodo"] for r in charts[0]["data"]] == ["2024-01", "2024-02"]

    def test_year_columns_keep_their_bar_chart(self):
        records = [{"año": "2023", "valor": 100}, {"año": "2024", "valor": 200}]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert charts[0]["type"] == "bar_chart"


class TestAdoptLlmTitles:
    def test_generic_sql_title_takes_the_models_title(self):
        det = [{"title": "Consulta SQL: Mostrame las tasas", "xKey": "indice_tiempo", "data": [1]}]
        llm = [{"title": "Tasas de interés del BCRA", "xKey": "indice_tiempo", "data": [9]}]
        out = adopt_llm_titles(det, llm)
        assert out[0]["title"] == "Tasas de interés del BCRA"
        assert out[0]["data"] == [1]  # los datos siguen siendo los determinísticos

    def test_real_dataset_title_is_kept(self):
        det = [{"title": "IPC (porcentaje)", "xKey": "fecha", "data": [1]}]
        llm = [{"title": "Otro", "xKey": "fecha"}]
        assert adopt_llm_titles(det, llm)[0]["title"] == "IPC (porcentaje)"

    def test_different_axis_is_not_mixed(self):
        det = [{"title": "Consulta SQL: x", "xKey": "indice_tiempo"}]
        llm = [{"title": "Por provincia", "xKey": "provincia"}]
        assert adopt_llm_titles(det, llm)[0]["title"] == "Consulta SQL: x"


class TestExtractLLMCharts:
    def test_extracts_valid_chart(self):
        text = (
            'Texto <!--CHART:{"type":"line_chart","title":"Test",'
            '"data":[{"x":1,"y":2},{"x":3,"y":4}],"xKey":"x","yKeys":["y"]}'
            "--> más texto"
        )
        charts = _extract_llm_charts(text)
        assert len(charts) == 1
        assert charts[0]["type"] == "line_chart"

    def test_skips_invalid_json(self):
        text = "<!--CHART:{invalid json}-->"
        charts = _extract_llm_charts(text)
        assert len(charts) == 0

    def test_skips_incomplete_chart(self):
        text = '<!--CHART:{"type":"line_chart"}-->'
        charts = _extract_llm_charts(text)
        assert len(charts) == 0

    def test_no_charts_in_text(self):
        charts = _extract_llm_charts("No hay gráficos aquí")
        assert len(charts) == 0
