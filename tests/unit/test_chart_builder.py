"""Tests for chart builder functions."""
from __future__ import annotations

from app.application.smart_query_service import SmartQueryService
from app.domain.entities.connectors.data_result import DataResult

# Bind static methods for convenience
_build_deterministic_charts = SmartQueryService._build_deterministic_charts
_extract_llm_charts = SmartQueryService._extract_llm_charts


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


class TestExtractLLMCharts:
    def test_extracts_valid_chart(self):
        text = (
            'Texto <!--CHART:{"type":"line_chart","title":"Test",'
            '"data":[{"x":1,"y":2},{"x":3,"y":4}],"xKey":"x","yKeys":["y"]}'
            '--> más texto'
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
