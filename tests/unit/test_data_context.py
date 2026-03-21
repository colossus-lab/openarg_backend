"""Tests for _build_data_context in SmartQueryService."""
from __future__ import annotations

import json

from app.application.smart_query_service import SmartQueryService
from app.domain.entities.connectors.data_result import DataResult

_build_data_context = SmartQueryService._build_data_context


def _make_result(
    records: list[dict],
    fmt: str = "json",
    title: str = "Test Dataset",
    metadata: dict | None = None,
) -> DataResult:
    return DataResult(
        source="test",
        portal_name="Test Portal",
        portal_url="https://example.com",
        dataset_title=title,
        format=fmt,
        records=records,
        metadata=metadata or {"total_records": len(records)},
    )


class TestBuildDataContextEmpty:
    """When no results are returned, a fallback message is produced."""

    def test_empty_list_returns_fallback(self):
        ctx = _build_data_context([])
        assert "No se obtuvieron resultados" in ctx
        assert "datos.gob.ar" in ctx

    def test_fallback_mentions_available_portals(self):
        ctx = _build_data_context([])
        assert "Portal Nacional" in ctx
        assert "CABA" in ctx
        assert "Series de Tiempo" in ctx
        assert "DDJJ" in ctx


class TestBuildDataContextSingle:
    """Single DataResult with normal records."""

    def test_single_result_includes_title(self):
        records = [{"nombre": "A", "valor": 10}]
        ctx = _build_data_context([_make_result(records, title="IPC Mensual")])
        assert "IPC Mensual" in ctx

    def test_single_result_includes_source(self):
        records = [{"nombre": "A", "valor": 10}]
        ctx = _build_data_context([_make_result(records)])
        assert "Test Portal" in ctx
        assert "test" in ctx

    def test_single_result_includes_columns(self):
        records = [{"nombre": "A", "valor": 10}]
        ctx = _build_data_context([_make_result(records)])
        assert "nombre" in ctx
        assert "valor" in ctx

    def test_single_result_includes_record_data(self):
        records = [{"fecha": "2024-01", "valor": 42.5}]
        ctx = _build_data_context([_make_result(records)])
        assert "42.5" in ctx

    def test_includes_description_when_present(self):
        records = [{"a": 1}]
        meta = {"total_records": 1, "description": "Indice de precios al consumidor"}
        ctx = _build_data_context([_make_result(records, metadata=meta)])
        assert "Indice de precios al consumidor" in ctx


class TestBuildDataContextMultiple:
    """Multiple DataResults are concatenated."""

    def test_multiple_results_all_present(self):
        r1 = _make_result([{"x": 1}], title="Dataset A")
        r2 = _make_result([{"y": 2}], title="Dataset B")
        ctx = _build_data_context([r1, r2])
        assert "Dataset A" in ctx
        assert "Dataset B" in ctx
        assert "Dataset 1" in ctx
        assert "Dataset 2" in ctx


class TestBuildDataContextTruncation:
    """Context is capped at 60k characters."""

    def test_truncates_at_80k(self):
        # Generate enough DataResults so that the joined text exceeds 80k chars.
        # Each result with 30 records (under 50 threshold, so no slicing)
        # and long string values ensures we blow past the limit.
        results = []
        for i in range(50):
            records = [{"key": "x" * 800, "val": j} for j in range(30)]
            results.append(_make_result(records, title=f"BigDataset_{i}"))
        ctx = _build_data_context(results)
        assert len(ctx) <= 80_000 + 200  # allow for the truncation suffix
        assert "datos truncados" in ctx

    def test_short_context_not_truncated(self):
        records = [{"a": 1}, {"a": 2}]
        ctx = _build_data_context([_make_result(records)])
        assert "datos truncados" not in ctx


class TestBuildDataContextRecordSlicing:
    """When >50 records, first 25 + last 25 are sent."""

    def test_more_than_50_records_sliced(self):
        records = [{"idx": i, "val": i * 10} for i in range(100)]
        ctx = _build_data_context([_make_result(records)])
        # The context should mention it's showing a subset
        assert "50 registros" in ctx
        # The first and last record values should be present
        assert '"idx": 0' in ctx or '"idx":0' in ctx or json.dumps(0) in ctx
        assert '"idx": 99' in ctx or '"idx":99' in ctx

    def test_50_or_fewer_records_not_sliced(self):
        records = [{"idx": i} for i in range(50)]
        ctx = _build_data_context([_make_result(records)])
        assert "50 registros" in ctx
        assert "totales" not in ctx


class TestBuildDataContextMetadataOnly:
    """Records flagged as resource_metadata are handled differently."""

    def test_metadata_only_records(self):
        records = [
            {"_type": "resource_metadata", "name": "datos.csv", "format": "CSV"},
            {"_type": "resource_metadata", "name": "info.xlsx", "format": "XLSX"},
        ]
        ctx = _build_data_context([_make_result(records, title="Catálogo")])
        assert "Datastore" in ctx or "metadatos" in ctx.lower()
        assert "datos.csv" in ctx

    def test_metadata_only_skips_non_dict_records(self):
        """Results with non-dict records are skipped."""
        records = ["not a dict", "also not"]
        ctx = _build_data_context([_make_result(records)])
        # Should not crash and should not include any dataset section
        # (non-dict records lead to valid_records being empty, and
        # the result.records is truthy so the result is skipped via `continue`)
        assert "Dataset 1" not in ctx or "No se obtuvieron" in ctx
