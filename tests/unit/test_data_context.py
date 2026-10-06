"""Tests for `app.application.pipeline.context_builder.build_data_context`.

Migrated 2026-05-09 from skip-marked legacy tests (spec 020). The
original lived inside `SmartQueryService._build_data_context`; the
function is now a free helper in `pipeline.context_builder` and the
tests are unmodified except for unskipping.
"""

from __future__ import annotations

import json

from app.application.pipeline.context_builder import build_data_context
from app.domain.entities.connectors.data_result import DataResult

_build_data_context = build_data_context


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
        assert "contexto recortado" in ctx

    def test_short_context_not_truncated(self):
        records = [{"a": 1}, {"a": 2}]
        ctx = _build_data_context([_make_result(records)])
        assert "contexto recortado" not in ctx


class TestSeriesSampling:
    """Una serie de tiempo larga se muestrea parejo, no por las puntas.

    Caso real (29-sep): 200 días del BCRA, el modelo recibía 1-25 de diciembre y
    25-may a 18-jun, y narró un "salto" en los meses que nunca vio.
    """

    @staticmethod
    def _series(days: int = 200) -> list[dict]:
        from datetime import date, timedelta

        start = date(2025, 12, 1)
        return [
            {"indice_tiempo": (start + timedelta(days=i)).isoformat(), "valor": i}
            for i in range(days)
        ]

    @staticmethod
    def _date(record: dict) -> str:
        # El context builder renombra las columnas para el modelo; la fecha es la primera.
        return next(iter(record.values()))

    @staticmethod
    def _sent(ctx: str) -> list[dict]:
        body = ctx.split("registros):\n", 1)[1]
        return json.JSONDecoder().raw_decode(body)[0]

    def test_points_cover_the_whole_period_without_big_holes(self):
        from datetime import date

        ctx = _build_data_context([_make_result(self._series())])
        sent = self._sent(ctx)
        assert len(sent) == 50
        dates = [date.fromisoformat(self._date(r)) for r in sent]
        assert dates[0] == date(2025, 12, 1) and dates[-1] == date(2026, 6, 18)
        gaps = [(b - a).days for a, b in zip(dates, dates[1:])]
        assert max(gaps) <= 5, f"hueco de {max(gaps)} días en lo que ve el modelo"

    def test_the_model_is_told_it_sees_a_sample(self):
        ctx = _build_data_context([_make_result(self._series())])
        assert "se muestran 50 de 200 filas" in ctx
        assert "intervalos parejos" in ctx

    def test_descending_series_keeps_both_ends(self):
        ctx = _build_data_context([_make_result(list(reversed(self._series())))])
        sent = self._sent(ctx)
        assert self._date(sent[0]) == "2026-06-18"
        assert self._date(sent[-1]) == "2025-12-01"

    def test_short_series_goes_whole_and_without_note(self):
        ctx = _build_data_context([_make_result(self._series(50))])
        assert len(self._sent(ctx)) == 50
        assert "se muestran" not in ctx


class TestSeriesSummary:
    """Números de subas, bajas, máximos y mínimos calculados sobre TODAS las filas.

    Caso real (29-sep, staging): con 200 días del BCRA la respuesta dijo que la
    BADLAR "subió de 26% a 33%" (bajó de 29,44 a 21; el 33 era la call) y que la
    call "osciló entre 19% y 42%" (el máximo fue 64%, un pico de tres días que no
    cayó en la muestra de 50 puntos).
    """

    @staticmethod
    def _bcra() -> list[dict]:
        from datetime import date, timedelta

        start = date(2025, 12, 1)
        rows = []
        for i in range(200):
            d = start + timedelta(days=i)
            call = 64.0 if date(2025, 12, 30) <= d <= date(2026, 1, 1) else 30.0
            if d == date(2025, 12, 4):
                call = 18.74
            badlar = 29.4375 - (8.4375 * i / 199)  # 29,44 → 21,00
            rows.append(
                {
                    "indice_tiempo": d.isoformat(),
                    "tasas_interes_call": call,
                    "tasas_interes_badlar": badlar,
                }
            )
        # la consulta vino ORDER BY DESC: el resumen no puede depender del orden
        return list(reversed(rows))

    def test_the_spike_the_sample_misses_is_in_the_summary(self):
        ctx = _build_data_context([_make_result(self._bcra())])
        assert "máximo 64 (2025-12-30)" in ctx
        assert "mínimo 18.74 (2025-12-04)" in ctx

    def test_each_series_has_its_own_first_and_last(self):
        ctx = _build_data_context([_make_result(self._bcra())])
        badlar = next(
            line for line in ctx.splitlines() if line.startswith("- tasas interes badlar")
        )
        assert "primero 29.44 (2025-12-01)" in badlar
        assert "último 21 (2026-06-18)" in badlar
        call = next(line for line in ctx.splitlines() if line.startswith("- tasas interes call"))
        assert "último 30 (2026-06-18)" in call

    def test_real_period_is_stated(self):
        ctx = _build_data_context([_make_result(self._bcra())])
        assert "SOBRE LAS 200 FILAS (período cubierto: 2025-12-01 a 2026-06-18)" in ctx
        assert "Si el período cubierto no es el que pidió el usuario, decilo." in ctx

    def test_summary_goes_before_the_rows(self):
        ctx = _build_data_context([_make_result(self._bcra())])
        assert ctx.index("RESUMEN CALCULADO") < ctx.index("registros):")

    def test_tables_without_dates_have_no_summary(self):
        records = [{"provincia": f"P{i}", "monto": i} for i in range(100)]
        assert "RESUMEN CALCULADO" not in _build_data_context([_make_result(records)])

    def test_a_birth_date_does_not_make_a_ranking_a_series(self):
        # Una búsqueda de DDJJ: el resumen decía que el patrimonio iba "de primero
        # a último" ordenado por fecha de nacimiento, y con más de 50 filas la
        # muestra se tomaba a intervalos de cumpleaños.
        records = [
            {"nombre": f"P{i}", "fecha_nacimiento": f"19{50 + i}-01-01", "patrimonio_cierre": i}
            for i in range(60)
        ]
        ctx = _build_data_context([_make_result(records, title='Búsqueda DDJJ: "p"')])
        assert "RESUMEN CALCULADO" not in ctx
        assert "MUESTRA DE LA SERIE" not in ctx
        assert "FILAS OMITIDAS" in ctx

    def test_a_birth_date_is_the_series_of_a_births_table(self):
        records = [{"fecha_nacimiento": f"2024-0{i}-01", "cantidad": i} for i in range(1, 5)]
        meta = {"total_records": 4, "served_table": "raw.caba__nacimientos__a1b2c3d4__v1"}
        ctx = _build_data_context([_make_result(records, metadata=meta)])
        assert "RESUMEN CALCULADO" in ctx

    def test_non_numeric_and_internal_columns_are_skipped(self):
        records = [
            {"fecha": f"2024-0{i}", "valor": i, "nota": "x", "_source_url": 1} for i in range(1, 5)
        ]
        ctx = _build_data_context([_make_result(records)])
        summary = [line for line in ctx.splitlines() if line.startswith("- ")]
        assert [line.split(":")[0] for line in summary] == ["- valor"]


class TestBuildDataContextRecordSlicing:
    """Tablas que no son series: >50 filas → primeras 25 + últimas 25, avisado."""

    def test_non_series_says_which_rows_are_missing(self):
        records = [{"idx": i, "val": i * 10} for i in range(100)]
        ctx = _build_data_context([_make_result(records)])
        assert "primeras 25 y las últimas 25 de 100 filas" in ctx

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
