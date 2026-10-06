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


class TestFechasNoIso:
    """Auditoría 4.1 / "lo que no vio" 8: `_sort_chart_rows` ordenaba con str()."""

    def test_d_m_aaaa_se_ordena_por_fecha_no_por_texto(self):
        records = [
            {"Fecha": "1/10/2017", "valor": 3},
            {"Fecha": "1/9/2017", "valor": 2},
            {"Fecha": "15/1/2018", "valor": 4},
            {"Fecha": "1/8/2017", "valor": 1},
        ]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert [r["valor"] for r in charts[0]["data"]] == [1, 2, 3, 4]

    def test_meses_en_castellano(self):
        records = [
            {"periodo": "Junio de 2026", "valor": 6},
            {"periodo": "Marzo de 2026", "valor": 3},
            {"periodo": "Enero de 2027", "valor": 13},
        ]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert [r["valor"] for r in charts[0]["data"]] == [3, 6, 13]

    def test_updated_at_no_es_el_eje_temporal(self):
        records = [
            {"updated_at": "2026-05-06", "nombre": "B", "valor": 2},
            {"updated_at": "2026-05-06", "nombre": "A", "valor": 1},
        ]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert charts[0]["xKey"] == "nombre" and charts[0]["type"] == "bar_chart"

    def test_publicacion_fecha_es_una_fecha(self):
        from app.application.pipeline.chart_builder import is_date_column

        assert is_date_column("PUBLICACION_FECHA")
        assert not is_date_column("updated_ts")
        assert not is_date_column("candidate")

    def test_fecha_de_nacimiento_no_es_el_eje_de_un_ranking_de_personas(self):
        # Las DDJJ traen `fecha_nacimiento` antes de las cifras: con la regla de
        # fechas de las olas el ranking se graficaba como línea por cumpleaños.
        records = [
            {"nombre": "A", "fecha_nacimiento": "1970-05-01", "patrimonio_cierre": 10.0},
            {"nombre": "B", "fecha_nacimiento": "1965-02-11", "patrimonio_cierre": 7.5},
        ]
        result = _make_result(records, format="json", title='Búsqueda DDJJ: "juan"')
        charts = _build_deterministic_charts([result])
        assert charts[0]["type"] == "bar_chart"
        assert charts[0]["xKey"] == "nombre"
        assert charts[0]["yKeys"] == ["patrimonio_cierre"]

    def test_fecha_de_nacimiento_es_el_eje_de_una_tabla_de_nacimientos(self):
        records = [
            {"fecha_nacimiento": "2024-02-01", "cantidad": 12},
            {"fecha_nacimiento": "2024-01-01", "cantidad": 10},
        ]
        result = _make_result(records, format="json", title="caba__nacimientos__a1b2c3d4__v1")
        charts = _build_deterministic_charts([result])
        assert charts[0]["type"] == "line_chart"
        assert charts[0]["xKey"] == "fecha_nacimiento"
        assert [r["cantidad"] for r in charts[0]["data"]] == [10, 12]

    def test_la_fecha_del_dato_gana_a_una_de_vencimiento_anterior(self):
        records = [
            {"fecha_vencimiento": "2030-01-01", "fecha": "2024-02-01", "monto": 2},
            {"fecha_vencimiento": "2029-01-01", "fecha": "2024-01-01", "monto": 1},
        ]
        charts = _build_deterministic_charts([_make_result(records, format="json")])
        assert charts[0]["xKey"] == "fecha"
        assert [r["monto"] for r in charts[0]["data"]] == [1, 2]

    def test_fecha_nac_abreviada_no_le_gana_a_la_fecha_del_dato(self):
        # raw.caba__personas_buscadas: `fecha_nac` antes que `fecha_extravio`.
        records = [
            {
                "apellido": "X",
                "nombre": "A",
                "fecha_nac": "1990-01-01",
                "edad": 30,
                "fecha_extravio": "2020-02-01",
            },
            {
                "apellido": "Y",
                "nombre": "B",
                "fecha_nac": "1980-01-01",
                "edad": 40,
                "fecha_extravio": "2020-01-01",
            },
        ]
        result = _make_result(records, format="json", title="Personas buscadas")
        charts = _build_deterministic_charts([result])
        assert charts[0]["xKey"] == "fecha_extravio"

    def test_un_vencimiento_abreviado_no_ordena_un_registro(self):
        # Transportes autorizados: con `fecha_vto` como eje salía una línea de
        # números de documento ordenados por vencimiento.
        records = [
            {
                "numero": "1",
                "fecha_vto": "2027-01-01",
                "apellido_y_nombre": "A",
                "numero_documento": 20111222,
            },
            {
                "numero": "2",
                "fecha_vto": "2026-01-01",
                "apellido_y_nombre": "B",
                "numero_documento": 30111222,
            },
        ]
        result = _make_result(records, format="json", title="Transportes autorizados de pasajeros")
        assert _build_deterministic_charts([result]) == []
        from app.application.pipeline.chart_builder import es_eje_temporal

        assert not es_eje_temporal("Fecha Primer Vto.")

    def test_en_nl2sql_el_eje_no_depende_de_como_se_pregunta(self):
        records = [
            {"fecha_fallecimiento": "2021-02-01", "casos": 5},
            {"fecha_fallecimiento": "2021-01-01", "casos": 3},
        ]
        con_palabra = _make_result(
            records, format="json", title="Consulta SQL: ¿Cuántos fallecimientos por mes?"
        )
        sin_palabra = _make_result(
            records, format="json", title="Consulta SQL: ¿Cuántos murieron por mes?"
        )
        assert _build_deterministic_charts([con_palabra]) == _build_deterministic_charts(
            [sin_palabra]
        )
        # La tabla servida sí dice de qué evento es la fecha.
        servida = _make_result(records, format="json", title="Consulta SQL: ¿Cuántos murieron?")
        servida.metadata["served_table"] = "raw.rosario__fallecimientos_covid__a1b2c3d4__v1"
        assert _build_deterministic_charts([servida])[0]["xKey"] == "fecha_fallecimiento"


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

    def test_nl2sql_dataset_title_takes_the_models_title(self):
        # Con la fuente real, el resultado NL2SQL se llama como el dataset
        # entero; el modelo describe el recorte ("dic 2025 – jun 2026").
        det = [{"title": "Principales tasas de interés", "xKey": "indice_tiempo"}]
        llm = [{"title": "Tasas del BCRA (dic 2025 – jun 2026)", "xKey": "indice_tiempo"}]
        out = adopt_llm_titles(det, llm, frozenset({"Principales tasas de interés"}))
        assert out[0]["title"] == "Tasas del BCRA (dic 2025 – jun 2026)"

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
