"""Tests for the INDEC hierarchical header parser (WS5)."""

from __future__ import annotations

import pandas as pd
import pytest

from app.application.pipeline.parsers import (
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    parse_hierarchical_headers,
)


def test_versions_exist():
    assert PARSER_VERSION
    assert NORMALIZATION_VERSION


def test_year_over_quarter_layout():
    """Mirrors INDEC EPH layout: row0=year, row1=quarter, row2+=data."""
    df = pd.DataFrame(
        [
            ["", "2003", "", "", "", "2004", "", "", ""],
            [
                "",
                "1° trimestre",
                "2° trimestre",
                "3° trimestre",
                "4° trimestre",
                "1° trimestre",
                "2° trimestre",
                "3° trimestre",
                "4° trimestre",
            ],
            ["Tasa", 35.0, 36.0, 37.0, 38.0, 35.5, 36.5, 37.5, 38.5],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    assert "2003_T1" in cols
    assert "2003_T4" in cols
    assert "2004_T1" in cols
    assert result.period_kind == "quarter"
    assert result.skipped_rows == 2


def test_year_over_month_layout():
    df = pd.DataFrame(
        [
            ["", "2024", "", "", "2025", "", ""],
            ["categoria", "enero", "febrero", "marzo", "enero", "febrero", "marzo"],
            ["A", 1, 2, 3, 4, 5, 6],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    assert "2024_01" in cols
    assert "2024_02" in cols
    assert "2025_03" in cols
    assert result.period_kind == "month"


def test_numeric_only_quarter_columns_under_year_row():
    """When the period row is bare numbers `1 2 3 4` interpreted as quarters."""
    df = pd.DataFrame(
        [
            ["", "2010", "", "", "", "2011", "", "", ""],
            ["concepto", 1, 2, 3, 4, 1, 2, 3, 4],
            ["x", 10, 20, 30, 40, 11, 21, 31, 41],
        ]
    )
    result = parse_hierarchical_headers(df)
    cols = result.columns
    # The 1-4 row should be detected as quarter (4 unique values match)
    assert "2010_T1" in cols
    assert "2011_T4" in cols


def test_no_temporal_falls_back_to_first_row():
    df = pd.DataFrame(
        [
            ["region", "indicador", "valor"],
            ["pais", "PBI", 100],
        ]
    )
    result = parse_hierarchical_headers(df)
    assert result.columns == ["region", "indicador", "valor"]
    assert result.period_kind is None


def test_dedup_collisions():
    df = pd.DataFrame(
        [
            ["", "2003", "", "2003", ""],
            ["concepto", "1° trimestre", "2° trimestre", "1° trimestre", "2° trimestre"],
            ["x", 1, 2, 3, 4],
        ]
    )
    cols = parse_hierarchical_headers(df).columns
    # second occurrence of 2003_T1 must be deduped, not silently overwritten
    assert cols.count("2003_T1") == 1
    assert any(c.startswith("2003_T1_") for c in cols)


def test_classify_handles_empty_dataframe_gracefully():
    df = pd.DataFrame()
    result = parse_hierarchical_headers(df)
    # Empty DF should not crash
    assert result is not None


@pytest.mark.parametrize(
    "label,expected_quarter",
    [
        ("1° trimestre", 1),
        ("2° trimestre", 2),
        ("T3", 3),
        ("4", 4),
    ],
)
def test_quarter_synonyms(label: str, expected_quarter: int):
    df = pd.DataFrame(
        [
            ["", "2024"],
            ["x", label],
            ["v", 1],
        ]
    )
    result = parse_hierarchical_headers(df)
    assert f"2024_T{expected_quarter}" in result.columns


# ── Cuadro 1.6 de la EPH (30-oct-2026) ──────────────────────────────────────
#
# La fila de años decía `Año 2017` y el regex pedía cuatro dígitos pelados, así
# que el parser se quedó con los trimestres sin año; NFKC convirtió `1º` en
# `1o` y esos ni siquiera eran trimestres. El LLM completó los años que faltaban
# con 2003-2011. Estos tests fijan lo que el lector tiene que reconocer solo.


@pytest.mark.parametrize(
    "celda",
    ["2016", "Año 2016", "año 2016", "2016.0", 2016.0, 2016, "2016 (1)", "2016*", "Año 2016 (2)"],
)
def test_celdas_de_anio_con_nota_o_en_float(celda) -> None:
    df = pd.DataFrame([["", celda], ["x", "1° trimestre"], ["v", 1.5]])
    assert "2016_T1" in parse_hierarchical_headers(df).columns


@pytest.mark.parametrize(
    ("celda", "trimestre"),
    [
        ("2o trimestre", 2),
        ("1º trimestre", 1),
        ("3° trimestre (1)", 3),
        ("3o trimestre (2)", 3),
        ("2° trimestre*", 2),
        ("1er trimestre", 1),
        ("4to trimestre", 4),
        ("Primer trimestre", 1),
        ("Q2", 2),
    ],
)
def test_trimestres_como_los_escribe_el_indec(celda: str, trimestre: int) -> None:
    df = pd.DataFrame([["", "Año 2024"], ["x", celda], ["v", 1.5]])
    assert f"2024_T{trimestre}" in parse_hierarchical_headers(df).columns


def _bloque_eph(preambulo: int) -> pd.DataFrame:
    """Título, `preambulo` filas vacías, caption, años, trimestres y datos,
    con la columna separadora que el INDEC deja entre un año y el siguiente."""
    nan = float("nan")
    filas = [["Cuadro X. Una tasa larga del mercado de trabajo, 31 aglomerados"] + [nan] * 9]
    filas += [[nan] * 10 for _ in range(preambulo)]
    filas += [
        ["Indicador", "Total 31 aglomerados urbanos"] + [nan] * 8,
        [nan, "Año 2017", nan, nan, nan, nan, "Año 2018", nan, nan, nan],
        [nan, "1° trimestre", "2º trimestre", "3° trimestre (1)", "4o trimestre", nan]
        + ["1° trimestre", "2° trimestre", "3° trimestre", "4° trimestre*"],
        ["Tasa de desocupación", 9.2, 8.7, 8.3, 7.2, nan, 9.1, 9.6, 9.0, 9.1],
        ["Mujeres hasta 29 años", 2.2, 2.2, 2.1, 2.0, 0.0, 2.4, 2.4, 2.3, 2.4],
    ]
    return pd.DataFrame(filas)


def test_bloque_eph_compone_anio_y_trimestre() -> None:
    r = parse_hierarchical_headers(_bloque_eph(preambulo=1))

    assert r.columns[0] == "Indicador"
    assert r.columns[1:5] == ["2017_T1", "2017_T2", "2017_T3", "2017_T4"]
    assert r.columns[6:10] == ["2018_T1", "2018_T2", "2018_T3", "2018_T4"]
    assert r.period_kind == "quarter"
    assert r.skipped_rows == 5


def test_la_columna_separadora_se_marca_para_descartar() -> None:
    """Ni `col_5` en la tabla ni el 0.0 que el INDEC deja adentro."""
    r = parse_hierarchical_headers(_bloque_eph(preambulo=1))

    assert r.separator_columns == [5]


def test_los_nombres_compuestos_son_columnas_de_tiempo() -> None:
    """Si no lo fueran, `unpivot_if_time_pivoted` dejaría la hoja ancha."""
    from app.application.pipeline.parsers.time_pivot import is_time_column

    r = parse_hierarchical_headers(_bloque_eph(preambulo=1))
    periodos = [c for i, c in enumerate(r.columns) if i not in (0, *r.separator_columns)]

    assert periodos
    assert all(is_time_column(c) for c in periodos), periodos
    assert not is_time_column(r.columns[0])


def test_el_titulo_no_nombra_la_columna_de_conceptos() -> None:
    """Antes la primera fila "label" era el título del cuadro."""
    r = parse_hierarchical_headers(_bloque_eph(preambulo=1))

    assert not r.columns[0].startswith("Cuadro")


def test_un_caption_sobre_todo_el_bloque_no_prefija_los_periodos() -> None:
    r = parse_hierarchical_headers(_bloque_eph(preambulo=1))

    assert not any("aglomerados" in c for c in r.columns)


def test_etiquetas_que_agrupan_periodos_prefijan_todo_su_grupo() -> None:
    nan = float("nan")
    df = pd.DataFrame(
        [
            ["Concepto", "Exportaciones", nan, "Importaciones", nan],
            [nan, "2024", "2025", "2024", "2025"],
            ["Total", 10.0, 11.0, 20.0, 21.0],
            ["Bienes", 5.0, 6.0, 7.0, 8.0],
        ]
    )
    cols = parse_hierarchical_headers(df).columns

    assert cols == [
        "Concepto",
        "Exportaciones__2024",
        "Exportaciones__2025",
        "Importaciones__2024",
        "Importaciones__2025",
    ]


def test_la_fila_de_anios_puede_estar_mas_abajo_de_la_octava() -> None:
    r = parse_hierarchical_headers(_bloque_eph(preambulo=10))

    assert r.year_row_idx == 12
    assert "2018_T4" in r.columns


def test_una_fila_de_datos_con_valores_tipo_anio_no_es_la_fila_de_anios() -> None:
    """`2016.0` cuenta como año ahora; una fila de datos desordenada no."""
    df = pd.DataFrame(
        [
            ["provincia", "a", "b", "c", "d"],
            ["Salta", 2010.0, 1995.0, 2031.0, 1990.0],
            ["Jujuy", 1.0, 2.0, 3.0, 4.0],
        ]
    )
    r = parse_hierarchical_headers(df)

    assert r.year_row_idx is None
    assert r.columns == ["provincia", "a", "b", "c", "d"]


def test_el_escaneo_no_entra_en_los_datos() -> None:
    """Una fila de trimestres debajo de los datos no es encabezado."""
    df = pd.DataFrame(
        [
            ["provincia", "x", "y", "z"],
            ["Salta", 10.5, 11.5, 12.5],
            ["", "1° trimestre", "2° trimestre", "3° trimestre"],
        ]
    )
    r = parse_hierarchical_headers(df)

    assert r.period_row_idx is None


def test_un_mes_escrito_como_fecha_es_un_mes() -> None:
    """La CVS de 2019 trae marzo y abril como `2019-03-01 00:00:00`."""
    df = pd.DataFrame(
        [
            ["Día", "Año 2019", "", ""],
            ["", "Febrero", "2019-03-01 00:00:00", "2019-04-01 00:00:00"],
            [1, 120, 121, 122],
        ]
    )
    assert parse_hierarchical_headers(df).columns == ["Día", "2019_02", "2019_03", "2019_04"]
