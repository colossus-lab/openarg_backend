"""El cuadro 1.6 de la EPH tiene que llegar con sus años reales.

En el hackatón del 30-oct-2026 el agente dijo que la serie de desocupación
"sólo llega hasta 2T-2011". La tabla tenía 1T-2017 a 2T-2025: el lector de
encabezados no reconoció `Año 2017`, guardó los trimestres sin año, y el
renombrado por LLM los fechó 2003-2011.

El fixture es la hoja `Cuadro 1.6 ` de `cuadros_tasas_indicadores_eph_09_26.xls`
(INDEC, publicada en sep-2026), recortada a las filas de encabezado, la tasa
general, el bloque de sexo y edad, la fila con el `-` en la columna separadora
y la fuente. Las celdas van tal cual: `1°` y `1º` mezclados, notas `(1)` y el
asterisco de dato provisorio.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from app.infrastructure.celery.tasks.indec_tasks import _parse_xls_sheet

FIXTURE = Path(__file__).parent.parent / "fixtures" / "indec_eph_cuadro_1_6_09_26.xlsx"
HOJA = "Cuadro 1.6 "


@pytest.fixture(scope="module")
def cuadro() -> pd.DataFrame:
    df = _parse_xls_sheet(FIXTURE.read_bytes(), HOJA)
    assert df is not None
    return df


def _valor(df: pd.DataFrame, indicador: str, periodo: str) -> float:
    fila = df[(df["Indicador"] == indicador) & (df["periodo"] == periodo)]
    assert len(fila) == 1, (indicador, periodo, fila)
    return float(fila["valor"].iloc[0])


def test_la_hoja_queda_en_formato_largo(cuadro: pd.DataFrame) -> None:
    assert list(cuadro.columns) == ["Indicador", "periodo", "valor"]


def test_los_periodos_son_los_del_archivo(cuadro: pd.DataFrame) -> None:
    esperados = [f"{a}_T{t}" for a in range(2017, 2026) for t in range(1, 5)]
    esperados += ["2026_T1", "2026_T2"]

    assert sorted(cuadro["periodo"].unique()) == esperados


def test_ningun_anio_anterior_a_2017(cuadro: pd.DataFrame) -> None:
    assert not cuadro["periodo"].str.startswith(("200", "2010", "2011")).any()


def test_la_tasa_de_desocupacion_coincide_con_la_publicada(cuadro: pd.DataFrame) -> None:
    """13,1 % en el 2T-2020 (pandemia) y 7,9 % en el 2T-2026, el último dato."""
    assert _valor(cuadro, "Tasa de desocupación", "2020_T2") == pytest.approx(13.1)
    assert _valor(cuadro, "Tasa de desocupación", "2025_T2") == pytest.approx(7.6)
    assert _valor(cuadro, "Tasa de desocupación", "2026_T2") == pytest.approx(7.9)


def test_los_trimestres_con_nota_conservan_su_dato(cuadro: pd.DataFrame) -> None:
    """`3° trimestre (1)` en 2019, `3° trimestre (2)` en 2020, `2° trimestre*` en 2026."""
    assert _valor(cuadro, "Tasa de desocupación", "2019_T3") == pytest.approx(9.0)
    assert _valor(cuadro, "Tasa de desocupación", "2020_T3") == pytest.approx(11.7)
    assert _valor(cuadro, "Mujeres hasta 29 años", "2026_T2") == pytest.approx(1.7)


def test_la_columna_separadora_no_llega_a_la_tabla(cuadro: pd.DataFrame) -> None:
    """La columna 25 tiene un `-` en "No definido"; antes sobrevivía como `col_25`."""
    assert not any(str(c).startswith("col_") for c in cuadro.columns)
    assert not cuadro["periodo"].str.startswith("col_").any()
    assert (cuadro["Indicador"] == "No definido").sum() == 38


def test_el_titulo_y_la_fuente_no_son_filas_de_datos(cuadro: pd.DataFrame) -> None:
    assert not cuadro["Indicador"].astype(str).str.startswith(("Cuadro", "Fuente")).any()


# ── Hojas con un bloque por año, o una serie por bloque ───────────────────────


def _xlsx(filas: list[list]) -> bytes:
    import io

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(filas).to_excel(w, sheet_name="hoja", header=False, index=False)
    return buf.getvalue()


NAN = float("nan")


def test_cada_bloque_apilado_lleva_su_propio_anio() -> None:
    """La CVS del INDEC: `Año 2016` con sus meses, los días, y otra vez con 2017.

    Leída con el primer encabezado solo, todo quedaba fechado 2016: el mismo
    error que el 1.6, pero hecho por el parser.
    """
    filas = [
        ["Coeficiente de variación salarial", NAN, NAN],
        ["Día", "Año 2016", NAN],
        [NAN, "Enero", "Febrero"],
        [1, 100.5, 101.5],
        [2, 100.6, 101.6],
        [NAN, NAN, "(continúa)"],
        ["Día", "Año 2017", NAN],
        [NAN, "Enero", "Febrero"],
        [1, 110.5, 111.5],
        [2, 110.6, 111.6],
    ]
    df = _parse_xls_sheet(_xlsx(filas), "hoja")

    assert df is not None
    periodos = set(df["periodo"])
    assert {"2016_01", "2016_02", "2017_01", "2017_02"} <= periodos
    enero_2017 = df[(df["periodo"] == "2017_01") & (df["Día"] == 1)]
    assert float(enero_2017["valor"].iloc[0]) == pytest.approx(110.5)
    assert "serie" not in df.columns  # sin títulos propios, no hay series que separar


def test_bloques_con_titulos_distintos_quedan_identificados() -> None:
    """El anexo de pobreza apila CBA, inversa de Engel y CBT con la misma forma."""
    filas = [
        ["Serie canasta básica alimentaria", NAN, NAN],
        [NAN, NAN, NAN],
        ["Región", 2026, NAN],
        [NAN, "Enero", "Febrero"],
        ["Cuyo", 180246.53, 185489.02],
        [NAN, NAN, NAN],
        ["Serie inversa del coeficiente de Engel", NAN, NAN],
        [NAN, NAN, NAN],
        ["Región", 2026, NAN],
        [NAN, "Enero", "Febrero"],
        ["Cuyo", 2.32, 2.31],
    ]
    df = _parse_xls_sheet(_xlsx(filas), "hoja")

    assert df is not None
    assert list(df.columns) == ["serie", "Región", "periodo", "valor"]
    engel = df[(df["serie"].str.startswith("Serie inversa")) & (df["periodo"] == "2026_01")]
    assert float(engel["valor"].iloc[0]) == pytest.approx(2.32)


def test_una_fila_de_datos_con_fecha_y_decimales_no_es_encabezado() -> None:
    """`canasta_basica|Variaciones`: `2017-02-01 | 1.8 | 3` no es una fila de meses."""
    from app.application.pipeline.parsers import parse_hierarchical_headers

    raw = pd.DataFrame(
        [
            ["Mes", "Canasta básica alimentaria", NAN, "Canasta básica total", NAN],
            [NAN, "Variación mensual", "Variación interanual", "Variación mensual", NAN],
            ["2017-02-01 00:00:00", 1.8, 3, "///", 2.6],
            ["2017-03-01 00:00:00", 2.1, 3, "///", 2.4],
        ]
    )

    assert parse_hierarchical_headers(raw).period_row_idx is None
