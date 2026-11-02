"""La regla de `formato_numeros`: con qué separadores se lee cada CSV."""

from __future__ import annotations

from app.application.pipeline.parsers.formato_numeros import (
    MAX_FILAS_MUESTRA,
    MIN_FILAS_MUESTRA,
    FormatoCsv,
    decidir_formato,
    filas_de_muestra,
)

# Cantidades con punto de miles, enteros sueltos y ceros finales: educación superior.
MILES = ["124", "1.475", "119", "2.134", "2", "1.470", "35", "2.130", "88", "12.300", "7"]
MILES += ["45.610", "3", "9.990", "640", "1.000", "17"]


def test_miles_con_ceros_finales_pasa_el_archivo_a_formato_argentino():
    formato = decidir_formato([["2014"] * len(MILES), ["Privado"] * len(MILES), MILES])

    assert formato == FormatoCsv(argentino=True, como_texto=())
    assert formato.opciones_read_csv() == {"thousands": ".", "decimal": ","}


def test_enut_sin_ceros_finales_no_decide_miles():
    """H001: «500» y «58.333» también son tres decimales ingleses."""
    enut = ["500", "58.333", "120", "241.667", "35", "47.125", "90", "300", "18.875"]

    formato = decidir_formato([["Mujer"] * len(enut), enut])

    assert formato == FormatoCsv(argentino=False, como_texto=(1,))
    assert formato.opciones_read_csv() == {"dtype": {1: str}}


def test_con_evidencia_inglesa_los_tres_decimales_se_leen_como_siempre():
    valores = ["500", "58.333", "12.5", "7.25", "33.1", "0.5", "41.75"]

    assert decidir_formato([valores]) == FormatoCsv()


def test_pocos_decimales_ingleses_no_alcanzan():
    assert decidir_formato([["500", "58.333", "12.5"]]) == FormatoCsv(como_texto=(0,))


def test_ambiguos_con_un_entero_de_cuatro_cifras_no_son_miles():
    con_1475 = [*MILES, "1475"]

    assert decidir_formato([con_1475]) == FormatoCsv(como_texto=(0,))


def test_en_formato_argentino_solo_se_leen_como_numero_las_columnas_revisadas():
    montos = ["1.234.567,5", "2.000.000", "12,5", "3,75", "1.000,25", "8"]
    columnas = [
        montos,
        ["30.10.2026", "01.11.2026", "02.11.2026", "03.11.2026", "04.11.2026", "05.11.2026"],
        ["-34.6037", "-34.61", "-34.7", "-34.62", "-34.59", "-34.58"],
        ["1,250", "3", "4", "5", "6", "7"],
        ["Ciencias Sociales"] * 6,
        ["2014", "2015", "2016", "2017", "2018", "2019"],
        ["2.134", "15", "1475", "3", "4", "5"],
    ]

    formato = decidir_formato(columnas)

    assert formato.argentino
    # fechas, longitudes, «1,250» sin evidencia propia, y «2.134» junto a «1475».
    assert formato.como_texto == (1, 2, 3, 6)


def test_valores_vacios_y_nan_no_cuentan():
    assert decidir_formato([[None, float("nan"), "", "  "]]) == FormatoCsv()


def test_filas_de_muestra_segun_el_ancho():
    assert filas_de_muestra(9) == MAX_FILAS_MUESTRA
    assert filas_de_muestra(100) == 2_500
    assert filas_de_muestra(5_000) == MIN_FILAS_MUESTRA
