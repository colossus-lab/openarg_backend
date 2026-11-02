"""Una hoja con una columna por semestre queda en formato largo."""

from __future__ import annotations

import io

import pandas as pd
import pytest
from openpyxl import Workbook

from app.infrastructure.celery.tasks import indec_tasks as indec

_SEMESTRES = [
    "2do. semestre 2016",
    "1er semestre 2020",
    "2do. semestre 2019 (1)",
    "1er. semestre 2026*",
]
_CUADRO_1 = [
    ["Pobreza", None, None, None, None],
    ["Hogares", 21.5, 30.4, 25.9, 24.4],
    ["Personas", 30.3, 40.9, 35.5, 32.3],
    ["Indigencia", None, None, None, None],
    ["Hogares ", 4.5, 8.1, 5.7, 5.6],
    ["Personas", 6.1, 10.5, 8.0, 7.5],
    ["(1) Los resultados del semestre no incluyen a Ushuaia.", None, None, None, None],
    ["Fuente: INDEC. Encuesta Permanente de Hogares.", None, None, None, None],
]


def _hoja() -> pd.DataFrame:
    return pd.DataFrame(_CUADRO_1, columns=["Indicador", *_SEMESTRES])


def test_cada_valor_lleva_su_medida_y_su_indicador() -> None:
    largo = indec._unpivot_semesters(_hoja())
    assert list(largo.columns) == ["seccion", "Indicador", "periodo", "valor"]
    assert len(largo) == 16
    fila = largo[(largo["periodo"] == "1er. semestre 2026*") & (largo["valor"] == 7.5)].iloc[0]
    assert (fila["seccion"], fila["Indicador"]) == ("Indigencia", "Personas")
    fila = largo[(largo["periodo"] == "1er. semestre 2026*") & (largo["valor"] == 24.4)].iloc[0]
    assert (fila["seccion"], fila["Indicador"]) == ("Pobreza", "Hogares")


def test_las_notas_no_son_titulos_ni_filas() -> None:
    largo = indec._unpivot_semesters(_hoja())
    assert set(largo["seccion"]) == {"Pobreza", "Indigencia"}
    assert set(largo["Indicador"]) == {"Hogares", "Personas"}


def test_sin_filas_de_titulo_no_inventa_la_seccion() -> None:
    hoja = pd.DataFrame(
        [["Hogares", 1.0, 2.0, 3.0, 4.0], ["Personas", 5.0, 6.0, 7.0, 8.0]],
        columns=["Indicador", *_SEMESTRES],
    )
    assert list(indec._unpivot_semesters(hoja).columns) == ["Indicador", "periodo", "valor"]


@pytest.mark.parametrize(
    "columnas",
    [
        # Más de una columna de rótulo: quedarían casi todas `col_N` y la puerta
        # de ingesta las rechazaría (cuadros 2.1 y 2.2 del mismo informe).
        ["col_0", "col_1", *_SEMESTRES[:3]],
        # Encabezados compuestos con el semestre adentro (cuadro 3.1).
        ["Grupos", "1er. semestre 2026*_Total_%", "1er. semestre 2026*_Pobres_%", "x_%", "y_%"],
        # Columnas que no son semestres (cuadro 5).
        ["Rubro", "1° semestre 2025", "2° semestre 2025", "Variación", "Otra"],
    ],
)
def test_otras_hojas_quedan_como_estaban(columnas: list[str]) -> None:
    hoja = pd.DataFrame([["a", 1.0, 2.0, 3.0, 4.0]], columns=columnas)
    pd.testing.assert_frame_equal(indec._unpivot_semesters(hoja), hoja)


def test_el_lector_deja_el_cuadro_1_en_formato_largo() -> None:
    libro = Workbook()
    hoja = libro.active
    hoja.title = "Cuadro 1"
    hoja.append(["Cuadro 1. Incidencia de la pobreza y de la indigencia"])
    hoja.append([])
    hoja.append(["Indicador", *_SEMESTRES])
    for fila in _CUADRO_1:
        hoja.append(fila)
    salida = io.BytesIO()
    libro.save(salida)
    largo = indec._parse_xls_sheet(salida.getvalue(), "Cuadro 1")
    assert largo is not None
    assert list(largo.columns) == ["seccion", "Indicador", "periodo", "valor"]
    assert len(largo) == 16
