"""Wire-level tests for `_post_parse_normalize` invocation inside the
collector parser (specs/021-parser-hardening Phase 2).

These exercise that the post-parse pass runs at the end of `_read_excel_frame`
and `_read_csv_preview` regardless of which retry path was taken, without
recreating the full collector environment (no DB, no Celery).
"""

from __future__ import annotations

import io
import random
from pathlib import Path

import pandas as pd

from app.infrastructure.celery.tasks.collector_tasks import (
    _detect_csv_params,
    _post_parse_normalize,
    _read_csv_preview,
    _read_excel_frame,
)


def test_post_parse_normalize_idempotent_on_clean():
    df = pd.DataFrame({"provincia": ["BA", "Córdoba"], "anio": [2020, 2021], "valor": [1.0, 2.0]})
    out = _post_parse_normalize(df)
    assert list(out.columns) == ["provincia", "anio", "valor"]
    assert len(out) == 2


def test_post_parse_normalize_dedupes_byte_collision():
    """Two long names sharing first 63 bytes collide at Postgres CREATE TABLE.
    The dedup must split them in byte space."""
    long_a = "Cuadro 3.1 Población por condición — A_" + "x" * 80
    long_b = "Cuadro 3.1 Población por condición — A_" + "y" * 80
    df = pd.DataFrame({long_a: [1.0], long_b: [2.0]})
    out = _post_parse_normalize(df)
    assert len(set(out.columns)) == 2
    assert all(len(c.encode("utf-8")) <= 63 for c in out.columns)


def test_post_parse_normalize_drops_string_nan_columns():
    """DEBT-021-004 root cause: pandas reads string sentinels like `'None'`
    AS-IS, so `dropna(how='all')` doesn't collapse cols that look
    populated but carry only NaN markers. The normalize pass converts
    sentinels to real NaN BEFORE the dropna call.
    """
    df = pd.DataFrame(
        {
            "real_col": ["a", "b", "c"],
            "ghost_None": ["None", "None", "None"],
            "ghost_mix": ["s/d", "n/a", "NaN"],
            "another_real": [1, 2, 3],
        }
    )
    out = _post_parse_normalize(df)
    assert "real_col" in out.columns
    assert "another_real" in out.columns
    assert "ghost_None" not in out.columns
    assert "ghost_mix" not in out.columns


def test_post_parse_normalize_unpivots_time_pivoted_layout():
    """DEBT-021-001 / Phase 5: a wide year-pivoted table gets melted to
    long format `(id, periodo, valor)`."""
    df = pd.DataFrame(
        {
            "Provincia": ["BA", "Córdoba", "Santa Fe"],
            "2020": [1.0, 2.0, 3.0],
            "2021": [1.1, 2.1, 3.1],
            "2022": [1.2, 2.2, 3.2],
            "2023": [1.3, 2.3, 3.3],
        }
    )
    out = _post_parse_normalize(df)
    # 4 of 5 cols are years (>= 0.5 threshold), 1 id col (Provincia)
    assert "periodo" in out.columns
    assert "valor" in out.columns
    assert "Provincia" in out.columns
    # 3 provinces × 4 periods = 12 long rows
    assert len(out) == 12


def test_post_parse_normalize_does_not_unpivot_long_input():
    """Already-long input passes through unchanged."""
    df = pd.DataFrame(
        {
            "provincia": ["BA", "Córdoba", "BA"],
            "periodo": ["2020", "2020", "2021"],
            "valor": [1.5, 2.5, 1.6],
        }
    )
    out = _post_parse_normalize(df)
    assert list(out.columns) == ["provincia", "periodo", "valor"]
    assert len(out) == 3


def test_post_parse_normalize_skips_unpivot_below_min_cols():
    """Tiny table with 3 cols including 2 years: no unpivot (cols < 5)."""
    df = pd.DataFrame(
        {
            "x": ["a", "b"],
            "2020": [1, 2],
            "2021": [3, 4],
        }
    )
    out = _post_parse_normalize(df)
    # Should pass through (cols < 5)
    assert list(out.columns) == ["x", "2020", "2021"]


def test_post_parse_normalize_keeps_partial_nan_cols():
    """Cols with SOME real values survive even if other cells are sentinels."""
    df = pd.DataFrame(
        {
            "partial": ["real", "None", "data"],
            "real": ["a", "b", "c"],
        }
    )
    out = _post_parse_normalize(df)
    assert "partial" in out.columns
    assert "real" in out.columns


def test_post_parse_normalize_recovers_col_n_with_buried_header():
    """`col_0..col_2` placeholder cols + real header in row 0 → cols recovered."""
    df = pd.DataFrame(
        {
            "col_0": ["provincia", "BA", "Córdoba"],
            "col_1": ["anio", "2020", "2021"],
            "col_2": ["valor", "1.5", "2.0"],
        }
    )
    out = _post_parse_normalize(df)
    assert "provincia" in out.columns
    assert "anio" in out.columns
    assert "valor" in out.columns
    assert len(out) == 2  # header row consumed


def test_read_csv_preview_applies_post_parse(tmp_path: Path):
    """End-to-end: a CSV file with a TITLE row promoted as header gets
    recovered.

    Realistic shape: row 0 has only the title in the first cell, pandas
    fills the rest with empties → on read with default header=0 we get
    `["title", "Unnamed:1", "Unnamed:2", "Unnamed:3", "Unnamed:4"]`. The
    auto-detect retry inside `_read_csv_preview` should kick in here, so we
    just assert the final columns are clean (no `Unnamed:`, no `Cuadro`).
    """
    csv_path = tmp_path / "input.csv"
    csv_path.write_text(
        "Cuadro 3.1 Población,,,,\n"
        "provincia,anio,valor,categoria,fuente\n"
        "BA,2020,1.5,A,X\n"
        "Córdoba,2021,2.0,B,Y\n",
        encoding="utf-8",
    )
    df = _read_csv_preview(str(csv_path), nrows=10)
    cols_lower = [str(c).lower() for c in df.columns]
    # The retry path should have found the real header at row 1.
    assert any("provincia" in c for c in cols_lower)
    assert any("anio" in c for c in cols_lower)
    # No leftover Unnamed: nor Cuadro titles.
    assert not any("unnamed" in c.lower() for c in df.columns)
    assert not any("cuadro" in c.lower() for c in df.columns)


def test_read_excel_frame_applies_post_parse(tmp_path: Path):
    """Same idea but for Excel — write an XLSX with title row + real header."""
    xlsx_path = tmp_path / "input.xlsx"
    df_in = pd.DataFrame(
        [
            ["Cuadro 3.1 Población", None, None],
            ["provincia", "anio", "valor"],
            ["BA", 2020, 1.5],
            ["Córdoba", 2021, 2.0],
        ]
    )
    df_in.to_excel(xlsx_path, index=False, header=False)

    df = _read_excel_frame(io.BytesIO(xlsx_path.read_bytes()), nrows=10)
    cols_str = [str(c) for c in df.columns]
    # No leftover "Cuadro" titles in cols
    assert not any("cuadro" in c.lower() for c in cols_str)
    # The real headers should be there one way or another
    assert any("provincia" in c.lower() for c in cols_str) or any(
        "anio" in c.lower() for c in cols_str
    )


# ── Separador de miles (hackatón del 30-oct-2026) ─────────────────────────
#
# `00._educ_superior_datos.csv` (datos.gob.ar 6ac26ff9): VALOR son cantidades
# de personas con punto de miles. Con los valores por defecto de pandas «2.134»
# se guardaba 2,134 y los estudiantes de grado de 2023 sumaban ~5.700.

EDUC_HEADER = (
    "ANIO;TIPO_UNIV;NIVEL_ACADEM;OF_ACADEM;DISCIP_OCDE;DISCIP_ESPECIF;TIPO_ALUMNO;U_MED;VALOR"
)
# Las primeras filas del archivo real, tal cual (latin-1).
EDUC_PRIMERAS = [
    "2014;Privado;Grado;Otros;Ciencias Agrícolas;Ciencias Agropecuarias;EGRESADOS;"
    "Cantidad de personas;124",
    "2014;Privado;Grado;Otros;Ciencias Agrícolas;Ciencias Agropecuarias;ESTUDIANTES;"
    "Cantidad de personas;1.475",
    "2014;Privado;Grado;Otros;Ciencias Agrícolas;Veterinaria;EGRESADOS;Cantidad de personas;119",
    "2014;Privado;Grado;Otros;Ciencias Agrícolas;Veterinaria;ESTUDIANTES;"
    "Cantidad de personas;2.134",
    "2014;Privado;Posgrado;Doctorado;Ciencias Agrícolas;Ciencias Agropecuarias;EGRESADOS;"
    "Cantidad de personas;2",
]


def _con_puntos(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def _educ_csv(path: Path, filas: int = 400) -> Path:
    """El formato real: las primeras filas del archivo y después cantidades de
    hasta seis cifras, todas con punto de miles (como en el archivo, que no
    trae ningún millón ni ninguna coma)."""
    rng = random.Random(30)
    lineas = [EDUC_HEADER, *EDUC_PRIMERAS]
    for i in range(filas):
        anio = 2014 + i % 10
        tipo = "ESTUDIANTES" if i % 2 else "EGRESADOS"
        valor = rng.randint(1_000, 120_000) if i % 2 else rng.randint(0, 999)
        lineas.append(
            f"{anio};Estatal;Grado;Otros;Ciencias Sociales;Disciplina {i % 37};{tipo};"
            f"Cantidad de personas;{_con_puntos(valor)}"
        )
    path.write_text("\n".join(lineas) + "\n", encoding="latin-1")
    return path


def test_educ_superior_lee_el_punto_como_miles(tmp_path: Path):
    csv_path = _educ_csv(tmp_path / "00._educ_superior_datos.csv")

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=1000, csv_params=params)

    assert (params["thousands"], params["decimal"]) == (".", ",")
    assert pd.api.types.is_numeric_dtype(df["VALOR"])
    assert df["VALOR"].head(5).tolist() == [124, 1475, 119, 2134, 2]
    assert df["VALOR"].max() > 1000
    assert pd.api.types.is_integer_dtype(df["ANIO"])
    assert df["U_MED"].iloc[0] == "Cantidad de personas"


def _enut_csv(path: Path, valores: list[str]) -> Path:
    lineas = ["actividad;sexo;minutos_por_dia"]
    lineas += [f"Actividad {i};{'Mujer' if i % 2 else 'Varón'};{v}" for i, v in enumerate(valores)]
    path.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return path


def test_enut_sin_evidencia_no_decide_miles(tmp_path: Path):
    """H001: «500» junto a «58.333» son también minutos con tres decimales
    ingleses. Sin evidencia no se decide miles: la columna queda como texto y
    `consultas.numeros` se niega a calcular en vez de leer mil veces más."""
    valores = ["500", "58.333", "120", "241.667", "35", "12.250", "90", "300", "47.125"]
    csv_path = _enut_csv(tmp_path / "enut.csv", valores)

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=100, csv_params=params)

    assert "thousands" not in params
    assert not pd.api.types.is_numeric_dtype(df["minutos_por_dia"])
    assert df["minutos_por_dia"].tolist() == valores


def test_tres_decimales_con_evidencia_inglesa_siguen_siendo_decimales(tmp_path: Path):
    valores = ["500", "58.333", "12.5", "7.25", "33.1", "0.5", "41.75", "120"]
    csv_path = _enut_csv(tmp_path / "enut_con_decimales.csv", valores)

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=100, csv_params=params)

    assert "thousands" not in params and "dtype" not in params
    assert df["minutos_por_dia"].tolist() == [500, 58.333, 12.5, 7.25, 33.1, 0.5, 41.75, 120]


def test_archivo_argentino_deja_como_texto_lo_que_pandas_leeria_mal(tmp_path: Path):
    """Con `thousands="."` pandas borra el punto en cualquier valor: «-34.6037»
    daría -346037 y «30.10.2026», 30102026. Esas columnas van como texto."""
    lineas = ["fecha;lat;monto;cantidad"]
    for i in range(30):
        lineas.append(
            f"{1 + i % 28:02d}.10.2026;-34.{6037 + i};{_con_puntos(1_000_000 + i * 1234)},5{i % 10};"
            f"{_con_puntos(i * 37)}"
        )
    csv_path = tmp_path / "montos.csv"
    csv_path.write_text("\n".join(lineas) + "\n", encoding="utf-8")

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=100, csv_params=params)

    assert (params["thousands"], params["decimal"]) == (".", ",")
    assert df["fecha"].iloc[0] == "01.10.2026"
    assert df["lat"].iloc[0] == "-34.6037"
    assert df["monto"].iloc[1] == 1_001_234.51
    assert df["cantidad"].iloc[28] == 1036


def test_ambiguo_con_enteros_de_cuatro_cifras_queda_como_texto(tmp_path: Path):
    """Quien escribe «2.134» con punto de miles no escribe «1475» sin él."""
    lineas = ["provincia;monto;poblacion"]
    for i in range(30):
        lineas.append(f"P{i};{_con_puntos(1_000_000 + i * 1234)},5;{_con_puntos(100 + i * 97)}")
    lineas.append("P30;2.000.000,5;1475")
    csv_path = tmp_path / "mezcla.csv"
    csv_path.write_text("\n".join(lineas) + "\n", encoding="utf-8")

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=100, csv_params=params)

    assert params["thousands"] == "."
    assert pd.api.types.is_float_dtype(df["monto"])
    assert not pd.api.types.is_numeric_dtype(df["poblacion"])
    assert df["poblacion"].iloc[20] == "2.040"
