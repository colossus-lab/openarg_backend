"""`codificacion`: en qué codificación está un CSV, con bytes como los de los portales."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from app.application.pipeline.parsers.codificacion import detectar_codificacion
from app.infrastructure.celery.tasks.collector_tasks import _detect_csv_params, _read_csv_preview

TEXTO = (
    "provincia;detalle;anio\n"
    "Córdoba;Diseño y Niñez;2020\n"
    "Neuquén;Educación Física;2021\n"
    "Entre Ríos;¿Señor Peña? Nº 5;2022\n"
)


def _detectar(datos: bytes) -> str:
    return detectar_codificacion(io.BytesIO(datos))


@pytest.mark.parametrize("codificacion", ["latin-1", "windows-1252"])
def test_latin1_y_windows1252_no_son_cp1250(codificacion: str):
    """`charset_normalizer` decía cp1250 y «Diseño» quedaba «Diseńo»."""
    detectada = _detectar(TEXTO.encode(codificacion))

    assert detectada == "windows-1252"
    assert TEXTO.encode(codificacion).decode(detectada) == TEXTO


def test_cp850_de_verdad():
    """Bytes reales de `lineas_y_empresas_de_transporte`: «COMPA¥ÖA» es COMPAÑÍA."""
    assert _detectar("COMPAÑÍA;Córdoba;Neuquén;vías\n".encode("cp850")) == "cp850"


def test_un_solo_byte_alto_en_cp850():
    """Archivos de VIH: el único byte alto es la «á» de «más» (0xA0)."""
    assert _detectar("edad;65 y más\n1;2\n".encode("cp850")) == "cp850"


def test_comillas_de_word_son_windows1252():
    """Sin acentos, sólo tipografía: 0x92 y 0x96 no son «í» ni «ñ» de Mac."""
    assert _detectar("a;Don’t – stop\n".encode("windows-1252")) == "windows-1252"


def test_byte_sin_definir_en_windows1252_pasa_a_latin1():
    """Python falla al decodificar 0x81 en windows-1252."""
    assert _detectar("Córdoba;Diseño\n".encode("latin-1") + b"\x81\n") == "latin-1"


def test_ascii_y_utf8_se_leen_como_utf8():
    assert _detectar(b"a;b\n1;2\n") == "utf-8-sig"
    assert _detectar(TEXTO.encode("utf-8")) == "utf-8-sig"
    assert _detectar(b"") == "utf-8-sig"


def test_utf8_doblemente_codificado_en_origen_se_lee_como_esta():
    """Es UTF-8 válido. Antes salía cp1251 o cp949, y quedaba peor."""
    roto = "ContrataciÃ³n;COMPAÃ‘ÃA\n".encode()

    assert _detectar(roto * 50) == "utf-8-sig"


@pytest.mark.parametrize(
    ("codificacion", "esperada"),
    [("utf-8-sig", "utf-8-sig"), ("utf-16", "utf-16"), ("utf-32", "utf-32")],
)
def test_bom(codificacion: str, esperada: str):
    assert _detectar(TEXTO.encode(codificacion)) == esperada


@pytest.mark.parametrize("codificacion", ["utf-16-le", "utf-16-be"])
def test_utf16_sin_bom(codificacion: str):
    assert _detectar(TEXTO.encode(codificacion)) == codificacion


def test_secuencia_utf8_partida_entre_bloques_sigue_siendo_utf8():
    """La «ñ» cae justo en el borde del primer bloque de 1 MiB."""
    relleno = b"a" * ((1 << 20) - 1)

    assert _detectar(relleno + "ñ;ó\n".encode()) == "utf-8-sig"


def test_primer_acento_despues_del_primer_bloque():
    relleno = b"a;b\n" * (1 << 19)

    assert _detectar(relleno + "Diseño;Córdoba\n".encode("latin-1")) == "windows-1252"


def test_la_tabla_queda_con_la_enie(tmp_path: Path):
    """De punta a punta: nombres de columna y valores, por donde carga el colector."""
    csv_path = tmp_path / "educ.csv"
    csv_path.write_bytes(("Año;Diseño\n2023;Nº 5\n2024;¿Señor?\n").encode("latin-1"))

    params = _detect_csv_params(str(csv_path))
    df = _read_csv_preview(str(csv_path), nrows=10, csv_params=params)

    assert params["encoding"] == "windows-1252"
    assert list(df.columns) == ["Año", "Diseño"]
    assert df["Diseño"].tolist() == ["Nº 5", "¿Señor?"]
