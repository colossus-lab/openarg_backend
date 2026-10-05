"""¿Este texto es un nombre de columna o un valor de una fila?

Lo comparten dos lados que tienen que estar de acuerdo:

- la inferencia de encabezado (`header_inference.token_looks_like_data`), que
  no debe promover a encabezado una fila con estos valores;
- el detector `header_from_data`, que marca las tablas cuyos nombres de columna
  ya son estos valores.

Tres clases:

- `data`: no puede ser un encabezado (UUID, URL, email, fecha ISO con hora `T`,
  fecha-hora SAS, código `HCDN285290`, expediente `0011-PE-2024`, dígitos
  largos, geometría en hexadecimal, una hora distinta de medianoche).
- `number`: número con formato de dato (`0.46`, `1.234.567`, `3812.0`, `45%`).
  Un decimal suelto (`1.1`) NO: en las encuestas del INDEC es un código de
  pregunta, y es también el `1` repetido que pandas desambigua.
- `period`: legítimo como columna de un cuadro ancho (años, `2015-11-01
  00:00:00`, meses, trimestres, `2016.1` que es el segundo `2016`).

Módulo listado en `parser_fingerprint._PARSER_MODULES`.
"""

from __future__ import annotations

import re

KIND_DATA = "data"
KIND_NUMBER = "number"
KIND_PERIOD = "period"

PG_NAME_LIMIT = 63

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_URL_RE = re.compile(r"^(?:https?|ftp)://|^www\.", re.I)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.I)
# Fecha con hora en formato ISO con `T`: así sale un valor de texto de un CSV o
# un JSON. Un encabezado de Excel con fecha llega como `2015-11-01 00:00:00`.
_ISO_T_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
# Fecha con hora separada por espacio: medianoche es un período, otra hora no.
_ISO_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} (\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?$")
# Fecha-hora con formato SAS: `08FEB1973:00:00:00`.
_SAS_DATETIME_RE = re.compile(r"^\d{2}[A-Z]{3}\d{4}(?::\d{2}:\d{2}:\d{2})?$", re.I)
# `HCDN285290`, `TMT30038070`: identificadores de un registro.
_CODE_RE = re.compile(r"^[A-Z]{2,6}\d{5,}$")
# Expedientes `0011-PE-2024`, `1234-D-2019`.
_EXPEDIENTE_RE = re.compile(r"^\d{1,5}-[A-Z]{1,4}-\d{2,4}$")
# Dígitos largos: CUIT, DNI, códigos de registro. Un año tiene 4.
_LONG_DIGITS_RE = re.compile(r"^\d{7,}$")
# Geometría PostGIS en hexadecimal (WKB).
_HEX_BLOB_RE = re.compile(r"^[0-9A-F]{24,}$")

_THOUSANDS_RE = re.compile(r"^[-+]?\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?$")
_DECIMALS_RE = re.compile(r"^[-+]?\d+[.,]\d{2,}$")
_FLOAT_INT_RE = re.compile(r"^[-+]?\d{3,}\.0+$")
_PERCENT_RE = re.compile(r"^[-+]?\d+(?:[.,]\d+)?\s?%$")
_NEGATIVE_RE = re.compile(r"^-\d+(?:[.,]\d+)?$")

_YEAR_RE = re.compile(r"^(?:1[89]\d{2}|20\d{2}|2100)(?:\.0)?\*?$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}(?:-\d{2})?$")
_YEAR_MONTH_RE = re.compile(r"^(?:\d{4}[-/]\d{1,2}|\d{1,2}[-/]\d{4}|\d{4}(?:0[1-9]|1[0-2]))$")
_DMY_RE = re.compile(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$")
_QUARTER_RE = re.compile(
    r"^(?:\d{4}\s*[-_ ]?\s*[QTS][1-4]|[QTS][1-4]\s*[-_ ]?\s*\d{4}|[IV]{1,3}[-_ ]\d{4})$", re.I
)
_MONTHS = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "setiembre",
    "octubre",
    "noviembre",
    "diciembre",
    "ene",
    "feb",
    "mar",
    "abr",
    "may",
    "jun",
    "jul",
    "ago",
    "sep",
    "set",
    "oct",
    "nov",
    "dic",
)
_MONTH_PERIOD_RE = re.compile(
    r"^(?:" + "|".join(_MONTHS) + r")\.?(?:[\s\-_/]*(?:de\s+)?(?:\d{2}|\d{4}))?$", re.I
)
_PERIOD_WORDS_RE = re.compile(r"\b(?:trimestre|semestre|bimestre|cuatrimestre)\b", re.I)

# Sufijo de deduplicación que agregan `make_unique_columns` (`_2`) o pandas (`.1`).
_DEDUP_SUFFIX_RE = re.compile(r"^(.+?)(?:_\d{1,3}|\.\d{1,3})$")


def normalize_name(name: object) -> str:
    """El nombre como lo deja Postgres: sin espacios de borde y cortado a 63 bytes."""
    value = str(name if name is not None else "").replace("\xa0", "").strip()
    encoded = value.encode("utf-8")
    if len(encoded) > PG_NAME_LIMIT:
        value = encoded[:PG_NAME_LIMIT].decode("utf-8", errors="ignore")
    return value


def strip_dedup_suffix(name: str) -> str:
    match = _DEDUP_SUFFIX_RE.match(name)
    return match.group(1) if match else name


def _classify_token(token: str) -> str | None:
    if not token:
        return None
    # Antes que los números: pandas escribe un año de Excel leído como float
    # como `2020.0`, y eso es un pivot, no un dato.
    if _YEAR_RE.match(token):
        return KIND_PERIOD
    if (
        _UUID_RE.match(token)
        or _URL_RE.match(token)
        or _EMAIL_RE.match(token)
        or _ISO_T_RE.match(token)
        or _SAS_DATETIME_RE.match(token)
        or _CODE_RE.match(token)
        or _EXPEDIENTE_RE.match(token)
        or _LONG_DIGITS_RE.match(token)
        or _HEX_BLOB_RE.match(token)
    ):
        return KIND_DATA
    time_match = _ISO_TIME_RE.match(token)
    if time_match:
        hh, mm, ss = time_match.group(1), time_match.group(2), time_match.group(3) or "00"
        return KIND_PERIOD if (hh, mm, ss) == ("00", "00", "00") else KIND_DATA
    if (
        _ISO_DATE_RE.match(token)
        or _YEAR_MONTH_RE.match(token)
        or _DMY_RE.match(token)
        or _QUARTER_RE.match(token)
        or _MONTH_PERIOD_RE.match(token)
        or _PERIOD_WORDS_RE.search(token)
    ):
        return KIND_PERIOD
    if (
        _THOUSANDS_RE.match(token)
        or _DECIMALS_RE.match(token)
        or _FLOAT_INT_RE.match(token)
        or _PERCENT_RE.match(token)
        or _NEGATIVE_RE.match(token)
    ):
        return KIND_NUMBER
    return None


def classify_column_name(name: object) -> str | None:
    """`"data"`, `"number"`, `"period"` o None (parece un encabezado común).

    Mira también las partes de un encabezado combinado (`A / B`, el separador de
    `combine_header_rows`) y el nombre sin el sufijo de deduplicación: `2016.1`
    es el segundo `2016` de un cuadro, no el número 2016,1.
    """
    value = normalize_name(name)
    if not value:
        return None
    own = _classify_token(value)
    base = strip_dedup_suffix(value)
    base_kind = _classify_token(base) if base != value else None
    parts = (
        [_classify_token(p.strip()) for p in value.split(" / ") if p.strip()]
        if " / " in value
        else []
    )
    kinds = {own, base_kind, *parts}
    if KIND_DATA in kinds:
        return KIND_DATA
    if base_kind == KIND_PERIOD and own in (KIND_NUMBER, None):
        return KIND_PERIOD
    if KIND_NUMBER in kinds:
        return KIND_NUMBER
    if KIND_PERIOD in kinds:
        return KIND_PERIOD
    return None
