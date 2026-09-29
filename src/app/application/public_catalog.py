"""Modo datos de la API pública: buscar, describir y leer tablas sin pasar por el LLM.

El MCP público ofrece dos modos. En el de respuestas (`/ask`) OpenArg razona y
paga Bedrock; en éste el modelo del usuario razona y OpenArg sólo sirve el
catálogo y las filas. Por eso acá no hay SQL del usuario: la consulta se arma
en código a partir de una tabla que existe, columnas que existen y filtros
con forma validada. Después pasa igual por el validador del sandbox, que
sigue siendo la segunda barrera.

Este módulo es puro (no toca la base) para poder testear cada rechazo.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from app.application.pipeline.chart_builder import is_date_column
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo
from app.domain.value_objects.table_reference import bare_name, quote_qualified

MAX_LIMIT = 500
DEFAULT_LIMIT = 100
MAX_COLUMNS = 30
MAX_FILTERS = 5
MAX_FILTER_VALUE = 200
SAMPLE_ROWS = 5

_DATE_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


class CatalogRequestError(ValueError):
    """Pedido inválido; el mensaje se le puede mostrar tal cual al usuario."""


def is_internal_column(name: str) -> bool:
    """Columnas de bookkeeping del colector (`_source_url`, `_ingested_at`…)."""
    return name.startswith("_")


def resolve_table(requested: str, tables: Iterable[CachedTableInfo]) -> CachedTableInfo | None:
    """La tabla consultable que corresponde a `requested` (calificada o pelada).

    Sólo se sirve lo que el sandbox lista como listo; cualquier otro nombre,
    aunque exista en la base, no es una tabla del catálogo.
    """
    wanted = bare_name(requested or "").lower()
    if not wanted:
        return None
    for table in tables:
        if bare_name(table.table_name).lower() == wanted:
            return table
    return None


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def date_column(columns: Iterable[str]) -> str | None:
    return next((c for c in columns if is_date_column(c)), None)


@dataclass(frozen=True)
class DataRequest:
    table: str  # nombre calificado, tal como lo lista el sandbox
    available_columns: list[str]
    columns: list[str] | None = None
    desde: str | None = None
    hasta: str | None = None
    filtros: dict[str, str] | None = None
    orden: str = "asc"
    limite: int = DEFAULT_LIMIT


def _check_date(value: str | None, field: str) -> str | None:
    if value is None or value == "":
        return None
    value = value.strip()
    if not _DATE_RE.match(value):
        raise CatalogRequestError(
            f"`{field}` tiene que ser una fecha AAAA, AAAA-MM o AAAA-MM-DD (recibí {value!r})."
        )
    return value


def build_data_query(req: DataRequest) -> tuple[str, list[str]]:
    """Arma el SELECT de un pedido de datos. Devuelve `(sql, columnas)`.

    Todo identificador sale de `available_columns` (el schema real de la
    tabla), así que nunca se cita un nombre que mandó el usuario. Los valores
    de filtro son strings acotados y escapados; las fechas tienen forma ISO.
    """
    visible = [c for c in req.available_columns if not is_internal_column(c)]
    if not visible:
        raise CatalogRequestError("La tabla no tiene columnas consultables.")

    if req.columns:
        unknown = [c for c in req.columns if c not in visible]
        if unknown:
            raise CatalogRequestError(
                f"Columnas que no existen en la tabla: {', '.join(unknown[:5])}. "
                "Usá describir_tabla para ver las disponibles."
            )
        columns = list(dict.fromkeys(req.columns))[:MAX_COLUMNS]
    else:
        columns = visible[:MAX_COLUMNS]

    if not 1 <= req.limite <= MAX_LIMIT:
        raise CatalogRequestError(f"`limite` tiene que estar entre 1 y {MAX_LIMIT}.")
    orden = (req.orden or "asc").lower()
    if orden not in ("asc", "desc"):
        raise CatalogRequestError("`orden` es 'asc' o 'desc'.")

    where: list[str] = []
    desde = _check_date(req.desde, "desde")
    hasta = _check_date(req.hasta, "hasta")
    fecha = date_column(visible)
    if (desde or hasta) and fecha is None:
        raise CatalogRequestError(
            "Esta tabla no tiene una columna de fecha para filtrar por período."
        )
    if fecha and desde:
        where.append(f"left({quote_ident(fecha)}::text, {len(desde)}) >= '{desde}'")
    if fecha and hasta:
        where.append(f"left({quote_ident(fecha)}::text, {len(hasta)}) <= '{hasta}'")

    filtros = req.filtros or {}
    if len(filtros) > MAX_FILTERS:
        raise CatalogRequestError(f"Como mucho {MAX_FILTERS} filtros.")
    for col, value in filtros.items():
        if col not in visible:
            raise CatalogRequestError(
                f"No se puede filtrar por {col!r}: no es una columna de la tabla."
            )
        text = str(value)
        if len(text) > MAX_FILTER_VALUE:
            raise CatalogRequestError(f"El valor del filtro {col!r} es demasiado largo.")
        escaped = text.replace("'", "''")
        where.append(f"{quote_ident(col)}::text = '{escaped}'")

    sql = f"SELECT {', '.join(quote_ident(c) for c in columns)} FROM {quote_qualified(req.table)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if fecha:
        sql += f" ORDER BY {quote_ident(fecha)} {orden.upper()}"
    sql += f" LIMIT {int(req.limite)}"
    return sql, columns


def build_sample_query(table: str, columns: list[str]) -> str:
    visible = [c for c in columns if not is_internal_column(c)][:MAX_COLUMNS]
    cols = ", ".join(quote_ident(c) for c in visible) or "*"
    return f"SELECT {cols} FROM {quote_qualified(table)} LIMIT {SAMPLE_ROWS}"


def build_date_range_query(table: str, fecha: str) -> str:
    col = quote_ident(fecha)
    return (
        f"SELECT min(left({col}::text, 10)) AS desde, max(left({col}::text, 10)) AS hasta "
        f"FROM {quote_qualified(table)} WHERE {col}::text ~ '^[0-9]{{4}}'"
    )
