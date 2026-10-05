"""Modo datos de la API pública: buscar, describir y leer tablas sin pasar por el LLM.

El MCP público ofrece dos modos. En el de respuestas (`/ask`) OpenArg razona y
paga Bedrock; en éste el modelo del usuario razona y OpenArg sólo sirve el
catálogo y las filas. Por eso acá no hay SQL del usuario: la consulta se arma
en código a partir de una tabla que existe, columnas que existen y filtros
con forma validada. Los valores viajan como parámetros ligados y la consulta
pasa igual por el validador del sandbox, que sigue siendo la segunda barrera.

Las mismas funciones las usa el agente (`answers/tools/catalogo.py`): las
piezas compartidas (fechas, filtros, números) viven en
`app.application.consultas`.

Este módulo es puro (no toca la base) para poder testear cada rechazo.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from app.application.consultas.fechas import (
    ColumnaFecha,
    condiciones_periodo,
    consulta_rango,
    orden_fecha,
    resolver_columna_fecha,
    sin_columna_fecha,
    validar_fecha,
)
from app.application.consultas.filtros import Filter, leer_filtros, sql_filtro, validar_filtros
from app.application.consultas.sql import CatalogRequestError, Params, quote_ident
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo
from app.domain.value_objects.table_reference import bare_name, quote_qualified

__all__ = [
    "DEFAULT_LIMIT",
    "MAX_COLUMNS",
    "MAX_FILTERS",
    "MAX_LIMIT",
    "CatalogRequestError",
    "DataQuery",
    "DataRequest",
    "build_data_query",
    "build_date_range_query",
    "build_sample_query",
    "date_column",
    "is_internal_column",
    "quote_ident",
    "resolve_date_column",
    "resolve_table",
]

MAX_LIMIT = 500
DEFAULT_LIMIT = 100
MAX_COLUMNS = 30
MAX_FILTERS = 5
MAX_FILTER_VALUE = 200
SAMPLE_ROWS = 5


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


def resolve_date_column(
    columns: Iterable[tuple[str, str]] | Iterable[str], chosen: str | None = None
) -> ColumnaFecha | None:
    """La columna de fecha (con su tipo), sin las internas del colector."""
    pares = [(c, "text") if isinstance(c, str) else (str(c[0]), str(c[1])) for c in columns]
    return resolver_columna_fecha([p for p in pares if not is_internal_column(p[0])], chosen)


def date_column(columns: Iterable[tuple[str, str]] | Iterable[str]) -> str | None:
    """El nombre de la columna de fecha (ver `resolve_date_column`)."""
    fecha = resolve_date_column(columns)
    return fecha.nombre if fecha else None


@dataclass(frozen=True)
class DataRequest:
    table: str  # nombre calificado, tal como lo lista el sandbox
    available_columns: list[str]
    columns: list[str] | None = None
    desde: str | None = None
    hasta: str | None = None
    # {columna: valor} (igualdad, la forma de siempre), una lista de
    # {columna, operador, valor} o filtros ya preparados.
    filtros: Mapping[str, Any] | list[Any] | None = None
    orden: str = "asc"
    limite: int = DEFAULT_LIMIT
    # [(columna, tipo)] de `get_column_types`. Sin esto, todo es texto.
    column_types: list[tuple[str, str]] | None = None
    # La columna de fecha que pidió el usuario, si no quiere la que se detecta.
    columna_fecha: str | None = None
    # Forma única de los valores de la columna de fecha, según `pg_stats`
    # (`consultas.preparar`): permite una expresión directa en vez del CASE.
    formato_fecha: str | None = None
    # Plegar mayúsculas y acentos fila por fila (ver `consultas.preparar`).
    tolerante: bool = True
    # Formato de las columnas de texto que se comparan como número.
    formatos: Mapping[str, str | None] | None = None


@dataclass(frozen=True)
class DataQuery:
    sql: str
    params: dict[str, Any]
    columns: list[str]
    fecha: ColumnaFecha | None
    filtros: list[Filter]
    # {columna visible: tipo}
    tipos: dict[str, str]
    desde: str | None = None
    hasta: str | None = None


def visible_types(
    available_columns: Iterable[str], column_types: Iterable[tuple[str, str]] | None
) -> dict[str, str]:
    known = dict(column_types or [])
    return {c: known.get(c, "text") for c in available_columns if not is_internal_column(c)}


def build_data_query(req: DataRequest) -> DataQuery:
    """Arma el SELECT de un pedido de datos.

    Todo identificador sale de `available_columns` (el schema real de la
    tabla), así que nunca se cita un nombre que mandó el usuario. Los valores
    de los filtros y las fechas van como parámetros ligados.
    """
    tipos = visible_types(req.available_columns, req.column_types)
    visible = list(tipos)
    if not visible:
        raise CatalogRequestError("La tabla no tiene columnas consultables.")

    if req.columns:
        unknown = [c for c in req.columns if c not in tipos]
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

    desde = validar_fecha(req.desde, "desde")
    hasta = validar_fecha(req.hasta, "hasta")
    fecha = resolver_columna_fecha(list(tipos.items()), req.columna_fecha)
    if fecha is not None and req.formato_fecha:
        fecha = replace(fecha, formato=req.formato_fecha)
    if (desde or hasta) and fecha is None:
        raise CatalogRequestError(sin_columna_fecha(visible))

    if isinstance(req.filtros, list) and all(isinstance(f, Filter) for f in req.filtros):
        crudos: list[Filter] = list(req.filtros)
    else:
        crudos = leer_filtros(req.filtros, MAX_FILTERS)
    filtros = validar_filtros(crudos, tipos)

    params = Params()
    where: list[str] = []
    if fecha is not None:
        where.extend(condiciones_periodo(fecha, desde, hasta, params))
    where.extend(
        sql_filtro(f, tipos, params, tolerante=req.tolerante, formatos=req.formatos)
        for f in filtros
    )

    sql = f"SELECT {', '.join(quote_ident(c) for c in columns)} FROM {quote_qualified(req.table)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if fecha is not None:
        # Las fechas que no se reconocen van al final en los dos sentidos:
        # antes, con el orden del texto crudo, "1/9/2025" quedaba como el
        # último dato de una serie que llega a 2026.
        sql += f" ORDER BY {orden_fecha(fecha)} {orden.upper()} NULLS LAST"
    sql += f" LIMIT {int(req.limite)}"
    return DataQuery(
        sql=sql,
        params=params.values,
        columns=columns,
        fecha=fecha,
        filtros=filtros,
        tipos=tipos,
        desde=desde,
        hasta=hasta,
    )


def build_sample_query(table: str, columns: list[str]) -> str:
    visible = [c for c in columns if not is_internal_column(c)][:MAX_COLUMNS]
    cols = ", ".join(quote_ident(c) for c in visible) or "*"
    return f"SELECT {cols} FROM {quote_qualified(table)} LIMIT {SAMPLE_ROWS}"


def build_date_range_query(table: str, fecha: ColumnaFecha | str, tipo: str = "text") -> str:
    """Desde, hasta, `reconocidas` y `con_valor` de la columna de fecha.

    Con `reconocidas = 0` y `con_valor > 0` la columna tiene fechas en un
    formato que no se sabe filtrar: hay que decirlo, no informar un período
    vacío.
    """
    columna = fecha if isinstance(fecha, ColumnaFecha) else ColumnaFecha(fecha, tipo)
    return consulta_rango(quote_qualified(table), columna)
