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
from decimal import Decimal
from typing import Any

from app.application.consultas.fechas import (
    ColumnaFecha,
    claves_orden,
    condiciones_periodo,
    consulta_rango,
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
    "MAX_OFFSET",
    "ORDEN_FISICO_MAX_FILAS",
    "CatalogRequestError",
    "DataQuery",
    "DataRequest",
    "build_data_query",
    "build_date_range_query",
    "build_sample_query",
    "date_column",
    "is_internal_column",
    "json_rows",
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
# Paginar más allá no tiene sentido con 500 filas por pedido: para eso están
# los filtros, el período o `agregar_datos`. Y con orden por fecha, Postgres
# guarda en memoria `offset + limite` filas para ordenarlas.
MAX_OFFSET = 10_000
# Hasta cuántas filas una tabla sin columna de fecha se ordena por su posición
# física (`ctid`, el orden del archivo). Las tablas del catálogo no tienen
# índices: `ORDER BY ctid` las recorre enteras. Medido en staging el 05-oct:
# de 80 ms a 6 s en 100.000 filas según la carga, 11 s en 300.000 y más del
# timeout en 600.000, contra 10-120 ms sin ordenar. Por encima se lee en el
# orden en que están guardadas y la respuesta lo dice.
ORDEN_FISICO_MAX_FILAS = 50_000


def is_internal_column(name: str) -> bool:
    """Columnas de bookkeeping del colector (`_source_url`, `_ingested_at`…)."""
    return name.startswith("_")


def _json_number(value: Decimal) -> int | float | None:
    if not value.is_finite():
        # `numeric` admite NaN e Infinity, que no son JSON.
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def json_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Filas con números de JSON: un ``Decimal`` sale como número, no como texto.

    Los agregados leen los números como ``::numeric`` y psycopg devuelve
    ``Decimal``, que Pydantic serializa como string dentro de un
    ``dict[str, Any]``: ``/catalogo/agregar`` devolvía
    ``"valor": "5793524.174913833"`` y un cliente que sumara o comparara
    valores concatenaba texto u ordenaba alfabéticamente (revisión del PR
    #139). Un entero sale como entero (``505``, no ``505.0``); el resto como
    float, la misma precisión que ya usa el agente (``plain_rows``).
    """
    return [
        {k: _json_number(v) if isinstance(v, Decimal) else v for k, v in row.items()}
        for row in rows
    ]


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
    # Filas a saltear, para pedir la página siguiente.
    offset: int = 0
    # Pedir `limite + 1` filas: si llegan todas, hay más. Antes `truncado` era
    # `cantidad >= limite` y una tabla de exactamente 100 filas decía que
    # había más (auditoría ok.3).
    una_de_mas: bool = False
    # Sin columna de fecha, ordenar por la posición física (`ctid`). Lo decide
    # quien llama según el tamaño de la tabla (`ORDEN_FISICO_MAX_FILAS`).
    orden_fisico: bool = False


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
    limite: int = DEFAULT_LIMIT
    # Cómo quedaron ordenadas las filas: "fecha", "fisico" (la posición en la
    # tabla, que es el orden del archivo) o None (el orden en que Postgres
    # las lea: estable en la práctica, sin garantía).
    orden: str | None = None


def _es_mart(table: str) -> bool:
    return table.strip().lower().replace('"', "").startswith("mart.")


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
    if not 0 <= req.offset <= MAX_OFFSET:
        raise CatalogRequestError(
            f"`offset` tiene que estar entre 0 y {MAX_OFFSET}. Para recorrer más filas acotá con "
            "`desde`/`hasta` o `filtros`, o calculá totales con agregar_datos."
        )
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
    # Desempate por posición física: dos filas con la misma fecha (una serie
    # por provincia) salían en cualquier orden, y la página siguiente podía
    # repetir o saltear filas. Los marts son vistas materializadas (tienen
    # `ctid`), pero una vista común no: ahí no se arriesga.
    desempate = "" if _es_mart(req.table) else "ctid"
    criterio: str | None = None
    if fecha is not None:
        # Las fechas que no se reconocen van al final en los dos sentidos:
        # antes, con el orden del texto crudo, "1/9/2025" quedaba como el
        # último dato de una serie que llega a 2026. En una tabla con año y
        # mes separados, el mes es la segunda clave: con el año solo,
        # `orden=desc` traía enero como el último dato (H010).
        sentido = f"{orden.upper()} NULLS LAST"
        sql += " ORDER BY " + ", ".join(f"{clave} {sentido}" for clave in claves_orden(fecha))
        if desempate:
            sql += f", {desempate}"
        criterio = "fecha"
    elif req.orden_fisico and desempate:
        sql += f" ORDER BY {desempate}"
        criterio = "fisico"
    sql += f" LIMIT {int(req.limite) + (1 if req.una_de_mas else 0)}"
    if req.offset:
        sql += f" OFFSET {int(req.offset)}"
    return DataQuery(
        sql=sql,
        params=params.values,
        columns=columns,
        fecha=fecha,
        filtros=filtros,
        tipos=tipos,
        desde=desde,
        hasta=hasta,
        limite=int(req.limite),
        orden=criterio,
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
