"""Agregados estructurados: la cuenta la arma nuestro código, no el modelo.

El agente no escribe SQL. Para sumar, contar o promediar elige una tabla, una
operación, columnas y filtros, y este módulo arma la consulta:

- todo identificador sale del esquema real de la tabla (nunca se cita un
  nombre que mandó el modelo);
- los valores de filtro van como parámetros ligados;
- las columnas de texto se convierten a número según su formato, decidido
  por columna con una muestra (``consultas.numeros``): "12.500" es doce mil
  quinientos en una columna argentina y doce coma cinco en una inglesa. Antes
  se aceptaba sólo el formato inglés y "12.500" se leía siempre como 12,5;
- la consulta cuenta cuántas filas entraron en el cálculo (``__filas``) y
  cuántas tenían un número (``__filas_con_valor``): sin eso, un filtro que no
  encontraba nada devolvía ``valor = 0`` o ``None``, indistinguible de un dato.
  Si la columna es de texto y su formato no se decidió, cuenta también las
  filas con un número ambiguo (``__filas_ambiguas``), que quedaron afuera
  aunque tengan número.

Existe por el caso Pinamar: el pipeline viejo contó 7.944 filas de una
encuesta y las presentó como 7.944 personas. Una encuesta se expande con su
ponderador; ``ponderar_por`` es esa columna y el resultado es la suma de los
pesos, no la cantidad de filas.

Puro (no toca la base) para poder probar cada rechazo.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from app.application.consultas.fechas import (
    ColumnaFecha,
    condiciones_periodo,
    es_nombre_de_fecha,
    orden_fecha,
    resolver_columna_fecha,
    sin_columna_fecha,
    validar_fecha,
)
from app.application.consultas.filtros import OPERADORES, Filter, sql_filtro, validar_filtros
from app.application.consultas.numeros import expresion_ambiguo, expresion_numero
from app.application.consultas.sql import Params
from app.application.public_catalog import (
    CatalogRequestError,
    is_internal_column,
    quote_ident,
)
from app.domain.value_objects.table_reference import quote_qualified

__all__ = [
    "COLUMNAS_DE_CONTROL",
    "FILAS",
    "FILAS_AMBIGUAS",
    "FILAS_AMBIGUAS_TOTAL",
    "FILAS_CON_VALOR",
    "FILAS_CON_VALOR_TOTAL",
    "FILAS_TOTAL",
    "FILTER_OPERATORS",
    "OPERATIONS",
    "AggregateQuery",
    "AggregateRequest",
    "Filter",
    "build_aggregate_query",
    "numeric_columns",
]

OPERATIONS = ("conteo", "suma", "promedio", "minimo", "maximo")
FILTER_OPERATORS = OPERADORES
MAX_GROUP_BY = 3
MAX_FILTERS = 6
MAX_LIMIT = 200
MAX_FILTER_VALUE = 200

# Alias de las columnas de control. Con guiones bajos para no chocar con una
# columna agrupada que se llame "filas".
FILAS = "__filas"
FILAS_CON_VALOR = "__filas_con_valor"
# Con agrupación: lo mismo sumado sobre TODOS los grupos (ventana sobre el
# resultado agrupado, antes del LIMIT). Si hay más grupos que `limite`, la
# suma de los grupos mostrados no es el total del cálculo.
FILAS_TOTAL = "__filas_total"
FILAS_CON_VALOR_TOTAL = "__filas_con_valor_total"
# Filas que quedaron afuera por tener un número ambiguo ("12.500") en una
# columna de texto cuyo formato no se decidió: no son filas "sin número" y el
# aviso las cuenta aparte (H041).
FILAS_AMBIGUAS = "__filas_ambiguas"
FILAS_AMBIGUAS_TOTAL = "__filas_ambiguas_total"
COLUMNAS_DE_CONTROL = (
    FILAS,
    FILAS_CON_VALOR,
    FILAS_TOTAL,
    FILAS_CON_VALOR_TOTAL,
    FILAS_AMBIGUAS,
    FILAS_AMBIGUAS_TOTAL,
)


@dataclass(frozen=True)
class AggregateRequest:
    table: str
    # [(columna, tipo)] tal como lo devuelve `get_column_types`.
    column_types: list[tuple[str, str]]
    operacion: str
    columna: str | None = None
    ponderar_por: str | None = None
    agrupar_por: list[str] = field(default_factory=list)
    filtros: list[Filter] = field(default_factory=list)
    desde: str | None = None
    hasta: str | None = None
    orden: str = "desc"
    limite: int = 50
    columna_fecha: str | None = None
    # Forma única de la columna de fecha según `pg_stats` (`consultas.preparar`).
    formato_fecha: str | None = None
    # "valor" (por defecto) o una de las columnas de `agrupar_por`: una serie
    # por año se lee mejor ordenada por año que por monto.
    ordenar_por: str | None = None
    # Plegar mayúsculas y acentos fila por fila (ver `consultas.preparar`).
    tolerante: bool = True
    # Formato ("ar"/"en"/None) de las columnas de texto leídas como número.
    formatos: Mapping[str, str | None] | None = None


@dataclass(frozen=True)
class AggregateQuery:
    sql: str
    params: dict[str, Any]
    # Columnas del resultado: las agrupadas, "valor" y las de control.
    columns: list[str]
    fecha: ColumnaFecha | None
    filtros: list[Filter]
    tipos: dict[str, str]
    # Se piden `limite + 1` filas: si llegan todas, el resultado está cortado.
    limite: int
    desde: str | None = None
    hasta: str | None = None


def numeric_columns(req: AggregateRequest) -> list[str]:
    """Las columnas que el cálculo lee como número (no las de los filtros)."""
    cols = [req.ponderar_por] if req.ponderar_por else []
    if req.operacion != "conteo" and req.columna:
        cols.insert(0, req.columna)
    return cols


def _measure(req: AggregateRequest, types: dict[str, str]) -> tuple[str, str | None]:
    """``(medida, expresión cuyo count() son las filas con valor)``."""
    formatos = req.formatos or {}

    def number(column: str) -> str:
        return expresion_numero(column, types[column], formatos.get(column))

    op = req.operacion
    weight = number(req.ponderar_por) if req.ponderar_por else None
    if op == "conteo":
        # Con ponderador, "cuántos" es la suma de los pesos: cada fila de una
        # encuesta representa a muchas personas.
        return (f"sum({weight})", weight) if weight else ("count(*)", None)
    value = number(req.columna or "")
    counted = f"({value} * {weight})" if weight else value
    if op == "suma":
        return (f"sum({value} * {weight})" if weight else f"sum({value})"), counted
    if op == "promedio":
        if weight:
            return (
                f"sum({value} * {weight}) / NULLIF(sum(CASE WHEN {value} IS NOT NULL "
                f"THEN {weight} END), 0)"
            ), counted
        return f"avg({value})", counted
    return f"{'min' if op == 'minimo' else 'max'}({value})", counted


def _ambiguas(req: AggregateRequest, types: dict[str, str]) -> str | None:
    """Condición de las filas con un número ambiguo que el cálculo no leyó.

    Sólo las columnas de texto leídas como número cuyo formato quedó sin
    decidir: con formato, los ambiguos se leen.
    """
    formatos = req.formatos or {}
    condiciones = [
        cond
        for col in numeric_columns(req)
        if formatos.get(col) is None and (cond := expresion_ambiguo(col, types[col])) is not None
    ]
    return " OR ".join(condiciones) if condiciones else None


def build_aggregate_query(req: AggregateRequest) -> AggregateQuery:
    """Arma el SELECT del agregado.

    La columna del valor se llama ``valor``; las de agrupación conservan su
    nombre; ``__filas`` y ``__filas_con_valor`` dicen sobre cuántas filas se
    calculó cada grupo, y con agrupación ``__filas_total`` y
    ``__filas_con_valor_total``, sobre cuántas en todos (también los que el
    LIMIT deja afuera). ``__filas_ambiguas`` (y su ``_total``) cuenta las
    filas con un número ambiguo que no se leyó por falta de formato.
    """
    types = {c: t for c, t in req.column_types if not is_internal_column(c)}
    if not types:
        raise CatalogRequestError("La tabla no tiene columnas consultables.")

    def known(column: str | None, what: str) -> str:
        if not column or column not in types:
            raise CatalogRequestError(
                f"{what} {column!r} no es una columna de la tabla. "
                "Usá describir_tabla para ver las disponibles."
            )
        return column

    if req.operacion not in OPERATIONS:
        raise CatalogRequestError(f"`operacion` es una de: {', '.join(OPERATIONS)}.")
    if req.operacion != "conteo":
        known(req.columna, "La columna")
    if req.ponderar_por:
        known(req.ponderar_por, "La columna de ponderación")
    if len(req.agrupar_por) > MAX_GROUP_BY:
        raise CatalogRequestError(f"Como mucho {MAX_GROUP_BY} columnas en `agrupar_por`.")
    groups = [known(c, "La columna de agrupación") for c in dict.fromkeys(req.agrupar_por)]
    if len(req.filtros) > MAX_FILTERS:
        raise CatalogRequestError(f"Como mucho {MAX_FILTERS} filtros.")
    filtros = validar_filtros(list(req.filtros), types)
    if not 1 <= req.limite <= MAX_LIMIT:
        raise CatalogRequestError(f"`limite` tiene que estar entre 1 y {MAX_LIMIT}.")
    orden = (req.orden or "desc").lower()
    if orden not in ("asc", "desc"):
        raise CatalogRequestError("`orden` es 'asc' o 'desc'.")
    ordenar_por = req.ordenar_por or "valor"
    if ordenar_por != "valor" and ordenar_por not in groups:
        raise CatalogRequestError(
            "`ordenar_por` es 'valor' o una de las columnas de `agrupar_por`."
        )

    params = Params()
    desde = validar_fecha(req.desde, "desde")
    hasta = validar_fecha(req.hasta, "hasta")
    fecha = resolver_columna_fecha(list(types.items()), req.columna_fecha)
    if fecha is not None and req.formato_fecha:
        fecha = replace(fecha, formato=req.formato_fecha)
    where: list[str] = []
    if desde or hasta:
        if fecha is None:
            raise CatalogRequestError(sin_columna_fecha(types))
        where.extend(condiciones_periodo(fecha, desde, hasta, params))
    where.extend(
        sql_filtro(f, types, params, tolerante=req.tolerante, formatos=req.formatos)
        for f in filtros
    )

    measure, counted = _measure(req, types)
    ambiguas = _ambiguas(req, types)
    select = [quote_ident(g) for g in groups] + [f"{measure} AS valor", f"count(*) AS {FILAS}"]
    columns = [*groups, "valor", FILAS]
    if counted is not None:
        select.append(f"count({counted}) AS {FILAS_CON_VALOR}")
        columns.append(FILAS_CON_VALOR)
    if ambiguas is not None:
        select.append(f"count(*) FILTER (WHERE {ambiguas}) AS {FILAS_AMBIGUAS}")
        columns.append(FILAS_AMBIGUAS)
    if groups:
        select.append(f"sum(count(*)) OVER () AS {FILAS_TOTAL}")
        columns.append(FILAS_TOTAL)
        if counted is not None:
            select.append(f"sum(count({counted})) OVER () AS {FILAS_CON_VALOR_TOTAL}")
            columns.append(FILAS_CON_VALOR_TOTAL)
        if ambiguas is not None:
            select.append(
                f"sum(count(*) FILTER (WHERE {ambiguas})) OVER () AS {FILAS_AMBIGUAS_TOTAL}"
            )
            columns.append(FILAS_AMBIGUAS_TOTAL)
    sql = f"SELECT {', '.join(select)} FROM {quote_qualified(req.table)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if groups:
        sql += " GROUP BY " + ", ".join(quote_ident(g) for g in groups)
        if ordenar_por == "valor":
            order = "valor"
        elif es_nombre_de_fecha(ordenar_por) or (fecha is not None and fecha.nombre == ordenar_por):
            order = orden_fecha(ColumnaFecha(ordenar_por, types[ordenar_por]))
        else:
            order = quote_ident(ordenar_por)
        sql += f" ORDER BY {order} {orden.upper()} NULLS LAST"
    sql += f" LIMIT {int(req.limite) + 1}"
    return AggregateQuery(
        sql=sql,
        params=params.values,
        columns=columns,
        fecha=fecha,
        filtros=filtros,
        tipos=types,
        limite=int(req.limite),
        desde=desde,
        hasta=hasta,
    )
