"""Agregados estructurados: la cuenta la arma nuestro código, no el modelo.

El agente no escribe SQL. Para sumar, contar o promediar elige una tabla, una
operación, columnas y filtros, y este módulo arma la consulta:

- todo identificador sale del esquema real de la tabla (nunca se cita un
  nombre que mandó el modelo);
- los valores de filtro van como literales escapados y acotados;
- las columnas de texto se convierten a número sólo cuando el texto es un
  número limpio (``1234.5``), así una celda "s/d" no tira la consulta.

Existe por el caso Pinamar: el pipeline viejo contó 7.944 filas de una
encuesta y las presentó como 7.944 personas. Una encuesta se expande con su
ponderador; ``ponderar_por`` es esa columna y el resultado es la suma de los
pesos, no la cantidad de filas.

Puro (no toca la base) para poder probar cada rechazo.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.application.public_catalog import (
    CatalogRequestError,
    date_column,
    is_internal_column,
    quote_ident,
)
from app.domain.value_objects.table_reference import quote_qualified

OPERATIONS = ("conteo", "suma", "promedio", "minimo", "maximo")
FILTER_OPERATORS = ("=", "!=", ">", ">=", "<", "<=", "contiene")
MAX_GROUP_BY = 3
MAX_FILTERS = 6
MAX_LIMIT = 200
MAX_FILTER_VALUE = 200

_NUMERIC_TYPES = re.compile(
    r"^(smallint|integer|bigint|numeric|real|double precision|decimal)", re.IGNORECASE
)
_NUMBER_RE = re.compile(r"^-?\d+(\.\d+)?$")
_DATE_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
# Lo que un texto tiene que parecer para convertirse a número dentro de la
# consulta. Formato con punto decimal y sin separador de miles.
_SQL_NUMBER_PATTERN = r"^\s*-?[0-9]+(\.[0-9]+)?\s*$"


@dataclass(frozen=True)
class Filter:
    columna: str
    operador: str
    valor: str


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


def _is_numeric_type(data_type: str) -> bool:
    return bool(_NUMERIC_TYPES.match(data_type or ""))


def _as_number(column: str, data_type: str) -> str:
    """La columna como número: directo si ya lo es, convertida si es texto."""
    ident = quote_ident(column)
    if _is_numeric_type(data_type):
        return ident
    return (
        f"(CASE WHEN {ident}::text ~ '{_SQL_NUMBER_PATTERN}' THEN trim({ident}::text)::numeric END)"
    )


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _measure(req: AggregateRequest, types: dict[str, str]) -> str:
    op = req.operacion
    weight = _as_number(req.ponderar_por, types[req.ponderar_por]) if req.ponderar_por else None
    if op == "conteo":
        # Con ponderador, "cuántos" es la suma de los pesos: cada fila de una
        # encuesta representa a muchas personas.
        return f"sum({weight})" if weight else "count(*)"
    value = _as_number(req.columna or "", types[req.columna or ""])
    if op == "suma":
        return f"sum({value} * {weight})" if weight else f"sum({value})"
    if op == "promedio":
        if weight:
            return (
                f"sum({value} * {weight}) / NULLIF(sum(CASE WHEN {value} IS NOT NULL "
                f"THEN {weight} END), 0)"
            )
        return f"avg({value})"
    return f"{'min' if op == 'minimo' else 'max'}({value})"


def _filter_sql(f: Filter, types: dict[str, str]) -> str:
    ident = quote_ident(f.columna)
    value = str(f.valor)
    if len(value) > MAX_FILTER_VALUE:
        raise CatalogRequestError(f"El valor del filtro sobre {f.columna!r} es demasiado largo.")
    if f.operador == "contiene":
        pattern = _escape(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        return f"{ident}::text ILIKE '%{pattern}%'"
    if f.operador in ("=", "!="):
        return f"{ident}::text {f.operador} '{_escape(value)}'"
    # Comparaciones de orden: sólo con números.
    if not _NUMBER_RE.match(value.strip()):
        raise CatalogRequestError(
            f"El filtro {f.operador} sobre {f.columna!r} necesita un número (recibí {value!r})."
        )
    return f"{_as_number(f.columna, types[f.columna])} {f.operador} {value.strip()}"


def _check_date(value: str | None, name: str) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not _DATE_RE.match(value):
        raise CatalogRequestError(f"`{name}` tiene que ser AAAA, AAAA-MM o AAAA-MM-DD.")
    return value


def build_aggregate_query(req: AggregateRequest) -> tuple[str, list[str]]:
    """Arma el SELECT del agregado. Devuelve ``(sql, columnas_del_resultado)``.

    La columna del valor se llama ``valor``; las de agrupación conservan su
    nombre.
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
    for f in req.filtros:
        known(f.columna, "La columna del filtro")
        if f.operador not in FILTER_OPERATORS:
            raise CatalogRequestError(
                f"Operador {f.operador!r}: usá {', '.join(FILTER_OPERATORS)}."
            )
    if not 1 <= req.limite <= MAX_LIMIT:
        raise CatalogRequestError(f"`limite` tiene que estar entre 1 y {MAX_LIMIT}.")
    orden = (req.orden or "desc").lower()
    if orden not in ("asc", "desc"):
        raise CatalogRequestError("`orden` es 'asc' o 'desc'.")

    where = [_filter_sql(f, types) for f in req.filtros]
    desde = _check_date(req.desde, "desde")
    hasta = _check_date(req.hasta, "hasta")
    if desde or hasta:
        fecha = date_column(types)
        if fecha is None:
            raise CatalogRequestError("Esta tabla no tiene una columna de fecha para filtrar.")
        if desde:
            where.append(f"left({quote_ident(fecha)}::text, {len(desde)}) >= '{desde}'")
        if hasta:
            where.append(f"left({quote_ident(fecha)}::text, {len(hasta)}) <= '{hasta}'")

    select = [quote_ident(g) for g in groups] + [f"{_measure(req, types)} AS valor"]
    sql = f"SELECT {', '.join(select)} FROM {quote_qualified(req.table)}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    if groups:
        sql += " GROUP BY " + ", ".join(quote_ident(g) for g in groups)
        sql += f" ORDER BY valor {orden.upper()} NULLS LAST"
    sql += f" LIMIT {int(req.limite)}"
    return sql, [*groups, "valor"]
