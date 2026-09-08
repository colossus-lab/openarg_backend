from __future__ import annotations

import re

from app.domain.value_objects.table_reference import split_qualified

_TABLE_NAME_RE = re.compile(r"^cache_[a-z0-9_]{1,100}$")

# Identificador sin calificar: lo que puede ir entre comillas sin más.
_RELATION_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,120}$")

# Schemas desde los que se acepta leer. Lista blanca a propósito: la
# alternativa (permitir cualquier punto) convierte un control en un colador.
_ALLOWED_SCHEMAS = frozenset({"public", "raw", "mart"})


def validate_table_name(name: str) -> bool:
    """Return True if the table name matches the expected cached-dataset pattern.

    Deliberadamente estricta y sin schema: `analyst_tasks` la llama con
    nombres pelados que salen de `cached_datasets`, y ahí tiene que seguir
    siendo un `cache_*` y nada más. Para relaciones calificadas está
    `safe_relation_query`.
    """
    return bool(_TABLE_NAME_RE.match(name))


def safe_table_query(table_name: str, template: str) -> str | None:
    """Validate table_name and return a formatted query, or None if invalid.

    ``template`` must contain exactly one ``{}`` placeholder for the table name.
    Example: ``'SELECT * FROM "{}" LIMIT 10'``
    """
    if not validate_table_name(table_name):
        return None
    return template.format(table_name)


def safe_relation_query(name: str, template: str) -> str | None:
    """Como `safe_table_query`, pero acepta `schema.tabla` y cita cada parte.

    Existe porque el regex de `validate_table_name` no admite el punto, así
    que desde que las tablas se reportan como `raw.cache_x` rechazaba todo y
    el `last_resort` de NL2SQL quedó apagado — para la capa raw y también
    para los marts, que nunca habían pasado.

    ``template`` lleva un ``{}`` **sin comillas**: las pone esta función,
    por parte, para no emitir `"raw.cache_x"` (un identificador con punto
    adentro, que Postgres busca en `public`).
    """
    schema, table = split_qualified(name)
    if schema not in _ALLOWED_SCHEMAS:
        return None
    if not _RELATION_NAME_RE.match(table):
        return None
    return template.format(f'"{schema}"."{table}"')
