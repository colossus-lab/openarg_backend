"""Cómo se nombra una tabla, cuando el mismo nombre viene de dos formas.

El sandbox reporta las tablas de la capa raw calificadas (`raw.cache_x`) y
las legacy de `public` peladas (`cache_x`). Todo el vocabulario que las
consume —el prompt del planner, `KEYWORD_ROUTES`, `table_catalog`, las notas
de compatibilidad— habla la forma pelada. Como el engine del sandbox corre
con `search_path = public,raw`, las dos formas resuelven a la misma tabla al
ejecutar: la diferencia importa sólo al **comparar** nombres.

Este módulo es el único lugar donde se decide qué significa "el mismo
nombre". Sin él, cada sitio que compara vuelve a inventar la regla y el
siguiente que se agregue la olvida — que es exactamente lo que pasó entre
mayo y septiembre de 2026.

Hay otras cuatro implementaciones de `split_qualified` repartidas por
infraestructura (`pg_sandbox_adapter`, `legacy_serving_adapter`,
`catalog_enrichment_tasks`, `ingestion_findings_sweep`). Son candidatas a
migrar acá, pero funcionan y no se tocan en este cambio.
"""

from __future__ import annotations

# Schemas que el `search_path` del sandbox resuelve solo, o sea donde un
# nombre pelado y uno calificado nombran la misma tabla. `mart` queda afuera
# a propósito: no está en el search_path, así que `mart.x` y `x` son cosas
# distintas y un glob `cache_*` no debe alcanzar un mart.
LAYER_TRANSPARENT_SCHEMAS = frozenset({"public", "raw"})

DEFAULT_SCHEMA = "public"


def split_qualified(name: str, *, default_schema: str = DEFAULT_SCHEMA) -> tuple[str, str]:
    """Parte `schema.tabla` en sus dos mitades, tolerando el nombre pelado.

    Acepta las tres formas que circulan por el sistema: pelada (`cache_x`),
    calificada (`raw.cache_x`) y calificada con comillas (`raw."cache_x"`,
    que es lo que emite el backfill del catálogo).
    """
    value = (name or "").strip().strip('"')
    if "." not in value:
        return default_schema, value
    schema, _, table = value.partition(".")
    return schema.strip('"') or default_schema, table.strip('"')


def bare_name(name: str) -> str:
    """El nombre de la tabla sin su schema."""
    return split_qualified(name)[1]


def schema_name(name: str) -> str:
    """El schema de la tabla; `public` si el nombre viene pelado."""
    return split_qualified(name)[0]


def quote_qualified(name: str) -> str:
    """Cita el nombre para SQL, cada parte por separado.

    `raw.cache_x` → `"raw"."cache_x"`. Citar el nombre entero produce
    `"raw.cache_x"`, un identificador único con un punto adentro que
    Postgres busca en `public` y no encuentra.
    """
    schema, table = split_qualified(name)
    if "." not in (name or "").strip().strip('"'):
        return f'"{table}"'
    return f'"{schema}"."{table}"'
