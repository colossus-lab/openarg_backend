"""Elegir la tabla física detrás de un hint del planner.

El vocabulario de este módulo (los alias legacy, los `startswith`) está
escrito en nombres **pelados**, porque así los emiten `KEYWORD_ROUTES` y el
prompt del planner. Las tablas, en cambio, pueden llegar calificadas
(`raw.cache_x`). Por eso todo lo que compara pasa por
`table_reference.bare_name`.
"""

from __future__ import annotations

import fnmatch
import re

from app.domain.value_objects.table_reference import (
    LAYER_TRANSPARENT_SCHEMAS,
    bare_name,
    split_qualified,
)

_RESOURCE_SUFFIX_RE = re.compile(r"_r[a-f0-9]{10}$")
_GROUP_SUFFIX_RE = re.compile(r"_g[a-f0-9]{8}$")
_SCHEMA_SUFFIX_RE = re.compile(r"_s[a-f0-9]{8}$")
_YEAR_SUFFIX_RE = re.compile(r"_(19\d{2}|20[0-2]\d)$")

_LEGACY_TABLE_ALIASES = {
    "cache_bcra_principales_variables": "cache_bcra_cotizaciones",
    "cache_series_tiempo_ipc": "cache_series_inflacion_ipc",
}

_LEGACY_HINT_ALIASES = {
    "cache_series_tiempo_*": "cache_series_*",
    "cache_presupuesto_nacional": "cache_presupuesto_*",
    "cache_coparticipacion": "cache_presupuesto_*",
    "cache_bcra_principales_variables": "cache_bcra_*",
}


def hint_matches_table(pattern: str, table_name: str) -> bool:
    """¿El hint del planner nombra a esta tabla?

    El planner y `KEYWORD_ROUTES` emiten globs pelados (`cache_indec_*`),
    pero el sandbox reporta las tablas de la capa raw calificadas
    (`raw.cache_indec_ipc`). Comparar el nombre completo contra el glob
    pelado no matchea nunca — eso dejó el ruteo roto desde que se activó
    `OPENARG_USE_RAW_LAYER` (2026-08).

    El orden de las cláusulas importa:

    1. Match directo, que cubre calificado↔calificado y los globs `mart.*`.
    2. Si el patrón nombra un schema explícito, se respeta y no se despela:
       pedir `raw.cache_*` no puede traer una tabla de `public`.
    3. Si la tabla vive en un schema que el `search_path` NO resuelve
       (`mart`), tampoco se despela. Así un glob `cache_*` sigue sin
       alcanzar un mart, que es la semántica que BUG-001 fijó: los marts
       entran por re-inyección, no por matcheo de globs.
    4. Recién ahí se compara contra el nombre sin schema.
    """
    if fnmatch.fnmatch(table_name, pattern):
        return True
    if "." in pattern:
        return False
    if split_qualified(table_name)[0] not in LAYER_TRANSPARENT_SCHEMAS:
        return False
    return fnmatch.fnmatch(bare_name(table_name), pattern)


def table_base_name(table_name: str) -> str:
    """Strip resource/schema/group suffixes used by collector-derived tables."""
    name = _RESOURCE_SUFFIX_RE.sub("", table_name)
    name = _GROUP_SUFFIX_RE.sub("", name)
    name = _SCHEMA_SUFFIX_RE.sub("", name)
    return name


def table_priority(table_name: str) -> int:
    """Lower is better: prefer consolidated group tables over staging/resource tables."""
    if _GROUP_SUFFIX_RE.search(table_name):
        return 0
    if _SCHEMA_SUFFIX_RE.search(table_name):
        return 1
    if _RESOURCE_SUFFIX_RE.search(table_name):
        return 3
    return 2


def prefer_consolidated_table(table_name: str, available_tables: list[str]) -> str:
    """Choose the best visible table for a logical dataset/group."""
    base = table_base_name(table_name)
    candidates = [name for name in available_tables if table_base_name(name) == base]
    if not candidates:
        return table_name
    return min(candidates, key=lambda name: (table_priority(name), name))


def expand_table_hints_compat(table_hints: list[str]) -> list[str]:
    """Expand legacy/stale table hints into current cache patterns."""
    expanded: list[str] = []
    for hint in table_hints:
        expanded.append(hint)
        alias = _LEGACY_HINT_ALIASES.get(hint)
        if alias:
            expanded.append(alias)
    return list(dict.fromkeys(expanded))


def _extract_year(table_name: str) -> int:
    match = _YEAR_SUFFIX_RE.search(table_name)
    return int(match.group(1)) if match else -1


def resolve_compat_table_name(table_name: str, available_tables: list[str]) -> str | None:
    """Map legacy exact table names to current physical cache tables when possible.

    Devuelve el nombre **tal como vino en `available_tables`** (calificado si
    así llegó), porque el retorno se sustituye dentro de SQL: devolver el
    pelado apuntaría a `public` cuando la tabla vive en `raw`.
    """
    # El vocabulario de alias es pelado; las tablas disponibles pueden no
    # serlo. Se indexa por nombre pelado y se devuelve el original.
    por_bare: dict[str, str] = {}
    for name in available_tables:
        por_bare.setdefault(bare_name(name), name)

    direct = _LEGACY_TABLE_ALIASES.get(bare_name(table_name))
    if direct and direct in por_bare:
        return por_bare[direct]

    if bare_name(table_name) == "cache_presupuesto_nacional":
        credito = [b for b in por_bare if b.startswith("cache_presupuesto_credito_")]
        if credito:
            return por_bare[max(credito, key=_extract_year)]
        budget = [b for b in por_bare if b.startswith("cache_presupuesto_")]
        if budget:
            return por_bare[max(budget, key=lambda name: (_extract_year(name), name))]

    return None


def build_table_compat_notes(available_tables: list[str]) -> str:
    """Describe legacy aliases so prompts and SQL-fixer steer toward real tables."""
    notes: list[str] = []
    # Los nombres de los alias son pelados; las tablas pueden venir
    # calificadas. Sin esto las notas desaparecían en silencio.
    available_tables = [bare_name(name) for name in available_tables]

    if "cache_series_inflacion_ipc" in available_tables:
        notes.append(
            "Alias legado: cache_series_tiempo_ipc -> cache_series_inflacion_ipc. "
            "Las series usan prefijo cache_series_*."
        )
    if "cache_bcra_cotizaciones" in available_tables:
        notes.append("Alias legado: cache_bcra_principales_variables -> cache_bcra_cotizaciones.")
    if any(name.startswith("cache_presupuesto_credito_") for name in available_tables):
        notes.append(
            "No existe una tabla unica cache_presupuesto_nacional. "
            "Presupuesto nacional vive en tablas cache_presupuesto_<endpoint>_<anio>, "
            "por ejemplo cache_presupuesto_credito_2026."
        )

    return "\n".join(notes)


def rewrite_legacy_sql_tables(sql: str, available_tables: list[str]) -> str:
    """Rewrite stale exact table identifiers in generated SQL to canonical names."""
    rewritten = sql
    candidates = list(_LEGACY_TABLE_ALIASES) + ["cache_presupuesto_nacional"]
    for legacy_name in candidates:
        target = resolve_compat_table_name(legacy_name, available_tables)
        if not target or target == legacy_name:
            continue
        rewritten = re.sub(
            rf"(?<![A-Za-z0-9_]){re.escape(legacy_name)}(?![A-Za-z0-9_])",
            target,
            rewritten,
        )
    return rewritten
