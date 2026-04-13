from __future__ import annotations

import re

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
    """Map legacy exact table names to current physical cache tables when possible."""
    direct = _LEGACY_TABLE_ALIASES.get(table_name)
    if direct and direct in available_tables:
        return direct

    if table_name == "cache_presupuesto_nacional":
        credito_tables = [name for name in available_tables if name.startswith("cache_presupuesto_credito_")]
        if credito_tables:
            return max(credito_tables, key=_extract_year)
        budget_tables = [name for name in available_tables if name.startswith("cache_presupuesto_")]
        if budget_tables:
            return max(budget_tables, key=lambda name: (_extract_year(name), name))

    return None


def build_table_compat_notes(available_tables: list[str]) -> str:
    """Describe legacy aliases so prompts and SQL-fixer steer toward real tables."""
    notes: list[str] = []

    if "cache_series_inflacion_ipc" in available_tables:
        notes.append(
            "Alias legado: cache_series_tiempo_ipc -> cache_series_inflacion_ipc. "
            "Las series usan prefijo cache_series_*."
        )
    if "cache_bcra_cotizaciones" in available_tables:
        notes.append(
            "Alias legado: cache_bcra_principales_variables -> cache_bcra_cotizaciones."
        )
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
            rf'(?<![A-Za-z0-9_]){re.escape(legacy_name)}(?![A-Za-z0-9_])',
            target,
            rewritten,
        )
    return rewritten
