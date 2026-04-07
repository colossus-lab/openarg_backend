from __future__ import annotations

import re

_RESOURCE_SUFFIX_RE = re.compile(r"_r[a-f0-9]{10}$")
_GROUP_SUFFIX_RE = re.compile(r"_g[a-f0-9]{8}$")
_SCHEMA_SUFFIX_RE = re.compile(r"_s[a-f0-9]{8}$")


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
