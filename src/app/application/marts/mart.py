"""Mart DTOs and YAML loading.

Schema (per `config/marts/<mart_id>.yaml`):

    id: presupuesto_consolidado
    version: 1.0.0
    description: |
      Vista unificada del presupuesto nacional + provincial.
    domain: presupuesto

    sources:
      - presupuesto                     # contract_id from staging
      - presupuesto_abierto

    canonical_columns:
      - name: anio
        type: int
        description: Año del ejercicio
      - name: provincia
        type: text
      - name: monto_pesos
        type: numeric
        description: Monto ejecutado en pesos argentinos

    sql: |
      SELECT
        anio::int AS anio,
        COALESCE(jurisdiccion, 'NACION') AS provincia,
        monto::numeric AS monto_pesos
      FROM staging.presupuesto__presupuesto_abierto

    refresh:
      policy: on_upstream_change
      unique_index: [anio, provincia]   # for REFRESH CONCURRENTLY support

The pipeline never reads raw `cache_*` tables — it queries marts. The
declared `canonical_columns` map onto `COMMENT ON COLUMN` so the planner
can fetch column-level semantics from `information_schema` directly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_VALID_REFRESH_POLICIES = frozenset({"manual", "daily", "hourly", "on_upstream_change"})

# `mart_id` becomes a Postgres identifier (`mart."<id>"`). Restrict to a
# safe charset so a YAML can't smuggle quotes / semicolons that escape
# the surrounding `"..."` and inject SQL.
_VALID_MART_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

# Canonical column types are a wider set than contracts (we expose what
# Postgres natively supports plus `numeric` for money / scientific data).
_VALID_COLUMN_TYPES = frozenset(
    {
        "text",
        "int",
        "bigint",
        "float",
        "numeric",
        "date",
        "timestamp",
        "bool",
        "jsonb",
    }
)


@dataclass(frozen=True)
class MartCanonicalColumn:
    name: str
    type: str
    description: str | None = None


@dataclass(frozen=True)
class MartRefreshPolicy:
    policy: str = "on_upstream_change"
    unique_index: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Mart:
    id: str
    version: str
    description: str
    domain: str | None
    # `source_portals` is the routing key: when a raw landing happens for
    # a portal listed here, this mart's `refresh_mart` is auto-enqueued.
    # `source_resource_patterns` is an optional finer filter (e.g.
    # ['bcra::*tasa*']) for marts that only want a subset of a portal.
    source_portals: list[str]
    source_resource_patterns: list[str]
    canonical_columns: list[MartCanonicalColumn]
    sql: str
    refresh: MartRefreshPolicy
    schema_name: str = "mart"

    @property
    def view_name(self) -> str:
        # The mart_id is the bare view name. Postgres limit applies.
        return self.id[:63]

    @property
    def qualified_name(self) -> str:
        return f'{self.schema_name}."{self.view_name}"'


class MartParseError(ValueError):
    """Raised when a YAML cannot be parsed into a `Mart`."""


def _require(d: dict, key: str, path: Path) -> Any:
    if key not in d:
        raise MartParseError(f"{path}: missing required field '{key}'")
    return d[key]


def _parse_canonical_column(raw: dict, path: Path) -> MartCanonicalColumn:
    name = _require(raw, "name", path)
    type_ = _require(raw, "type", path)
    if type_ not in _VALID_COLUMN_TYPES:
        raise MartParseError(
            f"{path}: column '{name}' has invalid type '{type_}' "
            f"(allowed: {sorted(_VALID_COLUMN_TYPES)})"
        )
    return MartCanonicalColumn(
        name=str(name),
        type=str(type_),
        description=raw.get("description"),
    )


def _parse_refresh(raw: dict | None, path: Path) -> MartRefreshPolicy:
    raw = raw or {}
    policy = raw.get("policy", "on_upstream_change")
    if policy not in _VALID_REFRESH_POLICIES:
        raise MartParseError(
            f"{path}: refresh.policy '{policy}' is invalid "
            f"(allowed: {sorted(_VALID_REFRESH_POLICIES)})"
        )
    unique_index = raw.get("unique_index") or []
    if not isinstance(unique_index, list):
        raise MartParseError(f"{path}: refresh.unique_index must be a list of column names")
    return MartRefreshPolicy(
        policy=str(policy),
        unique_index=[str(c) for c in unique_index],
    )


def load_mart(path: Path | str) -> Mart:
    """Read a single YAML and return a `Mart`."""
    p = Path(path)
    with p.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise MartParseError(f"{p}: top-level must be a mapping")

    id_ = str(_require(data, "id", p))
    if not _VALID_MART_ID_RE.match(id_):
        raise MartParseError(
            f"{p}: 'id' = {id_!r} must match {_VALID_MART_ID_RE.pattern} "
            f"(starts with lowercase letter, only [a-z0-9_], max 63 chars)"
        )
    version = str(_require(data, "version", p))
    description = str(data.get("description", ""))
    domain = data.get("domain")

    # The post-rebuild design uses `sources.portals` (routing key) and
    # optional `sources.resource_patterns` (finer filter).
    sources_block = data.get("sources") or {}
    if not isinstance(sources_block, dict):
        raise MartParseError(
            f"{p}: 'sources' must be a mapping with 'portals' and optional "
            f"'resource_patterns' lists (post-rebuild format)."
        )
    portals = sources_block.get("portals") or []
    patterns = sources_block.get("resource_patterns") or []
    if not isinstance(portals, list) or not isinstance(patterns, list):
        raise MartParseError(
            f"{p}: 'sources.portals' and 'sources.resource_patterns' must be lists"
        )
    portals = [str(s) for s in portals]
    patterns = [str(s) for s in patterns]
    # Empty `sources.portals` means NO raw landing will ever auto-trigger
    # a refresh of this mart — almost always a YAML mistake. Reject early
    # so the operator gets a clear error instead of silent under-refresh.
    if not portals:
        raise MartParseError(
            f"{p}: 'sources.portals' must list at least one portal — "
            f"otherwise no raw landing will trigger refresh of this mart"
        )

    columns_raw = data.get("canonical_columns") or []
    if not isinstance(columns_raw, list) or not columns_raw:
        raise MartParseError(f"{p}: 'canonical_columns' must be a non-empty list")
    canonical_columns = [_parse_canonical_column(c, p) for c in columns_raw]

    sql = str(_require(data, "sql", p)).strip()
    if not sql:
        raise MartParseError(f"{p}: 'sql' must not be empty")

    refresh = _parse_refresh(data.get("refresh"), p)

    return Mart(
        id=id_,
        version=version,
        description=description,
        domain=str(domain) if domain else None,
        source_portals=portals,
        source_resource_patterns=patterns,
        canonical_columns=canonical_columns,
        sql=sql,
        refresh=refresh,
    )


def load_all_marts(directory: Path | str) -> list[Mart]:
    """Read every `.yaml` / `.yml` under `directory` (non-recursive)."""
    d = Path(directory)
    if not d.exists() or not d.is_dir():
        return []
    marts: list[Mart] = []
    for entry in sorted(d.iterdir()):
        if entry.suffix.lower() not in {".yaml", ".yml"}:
            continue
        marts.append(load_mart(entry))
    return marts
