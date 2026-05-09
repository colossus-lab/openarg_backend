"""Auto-mart YAML scaffold generator (Fase 1 of "marts coverage" roadmap).

WHY THIS EXISTS
---------------
Today there are 7 marts vs ~21K ready raw datasets. Hand-writing each YAML is
the bottleneck. This tool reads the existing classification (`table_catalog`
domain/subdomain + raw `information_schema`) and emits a SKELETON YAML for one
cluster — operator reviews, tweaks `id`/`description`/SQL filters, then
`make build_mart mart_id=...`.

WHAT IT GENERATES
-----------------
A YAML at `config/marts/auto_<slug>.yaml` with:
  * `id`, `domain`, `subdomain`
  * `sources.portals` (intersection from matched tables)
  * `sources.resource_patterns` (the table_name glob the macro will expand)
  * `canonical_columns` (columns present in >= --threshold of matched tables;
    defaults to 70%) with the most-frequent SQL type detected per column
  * `sql` using `live_tables_by_table_pattern(<glob>, expected_columns=[...])`
  * `refresh.policy: manual` so it doesn't auto-refresh until reviewed

WHAT IT DOES NOT DO
-------------------
  * No DROP / no CREATE — only writes a YAML file
  * No row-count / quality validation of the SQL — that's `dbt run --select` or
    `openarg.build_mart(mart_id, dry_run=True)` after review
  * No translation of column names (canonical_columns keep raw names)

USAGE
-----
  # Cluster by subdomain (most common case)
  python scripts/scaffold_mart.py --domain gobierno --subdomain elecciones

  # Cluster by single portal (catch-all explorer mart)
  python scripts/scaffold_mart.py --portal cordoba_estadistica

  # Custom threshold for canonical columns (default 0.7 = 70%)
  python scripts/scaffold_mart.py --subdomain demografía --threshold 0.5

  # Custom output id and dry-run (print YAML, don't write)
  python scripts/scaffold_mart.py --subdomain elecciones --id elecciones_v1 --dry-run

REQUIRES
--------
  DATABASE_URL pointing to the OpenArg DB (staging RDS via pgbouncer or local).
  Run from the container if no local tunnel: `docker exec openarg_worker_ingest
  python /app/src/scripts/scaffold_mart.py ...`
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import indent
from typing import Any

from sqlalchemy import create_engine, text

# Threshold below which we don't bother making a mart — too few candidates
# means the pattern isn't real, the operator should pick a different filter.
MIN_TABLES = 5


@dataclass
class TableInfo:
    table_name: str
    schema_name: str
    portal: str
    columns: list[tuple[str, str]] = field(default_factory=list)  # (col_name, pg_type)


def _slugify(s: str) -> str:
    """Make a filesystem-safe slug from a domain/subdomain string."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s.lower()).strip("_")
    return s[:40]


def _common_token_pattern(table_names: list[str]) -> str:
    """Find a `*<core>*` glob covering most tables, biased toward the portal prefix.

    Strategy: take the longest common prefix, drop anything after the second
    `__` (portal__title__hash__vN convention) and emit `<prefix>*`. Falls back
    to `<portal>__*` when the prefix is too short.
    """
    if not table_names:
        return "*"
    # Find the LCP across all names.
    lcp = os.path.commonprefix(table_names)
    # Trim past the second "__" to keep portal+title-stem only.
    parts = lcp.split("__")
    if len(parts) >= 2 and parts[0] and parts[1]:
        return f"{parts[0]}__{parts[1]}*"
    if parts and parts[0]:
        return f"{parts[0]}__*"
    return "*"


def _fetch_target_tables(engine, *, domain: str | None, subdomain: str | None,
                        portal: str | None) -> list[TableInfo]:
    """Pull the candidate cluster from `table_catalog` joined to raw schema.

    Filters are AND-ed; passing none returns nothing (we never scaffold over
    the entire catalog by accident).
    """
    if not (domain or subdomain or portal):
        raise ValueError("Pass at least one of --domain / --subdomain / --portal")

    clauses = []
    params: dict[str, Any] = {}
    if domain:
        clauses.append("tc.domain = :domain")
        params["domain"] = domain
    if subdomain:
        clauses.append("tc.subdomain = :subdomain")
        params["subdomain"] = subdomain
    if portal:
        clauses.append(
            "EXISTS (SELECT 1 FROM cached_datasets cd "
            "JOIN datasets d ON d.id = cd.dataset_id "
            "WHERE cd.table_name = tc.table_name AND d.portal = :portal)"
        )
        params["portal"] = portal
    where = " AND ".join(clauses)

    # `table_catalog.table_name` is stored fully-qualified ("raw.<name>" or
    # bare "<name>" for legacy public.cache_*). For raw tables, the portal is
    # encoded as the first `__`-separated token (collector physical-namer
    # convention: `<portal>__<title>__<hash>__v<N>`). For cache_* legacy, we
    # rely on the cached_datasets join to find the portal. Joining cd EXCLUSIVELY
    # would miss raw tables whose collect_dataset row doesn't yet exist or got
    # cleaned, so we infer the portal from the table_name itself when possible.
    portal_filter_sql = ""
    if portal:
        portal_filter_sql = (
            "AND (CASE WHEN tc.table_name LIKE 'raw.%' "
            "          THEN split_part(split_part(tc.table_name,'.',2),'__',1) "
            "          ELSE COALESCE(d.portal, '') END) = :portal"
        )
    # Re-build WHERE without the broken portal-via-EXISTS clause.
    rebuilt_clauses = []
    if domain:
        rebuilt_clauses.append("tc.domain = :domain")
    if subdomain:
        rebuilt_clauses.append("tc.subdomain = :subdomain")
    rebuilt_where = " AND ".join(rebuilt_clauses) if rebuilt_clauses else "TRUE"

    sql = text(
        f"""
        SELECT DISTINCT tc.table_name AS qualified_name,
               CASE WHEN tc.table_name LIKE '%.%'
                    THEN split_part(tc.table_name, '.', 1)
                    ELSE 'public'
               END AS schema_name,
               CASE WHEN tc.table_name LIKE '%.%'
                    THEN split_part(tc.table_name, '.', 2)
                    ELSE tc.table_name
               END AS bare_name,
               CASE WHEN tc.table_name LIKE 'raw.%'
                    THEN split_part(split_part(tc.table_name,'.',2),'__',1)
                    ELSE COALESCE(d.portal, '')
               END AS portal
        FROM table_catalog tc
        LEFT JOIN cached_datasets cd
               ON cd.table_name = CASE WHEN tc.table_name LIKE '%.%'
                                       THEN split_part(tc.table_name, '.', 2)
                                       ELSE tc.table_name END
        LEFT JOIN datasets d ON d.id = cd.dataset_id
        WHERE {rebuilt_where}
        {portal_filter_sql}
        ORDER BY qualified_name
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    tables = [
        TableInfo(table_name=r.bare_name, schema_name=r.schema_name, portal=r.portal)
        for r in rows
    ]
    if not tables:
        return tables

    # Bulk-fetch columns from information_schema. Postgres' psycopg3 binding
    # rejects arrays of composite tuples ("anonymous composite types"), so
    # we group by schema and run one query per schema with a flat name array.
    by_schema: dict[str, list[str]] = {}
    for t in tables:
        by_schema.setdefault(t.schema_name, []).append(t.table_name)

    col_sql = text(
        """
        SELECT table_schema, table_name, column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = ANY(:names)
        ORDER BY table_schema, table_name, ordinal_position
        """
    )
    by_table: dict[tuple[str, str], list[tuple[str, str]]] = {}
    with engine.connect() as conn:
        for schema, names in by_schema.items():
            for r in conn.execute(col_sql, {"schema": schema, "names": names}).fetchall():
                by_table.setdefault((r.table_schema, r.table_name), []).append(
                    (r.column_name, r.udt_name)
                )
    for t in tables:
        t.columns = by_table.get((t.schema_name, t.table_name), [])
    return tables


def _infer_canonical_columns(
    tables: list[TableInfo], *, threshold: float
) -> list[dict[str, str]]:
    """Columns present in >= threshold fraction of tables become canonical.

    For each surviving column, pick the SQL type that occurs in the most
    matched tables (mode). Auxiliary columns the collector adds
    (`_source_dataset_id`, `_source_url`, etc.) are excluded — they are
    bookkeeping, not analytical.
    """
    aux_columns = {
        "_source_dataset_id",
        "_source_url",
        "_source_file_hash",
        "_parser_version",
        "_collector_version",
    }
    n_tables = len(tables)
    if n_tables == 0:
        return []
    presence: Counter[str] = Counter()
    types_per_col: dict[str, Counter[str]] = {}
    for t in tables:
        seen_in_this_table: set[str] = set()
        for col_name, udt in t.columns:
            if col_name in aux_columns:
                continue
            if col_name in seen_in_this_table:
                continue
            seen_in_this_table.add(col_name)
            presence[col_name] += 1
            types_per_col.setdefault(col_name, Counter())[udt] += 1

    min_count = max(1, int(n_tables * threshold))
    canonical = []
    for col, count in presence.most_common():
        if count < min_count:
            continue
        modal_type = types_per_col[col].most_common(1)[0][0]
        canonical.append(
            {
                "name": col,
                "type": _pg_to_yaml_type(modal_type),
                "description": f"(auto) present in {count}/{n_tables} matched tables",
                "_raw_type": modal_type,
            }
        )
    return canonical


def _pg_to_yaml_type(udt: str) -> str:
    """Map information_schema udt_name to the simple types our YAML schema uses.

    Falls back to 'text' for anything unknown — operator can refine.
    """
    udt = udt.lower()
    if udt in ("int2", "int4", "int8", "bigint", "smallint", "integer"):
        return "int"
    if udt in ("float4", "float8", "numeric", "decimal"):
        return "numeric"
    if udt.startswith("timestamp") or udt == "date":
        return "timestamp"
    if udt == "bool":
        return "bool"
    if udt in ("jsonb", "json"):
        return "jsonb"
    return "text"


def _build_yaml(
    *,
    mart_id: str,
    domain: str | None,
    subdomain: str | None,
    portals: list[str],
    pattern: str,
    canonical: list[dict[str, str]],
    n_matched: int,
) -> str:
    """Render the YAML skeleton. Hand-rolled (no PyYAML dep) so the output
    matches the comment style of the existing curated marts.
    """
    portal_list = "[" + ", ".join(portals) + "]"
    canonical_yaml = ""
    for c in canonical:
        canonical_yaml += (
            f"  - name: {c['name']}\n"
            f"    type: {c['type']}\n"
            f"    description: {c['description']}\n"
        )
    expected_cols = ", ".join(f"'{c['name']}'" for c in canonical)
    select_cols = ",\n    ".join(
        f"{c['name']}::{c['_raw_type']} AS {c['name']}" for c in canonical
    )
    yaml = f"""# AUTO-GENERATED by scripts/scaffold_mart.py — REVIEW BEFORE BUILDING.
# Matched {n_matched} raw tables via pattern: {pattern}
# Canonical columns picked by >= threshold occurrence across the cluster.
# Operator: edit description, refine SQL filters, set refresh.policy.

id: {mart_id}
version: 0.1.0
description: |
  (auto) {domain or ''} / {subdomain or ''} cluster covering {n_matched} raw
  tables. Edit this description to capture the analytical intent of the mart.
domain: {domain or 'otro'}

sources:
  portals: {portal_list}
  resource_patterns: ["{pattern}"]

canonical_columns:
{canonical_yaml.rstrip()}

# `live_tables_by_table_pattern` returns the schema-intersection over all
# matched tables (only canonical columns end up in the UNION ALL). With 0
# matches, falls back to an empty-shape SELECT WHERE FALSE so the mart
# builds idempotently even when raw is wiped.
sql: |
  SELECT
    {select_cols}
  FROM {{{{ live_tables_by_table_pattern(
      '{pattern}',
      expected_columns=[{expected_cols}]
  ) }}}} sub
  -- Optional: WHERE clauses, GROUP BY for aggregates, JOINs to dimension
  -- tables. Default: pass-through UNION ALL.

refresh:
  policy: manual          # set to daily/hourly when content stable
  unique_index: []        # add column names if your SELECT yields a natural PK
"""
    return yaml


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--domain")
    p.add_argument("--subdomain")
    p.add_argument("--portal")
    p.add_argument("--prefix",
                   help="Filter matched tables to those whose table_name starts with this prefix (after subdomain/portal filter)")
    p.add_argument("--threshold", type=float, default=0.7,
                   help="Min fraction of tables a column must appear in to be canonical (default 0.7)")
    p.add_argument("--id", help="Override generated mart_id (default auto_<slug>)")
    p.add_argument("--out-dir", default="config/marts", help="Where to write the YAML (default config/marts)")
    p.add_argument("--dry-run", action="store_true", help="Print YAML, don't write")
    args = p.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.stderr.write("ERROR: DATABASE_URL env var required.\n")
        return 2
    # SQLAlchemy + psycopg dialect
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    engine = create_engine(db_url)
    tables = _fetch_target_tables(
        engine,
        domain=args.domain,
        subdomain=args.subdomain,
        portal=args.portal,
    )
    # Apply optional --prefix to drill into a sub-cluster identified from
    # the heterogeneity report of a previous run.
    if args.prefix:
        # Treat trailing `*` as glob (just strip it), match by literal prefix.
        needle = args.prefix.rstrip("*")
        tables = [t for t in tables if t.table_name.startswith(needle)]
    if len(tables) < MIN_TABLES:
        sys.stderr.write(
            f"Found {len(tables)} matching tables; need at least {MIN_TABLES}. "
            "Pick a wider filter (different subdomain/portal) or lower MIN_TABLES.\n"
        )
        return 1

    canonical = _infer_canonical_columns(tables, threshold=args.threshold)
    if not canonical:
        # Heterogeneity report: show the operator where the sub-clusters live.
        # Group by the table_name prefix up to the second `__` (portal+title-stem)
        # and rank by count. Picking the top one as the next filter usually
        # finds a homogeneous sub-cluster.
        prefix_counts: Counter[str] = Counter()
        for t in tables:
            parts = t.table_name.split("__")
            key = "__".join(parts[:2]) + "*" if len(parts) >= 2 else t.table_name
            prefix_counts[key] += 1
        sys.stderr.write(
            f"No column met the {args.threshold:.0%} threshold across "
            f"{len(tables)} tables. The cluster is too heterogeneous.\n\n"
            f"Top sub-clusters by table_name prefix:\n"
        )
        for prefix, count in prefix_counts.most_common(10):
            sys.stderr.write(f"  {count:5d}  {prefix}\n")
        sys.stderr.write(
            "\nNext step: re-run with `--prefix <one of the above>` to scaffold "
            "a tighter, schema-homogeneous mart.\n"
        )
        return 1

    pattern = _common_token_pattern([t.table_name for t in tables])
    portals = sorted({t.portal for t in tables if t.portal})
    slug = args.id or f"auto_{_slugify(args.subdomain or args.portal or args.domain or 'cluster')}"
    yaml = _build_yaml(
        mart_id=slug,
        domain=args.domain,
        subdomain=args.subdomain,
        portals=portals,
        pattern=pattern,
        canonical=canonical,
        n_matched=len(tables),
    )

    if args.dry_run:
        sys.stdout.write(yaml)
        return 0

    out_path = Path(args.out_dir) / f"{slug}.yaml"
    if out_path.exists():
        sys.stderr.write(
            f"ERROR: {out_path} exists. Pass --id <other> or remove the file first.\n"
        )
        return 1
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml, encoding="utf-8")
    sys.stderr.write(
        f"Wrote {out_path}\n"
        f"  matched_tables={len(tables)}  canonical_columns={len(canonical)}\n"
        f"  pattern={pattern!r}  portals={portals}\n"
        f"Next: review the YAML, then `openarg.build_mart` with mart_id={slug}.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
