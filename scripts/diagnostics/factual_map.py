"""WS1 — Diagnóstico factual del estado del collector.

Produce el documento factual de cobertura prod/staging requerido por el plan:
  * conteos absolutos (datasets, cached_datasets, table_catalog, dataset_chunks, cache_*)
  * cruces (ready_with_table, ready_missing_table, cached_ready_without_catalog,
    catalog_without_cached, uncached_datasets)
  * gap por portal (ready_ratio per portal)
  * breakdown INDEC (col_%, .1/.2/.3, trimestre crudo)
  * **gap por familia temporal** (Padrón, Aprender, etc.)

Uso:
  python -m scripts.diagnostics.factual_map           # corre contra DATABASE_URL
  python -m scripts.diagnostics.factual_map --json    # solo JSON
  python -m scripts.diagnostics.factual_map --md      # solo markdown
  python -m scripts.diagnostics.factual_map --out factual.md

Read-only: no escribe en la DB. Pensado para staging y para un usuario
con read-only en prod.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------- Queries ----------

_BASIC_COUNTS = {
    "datasets": "SELECT COUNT(*) FROM datasets",
    "cached_datasets": "SELECT COUNT(*) FROM cached_datasets",
    "cached_datasets_ready": "SELECT COUNT(*) FROM cached_datasets WHERE status='ready'",
    "cached_datasets_error": "SELECT COUNT(*) FROM cached_datasets WHERE status='error'",
    "cached_datasets_pending": "SELECT COUNT(*) FROM cached_datasets WHERE status='pending'",
    "cached_datasets_permanently_failed": (
        "SELECT COUNT(*) FROM cached_datasets WHERE status='permanently_failed'"
    ),
    "table_catalog": "SELECT COUNT(*) FROM table_catalog",
    "dataset_chunks": "SELECT COUNT(*) FROM dataset_chunks",
    "cache_star_tables": (
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name LIKE 'cache\\_%' ESCAPE '\\'"
    ),
}

_CROSS_QUERIES = {
    "ready_with_table": (
        "SELECT COUNT(*) FROM cached_datasets cd "
        "JOIN information_schema.tables t "
        "  ON t.table_name = cd.table_name AND t.table_schema = 'public' "
        "WHERE cd.status='ready'"
    ),
    "ready_missing_table": (
        "SELECT COUNT(*) FROM cached_datasets cd "
        "LEFT JOIN information_schema.tables t "
        "  ON t.table_name = cd.table_name AND t.table_schema='public' "
        "WHERE cd.status='ready' AND t.table_name IS NULL"
    ),
    "catalog_join_ready": (
        "SELECT COUNT(*) FROM cached_datasets cd "
        "JOIN table_catalog tc ON tc.table_name = cd.table_name "
        "WHERE cd.status='ready'"
    ),
    "cached_ready_without_catalog": (
        "SELECT COUNT(*) FROM cached_datasets cd "
        "LEFT JOIN table_catalog tc ON tc.table_name = cd.table_name "
        "WHERE cd.status='ready' AND tc.table_name IS NULL"
    ),
    "catalog_without_cached": (
        "SELECT COUNT(*) FROM table_catalog tc "
        "LEFT JOIN cached_datasets cd ON cd.table_name = tc.table_name "
        "WHERE cd.id IS NULL"
    ),
    "uncached_datasets": (
        "SELECT COUNT(*) FROM datasets d "
        "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id "
        "WHERE cd.id IS NULL"
    ),
    "ready_with_size_zero": (
        "SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND size_bytes=0"
    ),
    "ready_with_row_count_zero": (
        "SELECT COUNT(*) FROM cached_datasets WHERE status='ready' AND row_count=0"
    ),
    "datasets_no_download_url": (
        "SELECT COUNT(*) FROM datasets WHERE COALESCE(download_url,'')=''"
    ),
}

_PORTAL_GAP_QUERY = """
SELECT d.portal,
       COUNT(*) AS total,
       COUNT(cd.id) FILTER (WHERE cd.status='ready') AS ready,
       COUNT(cd.id) FILTER (WHERE cd.status='error') AS error,
       COUNT(cd.id) FILTER (WHERE cd.status='permanently_failed') AS permanently_failed,
       COUNT(cd.id) FILTER (WHERE cd.status='pending') AS pending
FROM datasets d
LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id
GROUP BY d.portal
ORDER BY total DESC
"""

_INDEC_BREAKDOWN_QUERIES = {
    "datasets_indec": "SELECT COUNT(*) FROM datasets WHERE portal LIKE 'indec%' OR portal='indec'",
    "cached_indec": (
        "SELECT COUNT(*) FROM cached_datasets cd "
        "JOIN datasets d ON d.id=cd.dataset_id "
        "WHERE d.portal LIKE 'indec%' OR d.portal='indec'"
    ),
    "cache_indec_tables": (
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name LIKE 'cache_indec_%'"
    ),
    "indec_tables_with_col_underscore": (
        "SELECT COUNT(DISTINCT table_name) FROM information_schema.columns "
        "WHERE table_schema='public' "
        "  AND table_name LIKE 'cache_indec_%' "
        "  AND column_name ~ '^col_[0-9]+$'"
    ),
    "indec_tables_with_dotted_suffix": (
        "SELECT COUNT(DISTINCT table_name) FROM information_schema.columns "
        "WHERE table_schema='public' "
        "  AND table_name LIKE 'cache_indec_%' "
        "  AND column_name ~ '\\.[0-9]+$'"
    ),
}


# Detects normalised dataset family titles by stripping years/months/quarters
# (typical: "Padrón Oficial 2024", "Aprender 2023", "Resultados Elecciones 2019").
_TEMPORAL_TOKEN_RE = re.compile(
    r"\b(?:19|20)\d{2}\b"  # year
    r"|\bT[1-4]\b"  # quarter
    r"|\bQ[1-4]\b"
    r"|\b(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|setiembre|octubre|noviembre|diciembre)\b",
    flags=re.IGNORECASE,
)


def _normalize_title(title: str) -> str:
    if not title:
        return ""
    base = _TEMPORAL_TOKEN_RE.sub("", title)
    base = re.sub(r"\s+", " ", base).strip(" -—_,;:.")
    return base.lower()


@dataclass
class FamilyCoverage:
    family: str
    portal: str
    total: int
    ready: int
    ready_ratio: float
    examples: list[str] = field(default_factory=list)


def _scalar(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        res = conn.execute(text(sql))
        return int(res.scalar() or 0)


def _all_datasets_for_family_grouping(engine: Engine, *, limit_titles: int = 3) -> list[FamilyCoverage]:
    sql = text(
        "SELECT d.portal, d.title, "
        "       CASE WHEN cd.status='ready' THEN 1 ELSE 0 END AS is_ready "
        "FROM datasets d "
        "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id"
    )
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    with engine.connect() as conn:
        for row in conn.execute(sql).fetchall():
            family = _normalize_title(row.title or "")
            if not family or len(family) < 4:
                continue
            key = (row.portal or "", family)
            entry = buckets.setdefault(
                key, {"total": 0, "ready": 0, "examples": []}
            )
            entry["total"] += 1
            entry["ready"] += int(row.is_ready or 0)
            if len(entry["examples"]) < limit_titles:
                entry["examples"].append(row.title or "")
    coverages: list[FamilyCoverage] = []
    for (portal, family), agg in buckets.items():
        if agg["total"] < 3:  # not really a "family" if <3 versions
            continue
        ratio = agg["ready"] / agg["total"] if agg["total"] else 0.0
        coverages.append(
            FamilyCoverage(
                family=family,
                portal=portal,
                total=agg["total"],
                ready=agg["ready"],
                ready_ratio=round(ratio, 3),
                examples=agg["examples"],
            )
        )
    return coverages


# ---------- main ----------


@dataclass
class FactualReport:
    database: str
    counts: dict[str, int]
    crosses: dict[str, int]
    portal_gap: list[dict[str, Any]]
    indec: dict[str, int]
    worst_temporal_families: list[dict[str, Any]]


def build_report(database_url: str, *, top_families: int = 25) -> FactualReport:
    engine = create_engine(database_url, pool_pre_ping=True)
    counts = {k: _scalar(engine, q) for k, q in _BASIC_COUNTS.items()}
    crosses = {k: _scalar(engine, q) for k, q in _CROSS_QUERIES.items()}
    portal_gap_rows: list[dict[str, Any]] = []
    with engine.connect() as conn:
        for row in conn.execute(text(_PORTAL_GAP_QUERY)).fetchall():
            total = int(row.total or 0)
            ready = int(row.ready or 0)
            portal_gap_rows.append(
                {
                    "portal": row.portal,
                    "total": total,
                    "ready": ready,
                    "ready_ratio": round(ready / total, 3) if total else 0.0,
                    "error": int(row.error or 0),
                    "permanently_failed": int(row.permanently_failed or 0),
                    "pending": int(row.pending or 0),
                }
            )
    indec = {k: _scalar(engine, q) for k, q in _INDEC_BREAKDOWN_QUERIES.items()}
    families = _all_datasets_for_family_grouping(engine)
    families.sort(key=lambda c: (c.ready_ratio, -c.total))
    worst = [asdict(c) for c in families[:top_families]]

    return FactualReport(
        database=_safe_db_label(database_url),
        counts=counts,
        crosses=crosses,
        portal_gap=portal_gap_rows,
        indec=indec,
        worst_temporal_families=worst,
    )


def _safe_db_label(url: str) -> str:
    # Strip credentials before logging.
    return re.sub(r"://[^@]+@", "://<redacted>@", url)


def render_markdown(report: FactualReport) -> str:
    lines = [
        f"# Factual map — {report.database}",
        "",
        "## Counts",
        "",
        "| metric | value |",
        "| --- | ---: |",
    ]
    for k, v in report.counts.items():
        lines.append(f"| {k} | {v:,} |")
    lines += ["", "## Cross-checks", "", "| metric | value |", "| --- | ---: |"]
    for k, v in report.crosses.items():
        lines.append(f"| {k} | {v:,} |")
    lines += [
        "",
        "## Portal gap",
        "",
        "| portal | total | ready | ready_ratio | error | permanently_failed | pending |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in report.portal_gap:
        lines.append(
            f"| {r['portal']} | {r['total']:,} | {r['ready']:,} | "
            f"{r['ready_ratio']:.1%} | {r['error']:,} | {r['permanently_failed']:,} | {r['pending']:,} |"
        )
    lines += ["", "## INDEC", "", "| metric | value |", "| --- | ---: |"]
    for k, v in report.indec.items():
        lines.append(f"| {k} | {v:,} |")
    lines += [
        "",
        "## Worst-covered temporal families",
        "",
        "| portal | family | total | ready | ratio | examples |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for f in report.worst_temporal_families:
        examples = "; ".join(f["examples"][:2])
        lines.append(
            f"| {f['portal']} | {f['family']} | {f['total']} | {f['ready']} | "
            f"{f['ready_ratio']:.1%} | {examples} |"
        )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WS1 factual map")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="DB URL (defaults to $DATABASE_URL)",
    )
    parser.add_argument("--out", help="Write report to file instead of stdout")
    parser.add_argument("--json", action="store_true", help="Emit JSON only")
    parser.add_argument("--md", action="store_true", help="Emit markdown only")
    parser.add_argument(
        "--top-families", type=int, default=25, help="Worst-covered families to show"
    )
    args = parser.parse_args(argv)

    if not args.database_url:
        print("DATABASE_URL not set", file=sys.stderr)
        return 2

    report = build_report(args.database_url, top_families=args.top_families)
    payload = asdict(report)

    if args.json:
        out = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    elif args.md:
        out = render_markdown(report)
    else:
        out = render_markdown(report) + "\n```json\n" + json.dumps(
            payload, indent=2, ensure_ascii=False, default=str
        ) + "\n```\n"

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(out)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
