"""Backfill de las tablas que rompió el colector: encabezados de datos y truncadas.

Hasta el arreglo del 04-oct-2026 el colector volvía a inferir el encabezado en
cada escritura y en cada chunk (ver `pipeline/parsers/header_inference.py`).
Dejó dos huellas en tablas que hoy figuran como listas:

- **header_from_data**: nombres de columna que son valores de una fila
  (`'2025-12-26T00:00:00'`, `HCDN285290`, URLs, UUID). Misma regla que el
  detector `header_from_data`.
- **row_deficit**: la tabla tiene menos del 90 % de las filas que el catálogo
  anuncia. Es la huella del DROP + recreate por chunk, y no siempre deja el
  encabezado roto: si el último chunk no promovió nada, la tabla queda con
  nombres buenos y una fracción de las filas (staging, 04-oct:
  `personal_de_ciencia_y_tecnologia` con 7.048 de 185.618).

Arreglar el parser no alcanza: el archivo de origen casi nunca cambia, y
`_unchanged_since_last_collect` saltea el parseo de un archivo idéntico. Este
comando lista las candidatas y, con `--execute`, despacha
`collect_dataset(dataset_id, force_reparse=True)` para cada una — una por
dataset, espaciadas.

Uso, dentro del contenedor del backend (o de cualquier worker):

    python -m app.infrastructure.celery.tasks.header_backfill               # dry-run
    python -m app.infrastructure.celery.tasks.header_backfill --json        # dry-run, JSON
    python -m app.infrastructure.celery.tasks.header_backfill --execute --limit 50

Cada re-colecta crea una versión raw nueva y re-embebe el dataset (Bedrock):
el dry-run dice cuántas son antes de gastar nada. El dry-run sólo lee.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass

from sqlalchemy import text

from app.application.validation.detectors.headers import evaluate_header, parse_columns_json

logger = logging.getLogger(__name__)

REASON_HEADER = "header_from_data"
REASON_DEFICIT = "row_deficit"

# Por debajo de esto un faltante de filas no distingue un chunk perdido de un
# archivo chico que cambió: el chunk mínimo del colector es de 500 filas.
MIN_DECLARED_ROWS = 1000
DEFICIT_RATIO = 0.9

_CANDIDATE_SCHEMAS = ("raw", "public")

_ROWS_SQL = text(
    """
    SELECT cd.dataset_id::text AS dataset_id,
           cd.table_name,
           cd.row_count,
           cd.columns_json,
           d.portal,
           d.format,
           COALESCE(d.download_url, '') AS download_url
    FROM raw.cached_datasets cd
    JOIN datasets d ON d.id = cd.dataset_id
    WHERE cd.status = 'ready'
    ORDER BY cd.updated_at DESC NULLS LAST
    """
)

_PHYSICAL_SQL = text(
    """
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           c.reltuples::bigint AS approx_rows,
           array_agg(a.attname::text ORDER BY a.attnum) AS columns
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    WHERE n.nspname = ANY(:schemas) AND c.relkind = 'r'
    GROUP BY 1, 2, 3
    """
)


@dataclass(frozen=True)
class Candidate:
    dataset_id: str
    table_name: str
    portal: str
    format: str
    reason: str
    detail: str


@dataclass(frozen=True)
class _Physical:
    schema_name: str
    columns: list[str]
    approx_rows: int


def _split(table_name: str) -> tuple[str | None, str]:
    if "." in table_name:
        schema, bare = table_name.split(".", 1)
        return schema.strip('"'), bare.strip('"')
    return None, table_name


def _lookup(physical: dict[tuple[str, str], _Physical], table_name: str) -> _Physical | None:
    schema, bare = _split(table_name)
    if schema:
        return physical.get((schema, bare))
    for candidate in _CANDIDATE_SCHEMAS:
        found = physical.get((candidate, bare))
        if found:
            return found
    return None


def classify(
    rows: Iterable[object],
    physical: dict[tuple[str, str], _Physical],
    exact_count: Callable[[str, str], int | None],
    *,
    min_declared_rows: int = MIN_DECLARED_ROWS,
    deficit_ratio: float = DEFICIT_RATIO,
) -> tuple[list[Candidate], Counter]:
    """Decide qué filas de `cached_datasets` son candidatas. Pura salvo `exact_count`.

    `exact_count(schema, table) -> int | None` sólo se llama para confirmar un
    faltante que la estimación del planner ya sugiere: un `COUNT(*)` sobre
    30.000 tablas no es un dry-run, es una carga.
    """
    out: list[Candidate] = []
    skipped: Counter = Counter()
    seen: set[str] = set()
    for row in rows:
        dataset_id = str(getattr(row, "dataset_id", "") or "")
        table_name = str(getattr(row, "table_name", "") or "")
        if not dataset_id or not table_name or dataset_id in seen:
            continue
        phys = _lookup(physical, table_name)
        if phys is None:
            skipped["sin_tabla_fisica"] += 1
            continue
        reason = detail = ""
        verdict = evaluate_header(
            phys.columns, parse_columns_json(getattr(row, "columns_json", None))
        )
        if verdict is not None:
            reason = REASON_HEADER
            detail = f"{verdict.rule}: {', '.join(n[:40] for n in verdict.data_names[:3])}"
        else:
            declared = int(getattr(row, "row_count", 0) or 0)
            approx = phys.approx_rows
            if declared >= min_declared_rows and 0 <= approx < deficit_ratio * declared:
                _, bare = _split(table_name)
                real = exact_count(phys.schema_name, bare)
                if real is None:
                    skipped["conteo_fallido"] += 1
                elif real < deficit_ratio * declared:
                    reason = REASON_DEFICIT
                    detail = f"{real} de {declared} filas"
            elif declared >= min_declared_rows and approx < 0:
                # Tabla nunca analizada: sin estimación no se puede sospechar
                # nada barato. Se cuenta aparte para que no pase en silencio.
                skipped["sin_estimacion"] += 1
        if not reason:
            continue
        if not str(getattr(row, "download_url", "") or "").strip():
            skipped["sin_url"] += 1
            continue
        seen.add(dataset_id)
        out.append(
            Candidate(
                dataset_id=dataset_id,
                table_name=table_name,
                portal=str(getattr(row, "portal", "") or ""),
                format=str(getattr(row, "format", "") or ""),
                reason=reason,
                detail=detail,
            )
        )
    return out, skipped


def find_candidates(
    engine,
    *,
    portals: Sequence[str] | None = None,
    reasons: Sequence[str] | None = None,
    exact_counts: bool = True,
) -> tuple[list[Candidate], Counter]:
    """Sólo lectura: lista las tablas listas que el backfill re-colectaría."""
    with engine.connect() as conn:
        rows = conn.execute(_ROWS_SQL).fetchall()
        physical = {
            (r.schema_name, r.table_name): _Physical(
                schema_name=r.schema_name, columns=list(r.columns), approx_rows=int(r.approx_rows)
            )
            for r in conn.execute(_PHYSICAL_SQL, {"schemas": list(_CANDIDATE_SCHEMAS)}).fetchall()
        }
        conn.rollback()

    if portals:
        wanted = set(portals)
        rows = [r for r in rows if r.portal in wanted]

    def _exact(schema: str, bare: str) -> int | None:
        if not exact_counts:
            return None
        safe_schema = schema.replace('"', '""')
        safe_bare = bare.replace('"', '""')
        try:
            with engine.connect() as conn:
                value = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{safe_schema}"."{safe_bare}"')  # noqa: S608
                ).scalar()
                conn.rollback()
            return int(value or 0)
        except Exception:
            logger.warning("could not count %s.%s", schema, bare, exc_info=True)
            return None

    candidates, skipped = classify(rows, physical, _exact)
    if reasons:
        keep = set(reasons)
        candidates = [c for c in candidates if c.reason in keep]
    return candidates, skipped


def dispatch(candidates: Sequence[Candidate], *, step_seconds: int = 5) -> int:
    """Encola `collect_dataset(dataset_id, force_reparse=True)` para cada candidata."""
    from app.infrastructure.celery.tasks.collector_tasks import collect_dataset

    dispatched = 0
    for index, candidate in enumerate(candidates):
        try:
            collect_dataset.apply_async(
                args=[candidate.dataset_id],
                kwargs={"force_reparse": True},
                countdown=index * step_seconds,
            )
            dispatched += 1
        except Exception:
            logger.warning("could not dispatch %s", candidate.dataset_id, exc_info=True)
    return dispatched


def _summary(candidates: Sequence[Candidate], skipped: Counter) -> dict[str, object]:
    return {
        "candidatas": len(candidates),
        "por_motivo": dict(Counter(c.reason for c in candidates)),
        "por_portal": dict(Counter(c.portal for c in candidates).most_common(15)),
        "por_formato": dict(Counter(c.format for c in candidates)),
        "salteadas": dict(skipped),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="header_backfill",
        description="Lista (y con --execute re-colecta) las tablas que rompió el colector.",
    )
    parser.add_argument("--execute", action="store_true", help="despachar las re-colectas")
    parser.add_argument("--limit", type=int, default=None, help="máximo de datasets")
    parser.add_argument("--portal", action="append", default=None, help="sólo este portal")
    parser.add_argument(
        "--reason",
        action="append",
        choices=[REASON_HEADER, REASON_DEFICIT],
        default=None,
        help="sólo este motivo",
    )
    parser.add_argument("--no-exact-counts", action="store_true", help="no confirmar faltantes")
    parser.add_argument("--step-seconds", type=int, default=5, help="separación entre despachos")
    parser.add_argument("--json", action="store_true", help="salida JSON")
    args = parser.parse_args(argv)

    from app.infrastructure.celery.tasks._db import get_sync_engine

    engine = get_sync_engine()
    candidates, skipped = find_candidates(
        engine,
        portals=args.portal,
        reasons=args.reason,
        exact_counts=not args.no_exact_counts,
    )
    if args.limit is not None:
        candidates = candidates[: args.limit]

    summary = _summary(candidates, skipped)
    dispatched = dispatch(candidates, step_seconds=args.step_seconds) if args.execute else 0
    summary["dry_run"] = not args.execute
    summary["despachadas"] = dispatched

    if args.json:
        print(
            json.dumps(
                {"resumen": summary, "candidatas": [asdict(c) for c in candidates]},
                ensure_ascii=False,
                indent=1,
            )
        )
    else:
        for c in candidates:
            print(
                f"{c.reason:17} {c.portal:22} {c.format:5} {c.dataset_id} {c.table_name}  {c.detail}"
            )
        print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
