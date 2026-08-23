"""What a mart must be true for, checked after it is built.

The plan calls for manual expectations on marts. The measurement that preceded
this one argues for a narrower reading of "manual": on 2026-08-23 two marts were
degraded in production and neither would have been caught by a hand-written
rule, because nobody had written one — and nobody was going to write 69 of them
and keep them current.

So expectations come from two places, and the split is deliberate:

- **Declared**, in the mart's YAML beside its SQL. For facts a person knows and
  the data cannot say: this column is an identifier and must never be null,
  this mart is meaningless below N rows. Versioned with the query they describe,
  so a rewrite that invalidates them shows up in the same diff.

  The declared floors are set at half of what each mart actually held when the
  expectation was written, not at a round number someone liked. The first run of
  this check flagged `presupuesto_nacional_ejecutado` at 91,299 rows against a
  floor of 100,000 — and the floor was the thing that was wrong, invented rather
  than measured. A threshold nobody measured produces findings nobody can act
  on, and the second one of those teaches the reader to skim.
- **Derived**, from `mart_build_history`. For the question no human should have
  to answer per mart: *is this build normal for this mart?* A mart that has held
  two million rows for three months and now holds four hundred is a finding
  regardless of what anyone declared.

The derived side stays quiet until it has ground to stand on. With fewer than
`_MIN_HISTORY` builds recorded there is no such thing as normal yet, and a check
that fires on the second build ever would be measuring its own youth.

Every check answers with a reason a person can act on. "row_count 412 vs median
2,775,244 over 9 builds" is a finding; "expectation failed" is a shrug.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Below this many recorded builds, "normal" is not a thing that exists yet.
_MIN_HISTORY = 3

# How far a build may fall below its own median before it is a finding. Marts
# legitimately shrink — a source drops a year, a filter tightens — so this is
# set where a fall is hard to explain as ordinary movement rather than where it
# is merely noticeable.
_COLLAPSE_RATIO = 0.5


@dataclass(frozen=True)
class Finding:
    """One expectation a mart did not meet."""

    mart_id: str
    rule: str
    detail: str


def _declared_findings(engine: Engine, mart: Any) -> list[Finding]:
    """Check the rules the mart's YAML declares."""
    rules = getattr(mart, "expectations", None) or {}
    if not rules:
        return []
    out: list[Finding] = []
    qualified = f'"{mart.schema_name}"."{mart.view_name}"'

    min_rows = rules.get("min_rows")
    if isinstance(min_rows, int) and min_rows > 0:
        try:
            with engine.connect() as conn:
                n = conn.execute(text(f"SELECT count(*) FROM {qualified}")).scalar() or 0  # noqa: S608
                conn.rollback()
        except Exception:
            logger.debug("expectations: could not count %s", mart.id, exc_info=True)
            return out
        if n < min_rows:
            out.append(
                Finding(
                    mart_id=mart.id,
                    rule="min_rows",
                    detail=f"{n} filas, el yaml declara un mínimo de {min_rows}",
                )
            )

    for col in rules.get("not_null") or []:
        try:
            with engine.connect() as conn:
                nulls = (
                    conn.execute(
                        text(f'SELECT count(*) FROM {qualified} WHERE "{col}" IS NULL')  # noqa: S608
                    ).scalar()
                    or 0
                )
                conn.rollback()
        except Exception:
            # A column that no longer exists is itself worth saying, and saying
            # it beats failing the whole check for the mart.
            out.append(
                Finding(
                    mart_id=mart.id,
                    rule="not_null",
                    detail=f"no se pudo revisar la columna '{col}' (¿ya no existe?)",
                )
            )
            continue
        if nulls:
            out.append(
                Finding(
                    mart_id=mart.id,
                    rule="not_null",
                    detail=f"'{col}' tiene {nulls} nulos y el yaml dice que no debería",
                )
            )
    return out


_HISTORY_SQL = text(
    """
    SELECT row_count
    FROM public.mart_build_history
    WHERE mart_id = :m AND status IN ('built', 'refreshed') AND row_count IS NOT NULL
    ORDER BY built_at DESC
    LIMIT 12
    """
)


def _derived_findings(engine: Engine, mart_id: str, current_rows: int) -> list[Finding]:
    """Is this build normal for this mart, judged against its own past?

    Median rather than mean: one bad build in the window would drag an average
    down and quietly raise the bar for calling the next one a collapse.
    """
    try:
        with engine.connect() as conn:
            rows = [int(r.row_count) for r in conn.execute(_HISTORY_SQL, {"m": mart_id})]
            conn.rollback()
    except Exception:
        logger.debug("expectations: no history for %s", mart_id, exc_info=True)
        return []

    if len(rows) < _MIN_HISTORY:
        return []

    ordered = sorted(rows)
    mid = len(ordered) // 2
    median = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2
    if median <= 0:
        return []
    if current_rows < median * _COLLAPSE_RATIO:
        pct = round(100 * (1 - current_rows / median))
        return [
            Finding(
                mart_id=mart_id,
                rule="row_count_collapse",
                detail=(
                    f"{current_rows:,} filas contra una mediana de {median:,.0f} "
                    f"en {len(rows)} builds — cayó {pct} %"
                ),
            )
        ]
    return []


def check_mart(engine: Engine, mart: Any, current_rows: int) -> list[Finding]:
    """Every expectation for one mart, declared and derived."""
    return _declared_findings(engine, mart) + _derived_findings(
        engine, mart.id, current_rows
    )


def record_build(
    engine: Engine,
    *,
    mart_id: str,
    status: str,
    row_count: int | None,
    source_data_oldest: Any = None,
    error_message: str | None = None,
) -> None:
    """Append one build to the history. Best-effort: never fails a build."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO public.mart_build_history
                        (mart_id, status, row_count, source_data_oldest, error_message)
                    VALUES (:m, :s, :r, :o, :e)
                    """
                ),
                {
                    "m": mart_id,
                    "s": status,
                    "r": row_count,
                    "o": source_data_oldest,
                    "e": (error_message or None) and str(error_message)[:500],
                },
            )
    except Exception:
        logger.debug("expectations: could not record build of %s", mart_id, exc_info=True)
