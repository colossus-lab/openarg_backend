"""Drop the copies CKAN's re-identification made us keep.

When datos.gob.ar migrated to CKAN 2.11 around 2026-07-29 it regenerated
resource ids, so the same file arrived under a name we had never seen and was
collected again. Measured on 2026-08-23: 13,672 catalogue rows resolve to 5,622
real resources, and the duplicates hold 985 million rows across 115 GB.

**The naive reading of that is wrong, and measuring is what showed it.** Of the
7,201 rows that look redundant:

- **791 groups hold different row counts.** Same URL, same title, different
  content — the file changed between collections. Those are not copies and
  dropping either one loses something.
- **620 tables are named in a mart's SQL.** Dropping them breaks serving.
- **7,207 are live in `raw_table_versions`.** Dropping the table without
  retiring the registry row manufactures exactly the phantom rows that had to
  be cleaned out of production earlier the same day.

What survives all three filters is 5,522 tables in 4,148 groups, holding 193
million rows. That is the number this acts on, and it is smaller than the one
that motivated the work — which is the normal shape of a destructive job that
was measured before it ran.

**What happens to the catalogue row.** `cached_datasets.table_name` is UNIQUE,
so the duplicate cannot simply be pointed at the survivor's table. Its row is
deleted, and `datasets.is_cached` is set to `true` in the same transaction.

That last part is not bookkeeping. The dispatcher selects on
`is_cached = false`, and these rows were false — so the first version of this
sweep was a treadmill: it dropped 5,344 tables and left every one of them
queued to be collected again. It was caught in production minutes before the
next scheduled run, by checking what the value actually was rather than
trusting that nothing had written it. The test that was supposed to cover this
asserted the code does not *touch* `is_cached`, which was true and useless.

`true` is honest: the resource is cached, under the canonical twin that this
row's `original_identifier` names, and the candidate query has already proved
that twin exists and holds rows.

Its embedding chunks go too. A dataset with no table that still answers vector
searches is worse than one that was never indexed: it competes for a slot and
then has nothing behind it.

Nothing here can reach a user or a conversation. The tables it touches are named
by `cached_datasets`, which only ever names dataset tables, and the blocklist
below says so out loud anyway.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Belt and braces, the same list the reconciliation sweep carries. Nothing in
# this module can reach these, and a module that issues DROP TABLE should say
# what it will not drop regardless.
_NEVER_DROP = frozenset(
    {
        "users",
        "user_queries",
        "conversations",
        "conversation_messages",
        "messages",
        "checkpoints",
        "checkpoint_writes",
        "checkpoint_blobs",
        "api_keys",
        "api_usage",
        "datasets",
        "dataset_chunks",
        "cached_datasets",
        "raw_table_versions",
        "mart_definitions",
        "alembic_version",
    }
)

_SAFE_CANDIDATES_SQL = text(
    """
    WITH grupos AS (
        SELECT d.original_identifier AS oi
        FROM datasets d
        JOIN raw.cached_datasets cd ON cd.dataset_id = d.id
        WHERE d.original_identifier IS NOT NULL
          AND cd.status = 'ready' AND cd.row_count > 0
        GROUP BY 1
        HAVING count(*) > 1
           -- Every copy must hold the same number of rows. Where they differ
           -- the file changed between collections and they are not copies.
           AND min(cd.row_count) = max(cd.row_count)
    ),
    ranked AS (
        SELECT d.original_identifier AS oi, d.id AS dataset_id,
               cd.id AS cd_id, cd.table_name, cd.row_count,
               row_number() OVER (
                   PARTITION BY d.original_identifier
                   ORDER BY d.created_at DESC, d.id DESC
               ) AS rn
        FROM datasets d
        JOIN grupos g ON g.oi = d.original_identifier
        JOIN raw.cached_datasets cd ON cd.dataset_id = d.id
        WHERE cd.status = 'ready' AND cd.row_count > 0
    )
    SELECT r.oi, r.dataset_id, r.cd_id, r.table_name, r.row_count,
           (SELECT s.table_name FROM ranked s WHERE s.oi = r.oi AND s.rn = 1) AS survivor
    FROM ranked r
    WHERE r.rn > 1
      AND NOT EXISTS (
          SELECT 1 FROM mart_definitions m
          WHERE m.sql_definition LIKE '%' || r.table_name || '%'
      )
      -- The survivor has to exist on disk. Dropping the copy of something that
      -- is not there would leave the resource with nothing at all.
      AND EXISTS (
          SELECT 1 FROM ranked s
          JOIN information_schema.tables t
            ON t.table_name = s.table_name AND t.table_type = 'BASE TABLE'
           AND t.table_schema IN ('raw', 'public')
          WHERE s.oi = r.oi AND s.rn = 1 AND s.row_count > 0
      )
    ORDER BY r.row_count DESC
    LIMIT :limit
    """
)


@dataclass
class CleanupOutcome:
    run_id: uuid.UUID
    dry_run: bool
    dropped: list[str] = field(default_factory=list)
    rows_freed: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)

    def note(self, reason: str) -> None:
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "dry_run": self.dry_run,
            "dropped": len(self.dropped),
            "rows_freed": self.rows_freed,
            "by_reason": self.by_reason,
            "samples": self.dropped[:5],
        }


def cleanup_duplicate_tables(
    engine: Engine,
    *,
    run_id: uuid.UUID | None = None,
    dry_run: bool = True,
    limit: int = 200,
) -> CleanupOutcome:
    """Drop redundant copies, one coordinated transaction at a time."""
    run_id = run_id or uuid.uuid4()
    outcome = CleanupOutcome(run_id=run_id, dry_run=dry_run)

    with engine.connect() as conn:
        rows = conn.execute(_SAFE_CANDIDATES_SQL, {"limit": limit}).fetchall()
        conn.rollback()

    for row in rows:
        table = str(row.table_name)
        if table in _NEVER_DROP or not row.survivor:
            outcome.note("protected_or_no_survivor")
            continue

        if dry_run:
            outcome.note("would_drop")
            outcome.rows_freed += int(row.row_count or 0)
            outcome.dropped.append(f"{table} (sobrevive {str(row.survivor)[:40]})")
            continue

        try:
            with engine.begin() as conn:
                # Re-check the survivor inside the transaction. The candidate
                # list was built earlier and a sweep may have moved since.
                alive = conn.execute(
                    text(
                        """
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = :t AND table_type = 'BASE TABLE'
                          AND table_schema IN ('raw', 'public')
                        """
                    ),
                    {"t": str(row.survivor)},
                ).fetchone()
                if not alive:
                    raise RuntimeError("survivor_vanished")

                schema = conn.execute(
                    text(
                        """
                        SELECT table_schema FROM information_schema.tables
                        WHERE table_name = :t AND table_type = 'BASE TABLE'
                          AND table_schema IN ('raw', 'public') LIMIT 1
                        """
                    ),
                    {"t": table},
                ).scalar()
                if not schema:
                    raise RuntimeError("already_gone")

                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))
                # Retire the registry row in the same transaction: a dropped
                # table whose registry row still says live is a phantom, and
                # those had to be swept out of production this morning.
                conn.execute(
                    text(
                        "UPDATE public.raw_table_versions SET superseded_at = now() "
                        "WHERE table_name = :t AND superseded_at IS NULL"
                    ),
                    {"t": table},
                )
                conn.execute(
                    text("DELETE FROM raw.cached_datasets WHERE id = :i"), {"i": row.cd_id}
                )
                # Without this the cleanup is a treadmill. The dispatcher
                # selects on `is_cached = false`, and these rows were false —
                # so the next bulk collection would rebuild every duplicate
                # that was just dropped. Caught in production minutes before
                # the 01:45 run, by checking the value rather than trusting
                # that nothing had written it.
                #
                # `true` is honest here: the resource IS cached, under the
                # canonical twin this row's `original_identifier` names. The
                # candidate query already proved that twin exists and holds
                # rows, so this cannot strand a dataset with nowhere to look.
                conn.execute(
                    text("UPDATE datasets SET is_cached = true WHERE id = :d"),
                    {"d": row.dataset_id},
                )
                # A dataset with no table that still answers vector searches
                # competes for a slot and has nothing behind it.
                conn.execute(
                    text("DELETE FROM dataset_chunks WHERE dataset_id = :d"),
                    {"d": row.dataset_id},
                )
        except Exception as exc:
            outcome.note(f"failed:{str(exc)[:40]}")
            logger.warning("duplicate cleanup failed for %s", table, exc_info=True)
            continue

        outcome.note("dropped")
        outcome.rows_freed += int(row.row_count or 0)
        outcome.dropped.append(table)

    return outcome
