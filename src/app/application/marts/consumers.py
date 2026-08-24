"""Which marts read which raw tables — the edge neither half had.

Two subsystems know about a table and neither knows about the other. The repair
sweeps see `raw.caba__pauta_publicitaria__39691d8c__v1` as a table with one
column full of semicolons. The marts see `pauta_oficial` as a view that is
smaller than it should be. Nothing joins them, so a repair runs blind to what
consumes its result and a broken mart cannot name what broke it.

The edge already exists in the database: `mart_definitions.sql_definition` is
the **resolved** SQL, with every `live_table()` macro expanded into concrete
`FROM raw."cache_…"` references. Reading it backwards gives the mapping.

Measured against production on 2026-08-24: 74 marts reference **1,489 distinct
raw tables**, and **105 of the 3,160 tables carrying a parse defect feed a
mart**. That 105 is the difference between "3,160 broken tables" — a number too
large to act on — and a work list, ordered by whether anybody is served by the
result.

**It is also a brake, and that is the half that matters.** Renaming `col_1` to
`monto` fixes a table and breaks every mart whose SQL says `col_1`. Before this,
nothing could see that; a sweep that repairs unattended needs to know when its
repair is the more expensive of the two outcomes. `marts_referencing_column`
answers exactly that, and the escalation ladder refuses rather than guesses.

Read-only, no schema of its own, and derived from a query rather than kept in a
counter — so it cannot drift away from what the marts actually say.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# `FROM raw."cache_x"` and `JOIN public."cache_y"`. Schema-qualified and quoted
# is what the macro expander emits, and 1,703 of 1,703 references in production
# have that shape today.
#
# JOIN is matched even though production has zero JOIN references right now.
# Supporting it costs one alternation; missing one later would mean a mart
# silently absent from the consumer list, which is the direction that lets an
# unattended repair break something. A guard against the next mart, not a claim
# about the current ones.
_REF_RE = re.compile(r'(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\."([^"]+)"', re.IGNORECASE)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_MART_SQL = text(
    """
    SELECT mart_id, sql_definition
    FROM public.mart_definitions
    WHERE sql_definition IS NOT NULL
    """
)


def source_references(sql: str) -> set[tuple[str, str]]:
    """Every `(schema, table)` a piece of resolved mart SQL reads."""
    return {(s.lower(), t) for s, t in _REF_RE.findall(sql or "")}


@dataclass(frozen=True)
class ConsumerIndex:
    """Table → marts, plus the SQL, so a caller can ask the follow-up question."""

    by_table: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)
    sql_by_mart: dict[str, str] = field(default_factory=dict)

    @property
    def marts(self) -> int:
        return len(self.sql_by_mart)

    @property
    def tables(self) -> int:
        return len(self.by_table)

    def marts_for(self, schema: str, table: str) -> tuple[str, ...]:
        """Which marts read this table. Empty means nothing is served by it."""
        return self.by_table.get((schema.lower(), table), ())

    def marts_referencing_column(self, schema: str, table: str, column: str) -> tuple[str, ...]:
        """Which of the table's consumers name this column in their SQL.

        The brake on automatic renaming. A mart that projects `col_1` keeps
        working only while the column is called `col_1`.

        Deliberately crude, and deliberately crude in one direction: a bare name
        like `col_1` is common enough that another mart could mention it about a
        different table, and this will count that as a reference. The cost of a
        false positive is a repair postponed to a person; the cost of a false
        negative is a working mart broken by a sweep nobody was watching. Only
        one of those is worth being clever about.
        """
        hits: list[str] = []
        quoted = f'"{column}"'
        bare = re.compile(rf"\b{re.escape(column)}\b") if _IDENTIFIER_RE.match(column) else None
        for mart_id in self.marts_for(schema, table):
            sql = self.sql_by_mart.get(mart_id, "")
            if quoted in sql or (bare is not None and bare.search(sql)):
                hits.append(mart_id)
        return tuple(hits)


def build_consumer_index(engine: Engine) -> ConsumerIndex:
    """Read every mart's resolved SQL and invert it into table → marts."""
    by_table: dict[tuple[str, str], list[str]] = {}
    sql_by_mart: dict[str, str] = {}

    with engine.connect() as conn:
        rows = conn.execute(_MART_SQL).fetchall()
        conn.rollback()

    for row in rows:
        sql_by_mart[row.mart_id] = row.sql_definition or ""
        for ref in source_references(row.sql_definition or ""):
            by_table.setdefault(ref, []).append(row.mart_id)

    index = ConsumerIndex(
        by_table={k: tuple(sorted(set(v))) for k, v in by_table.items()},
        sql_by_mart=sql_by_mart,
    )
    logger.info(
        "consumer index: %d mart(s) reading %d distinct table(s)",
        index.marts,
        index.tables,
    )
    return index
