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

**Two sources, and the catalog is the better one.** Postgres already records
which relations a materialised view reads: it froze the OIDs when the view was
created, so `pg_depend` answers the question authoritatively, for free, and —
crucially here — **without depending on `search_path`**, which under PGBouncer is
a coin flip. `refobjsubid` carries the attribute number, so the catalog also
knows *which columns* each mart reads. Measured on production 2026-08-24: 73
built marts, 1,520 source tables, **13,967 column-level edges** — a fact the SQL
scan could not produce at all.

Reading the stored SQL stays, for the one thing the catalog cannot know: a mart
that failed to build has no dependency rows, and its sources still deserve the
brake. So the catalog is consulted first and the text scan fills the gap, rather
than the other way round.

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

# Reading the stored SQL is now the *fallback*, for marts the catalog does not
# know about because they never built. The catalog is authoritative for the rest,
# and this text scan carries every weakness that implies: a CTE named like a real
# table invents an edge, a `FROM` split by a comment loses one, and `SELECT *`
# yields no column information whatsoever.
#
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

# What Postgres itself knows about who reads what. `pg_rewrite` holds each
# materialised view's rule; `pg_depend` records the relations that rule depends
# on, with the OIDs resolved at CREATE time — so no name resolution, no
# `search_path`, no parsing. `refobjsubid` is the attribute number of the column
# depended on, or 0 for a whole-relation dependency.
_CATALOG_SQL = text(
    """
    SELECT DISTINCT
        dependent.relname AS mart_view,
        src_ns.nspname    AS schema_name,
        src.relname       AS table_name,
        a.attname         AS column_name
    FROM pg_depend d
    JOIN pg_rewrite r        ON r.oid = d.objid
    JOIN pg_class dependent  ON dependent.oid = r.ev_class
    JOIN pg_namespace dep_ns ON dep_ns.oid = dependent.relnamespace
    JOIN pg_class src        ON src.oid = d.refobjid
    JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
    LEFT JOIN pg_attribute a ON a.attrelid = src.oid
                            AND a.attnum = d.refobjsubid
                            AND d.refobjsubid > 0
    WHERE d.classid = 'pg_rewrite'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND dependent.oid <> src.oid
      AND dep_ns.nspname = :mart_schema
      AND src_ns.nspname = ANY(:source_schemas)
    """
)

# Marts the catalog cannot speak for: never built, so no view, so no dependency
# rows. Their sources still deserve the brake, so the text scan covers them.
_UNBUILT_MART_SQL = text(
    """
    SELECT mart_id, sql_definition
    FROM public.mart_definitions
    WHERE sql_definition IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = mart_definitions.mart_schema
          AND c.relname = mart_definitions.mart_view_name
          AND c.relkind IN ('m', 'v')
      )
    """
)

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
    # (schema, table, column) -> marts that read exactly that column, straight
    # from the catalog. Empty for a table the catalog knows nothing about, which
    # is why `marts_referencing_column` still has a text fallback.
    by_column: dict[tuple[str, str, str], tuple[str, ...]] = field(default_factory=dict)
    # Tables the catalog spoke for. Inside this set an empty `by_column` answer
    # is a fact — "no mart reads that column" — and outside it, it is ignorance.
    catalog_tables: frozenset[tuple[str, str]] = frozenset()

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
        """Which of the table's consumers read this column.

        The brake on automatic renaming. A mart that projects `col_1` keeps
        working only while the column is called `col_1`.

        **The catalog answers when it can.** For a table some built mart reads,
        Postgres knows exactly which columns each view depends on, and that is a
        fact rather than a guess — it says *no* as reliably as it says *yes*, so
        a rename nobody depends on is no longer refused out of caution.

        Outside that set the answer falls back to searching the stored SQL, and
        that search is deliberately crude in one direction: a bare name like
        `col_1` is common enough that another mart could mention it about a
        different table, and this counts that as a reference. The cost of a false
        positive is a repair postponed to a person; the cost of a false negative
        is a working mart broken by a sweep nobody was watching.
        """
        key = (schema.lower(), table)
        if key in self.catalog_tables:
            return self.by_column.get((schema.lower(), table, column), ())

        hits: list[str] = []
        quoted = f'"{column}"'
        bare = re.compile(rf"\b{re.escape(column)}\b") if _IDENTIFIER_RE.match(column) else None
        for mart_id in self.marts_for(schema, table):
            sql = self.sql_by_mart.get(mart_id, "")
            if quoted in sql or (bare is not None and bare.search(sql)):
                hits.append(mart_id)
        return tuple(hits)


def build_consumer_index(
    engine: Engine,
    *,
    mart_schema: str = "mart",
    source_schemas: tuple[str, ...] = ("raw", "public"),
) -> ConsumerIndex:
    """Who reads what: the catalog first, the stored SQL for the gaps.

    Never raises on the catalog half. If `pg_depend` cannot be read the index
    degrades to the text scan it used to be, which is worse but not broken.
    """
    by_table: dict[tuple[str, str], set[str]] = {}
    by_column: dict[tuple[str, str, str], set[str]] = {}
    catalog_tables: set[tuple[str, str]] = set()
    sql_by_mart: dict[str, str] = {}

    with engine.connect() as conn:
        try:
            catalog_rows = conn.execute(
                _CATALOG_SQL,
                {"mart_schema": mart_schema, "source_schemas": list(source_schemas)},
            ).fetchall()
        except Exception:
            logger.warning("consumer index: catalog unavailable, using SQL only", exc_info=True)
            catalog_rows = []

        for row in catalog_rows:
            ref = (row.schema_name.lower(), row.table_name)
            by_table.setdefault(ref, set()).add(row.mart_view)
            catalog_tables.add(ref)
            if row.column_name:
                by_column.setdefault((*ref, row.column_name), set()).add(row.mart_view)

        # Marts with no view yet. Their SQL is all there is to go on, and their
        # sources need the brake exactly as much.
        rows = conn.execute(_UNBUILT_MART_SQL if catalog_rows else _MART_SQL).fetchall()
        conn.rollback()

    for row in rows:
        sql_by_mart[row.mart_id] = row.sql_definition or ""
        for ref in source_references(row.sql_definition or ""):
            by_table.setdefault(ref, set()).add(row.mart_id)

    index = ConsumerIndex(
        by_table={k: tuple(sorted(v)) for k, v in by_table.items()},
        sql_by_mart=sql_by_mart,
        by_column={k: tuple(sorted(v)) for k, v in by_column.items()},
        catalog_tables=frozenset(catalog_tables),
    )
    logger.info(
        "consumer index: %d table(s), %d from the catalog with %d column edge(s), "
        "%d unbuilt mart(s) read from SQL",
        index.tables,
        len(catalog_tables),
        len(by_column),
        len(sql_by_mart),
    )
    return index
