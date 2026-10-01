"""Unir `query_analytics` en `public`

The table was never created by a migration: `history.save_query_attempt`
creates it lazily with `CREATE TABLE IF NOT EXISTS query_analytics` and writes
with an unqualified `INSERT`. Which table that reaches depends on the
connection's search_path, and the pool does not keep it stable (the database
default is `raw, public`; the app's `public, raw` is set by a connect
listener that a pool ROLLBACK can revert). So a second table appeared in
`raw`, and writes split between the two. On staging, 2026-09-30: 883 rows in
`public` — the one `/admin/analytics` reads — and 1,189 in `raw`, where every
write after a restart was going, unread.

The code now qualifies every use with `public.`. This revision merges what
landed in `raw` into `public` and drops the `raw` copy:

- only `raw` exists → `ALTER TABLE ... SET SCHEMA public` (rows, index);
- both exist → copy the rows (new ids; `ts` keeps the order) and drop `raw`;
- otherwise → nothing.

Revision ID: 0065
Revises: 0064
"""

from alembic import op

revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None

_COLUMNS = (
    "ts, question, served_table, mart_used, row_count, success, "
    "duration_ms, error_message, embedding"
)


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('raw.query_analytics') IS NULL THEN
                RETURN;
            END IF;
            IF to_regclass('public.query_analytics') IS NULL THEN
                ALTER TABLE raw.query_analytics SET SCHEMA public;
            ELSE
                INSERT INTO public.query_analytics ({_COLUMNS})
                SELECT {_COLUMNS} FROM raw.query_analytics ORDER BY ts;
                DROP TABLE raw.query_analytics;
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    # Nothing to undo: splitting the rows back would recreate the bug.
    pass
