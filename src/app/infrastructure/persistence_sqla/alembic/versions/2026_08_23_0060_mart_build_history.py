"""mart build history

`mart_definitions` keeps only the current `last_row_count`, so "this mart
collapsed" is unprovable: there is nothing to compare against. Two marts were
found degraded on 2026-08-23 — one at `build_failed` for weeks, one at zero rows
for eight days — and in both cases the current value alone could not distinguish
a mart that had always been small from one that had just lost its rows.

One append-only row per build. Small by construction: 69 marts times a handful
of builds a day is a few thousand rows a year, and the whole point is to be able
to ask what normal looked like before.

`built_at` is not the age of the data — that is `source_data_oldest` on the
definition, and it is carried here too so a row of history is self-contained.

Revision ID: 0060
Revises: 0059
"""

from alembic import op

revision = "0060"
down_revision = "0059"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.mart_build_history (
            id                  BIGSERIAL PRIMARY KEY,
            mart_id             TEXT        NOT NULL,
            built_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
            status              TEXT        NOT NULL,
            row_count           BIGINT,
            source_data_oldest  TIMESTAMPTZ,
            error_message       TEXT
        )
        """
    )
    # The only access pattern: "the last N builds of this mart, newest first."
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_mart_build_history_mart_time
        ON public.mart_build_history (mart_id, built_at DESC)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS public.ix_mart_build_history_mart_time")
    op.execute("DROP TABLE IF EXISTS public.mart_build_history")
