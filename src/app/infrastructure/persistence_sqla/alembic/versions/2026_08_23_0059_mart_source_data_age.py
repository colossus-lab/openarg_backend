"""mart source data age

A mart's `last_refreshed_at` says when the build ran, not when its data was
read. A mart rebuilt this morning over sources last collected in May holds
May's data with a fresh timestamp on it, and showing that timestamp as
freshness would reassure a reader about something nobody measured.

These two columns record the real span, taken at build time from the tables the
mart's macros actually resolved to — the only moment the system knows exactly
which tables a mart reads.

Nullable, because every existing mart has no answer until its next build, and
`data_age_for` must be able to say "I don't know" rather than guess.

Revision ID: 0059
Revises: 0058
"""

from alembic import op

revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.mart_definitions
            ADD COLUMN IF NOT EXISTS source_data_oldest TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS source_data_newest TIMESTAMPTZ
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.mart_definitions
            DROP COLUMN IF EXISTS source_data_oldest,
            DROP COLUMN IF EXISTS source_data_newest
        """
    )
