"""Add `source_portals` and `last_row_count` to `mart_definitions`.

Revision ID: 0043
Revises: 0042
Create Date: 2026-05-04

The B-rebuild medallion routes mart refreshes by upstream portal (the
landed dataset's portal triggers refresh of every mart whose YAML lists
that portal). The previous routing key was `source_contract_ids`, which
only made sense in the contracts/staging design that was removed in
mig 0042.

`last_row_count` is the post-refresh count, used by the Serving Port to
hide marts that resolve to 0 rows (otherwise the planner would suggest
empty marts and downstream NL2SQL would return "no data" which is
worse than not suggesting at all).

`source_contract_ids` is left as a zombie column for now — it has no
readers but nuking it requires coordinated downtime. A future migration
will drop it.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0043"
down_revision = "0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "mart_definitions",
        sa.Column(
            "source_portals",
            sa.dialects.postgresql.ARRAY(sa.Text),
            nullable=False,
            server_default="{}",
        ),
    )
    op.add_column(
        "mart_definitions",
        sa.Column("last_row_count", sa.BigInteger, nullable=True),
    )
    op.create_index(
        "ix_mart_definitions_source_portals",
        "mart_definitions",
        ["source_portals"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_mart_definitions_source_portals", table_name="mart_definitions")
    op.drop_column("mart_definitions", "last_row_count")
    op.drop_column("mart_definitions", "source_portals")
