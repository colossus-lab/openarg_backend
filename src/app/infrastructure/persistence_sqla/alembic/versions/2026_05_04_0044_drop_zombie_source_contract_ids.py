"""Drop zombie column `mart_definitions.source_contract_ids`.

Revision ID: 0044
Revises: 0043
Create Date: 2026-05-04

The column was used by the contracts/staging design (mig 0040), removed in
mig 0042 (B-rebuild). Mig 0043 added `source_portals` as the replacement
routing key but left `source_contract_ids` intact during the transition.
With no readers in code since 2026-05-04, this migration removes the
column for good. Closes DEBT-019-004.
"""

from __future__ import annotations

from alembic import op

revision = "0044"
down_revision = "0043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("mart_definitions", "source_contract_ids")


def downgrade() -> None:
    import sqlalchemy as sa

    op.add_column(
        "mart_definitions",
        sa.Column(
            "source_contract_ids",
            sa.dialects.postgresql.ARRAY(sa.Text),
            nullable=False,
            server_default="{}",
        ),
    )
