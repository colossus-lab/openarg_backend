"""Let a mart be withheld from serving without deleting it.

Revision ID: 0054
Revises: 0053
Create Date: 2026-07-26

Until now the only way the Serving Port could hide a mart was
`COALESCE(last_row_count, 0) > 0` — a gate for marts that are *empty*.
There was no way to say "this mart has rows, but they are wrong". On
2026-07-26 `mart.presupuesto_nacional_ejecutado` served a confident
ranking of budget transfers that was wrong by orders of magnitude
(TEXT amount columns with mixed decimal formats, a corrupt `'2.022'`
year value, and `require_all_columns=True` silently narrowing the union
to 32 of 560 source tables). For a platform whose purpose is checking
claims against real data, serving that is worse than serving nothing.

`serving_blocked` is that missing switch, and it is driven from the mart
YAML (`serving.blocked` / `serving.blocked_reason`) so it survives every
DROP+CREATE rebuild — a DB-only flag would be silently cleared by the
next `build_mart`.

The default FALSE keeps every existing mart exactly as it is.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0054"
down_revision = "0053"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "mart_definitions",
        sa.Column(
            "serving_blocked",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
    )
    op.add_column(
        "mart_definitions",
        sa.Column("serving_blocked_reason", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("mart_definitions", "serving_blocked_reason")
    op.drop_column("mart_definitions", "serving_blocked")
