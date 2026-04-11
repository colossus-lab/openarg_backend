"""Widen staff_changes.tipo to include 'update' and add changes_json column.

Revision ID: 0028
Revises: 0027
Create Date: 2026-04-11

FIX-008: the original migration 0010 created ``staff_changes`` with a
CHECK constraint ``tipo IN ('alta', 'baja')``. The diff logic in
``staff_tasks.py`` therefore could not record office transfers,
promotions, or convenio changes on employees that persisted across
snapshots — the whole category of "update" events was invisible.

This migration widens the constraint to accept ``'update'`` and adds a
nullable ``changes_json`` JSONB column that stores the exact field-by-
field diff for update events (``{"area_desempeno": {"from": ..., "to":
...}, ...}``). Altas and bajas continue to leave ``changes_json`` NULL.

The downgrade path is deliberately **destructive**: it first deletes
any rows with ``tipo = 'update'`` because the old CHECK constraint
rejects them, and the constraint rewrite would otherwise fail. An
operator rolling back knows they are discarding the newly-captured
update events — that is the price of reversibility and is documented
in FR-012 of specs/002-connectors/002f-staff/spec.md.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Add the changes_json JSONB column (nullable — altas/bajas leave it NULL).
    op.add_column(
        "staff_changes",
        sa.Column("changes_json", JSONB, nullable=True),
    )

    # Widen the CHECK constraint to accept 'update'.
    op.drop_constraint("ck_staff_changes_tipo", "staff_changes", type_="check")
    op.create_check_constraint(
        "ck_staff_changes_tipo",
        "staff_changes",
        "tipo IN ('alta', 'baja', 'update')",
    )


def downgrade() -> None:
    # FR-012: destructive downgrade — delete update events before restoring
    # the old CHECK constraint, because the old constraint rejects them.
    op.execute("DELETE FROM staff_changes WHERE tipo = 'update'")

    op.drop_constraint("ck_staff_changes_tipo", "staff_changes", type_="check")
    op.create_check_constraint(
        "ck_staff_changes_tipo",
        "staff_changes",
        "tipo IN ('alta', 'baja')",
    )

    op.drop_column("staff_changes", "changes_json")
