"""Add staff_snapshots and staff_changes tables for HCDN staff tracking

Revision ID: 0010
Revises: 0009
Create Date: 2026-03-01

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- staff_snapshots: weekly full snapshot of the HCDN payroll ---
    op.create_table(
        "staff_snapshots",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("legajo", sa.String(50), nullable=False),
        sa.Column("apellido", sa.String(300), nullable=False),
        sa.Column("nombre", sa.String(300), nullable=False),
        sa.Column("escalafon", sa.String(200), nullable=True),
        sa.Column("area_desempeno", sa.String(500), nullable=False),
        sa.Column("convenio", sa.String(200), nullable=True),
        sa.Column("snapshot_date", sa.Date, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_staff_snapshots_snapshot_date", "staff_snapshots", ["snapshot_date"])
    op.create_index("ix_staff_snapshots_area", "staff_snapshots", ["area_desempeno"])
    op.create_index("ix_staff_snapshots_legajo_date", "staff_snapshots", ["legajo", "snapshot_date"], unique=True)

    # --- staff_changes: altas/bajas detected by diffing consecutive snapshots ---
    op.create_table(
        "staff_changes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("legajo", sa.String(50), nullable=False),
        sa.Column("apellido", sa.String(300), nullable=False),
        sa.Column("nombre", sa.String(300), nullable=False),
        sa.Column("area_desempeno", sa.String(500), nullable=False),
        sa.Column("tipo", sa.String(10), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint("tipo IN ('alta', 'baja')", name="ck_staff_changes_tipo"),
    )
    op.create_index("ix_staff_changes_area", "staff_changes", ["area_desempeno"])
    op.create_index("ix_staff_changes_tipo_detected", "staff_changes", ["tipo", "detected_at"])
    op.create_index("ix_staff_changes_legajo", "staff_changes", ["legajo"])


def downgrade() -> None:
    op.drop_table("staff_changes")
    op.drop_table("staff_snapshots")
