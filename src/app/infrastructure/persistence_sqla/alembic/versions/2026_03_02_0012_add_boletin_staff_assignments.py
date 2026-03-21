"""Add boletin_staff_assignments table for legislator-employee mapping from Boletín Oficial

Revision ID: 0012
Revises: 0011
Create Date: 2026-03-02

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "boletin_staff_assignments",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("legajo", sa.String(50), nullable=True),
        sa.Column("employee_apellido", sa.String(300), nullable=False),
        sa.Column("employee_nombre", sa.String(300), nullable=True),
        sa.Column("legislator_name", sa.String(300), nullable=False),
        sa.Column("cargo", sa.String(200), nullable=True),
        sa.Column("boletin_date", sa.Date, nullable=False),
        sa.Column("boletin_numero", sa.String(100), nullable=True),
        sa.Column("resolution_id", sa.String(100), nullable=True),
        sa.Column("scraped_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_boletin_staff_legislator", "boletin_staff_assignments", ["legislator_name"])
    op.create_index("ix_boletin_staff_legajo", "boletin_staff_assignments", ["legajo"])
    op.create_index("ix_boletin_staff_date", "boletin_staff_assignments", ["boletin_date"])
    op.create_index(
        "uq_boletin_staff_employee_legislator_date",
        "boletin_staff_assignments",
        ["employee_apellido", "legislator_name", "boletin_date"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("boletin_staff_assignments")
