"""Add camara column to boletin_staff_assignments for Senado support

Revision ID: 0013
Revises: 0012
Create Date: 2026-03-02

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "boletin_staff_assignments",
        sa.Column("camara", sa.String(20), nullable=False, server_default="diputados"),
    )
    op.create_check_constraint(
        "ck_boletin_staff_camara",
        "boletin_staff_assignments",
        "camara IN ('diputados', 'senado')",
    )
    op.create_index(
        "ix_boletin_staff_camara",
        "boletin_staff_assignments",
        ["camara"],
    )
    # Drop old unique constraint and create new one including camara
    op.drop_index("uq_boletin_staff_employee_legislator_date")
    op.create_index(
        "uq_boletin_staff_employee_legislator_date_camara",
        "boletin_staff_assignments",
        ["employee_apellido", "legislator_name", "boletin_date", "camara"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_boletin_staff_employee_legislator_date_camara")
    op.create_index(
        "uq_boletin_staff_employee_legislator_date",
        "boletin_staff_assignments",
        ["employee_apellido", "legislator_name", "boletin_date"],
        unique=True,
    )
    op.drop_index("ix_boletin_staff_camara")
    op.drop_constraint("ck_boletin_staff_camara", "boletin_staff_assignments")
    op.drop_column("boletin_staff_assignments", "camara")
