"""Add senado_staff table for senator profile staff scraping

Revision ID: 0015
Revises: 0014
Create Date: 2026-03-02

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "senado_staff",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("senator_id", sa.String(10), nullable=False),
        sa.Column("senator_name", sa.String(300), nullable=False),
        sa.Column("bloque", sa.String(300), nullable=True),
        sa.Column("provincia", sa.String(200), nullable=True),
        sa.Column("employee_name", sa.String(500), nullable=False),
        sa.Column("categoria", sa.String(20), nullable=True),
        sa.Column("scraped_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_senado_staff_senator_name", "senado_staff", ["senator_name"])
    op.create_index("ix_senado_staff_senator_id", "senado_staff", ["senator_id"])
    op.create_index("ix_senado_staff_employee", "senado_staff", ["employee_name"])
    op.create_index(
        "uq_senado_staff_senator_employee",
        "senado_staff",
        ["senator_id", "employee_name"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_senado_staff_senator_employee")
    op.drop_index("ix_senado_staff_employee")
    op.drop_index("ix_senado_staff_senator_id")
    op.drop_index("ix_senado_staff_senator_name")
    op.drop_table("senado_staff")
