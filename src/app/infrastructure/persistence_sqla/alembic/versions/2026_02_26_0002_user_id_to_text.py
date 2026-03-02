"""Change user_id from uuid to text for flexible user identification.

Revision ID: 0002
Revises: 0001
Create Date: 2026-02-26
"""
import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "user_queries",
        "user_id",
        existing_type=sa.UUID(),
        type_=sa.Text(),
        existing_nullable=True,
        postgresql_using="user_id::text",
    )


def downgrade() -> None:
    op.alter_column(
        "user_queries",
        "user_id",
        existing_type=sa.Text(),
        type_=sa.UUID(),
        existing_nullable=True,
        postgresql_using="user_id::uuid",
    )
