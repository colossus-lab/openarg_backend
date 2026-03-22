"""Add save_history column to users table.

Revision ID: 0021
Revises: 0020
Create Date: 2026-03-21
"""
import sqlalchemy as sa
from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("save_history", sa.Boolean, nullable=False, server_default=sa.text("true")))


def downgrade() -> None:
    op.drop_column("users", "save_history")
