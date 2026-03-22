"""Add privacy_accepted_at column to users table.

Revision ID: 0020
Revises: 0019
Create Date: 2026-03-21
"""

import sqlalchemy as sa
from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users", sa.Column("privacy_accepted_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("users", "privacy_accepted_at")
