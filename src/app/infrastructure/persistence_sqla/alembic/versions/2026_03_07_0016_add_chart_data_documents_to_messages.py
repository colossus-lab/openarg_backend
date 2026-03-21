"""Add chart_data and documents columns to messages table

Revision ID: 0016
Revises: 0015
Create Date: 2026-03-07
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("messages", sa.Column("chart_data", JSONB, nullable=True))
    op.add_column("messages", sa.Column("documents", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("messages", "documents")
    op.drop_column("messages", "chart_data")
