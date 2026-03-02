"""Add s3_key column to cached_datasets for raw file storage references

Revision ID: 0009
Revises: 0008
Create Date: 2026-03-01

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("cached_datasets", sa.Column("s3_key", sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column("cached_datasets", "s3_key")
