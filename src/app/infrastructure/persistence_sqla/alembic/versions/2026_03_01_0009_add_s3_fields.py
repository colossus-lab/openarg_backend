"""Add s3_key column to cached_datasets for raw file storage references

Revision ID: 0009
Revises: 0008
Create Date: 2026-03-01

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("cached_datasets", sa.Column("s3_key", sa.String(500), nullable=True))


def downgrade() -> None:
    op.drop_column("cached_datasets", "s3_key")
