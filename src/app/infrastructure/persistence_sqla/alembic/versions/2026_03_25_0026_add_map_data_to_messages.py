"""Add map_data JSONB column to messages table.

Stores GeoJSON FeatureCollection for map visualization persistence
across conversation reloads.

Revision ID: 0026
Revises: 0025
Create Date: 2026-03-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0026"
down_revision: str | None = "0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS map_data JSONB")


def downgrade() -> None:
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS map_data")
