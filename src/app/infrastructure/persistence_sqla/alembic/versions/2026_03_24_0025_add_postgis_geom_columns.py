"""Enable PostGIS and add native geometry columns to geo-enabled cache tables.

For every cache_* table with a ``_geometry_geojson`` TEXT column:
  1. Enables the PostGIS extension (idempotent).
  2. Adds a ``geom geometry(Geometry, 4326)`` column.
  3. Populates it via ``ST_GeomFromGeoJSON(_geometry_geojson)``.
  4. Creates a GIST spatial index on ``geom``.

The original ``_geometry_geojson`` TEXT column is preserved for backward
compatibility (the analyst pipeline reads it to build GeoJSON responses).

Revision ID: 0025
Revises: 0024
Create Date: 2026-03-24
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0025"
down_revision: str | None = "0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Enable PostGIS extension (idempotent, available on RDS PG16)
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    # 2. Find all cache_* tables with _geometry_geojson column
    conn = op.get_bind()
    rows = conn.execute(
        text(
            "SELECT table_name FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND column_name = '_geometry_geojson' "
            "AND table_name LIKE 'cache\\_%' "
            "ORDER BY table_name"
        )
    ).fetchall()

    for (table_name,) in rows:
        tbl = f'"{table_name}"'

        # 3. Add geom column (skip if already exists)
        op.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = '{table_name}'
                      AND column_name = 'geom'
                ) THEN
                    EXECUTE 'ALTER TABLE {tbl} ADD COLUMN geom geometry(Geometry, 4326)';
                END IF;
            END $$
        """)

        # 4. Populate geom from GeoJSON text (skip invalid/empty rows)
        op.execute(f"""
            UPDATE {tbl}
            SET geom = ST_SetSRID(ST_GeomFromGeoJSON(_geometry_geojson), 4326)
            WHERE _geometry_geojson IS NOT NULL
              AND _geometry_geojson != ''
              AND _geometry_geojson ~ '^\\s*\\{{'
              AND geom IS NULL
        """)

        # 5. Create GIST spatial index
        idx_name = f"idx_{table_name}_geom"[:63]
        op.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {tbl} USING gist (geom)')


def downgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        text(
            "SELECT table_name FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND column_name = 'geom' "
            "AND table_name LIKE 'cache\\_%' "
            "ORDER BY table_name"
        )
    ).fetchall()

    for (table_name,) in rows:
        tbl = f'"{table_name}"'
        idx_name = f"idx_{table_name}_geom"[:63]
        op.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
        op.execute(f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS geom")

    # Keep postgis extension — other things may depend on it
