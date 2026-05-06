"""Phase 1 of MASTERPLAN — raw schema + version registry.

Revision ID: 0039
Revises: 0038
Create Date: 2026-05-04

Creates the `raw` schema (medallion layer 1) and the `raw_table_versions`
registry. The collector starts writing materialized tables there as
`raw.<portal_slug>__<dataset_slug>__v<N>`, keyed by `resource_identity`
from `catalog_resources`.

This migration is idempotent and pure additive — it does NOT touch existing
`public.cache_*` tables, `cached_datasets` rows, or `catalog_resources` rows.
The cutover from `cache_*` → `raw.*` is a separate operator action documented
in the MASTERPLAN Phase 1 runbook.
"""

from __future__ import annotations

from alembic import op

revision = "0039"
down_revision = "0038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS raw")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_table_versions (
            resource_identity TEXT NOT NULL,
            version           INTEGER NOT NULL,
            schema_name       TEXT NOT NULL DEFAULT 'raw',
            table_name        TEXT NOT NULL,
            row_count         BIGINT,
            size_bytes        BIGINT,
            source_url        TEXT,
            source_file_hash  TEXT,
            parser_version    TEXT,
            collector_version TEXT,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            superseded_at     TIMESTAMPTZ,
            PRIMARY KEY (resource_identity, version)
        );
        CREATE INDEX IF NOT EXISTS ix_raw_table_versions_resource
            ON raw_table_versions (resource_identity, version DESC);
        CREATE INDEX IF NOT EXISTS ix_raw_table_versions_created_at
            ON raw_table_versions (created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_table_versions_table_name
            ON raw_table_versions (schema_name, table_name);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS raw_table_versions")
    # NOTE: we do NOT DROP SCHEMA raw CASCADE here — that would silently
    # destroy every materialized raw table the operator might still need.
    # If a clean rollback is required, the operator drops the schema
    # explicitly after auditing what's inside.
