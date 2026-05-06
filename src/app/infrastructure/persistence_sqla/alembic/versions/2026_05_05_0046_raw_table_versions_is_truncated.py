"""Add is_truncated flag to raw_table_versions.

Revision ID: 0046
Revises: 0045
Create Date: 2026-05-05

The collector applies a hard cap at MAX_TABLE_ROWS (500_000) when loading
CSV/Excel/JSON files. Until now the cap was applied silently — downstream
consumers (mart layer, table_catalog, the LLM) had no way to know whether a
raw table represents the full dataset or a sampled prefix.

This migration adds an `is_truncated` flag to `raw_table_versions` so the
collector can mark capped versions explicitly. Mart SQL and the data API can
then surface a `sampled` warning instead of returning incomplete numbers as
if they were authoritative.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text as text_op

revision = "0046"
down_revision = "0045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Run the ADD COLUMN and the CREATE INDEX in independent connections.
    # The partial index is small but its build still needs ephemeral disk.
    # When the cluster is tight on space we want the column to land first
    # (the collector can start writing the flag immediately) and the index
    # to retry on its own. Both statements are IF NOT EXISTS / idempotent
    # so subsequent `alembic upgrade head` invocations are safe.
    bind = op.get_bind()
    bind.execute(
        text_op(
            "ALTER TABLE raw_table_versions "
            "ADD COLUMN IF NOT EXISTS is_truncated BOOLEAN NOT NULL DEFAULT FALSE"
        )
    )
    try:
        bind.execute(
            text_op(
                "CREATE INDEX IF NOT EXISTS ix_raw_table_versions_is_truncated "
                "ON raw_table_versions (is_truncated) "
                "WHERE is_truncated = TRUE"
            )
        )
    except Exception:
        # Don't roll back the ADD COLUMN if the cluster runs out of disk
        # mid-CREATE-INDEX. The column is what callers depend on; the index
        # is a perf optimization and can be created later with a manual
        # `CREATE INDEX CONCURRENTLY` once space is reclaimed.
        import logging

        logging.getLogger(__name__).warning(
            "0046: ADD COLUMN succeeded but CREATE INDEX failed (likely "
            "DiskFull); rerun the migration after reclaiming space",
            exc_info=True,
        )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_raw_table_versions_is_truncated")
    op.execute("ALTER TABLE raw_table_versions DROP COLUMN IF EXISTS is_truncated")
