"""Audit log for in-place parser repair operations.

Revision ID: 0048
Revises: 0047
Create Date: 2026-05-08

Records every column-rename / row-delete the repair scripts apply to an
already-landed table. The point is reversibility: if a repair turns out to
have been wrong, the audit row has enough info to write the inverse DDL.

Designed for low-volume (≤1000 rows during the rollout) so we don't bother
with partitioning or compaction. Once specs/021-parser-hardening Phase 8
closes, the table can be archived to S3 if desired.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text as text_op

revision = "0048"
down_revision = "0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        text_op(
            """
            CREATE TABLE IF NOT EXISTS parse_repair_audit (
                id                  BIGSERIAL PRIMARY KEY,
                run_id              UUID NOT NULL,
                phase               TEXT NOT NULL,
                table_schema        TEXT NOT NULL,
                table_name          TEXT NOT NULL,
                operation           TEXT NOT NULL,
                old_columns         JSONB,
                new_columns         JSONB,
                rows_deleted        INTEGER DEFAULT 0,
                ok                  BOOLEAN NOT NULL,
                error_message       TEXT,
                dry_run             BOOLEAN NOT NULL DEFAULT FALSE,
                applied_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
    bind.execute(
        text_op(
            "CREATE INDEX IF NOT EXISTS ix_parse_repair_audit_run "
            "ON parse_repair_audit (run_id, applied_at)"
        )
    )
    bind.execute(
        text_op(
            "CREATE INDEX IF NOT EXISTS ix_parse_repair_audit_table "
            "ON parse_repair_audit (table_schema, table_name)"
        )
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS parse_repair_audit")
