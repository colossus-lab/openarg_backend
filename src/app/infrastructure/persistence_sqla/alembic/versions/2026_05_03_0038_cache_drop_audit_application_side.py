"""Application-side cache_drop_audit (RDS cannot CREATE EVENT TRIGGER).

Revision ID: 0038
Revises: 0037
Create Date: 2026-05-03

Mig 0036 attempted to install a pg_event_trigger but AWS RDS does not
expose SUPERUSER, so the guard short-circuited and neither the trigger
nor the audit table ended up created. This migration creates the audit
table cleanly; the collector code now inserts into it before each
DROP TABLE so we still capture the operational drops (admin/manual drops
fall back to RDS CloudTrail).
"""

from __future__ import annotations

from alembic import op

revision = "0038"
down_revision = "0037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cache_drop_audit (
            id BIGSERIAL PRIMARY KEY,
            object_name TEXT NOT NULL,
            reason TEXT,
            actor TEXT,
            extra JSONB,
            dropped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS ix_cache_drop_audit_dropped_at
            ON cache_drop_audit (dropped_at DESC);
        CREATE INDEX IF NOT EXISTS ix_cache_drop_audit_object_name
            ON cache_drop_audit (object_name);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cache_drop_audit")
