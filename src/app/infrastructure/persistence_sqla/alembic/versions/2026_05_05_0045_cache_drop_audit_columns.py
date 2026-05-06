"""Add `reason`, `actor`, `extra` columns to `cache_drop_audit`.

Revision ID: 0045
Revises: 0044
Create Date: 2026-05-05

The `_record_cache_drop` helper in `collector_tasks.py` writes
`(object_name, reason, actor, extra)` on every DROP TABLE of a `cache_*`
table for operational forensics. The columns existed in the original
plan but never landed in any migration. Result: every drop logs a
`UndefinedColumn: "reason"` traceback (caught by an except, but noisy).
This migration introduces the three columns the code expects.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0045"
down_revision = "0044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cache_drop_audit",
        sa.Column("reason", sa.Text(), nullable=True),
    )
    op.add_column(
        "cache_drop_audit",
        sa.Column(
            "actor",
            sa.Text(),
            nullable=False,
            server_default="collector",
        ),
    )
    op.add_column(
        "cache_drop_audit",
        sa.Column("extra", JSONB(), nullable=True),
    )
    op.create_index(
        "ix_cache_drop_audit_reason",
        "cache_drop_audit",
        ["reason"],
    )


def downgrade() -> None:
    op.drop_index("ix_cache_drop_audit_reason", table_name="cache_drop_audit")
    op.drop_column("cache_drop_audit", "extra")
    op.drop_column("cache_drop_audit", "actor")
    op.drop_column("cache_drop_audit", "reason")
