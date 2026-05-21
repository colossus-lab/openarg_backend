"""Add ingestion_findings table for WS0 ingestion validation.

Revision ID: 0033
Revises: 0032
Create Date: 2026-04-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_findings",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        # During WS0/WS2 transition this references cached_datasets.id (or
        # cached_datasets.dataset_id encoded as text). Once catalog_resources
        # is in place (WS2) the resource_id will point there. Keeping it as
        # plain text avoids coupling migrations across workstreams.
        sa.Column("resource_id", sa.String(length=255), nullable=False),
        sa.Column("detector_name", sa.String(length=100), nullable=False),
        sa.Column("detector_version", sa.String(length=20), nullable=False),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("mode", sa.String(length=30), nullable=False),
        sa.Column(
            "payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="{}"
        ),
        sa.Column("should_redownload", sa.Boolean, nullable=False, server_default=sa.text("false")),
        sa.Column("message", sa.Text, nullable=True),
        sa.Column("input_hash", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("found_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "severity IN ('info','warn','critical')", name="ck_ingestion_findings_severity"
        ),
        sa.CheckConstraint(
            "mode IN ('pre_parse','post_parse','retrospective','state_invariant')",
            name="ck_ingestion_findings_mode",
        ),
    )
    op.create_index(
        "ix_ingestion_findings_resource_open",
        "ingestion_findings",
        ["resource_id", "resolved_at"],
    )
    op.create_index(
        "ix_ingestion_findings_detector_severity",
        "ingestion_findings",
        ["detector_name", "severity"],
    )
    op.create_index(
        "ix_ingestion_findings_found_at",
        "ingestion_findings",
        ["found_at"],
    )
    # Idempotency: a (resource, detector, version, mode, input_hash) tuple is unique;
    # repeated runs upsert and bump found_at.
    op.create_unique_constraint(
        "uq_ingestion_findings_dedup",
        "ingestion_findings",
        ["resource_id", "detector_name", "detector_version", "mode", "input_hash"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_ingestion_findings_dedup", "ingestion_findings", type_="unique"
    )
    op.drop_index("ix_ingestion_findings_found_at", table_name="ingestion_findings")
    op.drop_index(
        "ix_ingestion_findings_detector_severity", table_name="ingestion_findings"
    )
    op.drop_index(
        "ix_ingestion_findings_resource_open", table_name="ingestion_findings"
    )
    op.drop_table("ingestion_findings")
