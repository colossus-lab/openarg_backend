"""Preserve a table's shape before it is dropped.

Four code paths drop raw/cache tables, and between them they have dropped
58,155 tables. The largest single reason is `schema_mismatch_recreate`
(19,293 drops): a re-ingest arrives with incompatible columns, `to_sql`
fails, and `_to_sql_safe` responds with DROP + CREATE in place.

That means the evidence of a format change is destroyed by the act of
handling the format change. Measured on production 2026-08-21: of 644
consecutive `(v1, v2)` version pairs in `raw_table_versions`, 642 have no
physical `v1` left to compare against — so the system cannot answer "did
this resource change shape, and how" for any of them.

This table is the memory that was missing. Before any audited drop, we
record what the table looked like: its columns, their types and order, a
hash of the column set, and — when PostgreSQL has already computed them —
per-column statistics from `pg_stats` (null fraction, distinct estimate,
most common values, histogram bounds).

Deliberately NOT a foreign key to `raw_table_versions`: several of the
dropping paths delete that row in the same transaction, so a reference
would either block the drop or cascade the snapshot away with it. The
identity is kept as plain text so the record outlives everything it
describes.

Cost: `pg_stats` is a catalog read on an index scan, not a table scan, so
capturing a snapshot does not touch the data being dropped. Roughly 4 KB
per table, against the terabytes that keeping the tables themselves would
cost.

Revision ID: 0056
Revises: 0055
Create Date: 2026-08-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "raw_schema_snapshots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("schema_name", sa.String(length=63), nullable=False),
        sa.Column("table_name", sa.String(length=63), nullable=False),
        # Best-effort: resolved from `raw_table_versions` when the row is
        # still there. Null when the drop is of a legacy `cache_*` table
        # that was never registered — which is most of production today.
        sa.Column("resource_identity", sa.String(length=512), nullable=True),
        sa.Column("version", sa.Integer(), nullable=True),
        # Why the table was about to disappear. Mirrors
        # `cache_drop_audit.reason` so the two can be joined.
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("actor", sa.String(length=128), nullable=False),
        sa.Column("column_count", sa.Integer(), nullable=False, server_default="0"),
        # Planner estimate. -1 (never analysed) is normalised to NULL by the
        # writer so "unknown" is never mistaken for "empty".
        sa.Column("row_count_estimate", sa.BigInteger(), nullable=True),
        # sha1 of the sorted column names, same construction as
        # `collector_tasks._schema_suffix`. Two snapshots sharing it have the
        # same shape; two that differ are the drift signal itself.
        sa.Column("schema_hash", sa.String(length=40), nullable=False),
        # [{name, ordinal, pg_type, null_frac, n_distinct,
        #   most_common_vals[], histogram_sample[]}]
        sa.Column(
            "columns_profile",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        # False when PostgreSQL had not analysed the table yet, so the shape
        # is recorded but the value profile is empty. Lets a consumer tell
        # "no statistics" from "all columns are null".
        sa.Column("stats_available", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("extra", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        schema="raw",
    )
    # The lookup that matters: "what did this table look like the last few
    # times it was dropped".
    op.create_index(
        "ix_raw_schema_snapshots_table",
        "raw_schema_snapshots",
        ["schema_name", "table_name", sa.text("captured_at DESC")],
        schema="raw",
    )
    # The lookup that answers the drift question across renames.
    op.create_index(
        "ix_raw_schema_snapshots_identity",
        "raw_schema_snapshots",
        ["resource_identity", sa.text("captured_at DESC")],
        schema="raw",
        postgresql_where=sa.text("resource_identity IS NOT NULL"),
    )
    # "Show me every shape change" without reading the JSON.
    op.create_index(
        "ix_raw_schema_snapshots_hash",
        "raw_schema_snapshots",
        ["schema_hash"],
        schema="raw",
    )


def downgrade() -> None:
    op.drop_index("ix_raw_schema_snapshots_hash", table_name="raw_schema_snapshots", schema="raw")
    op.drop_index(
        "ix_raw_schema_snapshots_identity", table_name="raw_schema_snapshots", schema="raw"
    )
    op.drop_index("ix_raw_schema_snapshots_table", table_name="raw_schema_snapshots", schema="raw")
    op.drop_table("raw_schema_snapshots", schema="raw")
