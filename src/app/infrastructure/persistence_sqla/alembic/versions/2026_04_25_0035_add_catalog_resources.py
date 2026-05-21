"""WS2 — add `catalog_resources` (logical catalog).

Revision ID: 0035
Revises: 0034
Create Date: 2026-04-25

`catalog_resources` is the new authoritative logical catalog. Decoupled from
physical table existence — a row can describe a not-yet-materialized resource,
a connector endpoint with no table at all, a non-tabular bundle, etc.

`table_catalog` keeps existing for the transition (semantic index of
materialized tables). The new table includes a vector(1024) column wired the
same way as `table_catalog` (HNSW + cosine).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0035"
down_revision = "0034"
branch_labels = None
depends_on = None


_MATERIALIZATION_STATUSES = (
    "pending",
    "ready",
    "live_api",
    "non_tabular",
    "materialization_corrupted",
    "failed",
)
_RESOURCE_KINDS = (
    "file",
    "sheet",
    "cuadro",
    "zip_member",
    "document_bundle",
    "connector_endpoint",
)


def upgrade() -> None:
    op.create_table(
        "catalog_resources",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("resource_identity", sa.String(length=255), nullable=False, unique=True),
        sa.Column("dataset_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "parent_resource_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("catalog_resources.id", ondelete="CASCADE"),
            nullable=True,
        ),
        # Provenance
        sa.Column("portal", sa.String(length=100), nullable=False, server_default=""),
        sa.Column("source_id", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("s3_key", sa.String(length=500), nullable=True),
        sa.Column("filename", sa.String(length=500), nullable=True),
        sa.Column("sub_path", sa.String(length=500), nullable=True),
        sa.Column("sheet_name", sa.String(length=255), nullable=True),
        sa.Column("cuadro_numero", sa.String(length=50), nullable=True),
        sa.Column("provincia", sa.String(length=100), nullable=True),
        # Naming
        sa.Column("raw_title", sa.Text, nullable=False, server_default=""),
        sa.Column("canonical_title", sa.Text, nullable=False, server_default=""),
        sa.Column("display_name", sa.Text, nullable=False, server_default=""),
        sa.Column("title_source", sa.String(length=30), nullable=False, server_default="fallback"),
        sa.Column(
            "title_confidence", sa.Float, nullable=False, server_default=sa.text("0.0")
        ),
        # Taxonomy
        sa.Column("domain", sa.String(length=100), nullable=True),
        sa.Column("subdomain", sa.String(length=100), nullable=True),
        sa.Column("taxonomy_key", sa.String(length=255), nullable=True),
        # Materialization
        sa.Column(
            "resource_kind",
            sa.String(length=30),
            nullable=False,
            server_default="file",
        ),
        sa.Column(
            "materialization_status",
            sa.String(length=40),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("materialized_table_name", sa.String(length=255), nullable=True),
        sa.Column("parser_version", sa.String(length=20), nullable=True),
        sa.Column("normalization_version", sa.String(length=20), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "materialization_status IN ("
            + ",".join(f"'{s}'" for s in _MATERIALIZATION_STATUSES)
            + ")",
            name="ck_catalog_resources_materialization_status",
        ),
        sa.CheckConstraint(
            "resource_kind IN ("
            + ",".join(f"'{k}'" for k in _RESOURCE_KINDS)
            + ")",
            name="ck_catalog_resources_resource_kind",
        ),
        sa.CheckConstraint(
            "title_confidence BETWEEN 0.0 AND 1.0",
            name="ck_catalog_resources_title_confidence",
        ),
    )
    op.create_index(
        "ix_catalog_resources_portal_source",
        "catalog_resources",
        ["portal", "source_id"],
    )
    op.create_index(
        "ix_catalog_resources_dataset_id",
        "catalog_resources",
        ["dataset_id"],
    )
    op.create_index(
        "ix_catalog_resources_materialization_status",
        "catalog_resources",
        ["materialization_status"],
    )
    op.create_index(
        "ix_catalog_resources_materialized_table_name",
        "catalog_resources",
        ["materialized_table_name"],
    )
    op.create_index(
        "ix_catalog_resources_parent",
        "catalog_resources",
        ["parent_resource_id"],
    )
    # pgvector embedding (1024 dims, like table_catalog).
    op.execute("ALTER TABLE catalog_resources ADD COLUMN embedding vector(1024)")
    op.execute(
        "CREATE INDEX ix_catalog_resources_embedding "
        "ON catalog_resources USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_catalog_resources_embedding")
    op.drop_index("ix_catalog_resources_parent", table_name="catalog_resources")
    op.drop_index(
        "ix_catalog_resources_materialized_table_name", table_name="catalog_resources"
    )
    op.drop_index(
        "ix_catalog_resources_materialization_status", table_name="catalog_resources"
    )
    op.drop_index("ix_catalog_resources_dataset_id", table_name="catalog_resources")
    op.drop_index("ix_catalog_resources_portal_source", table_name="catalog_resources")
    op.drop_table("catalog_resources")
