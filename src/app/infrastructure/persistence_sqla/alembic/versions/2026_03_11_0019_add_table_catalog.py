"""Add table_catalog for semantic metadata enrichment

Stores semantic metadata (display name, description, domain, tags,
embedding) for cached dataset tables.  Used by NL2SQL to provide
richer context and by vector search to discover relevant tables.

Revision ID: 0019
Revises: 0018
Create Date: 2026-03-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0019"
down_revision: str | None = "0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "table_catalog",
        sa.Column("id", sa.UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("table_name", sa.Text, unique=True, nullable=False),
        sa.Column("display_name", sa.Text),
        sa.Column("description", sa.Text),
        sa.Column("domain", sa.Text),
        sa.Column("subdomain", sa.Text),
        sa.Column("geographic_scope", sa.Text),
        sa.Column("temporal_scope", sa.Text),
        sa.Column("key_columns", sa.dialects.postgresql.JSONB),
        sa.Column("column_types", sa.dialects.postgresql.JSONB),
        sa.Column("sample_queries", sa.dialects.postgresql.JSONB),
        sa.Column("tags", sa.dialects.postgresql.JSONB),
        sa.Column("row_count", sa.Integer),
        sa.Column("quality_score", sa.Float),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # Add the vector column via raw SQL (pgvector 768 dims)
    op.execute("ALTER TABLE table_catalog ADD COLUMN catalog_embedding vector(768)")

    # HNSW index on catalog_embedding for fast vector search
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_table_catalog_embedding_hnsw "
        "ON table_catalog USING hnsw (catalog_embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )

    # GIN index on tags for fast tag-based filtering
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_table_catalog_tags_gin "
        "ON table_catalog USING gin (tags)"
    )

    # B-tree index on (domain, subdomain) for category lookups
    op.create_index(
        "idx_table_catalog_domain_subdomain",
        "table_catalog",
        ["domain", "subdomain"],
    )


def downgrade() -> None:
    op.drop_index("idx_table_catalog_domain_subdomain", table_name="table_catalog")
    op.execute("DROP INDEX IF EXISTS idx_table_catalog_tags_gin")
    op.execute("DROP INDEX IF EXISTS idx_table_catalog_embedding_hnsw")
    op.drop_table("table_catalog")
