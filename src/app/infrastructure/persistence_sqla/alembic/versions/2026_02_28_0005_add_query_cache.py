"""Add query_cache table for semantic caching

Revision ID: 0005
Revises: 0004
Create Date: 2026-02-28

"""
from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.execute("""
        CREATE TABLE query_cache (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            question_hash VARCHAR(64) NOT NULL UNIQUE,
            question TEXT NOT NULL,
            embedding vector(1536),
            response JSONB NOT NULL,
            ttl_seconds INTEGER NOT NULL DEFAULT 1800,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL
        )
    """)

    op.execute("""
        CREATE INDEX idx_query_cache_embedding
        ON query_cache
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    op.execute("CREATE INDEX idx_query_cache_expires ON query_cache (expires_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS query_cache")
