"""Resize embedding columns from vector(1536) to vector(768) for Gemini migration

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-01

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- dataset_chunks: clear embeddings and resize ---
    op.execute("UPDATE dataset_chunks SET embedding = NULL")
    op.execute("ALTER TABLE dataset_chunks ALTER COLUMN embedding TYPE vector(768)")

    # --- sesion_chunks: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_sesion_chunks_embedding")
    op.execute("TRUNCATE sesion_chunks")
    op.execute("ALTER TABLE sesion_chunks ALTER COLUMN embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_sesion_chunks_embedding
        ON sesion_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- query_cache: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_query_cache_embedding")
    op.execute("TRUNCATE query_cache")
    op.execute("ALTER TABLE query_cache ALTER COLUMN embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_query_cache_embedding
        ON query_cache
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)


def downgrade() -> None:
    # --- query_cache: revert to 1536 ---
    op.execute("DROP INDEX IF EXISTS idx_query_cache_embedding")
    op.execute("TRUNCATE query_cache")
    op.execute("ALTER TABLE query_cache ALTER COLUMN embedding TYPE vector(1536)")
    op.execute("""
        CREATE INDEX idx_query_cache_embedding
        ON query_cache
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- sesion_chunks: revert to 1536 ---
    op.execute("DROP INDEX IF EXISTS idx_sesion_chunks_embedding")
    op.execute("TRUNCATE sesion_chunks")
    op.execute("ALTER TABLE sesion_chunks ALTER COLUMN embedding TYPE vector(1536)")
    op.execute("""
        CREATE INDEX idx_sesion_chunks_embedding
        ON sesion_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- dataset_chunks: revert to 1536 ---
    op.execute("UPDATE dataset_chunks SET embedding = NULL")
    op.execute("ALTER TABLE dataset_chunks ALTER COLUMN embedding TYPE vector(1536)")
