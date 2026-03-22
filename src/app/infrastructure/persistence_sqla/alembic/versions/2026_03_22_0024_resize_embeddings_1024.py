"""Resize embedding columns from vector(768) to vector(1024) for Bedrock Cohere migration.

All embedding data is truncated — will be re-embedded with the new model.
HNSW indexes are dropped and recreated with new dimensions.

Revision ID: 0024
Revises: 0023
Create Date: 2026-03-22
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0024"
down_revision: str | None = "0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- dataset_chunks: clear embeddings and resize ---
    op.execute("UPDATE dataset_chunks SET embedding = NULL")
    op.execute("ALTER TABLE dataset_chunks ALTER COLUMN embedding TYPE vector(1024)")

    # --- sesion_chunks: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_sesion_chunks_embedding")
    op.execute("TRUNCATE sesion_chunks")
    op.execute("ALTER TABLE sesion_chunks ALTER COLUMN embedding TYPE vector(1024)")
    op.execute("""
        CREATE INDEX idx_sesion_chunks_embedding
        ON sesion_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- query_cache: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_query_cache_embedding")
    op.execute("TRUNCATE query_cache")
    op.execute("ALTER TABLE query_cache ALTER COLUMN embedding TYPE vector(1024)")
    op.execute("""
        CREATE INDEX idx_query_cache_embedding
        ON query_cache
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- table_catalog: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_table_catalog_embedding_hnsw")
    op.execute("TRUNCATE table_catalog")
    op.execute("ALTER TABLE table_catalog ALTER COLUMN catalog_embedding TYPE vector(1024)")
    op.execute("""
        CREATE INDEX idx_table_catalog_embedding_hnsw
        ON table_catalog
        USING hnsw (catalog_embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- successful_queries: drop index, truncate, resize, recreate index ---
    op.execute("DROP INDEX IF EXISTS idx_successful_queries_embedding")
    op.execute("TRUNCATE successful_queries")
    op.execute("ALTER TABLE successful_queries ALTER COLUMN embedding TYPE vector(1024)")
    op.execute("""
        CREATE INDEX idx_successful_queries_embedding
        ON successful_queries
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)


def downgrade() -> None:
    # --- successful_queries: revert to 768 ---
    op.execute("DROP INDEX IF EXISTS idx_successful_queries_embedding")
    op.execute("TRUNCATE successful_queries")
    op.execute("ALTER TABLE successful_queries ALTER COLUMN embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_successful_queries_embedding
        ON successful_queries
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- table_catalog: revert to 768 ---
    op.execute("DROP INDEX IF EXISTS idx_table_catalog_embedding_hnsw")
    op.execute("TRUNCATE table_catalog")
    op.execute("ALTER TABLE table_catalog ALTER COLUMN catalog_embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_table_catalog_embedding_hnsw
        ON table_catalog
        USING hnsw (catalog_embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- query_cache: revert to 768 ---
    op.execute("DROP INDEX IF EXISTS idx_query_cache_embedding")
    op.execute("TRUNCATE query_cache")
    op.execute("ALTER TABLE query_cache ALTER COLUMN embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_query_cache_embedding
        ON query_cache
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- sesion_chunks: revert to 768 ---
    op.execute("DROP INDEX IF EXISTS idx_sesion_chunks_embedding")
    op.execute("TRUNCATE sesion_chunks")
    op.execute("ALTER TABLE sesion_chunks ALTER COLUMN embedding TYPE vector(768)")
    op.execute("""
        CREATE INDEX idx_sesion_chunks_embedding
        ON sesion_chunks
        USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64)
    """)

    # --- dataset_chunks: revert to 768 ---
    op.execute("UPDATE dataset_chunks SET embedding = NULL")
    op.execute("ALTER TABLE dataset_chunks ALTER COLUMN embedding TYPE vector(768)")
