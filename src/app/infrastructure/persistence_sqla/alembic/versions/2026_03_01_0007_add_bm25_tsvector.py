"""Add tsvector column and GIN index for BM25 full-text search on dataset_chunks

Revision ID: 0007
Revises: 0006
Create Date: 2026-03-01

"""

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Add tsvector column for BM25 full-text search
    op.execute("ALTER TABLE dataset_chunks ADD COLUMN IF NOT EXISTS tsv tsvector")

    # Populate tsvector from existing content
    op.execute("UPDATE dataset_chunks SET tsv = to_tsvector('spanish', COALESCE(content, ''))")

    # Create trigger function to keep tsv in sync
    op.execute("""
        CREATE OR REPLACE FUNCTION dataset_chunks_tsv_trigger() RETURNS trigger AS $$
        BEGIN
            NEW.tsv := to_tsvector('spanish', COALESCE(NEW.content, ''));
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql
    """)

    # Create trigger
    op.execute("""
        DROP TRIGGER IF EXISTS trg_dataset_chunks_tsv ON dataset_chunks;
        CREATE TRIGGER trg_dataset_chunks_tsv
        BEFORE INSERT OR UPDATE OF content ON dataset_chunks
        FOR EACH ROW EXECUTE FUNCTION dataset_chunks_tsv_trigger()
    """)

    # GIN index for full-text search
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_dataset_chunks_tsv ON dataset_chunks USING GIN (tsv)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_dataset_chunks_tsv")
    op.execute("DROP TRIGGER IF EXISTS trg_dataset_chunks_tsv ON dataset_chunks")
    op.execute("DROP FUNCTION IF EXISTS dataset_chunks_tsv_trigger()")
    op.execute("ALTER TABLE dataset_chunks DROP COLUMN IF EXISTS tsv")
