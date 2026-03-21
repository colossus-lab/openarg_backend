"""Add sesion_chunks table for pgvector search on congressional sessions

Revision ID: 0004
Revises: 0003
Create Date: 2026-02-28

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sesion_chunks",
        sa.Column(
            "id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True
        ),
        sa.Column("periodo", sa.Integer, nullable=False),
        sa.Column("reunion", sa.Integer, nullable=False),
        sa.Column("fecha", sa.String(20), nullable=True),
        sa.Column("tipo_sesion", sa.String(200), nullable=True),
        sa.Column("pdf_url", sa.Text, nullable=True),
        sa.Column("total_pages", sa.Integer, nullable=True),
        sa.Column("speaker", sa.String(500), nullable=True),
        sa.Column("chunk_index", sa.Integer, nullable=True),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    # pgvector embedding column (OpenAI text-embedding-3-small, 1536 dims)
    op.execute("ALTER TABLE sesion_chunks ADD COLUMN embedding vector(1536)")
    # HNSW index for fast ANN search
    op.execute(
        "CREATE INDEX ix_sesion_chunks_embedding ON sesion_chunks "
        "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
    )
    # Indexes for filtering
    op.create_index("ix_sesion_chunks_periodo", "sesion_chunks", ["periodo"])
    op.create_index("ix_sesion_chunks_speaker", "sesion_chunks", ["speaker"])


def downgrade() -> None:
    op.drop_table("sesion_chunks")
