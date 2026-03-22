"""Add successful_queries table for NL2SQL few-shot learning.

Stores question/SQL pairs that executed successfully, with embeddings
for similarity search to provide dynamic few-shot examples.

Revision ID: 0022
Revises: 0021
Create Date: 2026-03-21
"""

import sqlalchemy as sa
from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "successful_queries",
        sa.Column("id", sa.Text, primary_key=True, server_default=sa.text("gen_random_uuid()::text")),
        sa.Column("question", sa.Text, nullable=False),
        sa.Column("sql", sa.Text, nullable=False),
        sa.Column("table_name", sa.Text, nullable=False),
        sa.Column("row_count", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    # Embedding column added via raw SQL (pgvector type)
    op.execute("ALTER TABLE successful_queries ADD COLUMN embedding vector(768)")
    op.execute(
        "CREATE INDEX idx_successful_queries_embedding ON successful_queries "
        "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
    )


def downgrade() -> None:
    op.drop_table("successful_queries")
