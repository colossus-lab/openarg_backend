"""Create OpenArg base tables

Revision ID: 0001
Revises:
Create Date: 2026-02-26

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # --- datasets ---
    op.create_table(
        "datasets",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("source_id", sa.String(500), nullable=False),
        sa.Column("title", sa.String(1000), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("organization", sa.String(500), nullable=True),
        sa.Column("portal", sa.String(100), nullable=False, index=True),
        sa.Column("url", sa.Text, nullable=True),
        sa.Column("download_url", sa.Text, nullable=True),
        sa.Column("format", sa.String(50), nullable=True),
        sa.Column("columns", sa.Text, nullable=True),
        sa.Column("sample_rows", sa.Text, nullable=True),
        sa.Column("tags", sa.Text, nullable=True),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_cached", sa.Boolean, server_default="false"),
        sa.Column("row_count", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_datasets_source_portal", "datasets", ["source_id", "portal"], unique=True)

    # --- dataset_chunks (with pgvector embedding) ---
    op.create_table(
        "dataset_chunks",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("dataset_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    # Add pgvector column
    op.execute("ALTER TABLE dataset_chunks ADD COLUMN embedding vector(1536)")
    # HNSW index for fast approximate nearest neighbor search
    op.execute(
        "CREATE INDEX ix_dataset_chunks_embedding ON dataset_chunks "
        "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
    )

    # --- cached_datasets ---
    op.create_table(
        "cached_datasets",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("dataset_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("table_name", sa.String(255), nullable=False, unique=True),
        sa.Column("row_count", sa.Integer, server_default="0"),
        sa.Column("columns_json", sa.Text, nullable=True),
        sa.Column("size_bytes", sa.Integer, server_default="0"),
        sa.Column("status", sa.String(50), server_default="pending"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # --- user_queries ---
    op.create_table(
        "user_queries",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), nullable=True, index=True),
        sa.Column("question", sa.Text, nullable=False),
        sa.Column("status", sa.String(50), server_default="pending", index=True),
        sa.Column("plan_json", sa.Text, nullable=True),
        sa.Column("datasets_used", sa.Text, nullable=True),
        sa.Column("analysis_result", sa.Text, nullable=True),
        sa.Column("sources_json", sa.Text, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("tokens_used", sa.Integer, server_default="0"),
        sa.Column("duration_ms", sa.Integer, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # --- query_dataset_links ---
    op.create_table(
        "query_dataset_links",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("query_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("dataset_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("relevance_score", sa.Float, server_default="0.0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # --- agent_tasks ---
    op.create_table(
        "agent_tasks",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("query_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("agent_type", sa.String(50), nullable=False, index=True),
        sa.Column("status", sa.String(50), server_default="pending"),
        sa.Column("input_json", sa.Text, nullable=True),
        sa.Column("output_json", sa.Text, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("tokens_used", sa.Integer, server_default="0"),
        sa.Column("duration_ms", sa.Integer, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("agent_tasks")
    op.drop_table("query_dataset_links")
    op.drop_table("user_queries")
    op.drop_table("cached_datasets")
    op.drop_table("dataset_chunks")
    op.drop_table("datasets")
    op.execute("DROP EXTENSION IF EXISTS vector")
