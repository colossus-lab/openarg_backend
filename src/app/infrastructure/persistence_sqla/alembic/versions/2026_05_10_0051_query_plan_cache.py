"""Add query_plan_cache for ExecutionPlan reuse by embedding similarity.

Revision ID: 0051
Revises: 0050
Create Date: 2026-05-10

Caches the LangGraph `ExecutionPlan` produced by the planner LLM, keyed
by the user query embedding. On a query whose embedding cosine similarity
to a cached plan is at or above `OPENARG_PLAN_CACHE_HIT_THRESHOLD`
(default 0.95), the planner_node skips the planner LLM call (~3-4s) and
reuses the cached plan. SQL execution and analyst answer generation run
fresh — only the strategy is cached, not the data or the response.

Why a separate table from `query_cache`:
  - Different TTL: plans rarely go stale (7 days default), responses do
    (30 min — data changes).
  - Different invalidation: response cache is invalidated by mart
    refresh; plan cache is invalidated only by mart YAML changes
    (table/intent shape change).
  - Avoids coupling plan-strategy storage with response-data storage.
"""

from __future__ import annotations

from alembic import op

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS query_plan_cache (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            question_hash VARCHAR(64) NOT NULL UNIQUE,
            question TEXT NOT NULL,
            embedding vector(1024),
            plan_json JSONB NOT NULL,
            ttl_seconds INTEGER NOT NULL DEFAULT 604800,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
            expires_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_query_plan_cache_embedding
            ON query_plan_cache USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_query_plan_cache_expires ON query_plan_cache (expires_at)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_query_plan_cache_expires")
    op.execute("DROP INDEX IF EXISTS idx_query_plan_cache_embedding")
    op.execute("DROP TABLE IF EXISTS query_plan_cache")
