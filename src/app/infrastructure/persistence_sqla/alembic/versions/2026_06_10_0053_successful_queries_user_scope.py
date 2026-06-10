"""Scope `successful_queries` by user.

Revision ID: 0053
Revises: 0052
Create Date: 2026-06-10

Round v46 H4: pre-fix, `save_successful_query` persisted every successful
NL2SQL pair (question + SQL) without any owner, and `get_few_shot_examples`
retrieved them by cosine similarity across the entire table. Two users
asking similar questions saw each other's queries pulled into the
NL2SQL system prompt as few-shot examples — a stored cross-tenant
prompt-poisoning channel: a malicious user crafts a question that
nudges the planner toward a wrong SQL shape, the embedding lands close
to legitimate user queries, and every subsequent caller with a
semantically similar prompt gets the poison spliced into their context.

Fix layout:
  * `user_id` text NOT NULL DEFAULT 'legacy' — the default lets historical
    rows still surface as few-shot examples (we trust pre-v46 data as
    operator-curated). New rows record the caller's email so future
    queries scope to (caller's own rows) + (operator-curated legacy
    rows). The 'legacy' string is not a real email, so a malicious
    user can't make their rows masquerade as curated unless they
    spoof the JWT — and the v46 controller refuses that case.
  * Index on (user_id) so the WHERE-filter is cheap when combined with
    the HNSW cosine scan. We deliberately do NOT create a composite
    btree on (user_id, embedding): pgvector's HNSW index is already
    the planner's first choice and a btree on a vector column is not
    useful. Postgres will combine the HNSW scan with a bitmap filter
    on user_id.

The application-layer fix (`save_successful_query` / `get_few_shot_examples`
take `user_id` + sanitize `question` with `is_suspicious`) ships in the
same commit but is independent of this schema change — it is the gate
that prevents *new* cross-tenant rows from entering the table.
"""

from __future__ import annotations

from alembic import op

revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE successful_queries
        ADD COLUMN IF NOT EXISTS user_id text NOT NULL DEFAULT 'legacy'
        """
    )
    # Btree index for the WHERE filter — cheap, helps the planner
    # combine the HNSW vector scan with an owner filter.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_successful_queries_user_id
        ON successful_queries (user_id)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_successful_queries_user_id")
    op.execute("ALTER TABLE successful_queries DROP COLUMN IF EXISTS user_id")
