"""Add missing indexes and foreign keys for referential integrity

Adds explicit named indexes on frequently-queried columns and foreign key
constraints with ON DELETE CASCADE on all UUID reference columns that were
created without FKs in migration 0001.

Revision ID: 0018
Revises: 0017
Create Date: 2026-03-09
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# ---------------------------------------------------------------------------
# Foreign key definitions: (constraint_name, source_table, source_col,
#                           referent_table, referent_col)
# ---------------------------------------------------------------------------
_FOREIGN_KEYS: list[tuple[str, str, str, str, str]] = [
    ("fk_dataset_chunks_dataset_id", "dataset_chunks", "dataset_id", "datasets", "id"),
    ("fk_cached_datasets_dataset_id", "cached_datasets", "dataset_id", "datasets", "id"),
    ("fk_query_dataset_links_query_id", "query_dataset_links", "query_id", "user_queries", "id"),
    ("fk_query_dataset_links_dataset_id", "query_dataset_links", "dataset_id", "datasets", "id"),
    ("fk_agent_tasks_query_id", "agent_tasks", "query_id", "user_queries", "id"),
]

# ---------------------------------------------------------------------------
# Index definitions: (index_name, table, columns)
# ---------------------------------------------------------------------------
_INDEXES: list[tuple[str, str, list[str]]] = [
    ("ix_dataset_chunks_dataset_id", "dataset_chunks", ["dataset_id"]),
    ("ix_cached_datasets_status_updated", "cached_datasets", ["status", "updated_at"]),
    ("ix_user_queries_user_id", "user_queries", ["user_id"]),
    ("ix_messages_conversation_id", "messages", ["conversation_id"]),
    ("ix_query_dataset_links_query_id", "query_dataset_links", ["query_id"]),
    ("ix_query_dataset_links_dataset_id", "query_dataset_links", ["dataset_id"]),
    ("ix_agent_tasks_query_id", "agent_tasks", ["query_id"]),
]


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. Create indexes (idempotent — skip if already present)
    # ------------------------------------------------------------------
    for ix_name, table, columns in _INDEXES:
        op.create_index(ix_name, table, columns, if_not_exists=True)

    # ------------------------------------------------------------------
    # 2. Clean up orphaned rows before adding FK constraints.
    #    Any row whose reference target no longer exists would cause the
    #    ALTER TABLE … ADD CONSTRAINT to fail.
    # ------------------------------------------------------------------
    _cleanup_orphans = [
        # (source_table, source_col, referent_table, referent_col)
        ("dataset_chunks", "dataset_id", "datasets", "id"),
        ("cached_datasets", "dataset_id", "datasets", "id"),
        ("query_dataset_links", "query_id", "user_queries", "id"),
        ("query_dataset_links", "dataset_id", "datasets", "id"),
        ("agent_tasks", "query_id", "user_queries", "id"),
    ]
    for src_table, src_col, ref_table, ref_col in _cleanup_orphans:
        op.execute(
            f"DELETE FROM {src_table} "
            f"WHERE {src_col} IS NOT NULL "
            f"AND {src_col} NOT IN (SELECT {ref_col} FROM {ref_table})"
        )

    # ------------------------------------------------------------------
    # 3. Add foreign key constraints
    # ------------------------------------------------------------------
    for fk_name, src_table, src_col, ref_table, ref_col in _FOREIGN_KEYS:
        op.create_foreign_key(
            fk_name,
            src_table,
            ref_table,
            [src_col],
            [ref_col],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    # Drop foreign keys (reverse order)
    for fk_name, src_table, _src_col, ref_table, _ref_col in reversed(_FOREIGN_KEYS):
        op.drop_constraint(fk_name, src_table, type_="foreignkey")

    # Drop indexes (reverse order). The composite index is truly new;
    # single-column indexes may have been auto-created by SQLAlchemy
    # (with different names), so we only drop the ones we explicitly created.
    for ix_name, table, _columns in reversed(_INDEXES):
        op.drop_index(ix_name, table_name=table, if_exists=True)
