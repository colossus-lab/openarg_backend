"""Phase 3 of MASTERPLAN — mart schema + definitions registry.

Revision ID: 0041
Revises: 0040
Create Date: 2026-05-04

Creates the `mart` schema (medallion layer 3) and `mart_definitions`, a
catalog of materialized views the LangGraph pipeline serves. Each mart is
declared in `config/marts/<mart_id>.yaml`; `openarg.build_mart` reads the
YAML and CREATE-or-REPLACEs the materialized view here.

`embedding vector(1024)` lets the Serving Port adapter combine catalog
discovery (`catalog_resources.embedding`) with mart discovery in a single
vector search, so the planner sees marts and raw resources side-by-side.

Pure additive — no `cached_datasets`, `catalog_resources`,
`raw_table_versions`, or `staging_contract_state` rows are touched.
"""

from __future__ import annotations

from alembic import op

revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS mart")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS mart_definitions (
            mart_id              TEXT PRIMARY KEY,
            mart_schema          TEXT NOT NULL DEFAULT 'mart',
            mart_view_name       TEXT NOT NULL,
            description          TEXT,
            domain               TEXT,
            source_contract_ids  TEXT[] NOT NULL DEFAULT '{}',
            sql_definition       TEXT NOT NULL,
            canonical_columns_json JSONB,
            refresh_policy       TEXT NOT NULL DEFAULT 'on_upstream_change',
            unique_index_columns TEXT[],
            last_refreshed_at    TIMESTAMPTZ,
            last_refresh_status  TEXT,
            last_refresh_error   TEXT,
            yaml_version         TEXT NOT NULL,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT ck_mart_definitions_refresh_policy
                CHECK (refresh_policy IN ('manual', 'daily', 'hourly', 'on_upstream_change'))
        );
        CREATE INDEX IF NOT EXISTS ix_mart_definitions_domain
            ON mart_definitions (domain);
        CREATE INDEX IF NOT EXISTS ix_mart_definitions_refresh_policy
            ON mart_definitions (refresh_policy);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_definitions_view
            ON mart_definitions (mart_schema, mart_view_name);
        """
    )
    # Add the embedding column via raw SQL (pgvector type isn't a SA core type).
    op.execute("ALTER TABLE mart_definitions ADD COLUMN embedding vector(1024)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_mart_definitions_embedding "
        "ON mart_definitions USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_mart_definitions_embedding")
    op.execute("DROP TABLE IF EXISTS mart_definitions")
    # See migrations 0039/0040: schema is NOT dropped CASCADE because that
    # would destroy materialized views the operator may still need.
