"""Phase 2 of MASTERPLAN — staging schema + contract state.

Revision ID: 0040
Revises: 0039
Create Date: 2026-05-04

Creates the `staging` schema (medallion layer 2) and `staging_contract_state`,
the registry that records the latest contract validation outcome per
`resource_identity`. The `openarg.promote_to_staging` task writes here.

Status semantics:
  - `pass`     — contract validated, table promoted to staging.
  - `warn`     — contract validated with non-fatal findings; promoted anyway
                 under the `OPENARG_STAGING_PROMOTE_ON_WARN` flag.
  - `fail`     — contract failed; staging table NOT updated, raw kept as-is.
  - `skipped`  — no applicable contract for this resource (default for new
                 resources until a YAML is registered).

Pure additive — no `cached_datasets`, `catalog_resources`, `raw_table_versions`
rows are touched. Operators control rollout via the env flag plus the
contracts directory contents.
"""

from __future__ import annotations

from alembic import op

revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS staging_contract_state (
            resource_identity   TEXT NOT NULL,
            contract_id         TEXT NOT NULL,
            contract_version    TEXT NOT NULL,
            status              TEXT NOT NULL,
            staging_schema      TEXT NOT NULL DEFAULT 'staging',
            staging_table_name  TEXT,
            findings_json       JSONB,
            row_count           BIGINT,
            last_validated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_staging_contract_state
                PRIMARY KEY (resource_identity, contract_id, contract_version),
            CONSTRAINT ck_staging_contract_state_status
                CHECK (status IN ('pass', 'warn', 'fail', 'skipped'))
        );
        CREATE INDEX IF NOT EXISTS ix_staging_contract_state_resource
            ON staging_contract_state (resource_identity);
        CREATE INDEX IF NOT EXISTS ix_staging_contract_state_status
            ON staging_contract_state (status, last_validated_at DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS staging_contract_state")
    # See migration 0039: we do NOT DROP SCHEMA staging CASCADE because that
    # would silently destroy every promoted staging table the operator might
    # still need. Manual schema drop required for a clean rollback.
