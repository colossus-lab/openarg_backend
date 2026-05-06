"""Empty `resource_identity_aliases` table — slot reservado.

Revision ID: 0047
Revises: 0046
Create Date: 2026-05-05

The catalog (`catalog_resources`) and the registry (`raw_table_versions`)
use different identity conventions for the same physical resource:

  - Catalog canonical:   `{portal}::{datasets.source_id}` — the literal
    portal+source_id from the upstream catalog scrape (e.g.
    `bcra::bcra-cotizaciones`).
  - Registry curated:    `{portal}::{curated_key}` — emitted by vía-B
    writers (BCRA, presupuesto dimensions, senado_listado_*) when they
    register the rtv (`bcra::cotizaciones`).

Today only ~11 rows in `raw_table_versions` use the curated form. Marts
reference those curated identities directly via `live_table()`; the
serving port reconciles via `materialized_table_name` (Sprint 0.6 fix).
That covers the runtime use case.

This table is the dormant slot for the future case where a consumer
needs to translate between the two systems bidirectionally (lineage
queries, cross-system joins). The full rollout (helpers + macros +
writers + seed) is documented as fases B-D in our spec but NOT
implemented yet — the cost/benefit analysis at this point in time
favors leaving the table empty until a real consumer appears.

Schema:

  alias_identity      TEXT  PRIMARY KEY  — what marts/curated writers use
  canonical_identity  TEXT  NOT NULL     — what the catalog emits ({portal}::{source_id})
  source              TEXT  NOT NULL     — which writer registered the alias (e.g. 'bcra_tasks', 'manual_seed')
  created_at          TIMESTAMPTZ DEFAULT NOW()

Constraints:
  - alias_identity != canonical_identity (a self-alias is meaningless)
  - implicit many-to-one: one canonical can have several aliases.

Read path (when fases B-D land):
  resolve_canonical(alias) → canonical | alias (passthrough if no row)
  resolve_aliases_for(canonical) → [alias1, alias2, ...]

The migration intentionally creates NO indexes beyond the PK — the
table stays empty for now, and PG can add indexes lazily when a
consumer starts querying it.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text as text_op

revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        text_op(
            """
            CREATE TABLE IF NOT EXISTS resource_identity_aliases (
                alias_identity      TEXT NOT NULL PRIMARY KEY,
                canonical_identity  TEXT NOT NULL,
                source              TEXT NOT NULL,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_resource_identity_aliases_self
                    CHECK (alias_identity <> canonical_identity)
            )
            """
        )
    )
    bind.execute(
        text_op(
            "CREATE INDEX IF NOT EXISTS ix_resource_identity_aliases_canonical "
            "ON resource_identity_aliases (canonical_identity)"
        )
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS resource_identity_aliases")
