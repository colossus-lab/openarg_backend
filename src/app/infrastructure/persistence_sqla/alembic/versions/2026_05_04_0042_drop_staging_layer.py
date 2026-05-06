"""Drop the staging layer: replaced by mart-on-raw with macros.

Revision ID: 0042
Revises: 0041
Create Date: 2026-05-04

The contracts/staging design (mig 0040) treated contracts as gatekeepers:
only datasets that matched a domain-specific contract were promoted to
`staging.<contract>__<slug>`. In practice, ~95% of resources never matched
any contract, so staging stayed empty and marts (which depended on
staging) had nothing to read.

The new design (B-rebuild) eliminates staging as a layer. Marts apply
domain-specific cast/filter logic inline against `raw.*` via
`{{ live_table(...) }}` macros that build_mart resolves at build time.
This keeps the medallion's spirit (separate "what came from upstream"
from "what we serve") without the gating problem.

This migration is purely destructive:
  - DROP SCHEMA staging CASCADE
  - DROP TABLE staging_contract_state

The mart layer (mig 0041) survives — it is the new direct consumer of raw.
"""

from __future__ import annotations

from alembic import op

revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the registry first; the schema goes second so a half-applied
    # migration leaves the rollback path clean.
    op.execute("DROP TABLE IF EXISTS staging_contract_state CASCADE")
    op.execute("DROP SCHEMA IF EXISTS staging CASCADE")


def downgrade() -> None:
    # Recreate the schema only (the registry's full shape lives in mig 0040;
    # invoking that migration's upgrade here would be a circular import).
    # Operators downgrading past this point should also re-run mig 0040
    # explicitly to restore `staging_contract_state`.
    op.execute("CREATE SCHEMA IF NOT EXISTS staging")
