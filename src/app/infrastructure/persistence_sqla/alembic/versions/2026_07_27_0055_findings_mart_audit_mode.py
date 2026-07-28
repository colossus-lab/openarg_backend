"""Allow mode='mart_audit' in ingestion_findings.

`ingestion_findings` already carries the lifecycle a recurring audit needs:
idempotent upsert per (resource, detector, version, mode, input_hash), and a
`resolved_at` that the upsert clears so a re-appearing problem re-opens rather
than piling up duplicate rows. The mart quality auditor reuses it instead of
standing up a parallel table nobody's dashboards read.

What it cannot reuse is the mode vocabulary: the CHECK from 0033 enumerates
the four ingestion phases, and a mart audit is none of them. Folding it into
`state_invariant` would work at the cost of making the two indistinguishable
in every query — the ops surface would lose the ability to ask "what is wrong
with the marts" separately from "what is wrong with the state machine".

Revision ID: 0055
Revises: 0054
"""

from alembic import op

revision = "0055"
down_revision = "0054"
branch_labels = None
depends_on = None

_MODES_BEFORE = "'pre_parse','post_parse','retrospective','state_invariant'"
_MODES_AFTER = f"{_MODES_BEFORE},'mart_audit'"


def upgrade() -> None:
    op.drop_constraint("ck_ingestion_findings_mode", "ingestion_findings", type_="check")
    op.create_check_constraint(
        "ck_ingestion_findings_mode",
        "ingestion_findings",
        f"mode IN ({_MODES_AFTER})",
    )


def downgrade() -> None:
    # Rows in the new mode would violate the narrower constraint, so drop them
    # before restoring it. They are audit output, reproducible by re-running
    # the sweep — no source of truth is lost.
    op.execute("DELETE FROM ingestion_findings WHERE mode = 'mart_audit'")
    op.drop_constraint("ck_ingestion_findings_mode", "ingestion_findings", type_="check")
    op.create_check_constraint(
        "ck_ingestion_findings_mode",
        "ingestion_findings",
        f"mode IN ({_MODES_BEFORE})",
    )
