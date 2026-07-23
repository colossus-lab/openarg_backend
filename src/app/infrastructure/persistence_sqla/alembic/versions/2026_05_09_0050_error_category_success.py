"""Add 'success' to error_category enum + reclassify ready+NULL rows.

Revision ID: 0050
Revises: 0049
Create Date: 2026-05-09

Closes the last bit of `error_category=unknown` ambiguity. Pre-rollout
the column was conflating two very different things:
  - 22,075 rows with `status='ready'` AND `error_message IS NULL`
    (these are SUCCESS — no error at all)
  - A handful of rows with real errors that didn't match any classifier
    pattern.

Both ended up bucketed as `'unknown'` because the column is NOT NULL
and the classifier returned `'unknown'` for empty/null messages. A
metric like `unknown_rate` was therefore noise-dominated by success
rows.

This migration:
  - Extends the CHECK constraint with `'success'`.
  - UPDATE rows where status='ready' AND error_message IS NULL → 'success'.
  - Leaves real-unknown rows (with text) alone — those are the genuine
    classifier gaps to investigate.

After this, `error_category='unknown'` means "real error, not yet
classified" — a useful, actionable metric.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text as text_op

revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None


_NEW_CATEGORIES = (
    "unknown",
    "download_network",
    "download_http_error",
    "download_timeout",
    "parse_format",
    "parse_encoding",
    "parse_schema_mismatch",
    "materialize_table_collision",
    "materialize_disk_full",
    "validation_failed",
    "policy_too_large",
    "policy_non_tabular",
    "metadata_no_url",
    "orchestration_recovery_loop",
    "orchestration_table_missing",
    "header_degraded",
    "orchestration_rerouted",
    "truncation_sampled",
    # 0050 addition:
    "success",
)

_OLD_CATEGORIES = _NEW_CATEGORIES[:-1]


def _build_check(values: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"((error_category)::text = ANY (ARRAY[{quoted}]::text[]))"


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(
        text_op(
            "ALTER TABLE cached_datasets DROP CONSTRAINT "
            "ck_cached_datasets_ck_cached_datasets_error_category"
        )
    )
    bind.execute(
        text_op(
            "ALTER TABLE cached_datasets ADD CONSTRAINT "
            "ck_cached_datasets_ck_cached_datasets_error_category "
            f"CHECK ({_build_check(_NEW_CATEGORIES)})"
        )
    )
    # Reclassify success-path rows.
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'success'
             WHERE status = 'ready'
               AND error_category = 'unknown'
               AND error_message IS NULL
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Revert success rows back to unknown so the constraint rollback works.
    bind.execute(
        text_op(
            "UPDATE cached_datasets SET error_category = 'unknown' WHERE error_category = 'success'"
        )
    )
    bind.execute(
        text_op(
            "ALTER TABLE cached_datasets DROP CONSTRAINT "
            "ck_cached_datasets_ck_cached_datasets_error_category"
        )
    )
    bind.execute(
        text_op(
            "ALTER TABLE cached_datasets ADD CONSTRAINT "
            "ck_cached_datasets_ck_cached_datasets_error_category "
            f"CHECK ({_build_check(_OLD_CATEGORIES)})"
        )
    )
