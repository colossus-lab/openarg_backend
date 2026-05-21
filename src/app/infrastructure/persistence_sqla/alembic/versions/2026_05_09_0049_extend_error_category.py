"""Extend error_category with 3 new buckets + re-classify legacy unknowns.

Revision ID: 0049
Revises: 0048
Create Date: 2026-05-09

specs/021-parser-hardening Phase 6.

Diagnosis (run on staging 2026-05-09):
  - 22,517 / 26,704 (84%) of rows have `error_category='unknown'`.
  - Top patterns inside those unknowns:
    * 299× `header_quality:degraded;layout_profile:*` → header was usable
      but degraded; data DID load. NOT a parse failure, more like a
      health note.
    * 87× `rerouted_heavy:*` → orchestration routed the dataset to the
      heavy queue, not an error at all (just an audit trail line that
      got captured in error_message).
    *  46× `sampled: first N rows kept` → data was truncated by row
      cap policy; informational.

These three buckets account for ~432 of the 22.5k unknowns. The rest are
small-count tails that map cleanly to existing categories (parse_format,
download_network, etc.) — handled by the classifier extension in
collector_tasks.py.

Three new enum values:
  - `header_degraded`         — header detected but quality is `degraded`
  - `orchestration_rerouted`  — dataset routed to a different queue
  - `truncation_sampled`      — row count exceeded cap, data truncated
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text as text_op

revision = "0049"
down_revision = "0048"
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
    # Phase 6 additions:
    "header_degraded",
    "orchestration_rerouted",
    "truncation_sampled",
)

_OLD_CATEGORIES = _NEW_CATEGORIES[:-3]


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

    # Re-classify legacy `unknown` rows using the patterns the new
    # classifier code recognises. Idempotent: re-running this UPDATE
    # against rows already classified is a no-op (the WHERE filter
    # excludes them).
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'header_degraded'
             WHERE error_category = 'unknown'
               AND error_message LIKE 'header_quality:degraded%%'
            """
        )
    )
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'orchestration_rerouted'
             WHERE error_category = 'unknown'
               AND error_message LIKE 'rerouted_heavy:%%'
            """
        )
    )
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'truncation_sampled'
             WHERE error_category = 'unknown'
               AND error_message LIKE 'sampled:%%'
            """
        )
    )
    # Small-count tails that map to existing categories.
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'download_network'
             WHERE error_category = 'unknown'
               AND error_message ~* 'ssl.*record layer failure|ssl.*verification|ssl.*handshake'
            """
        )
    )
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'parse_format'
             WHERE error_category = 'unknown'
               AND (
                  error_message ILIKE 'low_memory%%not supported%%python engine%%'
                  OR error_message ILIKE 'truth value of an array%%ambiguous%%'
                  OR error_message ILIKE 'could not determine delimiter%%'
                  OR error_message ILIKE 'unmatched %%when decoding%%'
                  OR error_message ILIKE 'expecting value: line 1 column%%'
                  OR error_message ILIKE 'unexpected end of data%%'
                  OR error_message ILIKE 'list index out of range%%'
                  OR error_message ILIKE 'excel_no_worksheets%%'
                  OR error_message ILIKE 'xml_parse_failed%%'
               )
            """
        )
    )
    bind.execute(
        text_op(
            """
            UPDATE cached_datasets SET error_category = 'parse_schema_mismatch'
             WHERE error_category = 'unknown'
               AND error_message ILIKE '%%NumericValueOutOfRange%%'
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Revert re-classifications back to 'unknown' (so the constraint
    # rollback doesn't fail).
    bind.execute(
        text_op(
            "UPDATE cached_datasets SET error_category = 'unknown' "
            "WHERE error_category IN "
            "('header_degraded', 'orchestration_rerouted', 'truncation_sampled')"
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
