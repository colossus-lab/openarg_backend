"""Add error_category to cached_datasets + state-machine invariants (WS0.5).

Revision ID: 0034
Revises: 0033
Create Date: 2026-04-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0034"
down_revision = "0033"
branch_labels = None
depends_on = None


# Closed taxonomy. Categories with NULL/no match get 'unknown' on backfill so
# operational queries can group safely.
_ALLOWED = (
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
)

# (substring matched against error_message) -> category. Order matters: first
# match wins. Patterns are case-insensitive (handled with LOWER()).
_BACKFILL_RULES: list[tuple[str, str]] = [
    # orchestration first (most specific)
    ("recovered from schema_mismatch", "orchestration_recovery_loop"),
    ("exhausted retries while stuck in downloading", "orchestration_recovery_loop"),
    ("table missing", "orchestration_table_missing"),
    # downloads
    ("connection refused", "download_network"),
    ("no route to host", "download_network"),
    ("name or service not known", "download_network"),
    ("dns", "download_network"),
    ("timed out", "download_timeout"),
    ("read timeout", "download_timeout"),
    ("503 ", "download_http_error"),
    ("502 ", "download_http_error"),
    ("500 ", "download_http_error"),
    ("404 ", "download_http_error"),
    ("410 ", "download_http_error"),
    ("403 ", "download_http_error"),
    ("http", "download_http_error"),
    # policies
    ("zip_too_large", "policy_too_large"),
    ("file_too_large", "policy_too_large"),
    ("zip_no_parseable_file", "policy_non_tabular"),
    ("zip_document_bundle", "policy_non_tabular"),
    # parse
    ("schema_mismatch", "parse_schema_mismatch"),
    ("encoding", "parse_encoding"),
    ("bad_zip_file", "parse_format"),
    ("excel_no_worksheets", "parse_format"),
    ("xml_parse_failed", "parse_format"),
    ("unsupported_format", "parse_format"),
    ("geojson_no_features", "parse_format"),
    # materialize
    ("pg_type_typname_nsp_index", "materialize_table_collision"),
    ("no space left on device", "materialize_disk_full"),
    ("disk full", "materialize_disk_full"),
    # metadata
    ("no_download_url", "metadata_no_url"),
    # validator (WS0)
    ("ingestion_validation_failed", "validation_failed"),
]


def upgrade() -> None:
    op.add_column(
        "cached_datasets",
        sa.Column(
            "error_category",
            sa.String(length=50),
            nullable=False,
            server_default="unknown",
        ),
    )
    allowed_quoted = ",".join(f"'{c}'" for c in _ALLOWED)
    op.create_check_constraint(
        "ck_cached_datasets_error_category",
        "cached_datasets",
        f"error_category IN ({allowed_quoted})",
    )
    op.create_index(
        "ix_cached_datasets_error_category",
        "cached_datasets",
        ["error_category"],
    )

    # Backfill — first matching rule wins.
    bind = op.get_bind()
    for pattern, category in _BACKFILL_RULES:
        bind.execute(
            sa.text(
                "UPDATE cached_datasets "
                "SET error_category = :cat "
                "WHERE error_category = 'unknown' "
                "  AND error_message IS NOT NULL "
                "  AND LOWER(error_message) LIKE :pat"
            ),
            {"cat": category, "pat": f"%{pattern}%"},
        )

    # State-machine invariant trigger:
    #   retry_count >= MAX => status MUST be 'permanently_failed'.
    # We can't easily express MAX as a SQL constant without coupling, so we
    # encode it (5) here. If MAX_TOTAL_ATTEMPTS in collector_tasks.py changes,
    # bump this constant in a follow-up migration.
    bind.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION enforce_cached_datasets_retry_invariant()
            RETURNS trigger AS $$
            BEGIN
                -- Terminal state: never auto-touch a row that is already
                -- permanently_failed. Rejects accidental retry_count bumps
                -- from concurrent code paths and preserves audit trail.
                IF OLD IS NOT NULL AND OLD.status = 'permanently_failed' THEN
                    IF NEW.status = 'pending' AND NEW.retry_count = 0 THEN
                        RETURN NEW;
                    END IF;
                    NEW.status := 'permanently_failed';
                    NEW.retry_count := OLD.retry_count;
                    RETURN NEW;
                END IF;
                IF NEW.retry_count >= 5 AND NEW.status = 'error' THEN
                    NEW.status := 'permanently_failed';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )
    )
    bind.execute(
        sa.text(
            """
            DROP TRIGGER IF EXISTS trg_cached_datasets_retry_invariant ON cached_datasets;
            CREATE TRIGGER trg_cached_datasets_retry_invariant
            BEFORE INSERT OR UPDATE ON cached_datasets
            FOR EACH ROW EXECUTE FUNCTION enforce_cached_datasets_retry_invariant();
            """
        )
    )

    # Heal historical rows that violated the invariant before the trigger
    # existed (the 250 rows with retry_count=4 still in 'error'). Run in
    # bounded batches so the migration does not hold a single long-running
    # update lock if the population grows.
    while True:
        result = bind.execute(
            sa.text(
                """
                WITH to_fix AS (
                    SELECT ctid
                    FROM cached_datasets
                    WHERE retry_count >= 4
                      AND status = 'error'
                    LIMIT 100
                )
                UPDATE cached_datasets cd
                SET status = 'permanently_failed',
                    updated_at = NOW()
                WHERE cd.ctid IN (SELECT ctid FROM to_fix)
                """
            )
        )
        if (result.rowcount or 0) < 100:
            break


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP TRIGGER IF EXISTS trg_cached_datasets_retry_invariant ON cached_datasets"))
    bind.execute(sa.text("DROP FUNCTION IF EXISTS enforce_cached_datasets_retry_invariant()"))
    op.drop_index("ix_cached_datasets_error_category", table_name="cached_datasets")
    op.drop_constraint(
        "ck_cached_datasets_error_category", "cached_datasets", type_="check"
    )
    op.drop_column("cached_datasets", "error_category")
