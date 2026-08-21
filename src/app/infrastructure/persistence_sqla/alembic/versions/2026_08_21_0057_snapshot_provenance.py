"""Record which of OUR versions produced a table's shape.

Migration 0056 preserves a table's shape before it is dropped, so a change
in shape between two versions becomes visible. It does not record *whose*
change it was, and that turns out to be the difference between a useful
signal and a noisy one.

Five transformations of ours rewrite a table's schema without the source
having changed anything: `promote_buried_headers`, `unpivot_if_time_pivoted`
(threshold-gated, so an extra year of data can flip it), `dedupe_column_names`,
`_compact_wide_dataframe`, and the `MAX_TABLE_ROWS` cut. On top of that,
~9.500 `parse_repair` operations have renamed columns in place. A snapshot
diff that ignores this attributes our own parser improvements to the portal.

These five columns let a consumer *exonerate* a diff — prove it is not
upstream drift — instead of having to guess:

  parser_version / normalization_version   did our parser change between
                                           the two snapshots?
  layout_profile / header_quality          did the parse take a different
                                           path through the same file?
  is_truncated                             did one side hit the row cap?

All of them already exist elsewhere (`catalog_resources`, `cached_datasets`,
`raw_table_versions`); this migration copies them onto the snapshot so the
record stays self-contained after the rows they came from are deleted — which
is the same reason 0056 avoided a foreign key.

Nullable throughout: provenance is best-effort, and a snapshot without it is
still worth having.

Revision ID: 0057
Revises: 0056
Create Date: 2026-08-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0057"
down_revision = "0056"
branch_labels = None
depends_on = None

_COLUMNS = (
    ("parser_version", sa.String(length=40)),
    ("normalization_version", sa.String(length=40)),
    ("layout_profile", sa.String(length=40)),
    ("header_quality", sa.String(length=20)),
    ("is_truncated", sa.Boolean()),
)


def upgrade() -> None:
    for name, type_ in _COLUMNS:
        op.add_column(
            "raw_schema_snapshots",
            sa.Column(name, type_, nullable=True),
            schema="raw",
        )
    # The exoneration query is "same resource, different parser version".
    op.create_index(
        "ix_raw_schema_snapshots_provenance",
        "raw_schema_snapshots",
        ["parser_version", "normalization_version"],
        schema="raw",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_raw_schema_snapshots_provenance",
        table_name="raw_schema_snapshots",
        schema="raw",
    )
    for name, _type in reversed(_COLUMNS):
        op.drop_column("raw_schema_snapshots", name, schema="raw")
