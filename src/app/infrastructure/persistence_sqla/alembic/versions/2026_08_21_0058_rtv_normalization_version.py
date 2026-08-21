"""Record which normalisation produced a version, alongside which parser did.

`raw_table_versions` has carried `parser_version` since the raw layer was
introduced, and it is per-version — the right granularity, and the one
`catalog_resources` cannot offer because it holds a single row per resource.
There has never been a matching column for normalisation, so a table repaired
after ingest looks identical to one that was never touched.

Shipped together with the derived fingerprints rather than ahead of them. Until
something computed a real value, this column would only have repeated the defect
it is meant to remove: `parser_version` already existed, was already written,
and held the literal string `2026-05-04` for 21,989 rows because the environment
variable feeding it was set to a date. A second field with nothing behind it
would have been worse than none.

No backfill. Historical rows genuinely do not know which normalisation ran, and
inventing a value is exactly how the existing column stopped meaning anything.
NULL is the honest record, and the drift classifier reads it as
`UNATTRIBUTABLE` rather than as an unexplained change.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "raw_table_versions"
_COLUMN = "normalization_version"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.String(length=64), nullable=True))
    # Partial: the rows worth finding are the ones that carry provenance, and
    # they are the minority for as long as the backlog of unattributable
    # versions dominates.
    op.create_index(
        f"ix_{_TABLE}_provenance",
        _TABLE,
        ["parser_version", _COLUMN],
        unique=False,
        postgresql_where=sa.text("parser_version IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(f"ix_{_TABLE}_provenance", table_name=_TABLE)
    op.drop_column(_TABLE, _COLUMN)
