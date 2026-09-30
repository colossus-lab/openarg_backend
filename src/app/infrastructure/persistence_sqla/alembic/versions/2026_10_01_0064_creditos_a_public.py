"""Mover las tablas de Fundadores y créditos de `raw` a `public`

0063 created them with unqualified names, and the database default
search_path is `raw, public`, so on staging they landed in `raw` — next to the
ingested data, not next to `api_keys` and `api_usage` where they belong. The
app kept working only because its search_path also includes `raw`; that is the
same trap that broke ~4.4k `cache_*` lookups before, so it is fixed at the
root rather than relied upon.

0063 now qualifies everything with `public.`, so a fresh database never takes
this path. This revision only acts where a table exists in `raw` and not in
`public`: `ALTER TABLE ... SET SCHEMA` moves it with its rows, indexes,
constraints and foreign keys. Everywhere else it is a no-op.

Revision ID: 0064
Revises: 0063
"""

from alembic import op

revision = "0064"
down_revision = "0063"
branch_labels = None
depends_on = None

_TABLES = ("api_supporters", "api_credit_balances", "api_credit_movements")


def upgrade() -> None:
    for table in _TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF to_regclass('raw.{table}') IS NOT NULL
                   AND to_regclass('public.{table}') IS NULL THEN
                    ALTER TABLE raw.{table} SET SCHEMA public;
                END IF;
            END
            $$
            """
        )


def downgrade() -> None:
    # Nothing to undo: `public` is where these tables belong.
    pass
