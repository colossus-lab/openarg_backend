"""Enforce normalized `mart_definitions.domain` via DB-level trigger.

Revision ID: 0052
Revises: 0051
Create Date: 2026-05-13

Architectural fix for the recurring `economía` vs `economia` drift
(BUG-007). The Python loader (`app.application.marts.mart._normalize_domain`)
already normalizes domain at parse time, but `refresh_mart` re-reads the
YAML on every raw landing — any YAML that still has an accented domain
(or any out-of-band raw INSERT/UPDATE) would silently re-introduce the
drift. A `BEFORE INSERT OR UPDATE OF domain` trigger makes the invariant
impossible to bypass regardless of the entry path (UPSERT, raw SQL,
admin tools, future migrations).

Function semantics — accepts `text`, returns `text`:
  - NULL or all-whitespace → NULL
  - Otherwise: NFD-decompose, drop combining marks (Unicode category Mn),
    lowercase, collapse internal whitespace runs to underscore.

Backfill is included so existing rows match the trigger's invariant
immediately and the trigger never sees a mix of normalized and
un-normalized values on subsequent updates.
"""

from __future__ import annotations

from alembic import op

revision = "0052"
down_revision = "0051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION normalize_domain_token(s text) RETURNS text AS $$
          SELECT CASE
            WHEN s IS NULL OR btrim(s) = '' THEN NULL
            ELSE lower(
              regexp_replace(
                regexp_replace(
                  normalize(btrim(s), NFD),
                  '[\\u0300-\\u036f]', '', 'g'
                ),
                '\\s+', '_', 'g'
              )
            )
          END
        $$ LANGUAGE sql IMMUTABLE;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION mart_definitions_normalize_domain_fn() RETURNS trigger AS $$
        BEGIN
          NEW.domain := normalize_domain_token(NEW.domain);
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # Drop if exists to keep the migration idempotent under partial reruns.
    op.execute(
        "DROP TRIGGER IF EXISTS mart_definitions_normalize_domain_trg "
        "ON mart_definitions"
    )
    op.execute(
        """
        CREATE TRIGGER mart_definitions_normalize_domain_trg
          BEFORE INSERT OR UPDATE OF domain ON mart_definitions
          FOR EACH ROW EXECUTE FUNCTION mart_definitions_normalize_domain_fn();
        """
    )

    # One-time backfill so existing rows match the trigger invariant.
    op.execute(
        "UPDATE mart_definitions "
        "SET domain = normalize_domain_token(domain) "
        "WHERE domain IS DISTINCT FROM normalize_domain_token(domain)"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS mart_definitions_normalize_domain_trg "
        "ON mart_definitions"
    )
    op.execute("DROP FUNCTION IF EXISTS mart_definitions_normalize_domain_fn()")
    op.execute("DROP FUNCTION IF EXISTS normalize_domain_token(text)")
