"""Fundadores y créditos de la API pública

The public API and MCP quotas move from daily to monthly (10 questions and 200
data-mode requests per month). Two things sit on top of that free allowance,
modelled on Tomi (Plataforma de Políticas Públicas) but fixing its two known
gaps — a non-atomic check-then-debit and no record of movements:

- `api_supporters`: people who sustain OpenArg with a donation to Fundación
  Colossus Lab (or were given it as a courtesy). Active while `hasta` is NULL
  or in the future; they get a larger monthly allowance.
- `api_credit_balances`: extra credits, spent only after the month's
  allowance runs out. One row per person; the debit is a single conditional
  UPDATE (`... WHERE preguntas > 0`), so concurrent requests cannot overspend.
  The CHECK constraints make a negative balance impossible even by hand.
- `api_credit_movements`: every grant and every spend. The partial unique
  index on (motivo, referencia) is what will make a Mercado Pago webhook
  idempotent later: the same payment id cannot be credited twice.

Revision ID: 0063
Revises: 0062
"""

from alembic import op

revision = "0063"
down_revision = "0062"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS api_supporters (
            user_id     UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            nivel       VARCHAR(16) NOT NULL DEFAULT 'fundador',
            desde       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            hasta       TIMESTAMPTZ,
            origen      TEXT,
            nota        TEXT,
            created_by  VARCHAR(255),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS api_credit_balances (
            user_id     UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            preguntas   INTEGER NOT NULL DEFAULT 0 CHECK (preguntas >= 0),
            datos       INTEGER NOT NULL DEFAULT 0 CHECK (datos >= 0),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS api_credit_movements (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            tipo        VARCHAR(12) NOT NULL CHECK (tipo IN ('preguntas', 'datos')),
            delta       INTEGER NOT NULL,
            motivo      VARCHAR(16) NOT NULL
                        CHECK (motivo IN ('admin', 'donacion', 'consumo', 'ajuste')),
            referencia  VARCHAR(255),
            created_by  VARCHAR(255),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_api_credit_movements_user "
        "ON api_credit_movements (user_id, created_at)"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_api_credit_movements_ref "
        "ON api_credit_movements (motivo, referencia, tipo) WHERE referencia IS NOT NULL"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS api_credit_movements")
    op.execute("DROP TABLE IF EXISTS api_credit_balances")
    op.execute("DROP TABLE IF EXISTS api_supporters")
