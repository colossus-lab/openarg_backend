"""`api_usage`: modelo y costo de cada respuesta

Hasta acá el tablero /admin/mcp estimaba el gasto multiplicando las
respuestas por un costo fijo (US$ 0,034, medido en CloudWatch en septiembre
con el pipeline viejo y Haiku). El agente responde con Sonnet y un número
variable de vueltas, así que el costo sale de los tokens reales de cada turno
(`answers/pricing.py`) y se guarda en la fila.

Las dos columnas son nulas: las filas viejas, el modo datos y las respuestas
del motor viejo (que no mide su costo entero) quedan sin costo y el tablero
las sigue estimando con el valor fijo.

Esquema explícito, como pide la lección de la 0063.

Revision ID: 0066
Revises: 0065
"""

from alembic import op

revision = "0066"
down_revision = "0065"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.api_usage
            ADD COLUMN IF NOT EXISTS model VARCHAR(100),
            ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(12, 6)
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.api_usage
            DROP COLUMN IF EXISTS cost_usd,
            DROP COLUMN IF EXISTS model
        """
    )
