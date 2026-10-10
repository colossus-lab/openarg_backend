"""DDJJ: columnas `alto_cargo` e `inverosimil` en la tabla viva

`cache_ddjj_declaraciones` la crea y la reemplaza `ddjj_tasks` en cada carga,
así que la próxima carga ya trae las dos columnas. Pero el adaptador las lee
desde el deploy, y la carga forzada tarda ~18 minutos: sin esto, el ranking y
las estadísticas de DDJJ fallarían en esa ventana. Se agregan vacías (todas
`alto_cargo = false`, ninguna inverosímil) y la carga las llena.

`ALTER TABLE ... ADD COLUMN` con un default constante no reescribe la tabla
(PG 11+). Si la tabla no existe (base nueva, CI), no hace nada.

Revision ID: 0069
Revises: 0068
"""

from alembic import op

revision = "0069"
down_revision = "0068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE IF EXISTS raw.cache_ddjj_declaraciones
            ADD COLUMN IF NOT EXISTS alto_cargo boolean NOT NULL DEFAULT false,
            ADD COLUMN IF NOT EXISTS inverosimil text[]
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE IF EXISTS raw.cache_ddjj_declaraciones
            DROP COLUMN IF EXISTS alto_cargo,
            DROP COLUMN IF EXISTS inverosimil
        """
    )
