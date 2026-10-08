"""`ddjj_cargas`: registro de las cargas de declaraciones juradas

Cada corrida de `openarg.ingest_ddjj_oa` (y de las otras fuentes de DDJJ) deja
una fila: si escribió, se negó o falló, con qué recursos de la fuente
(`manifiesto`) y qué cargó (`resumen`: el archivo elegido por año, los cortes
descartados y por qué, las filas por año, las correcciones ×10). El manifiesto
de la última carga escrita es lo que permite no bajar ~1 GB por semana cuando
la Oficina Anticorrupción no publicó nada nuevo.

Las tablas de datos (`raw.cache_ddjj_*`) las crea la tarea, como las de series y
BCRA: se reemplazan enteras con un RENAME y la definición vive en
`ddjj_tasks._DDL`.

Esquema explícito, como pide la lección de la 0063.

Revision ID: 0067
Revises: 0066
"""

from alembic import op

revision = "0067"
down_revision = "0066"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS public.ddjj_cargas (
            id BIGSERIAL PRIMARY KEY,
            fuente VARCHAR(50) NOT NULL,
            estado VARCHAR(20) NOT NULL,
            inicio TIMESTAMPTZ NOT NULL,
            fin TIMESTAMPTZ NOT NULL DEFAULT now(),
            manifiesto JSONB,
            resumen JSONB,
            detalle TEXT
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_ddjj_cargas_fuente_fin "
        "ON public.ddjj_cargas (fuente, fin DESC)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS public.ddjj_cargas")
