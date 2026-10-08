"""Retirar los marts de DDJJ que reemplazan `raw.cache_ddjj_*`

`ddjj_funcionarios_federales` (deudas) y `ddjj_patrimonio_declarado` (bienes) leían
las tablas que el colector genérico armaba con los CSV de la Oficina
Anticorrupción, con años cortados en 500.000 filas y sin deduplicar. Las DDJJ
ahora salen de `raw.cache_ddjj_declaraciones`, `_bienes` y `_deudas`
(`ddjj_tasks`), completas desde 2012.

Borrar el YAML no alcanza:

- nada borra la vista ni la fila de `mart_definitions`;
- el ruteo sigue sirviendo el mart mientras `last_row_count > 0`;
- la vista protege a las tablas viejas de cualquier limpieza.

Esta migración hace las tres cosas. La vista se borra sin CASCADE: si algo
dependiera de ella, se avisa y se deja, en vez de llevarse lo que dependa.

`mart_sample_queries` existe en `public` y, en staging y prod, también en `raw`.

Revision ID: 0068
Revises: 0067
"""

from alembic import op

revision = "0068"
down_revision = "0067"
branch_labels = None
depends_on = None

_MARTS = ("ddjj_funcionarios_federales", "ddjj_patrimonio_declarado")


def upgrade() -> None:
    for mart in _MARTS:
        op.execute(
            f"""
            DO $$
            BEGIN
                DROP MATERIALIZED VIEW IF EXISTS mart."{mart}";
            EXCEPTION WHEN dependent_objects_still_exist THEN
                RAISE NOTICE 'mart.{mart}: tiene dependencias, queda sin borrar';
            END $$;
            """
        )
        op.execute(f"DELETE FROM public.mart_definitions WHERE mart_id = '{mart}'")
        for esquema in ("public", "raw"):
            op.execute(
                f"""
                DO $$
                BEGIN
                    IF to_regclass('{esquema}.mart_sample_queries') IS NOT NULL THEN
                        DELETE FROM {esquema}.mart_sample_queries WHERE mart_id = '{mart}';
                    END IF;
                END $$;
                """
            )


def downgrade() -> None:
    # Volver atrás es restaurar los YAML y correr `build_mart`: la vista y su
    # fila se rearman desde ahí. La migración no guarda nada que reponer.
    pass
