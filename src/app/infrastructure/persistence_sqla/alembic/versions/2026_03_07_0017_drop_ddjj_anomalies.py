"""Drop ddjj_anomalies table

Revision ID: 0017
Revises: 0016
Create Date: 2026-03-07
"""

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_ddjj_anomalies_anomalous", table_name="ddjj_anomalies", if_exists=True)
    op.drop_table("ddjj_anomalies")


def downgrade() -> None:
    pass
