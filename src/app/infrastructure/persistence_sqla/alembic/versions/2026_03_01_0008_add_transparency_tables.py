"""Add transparency analysis tables: dataset health scores, DDJJ anomalies, session topics

Revision ID: 0008
Revises: 0007
Create Date: 2026-03-01

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- Feature 1 & 2: Dataset Health Index + Ghost Data Monitor ---
    op.create_table(
        "dataset_health_scores",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("dataset_id", UUID(as_uuid=True), nullable=False, index=True),
        sa.Column("portal", sa.String(100), nullable=False, index=True),
        # 6 quality dimensions (0.0 to 1.0)
        sa.Column("freshness_score", sa.Float, server_default="0"),
        sa.Column("accessibility_score", sa.Float, server_default="0"),
        sa.Column("machine_readability_score", sa.Float, server_default="0"),
        sa.Column("completeness_score", sa.Float, server_default="0"),
        sa.Column("license_score", sa.Float, server_default="0"),
        sa.Column("contactability_score", sa.Float, server_default="0"),
        # Composite
        sa.Column("overall_score", sa.Float, server_default="0"),
        # Freshness classification
        sa.Column("freshness_status", sa.String(20), server_default="'unknown'"),
        sa.Column("scored_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("details_json", sa.Text, nullable=True),
    )
    op.create_index("ix_health_portal_scored", "dataset_health_scores", ["portal", "scored_at"])

    # --- Feature 3: DDJJ Anomaly Detector ---
    op.create_table(
        "ddjj_anomalies",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("cuit", sa.String(20), nullable=False),
        sa.Column("nombre", sa.String(500), nullable=False),
        sa.Column("anio_declaracion", sa.String(10), nullable=False),
        # Financial data
        sa.Column("patrimonio_cierre", sa.Float, server_default="0"),
        sa.Column("bienes_inicio", sa.Float, server_default="0"),
        sa.Column("bienes_cierre", sa.Float, server_default="0"),
        sa.Column("ingresos_trabajo_neto", sa.Float, server_default="0"),
        sa.Column("gastos_personales", sa.Float, server_default="0"),
        # Anomaly metrics
        sa.Column("variacion_patrimonial", sa.Float, server_default="0"),
        sa.Column("brecha_inexplicable", sa.Float, server_default="0"),
        sa.Column("ratio_crecimiento", sa.Float, server_default="0"),
        # Flags
        sa.Column("is_anomalous", sa.Boolean, server_default="false"),
        sa.Column("anomaly_type", sa.String(100), nullable=True),
        sa.Column("severity", sa.String(20), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("details_json", sa.Text, nullable=True),
    )
    op.create_index("ix_ddjj_anomalies_anomalous", "ddjj_anomalies", ["is_anomalous", "severity"])

    # --- Feature 4: Session Topic Analysis ---
    op.create_table(
        "session_topics",
        sa.Column(
            "id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")
        ),
        sa.Column("periodo", sa.Integer, nullable=False),
        sa.Column("reunion", sa.Integer, nullable=False),
        sa.Column("fecha", sa.String(20), nullable=True),
        sa.Column("tipo_sesion", sa.String(200), nullable=True),
        # Topic info
        sa.Column("topic_name", sa.String(200), nullable=False),
        sa.Column("topic_category", sa.String(100), nullable=True),
        sa.Column("mention_count", sa.Integer, server_default="0"),
        sa.Column("relevance_score", sa.Float, server_default="0"),
        # Speakers
        sa.Column("top_speakers_json", sa.Text, nullable=True),
        sa.Column("sample_excerpts_json", sa.Text, nullable=True),
        sa.Column("analyzed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_session_topics_periodo", "session_topics", ["periodo", "reunion"])
    op.create_index("ix_session_topics_category", "session_topics", ["topic_category"])


def downgrade() -> None:
    op.drop_table("session_topics")
    op.drop_table("ddjj_anomalies")
    op.drop_table("dataset_health_scores")
