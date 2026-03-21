"""Add feedback and feedback_comment columns to messages table

Revision ID: 0011
Revises: 0010
Create Date: 2026-03-02

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("messages", sa.Column("feedback", sa.String(10), nullable=True))
    op.add_column("messages", sa.Column("feedback_comment", sa.Text, nullable=True))
    op.create_index(
        "ix_messages_feedback",
        "messages",
        ["feedback"],
        postgresql_where=sa.text("feedback IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_messages_feedback", table_name="messages")
    op.drop_column("messages", "feedback_comment")
    op.drop_column("messages", "feedback")
