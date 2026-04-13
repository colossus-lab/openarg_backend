"""Add messages.confidence column for persisted assistant result quality.

Revision ID: 0031
Revises: 0030
Create Date: 2026-04-12

The chat bridge now surfaces confidence live in the UI, but
conversation reloads lost that metadata because the assistant message
payload did not persist it. This migration adds a nullable FLOAT
column so confidence survives a page refresh and conversation reload.

See specs/001-chat-bridge/001d-conversation-lifecycle/spec.md and
specs/002-chat-ui/spec.md.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0031"
down_revision: str | None = "0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "messages",
        sa.Column(
            "confidence",
            sa.Float(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("messages", "confidence")
