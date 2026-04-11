"""Add messages.errored column for partial/failed assistant responses.

Revision ID: 0029
Revises: 0028
Create Date: 2026-04-11

Follow-up to the 2026-04-10 DEBT-002 fix. That change added an
``errored`` field to the ``MessageCreate`` request schema as a
forward-compatible pass-through, but there was no DB column to
persist it — the backend silently dropped the flag. This migration
adds the column as ``BOOLEAN NOT NULL DEFAULT FALSE`` so the
frontend can render a "Regenerar" affordance on messages that were
saved on an error path (stream broken, WS-emitted error, caught
exception).

See specs/001-chat-bridge/001d-conversation-lifecycle/spec.md FR-015
and specs/002-chat-ui/spec.md FR-012a/b for the behavior contract.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0029"
down_revision: str | None = "0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "messages",
        sa.Column(
            "errored",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("messages", "errored")
