"""Drop api_keys.expires_at dead column.

Revision ID: 0030
Revises: 0029
Create Date: 2026-04-11

The ``api_keys.expires_at`` column was added in migration 0027 but
was never written by any endpoint. ``POST /api/v1/developers/keys``
constructs ``ApiKey(..., plan='free')`` without an ``expires_at``
argument, and no cron task, UI flow, or request handler ever sets
it. ``api_key_service.verify_api_key`` reads it and compares it to
``now()``, but because the column is always NULL the check
short-circuits on the ``api_key.expires_at and ...`` guard.

This migration drops the dead column in the spirit of §0 of the
constitution ("delete before you add", "abstractions only when a
second caller exists"). If key expiration ever becomes a real
feature, it should be reintroduced with a full FR — a UI flow
that actually sets a value, an optional env-driven default TTL,
and a consistent test for the verify path.

Downgrade re-creates the column as nullable so the table reverts
to the pre-0030 shape. Any data that used to live there is lost;
since nothing ever wrote to it, there is nothing to restore.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0030"
down_revision: str | None = "0029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("api_keys", "expires_at")


def downgrade() -> None:
    op.add_column(
        "api_keys",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
