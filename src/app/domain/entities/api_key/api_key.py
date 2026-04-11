"""API key and usage entities for public API access."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from app.domain.entities.base import BaseEntity

_ZERO_UUID = UUID("00000000-0000-0000-0000-000000000000")


@dataclass
class ApiKey(BaseEntity):
    """An API key issued to a user for programmatic access.

    Keys never expire: ``expires_at`` used to exist on this entity and
    on the ``api_keys`` table but was only ever read, never written
    (Alembic 0030 dropped the column 2026-04-11). If expiration is
    ever needed, it becomes a new feature with a UI flow that actually
    sets a value — see specs/008-developers-keys/[DEBT-003].
    """

    user_id: UUID = field(default=_ZERO_UUID)  # Must be set explicitly
    key_hash: str = ""
    key_prefix: str = ""  # "oarg_sk_a1b2c3d4" (first 16 chars for display)
    name: str = ""
    plan: str = "free"  # free | basic | pro
    is_active: bool = True
    last_used_at: datetime | None = None


@dataclass
class ApiUsage(BaseEntity):
    """A single API request log entry (append-only)."""

    api_key_id: UUID = field(default=_ZERO_UUID)  # Must be set explicitly
    endpoint: str = ""
    question: str = ""  # truncated to 200 chars
    status_code: int = 200
    tokens_used: int = 0
    duration_ms: int = 0
