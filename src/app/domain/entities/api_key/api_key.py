"""API key and usage entities for public API access."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4

from app.domain.entities.base import BaseEntity


@dataclass
class ApiKey(BaseEntity):
    """An API key issued to a user for programmatic access."""

    user_id: UUID = field(default_factory=uuid4)
    key_hash: str = ""
    key_prefix: str = ""  # "oarg_sk_a1b2c3d4" (first 16 chars for display)
    name: str = ""
    plan: str = "free"  # free | basic | pro
    is_active: bool = True
    last_used_at: datetime | None = None
    expires_at: datetime | None = None


@dataclass
class ApiUsage(BaseEntity):
    """A single API request log entry (append-only)."""

    api_key_id: UUID = field(default_factory=uuid4)
    endpoint: str = ""
    question: str = ""  # truncated to 200 chars
    status_code: int = 200
    tokens_used: int = 0
    duration_ms: int = 0
