from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.domain.entities.base import BaseEntity


@dataclass
class User(BaseEntity):
    """Usuario sincronizado desde Google OAuth (NextAuth)."""

    email: str = ""
    name: str = ""
    image_url: str = ""
    privacy_accepted_at: datetime | None = field(default=None)
    save_history: bool = field(default=True)
