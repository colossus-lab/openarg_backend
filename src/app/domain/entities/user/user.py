from __future__ import annotations

from dataclasses import dataclass

from app.domain.entities.base import BaseEntity


@dataclass
class User(BaseEntity):
    """Usuario sincronizado desde Google OAuth (NextAuth)."""

    email: str = ""
    name: str = ""
    image_url: str = ""
