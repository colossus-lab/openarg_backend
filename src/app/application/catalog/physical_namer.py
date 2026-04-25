"""Deterministic naming for `materialized_table_name` (WS4).

Rules (from collector_plan.md WS4):
  - hard limit 63 chars (Postgres `typname`)
  - charset `[a-z0-9_]` only
  - prefix `cache_<portal>_<slug>`
  - on collision: stable 8-char hash of `(portal, source_id)` appended,
    NOT a random suffix — so the same logical resource always maps to the
    same physical name.
  - idempotent: same inputs ⇒ same output

Avoids the 10 `pg_type_typname_nsp_index` collisions confirmed in prod.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass

# `cache_` prefix + `_` separator + 8-char discriminator + `_` separator
# leaves space for portal + slug:
#     63 = 6 (cache_) + len(portal) + 1 + len(slug) + 1 + 8 (discriminator)
# We also need to leave headroom for portal_slug separator. Concretely:
MAX_TABLE_NAME_LENGTH = 63
DISCRIMINATOR_LENGTH = 8
PREFIX = "cache_"
_DISCRIM_SEP = "__"


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_LEADING_DIGITS_RE = re.compile(r"^\d+")
_TRIM_UNDERSCORES_RE = re.compile(r"_+")


@dataclass
class PhysicalName:
    table_name: str
    discriminator: str
    truncated: bool


def _slugify(value: str) -> str:
    if not value:
        return ""
    norm = unicodedata.normalize("NFKD", value)
    norm = norm.encode("ascii", "ignore").decode("ascii").lower()
    norm = _NON_ALNUM_RE.sub("_", norm)
    norm = _TRIM_UNDERSCORES_RE.sub("_", norm).strip("_")
    return norm


def _stable_discriminator(portal: str, source_id: str) -> str:
    raw = f"{portal}\x00{source_id}".encode()
    return hashlib.blake2b(raw, digest_size=8).hexdigest()[:DISCRIMINATOR_LENGTH]


class PhysicalNamer:
    """Build `materialized_table_name` deterministically.

    Always returns a name ≤63 chars including the discriminator suffix.
    """

    def __init__(
        self,
        *,
        prefix: str = PREFIX,
        max_length: int = MAX_TABLE_NAME_LENGTH,
        discriminator_length: int = DISCRIMINATOR_LENGTH,
    ) -> None:
        if discriminator_length < 4 or discriminator_length > 32:
            raise ValueError("discriminator_length must be in [4, 32]")
        self._prefix = prefix
        self._max_length = max_length
        self._discriminator_length = discriminator_length

    def build(self, portal: str, source_id: str, *, slug_hint: str = "") -> PhysicalName:
        portal_slug = _slugify(portal) or "unknown"
        # Slug source prefers slug_hint (e.g. dataset title) over source_id
        slug_source = slug_hint or source_id
        slug = _slugify(slug_source) or "resource"
        discriminator = _stable_discriminator(portal, source_id)

        # Reserve the discriminator suffix slot up front.
        suffix = f"{_DISCRIM_SEP}{discriminator}"
        # Always include `cache_<portal>_` then the slug, then suffix.
        head = f"{self._prefix}{portal_slug}_"
        budget = self._max_length - len(head) - len(suffix)
        if budget < 4:
            # Pathological portal name. Truncate portal too.
            head = head[: self._max_length - len(suffix) - 5]  # leave 5 for slug
            budget = self._max_length - len(head) - len(suffix)
        truncated = len(slug) > budget
        slug = slug[:budget].rstrip("_") or "x"
        # Numbers can't lead a Postgres identifier — prepend `t_` if needed.
        if _LEADING_DIGITS_RE.match(slug):
            # Shrink slug to make room
            slug = "t_" + slug
            slug = slug[: budget].rstrip("_") or "t_x"
        name = f"{head}{slug}{suffix}"
        # Final safety clamp
        if len(name) > self._max_length:
            name = name[: self._max_length].rstrip("_")
        return PhysicalName(table_name=name, discriminator=discriminator, truncated=truncated)


_default_namer = PhysicalNamer()


def physical_table_name(portal: str, source_id: str, *, slug_hint: str = "") -> str:
    """Convenience function returning just the table name."""
    return _default_namer.build(portal, source_id, slug_hint=slug_hint).table_name
