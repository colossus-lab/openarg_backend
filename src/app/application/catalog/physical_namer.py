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


def _stable_discriminator(portal: str, source_id: str, *, length: int = DISCRIMINATOR_LENGTH) -> str:
    """Stable hex digest used as the trailing identifier of a physical
    table name. `length` defaults to the module constant so callers
    that don't care keep working unchanged. PhysicalNamer overrides it
    when the caller passed a custom `discriminator_length` — the
    earlier version ignored that override and always used 8 chars,
    silently breaking the public API contract."""
    # blake2b returns 16 hex chars per byte of digest, and we only need
    # `ceil(length / 2)` bytes to cover any value in [4, 32].
    digest_bytes = max(2, (length + 1) // 2)
    raw = f"{portal}\x00{source_id}".encode()
    return hashlib.blake2b(raw, digest_size=digest_bytes).hexdigest()[:length]


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
        # Use the per-instance length so an override at __init__ actually
        # propagates. Without `length=`, the API exposed `discriminator_length`
        # but every call still produced an 8-char tail.
        discriminator = _stable_discriminator(
            portal, source_id, length=self._discriminator_length
        )

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


# ─── Phase 1 (MASTERPLAN) — raw schema + versioning ─────────────────────────
#
# Format: `<portal>__<slug>__<discriminator>__v<N>`
# Lives in schema `raw`; version monotonic per resource_identity.
# Layout choices:
#   - `__` separators (double underscore) so single underscores in slug stay
#     visible.
#   - Discriminator BEFORE version so the resource identity is stable across
#     versions (renaming v1 → v2 only swaps the suffix, never the body).
#   - Max version assumed v999 → 4 chars for `v<N>`.

_RAW_DISCRIM_SEP = "__"
_RAW_VERSION_PREFIX = "__v"
_RAW_BUDGET_RESERVE = (
    len(_RAW_DISCRIM_SEP)         # before discriminator
    + DISCRIMINATOR_LENGTH        # 8 chars hex
    + len(_RAW_VERSION_PREFIX)    # 3 chars
    + 3                           # version number budget (up to 999)
)


@dataclass
class RawPhysicalName:
    """Output of `RawPhysicalNamer.build`.

    `bare_name` is the table name alone (no schema). Use `qualified` for SQL
    `INSERT INTO ...` statements that need the schema prefix.
    """

    bare_name: str
    discriminator: str
    version: int
    truncated: bool

    @property
    def qualified(self) -> str:
        return f'raw."{self.bare_name}"'

    @property
    def stem(self) -> str:
        """`bare_name` without the version suffix.

        Useful to detect "all versions of resource X" via LIKE
        `<stem>__v%` queries.
        """
        return self.bare_name.rsplit(_RAW_VERSION_PREFIX, 1)[0]


class RawPhysicalNamer:
    """Build the schema-`raw` table name for a logical resource at version N.

    Naming is identity-preserving: same `(portal, source_id)` always produces
    the same `stem`, regardless of `version`. Bumping a version only swaps
    the `__v<N>` suffix.
    """

    def __init__(
        self,
        *,
        max_length: int = MAX_TABLE_NAME_LENGTH,
        discriminator_length: int = DISCRIMINATOR_LENGTH,
    ) -> None:
        if discriminator_length < 4 or discriminator_length > 32:
            raise ValueError("discriminator_length must be in [4, 32]")
        self._max_length = max_length
        self._discriminator_length = discriminator_length

    def build(
        self,
        portal: str,
        source_id: str,
        *,
        version: int,
        slug_hint: str = "",
    ) -> RawPhysicalName:
        if version < 1 or version > 999:
            raise ValueError("version must be in [1, 999]")

        portal_slug = _slugify(portal) or "unknown"
        slug_source = slug_hint or source_id
        slug = _slugify(slug_source) or "resource"
        discriminator = _stable_discriminator(
            portal, source_id, length=self._discriminator_length
        )

        version_suffix = f"{_RAW_VERSION_PREFIX}{version}"
        discrim_suffix = f"{_RAW_DISCRIM_SEP}{discriminator}"
        suffix_total_len = len(discrim_suffix) + len(version_suffix)

        # Body = "<portal>__<slug>"
        head_sep = _RAW_DISCRIM_SEP
        budget = self._max_length - len(portal_slug) - len(head_sep) - suffix_total_len
        if budget < 4:
            # Pathological portal name; truncate portal_slug to make room.
            portal_slug = portal_slug[: max(4, self._max_length - suffix_total_len - 8)]
            budget = self._max_length - len(portal_slug) - len(head_sep) - suffix_total_len

        truncated = len(slug) > budget
        slug = slug[:budget].rstrip("_") or "x"
        if _LEADING_DIGITS_RE.match(slug):
            slug = "t_" + slug
            slug = slug[:budget].rstrip("_") or "t_x"

        bare = f"{portal_slug}{head_sep}{slug}{discrim_suffix}{version_suffix}"
        if len(bare) > self._max_length:
            # Final clamp; preserve discriminator + version, eat from slug.
            overshoot = len(bare) - self._max_length
            slug = slug[: max(1, len(slug) - overshoot)].rstrip("_") or "x"
            bare = f"{portal_slug}{head_sep}{slug}{discrim_suffix}{version_suffix}"
        return RawPhysicalName(
            bare_name=bare,
            discriminator=discriminator,
            version=version,
            truncated=truncated,
        )


_default_raw_namer = RawPhysicalNamer()


def raw_table_name(
    portal: str,
    source_id: str,
    *,
    version: int,
    slug_hint: str = "",
) -> str:
    """Convenience helper returning the bare raw-schema table name."""
    return _default_raw_namer.build(
        portal, source_id, version=version, slug_hint=slug_hint
    ).bare_name


def raw_table_qualified(
    portal: str,
    source_id: str,
    *,
    version: int,
    slug_hint: str = "",
) -> str:
    """Schema-qualified name for SQL: `raw."<bare>"`."""
    return _default_raw_namer.build(
        portal, source_id, version=version, slug_hint=slug_hint
    ).qualified
