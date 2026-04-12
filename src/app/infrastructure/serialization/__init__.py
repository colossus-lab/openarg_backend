"""Shared (de)serialization helpers.

Currently exposes :mod:`json_safe` — a defensive JSON encoder that handles
the non-primitive types that regularly end up inside pipeline state
(``datetime``, ``Decimal``, ``UUID``, ``bytes``, ``set``, ``Path``...).

See ``specs/FIX_BACKLOG.md#FIX-017`` for the incident that motivated it.
"""

from app.infrastructure.serialization.json_safe import (
    json_default,
    safe_dumps,
    to_json_safe,
)

__all__ = ["json_default", "safe_dumps", "to_json_safe"]
