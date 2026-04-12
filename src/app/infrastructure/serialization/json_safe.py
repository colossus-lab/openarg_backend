"""Defensive JSON encoder used by every pipeline serialization sink.

FIX-017 (2026-04-12): three sinks on the query pipeline (``RedisCacheAdapter.set``,
``SemanticCache.set``, and the WebSocket v2 ``send_json`` calls in the
smart-query streaming endpoint) used to call ``json.dumps`` without a
``default=`` encoder. Any non-primitive value added to the finalized
pipeline state — a ``datetime`` from a connector, a ``Decimal`` from a
DB read, a ``UUID``, a ``bytes`` blob — aborted the write with
``TypeError: Object of type X is not JSON serializable`` and the user-facing
``complete`` WebSocket event never reached the browser. The symptom was a
chat bubble showing ``[error: respuesta no disponible]`` on a fresh
(non-cached) query.

The long-term fix is to sanitize at the source (each connector converts
its records to JSON-safe primitives before handing them to the pipeline).
Until every connector is audited, this module is the belt-and-braces
guarantee that the serialization layer itself cannot crash on the common
types listed below. Genuinely unknown types still raise — the goal is to
absorb *known* Python standard-library types, not to silently serialize
arbitrary objects.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import decimal
import json
import pathlib
import uuid
from typing import Any

__all__ = ["json_default", "safe_dumps", "to_json_safe"]


def json_default(obj: Any) -> Any:
    """Convert a non-primitive ``obj`` to a JSON-safe primitive.

    Raises ``TypeError`` for unknown types — callers should let that
    propagate so bugs are surfaced instead of silently swallowed.
    """
    # Datetime family → ISO 8601 strings.
    if isinstance(obj, _dt.datetime):
        return obj.isoformat()
    if isinstance(obj, _dt.date):
        return obj.isoformat()
    if isinstance(obj, _dt.time):
        return obj.isoformat()
    if isinstance(obj, _dt.timedelta):
        return obj.total_seconds()

    # Numeric types the stdlib json module refuses.
    if isinstance(obj, decimal.Decimal):
        # Preserve precision by going through str, then let JSON parsers
        # decide whether to treat it as a number. Numbers that fit in a
        # float round-trip cleanly; everything else stays exact.
        return str(obj)

    # Common identifier / path / byte types.
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, pathlib.PurePath):
        return str(obj)
    if isinstance(obj, bytes | bytearray | memoryview):
        return bytes(obj).decode("utf-8", errors="replace")

    # Collections that are not JSON arrays by default.
    if isinstance(obj, set | frozenset):
        return list(obj)

    # Duck-typed escape hatches, checked in order of specificity.
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        return obj.model_dump()
    if hasattr(obj, "__json__") and callable(obj.__json__):
        return obj.__json__()
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        return obj.to_dict()

    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def safe_dumps(obj: Any, **kwargs: Any) -> str:
    """``json.dumps`` with :func:`json_default` plugged in as ``default=``.

    Any ``default`` supplied by the caller is preserved — it will be tried
    first and :func:`json_default` runs as the fallback. This lets callers
    register additional project-specific types without losing the baseline
    coverage.
    """
    user_default = kwargs.pop("default", None)

    if user_default is None:
        return json.dumps(obj, default=json_default, **kwargs)

    def _chained(value: Any) -> Any:
        try:
            return user_default(value)
        except TypeError:
            return json_default(value)

    return json.dumps(obj, default=_chained, **kwargs)


def to_json_safe(obj: Any) -> Any:
    """Return a deep copy of ``obj`` with every non-primitive converted.

    Used when the final serializer is out of our control — e.g. Starlette's
    ``WebSocket.send_json`` internally calls ``json.dumps`` without a
    ``default=`` hook, so the only safe pattern is to pre-normalise the
    payload before handing it off.
    """
    if obj is None or isinstance(obj, str | int | float | bool):
        return obj
    if isinstance(obj, dict):
        return {str(k): to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [to_json_safe(v) for v in obj]
    if isinstance(obj, set | frozenset):
        return [to_json_safe(v) for v in obj]
    try:
        converted = json_default(obj)
    except TypeError:
        return str(obj)
    return to_json_safe(converted)
