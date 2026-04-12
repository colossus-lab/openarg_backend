"""Unit tests for the defensive JSON encoder introduced in FIX-017.

The encoder's job is to absorb the non-primitive Python types that
regularly end up inside finalized pipeline state (datetime, Decimal,
UUID, bytes, set, Path, dataclasses, Pydantic-ish objects) so that the
three serialization sinks — Redis cache write, semantic cache insert,
and the WebSocket v2 ``complete`` event — cannot crash the user-visible
reply. Genuinely unknown types must still raise so real bugs surface.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import decimal
import json
import pathlib
import uuid

import pytest

from app.infrastructure.serialization.json_safe import (
    json_default,
    safe_dumps,
    to_json_safe,
)

# ---------------------------------------------------------------------------
# json_default — type-by-type coverage
# ---------------------------------------------------------------------------


def test_datetime_is_serialized_as_isoformat() -> None:
    value = _dt.datetime(2026, 4, 12, 1, 49, 0, tzinfo=_dt.UTC)
    assert json_default(value) == value.isoformat()


def test_date_is_serialized_as_isoformat() -> None:
    value = _dt.date(2026, 4, 12)
    assert json_default(value) == "2026-04-12"


def test_time_is_serialized_as_isoformat() -> None:
    value = _dt.time(14, 30, 0)
    assert json_default(value) == "14:30:00"


def test_timedelta_is_serialized_as_total_seconds() -> None:
    value = _dt.timedelta(hours=1, minutes=30)
    assert json_default(value) == pytest.approx(5400.0)


def test_decimal_is_serialized_as_string_preserving_precision() -> None:
    value = decimal.Decimal("123456789.987654321")
    result = json_default(value)
    assert result == "123456789.987654321"


def test_uuid_is_serialized_as_string() -> None:
    value = uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert json_default(value) == "12345678-1234-5678-1234-567812345678"


def test_path_is_serialized_as_string() -> None:
    value = pathlib.PurePosixPath("/opt/docker/openarg/.env")
    assert json_default(value) == "/opt/docker/openarg/.env"


def test_bytes_are_decoded_replacing_invalid_sequences() -> None:
    assert json_default(b"hola") == "hola"
    # Invalid UTF-8 must not raise.
    assert json_default(b"\xff\xfe") == "\ufffd\ufffd"


def test_bytearray_and_memoryview_are_decoded() -> None:
    assert json_default(bytearray(b"abc")) == "abc"
    assert json_default(memoryview(b"abc")) == "abc"


def test_set_and_frozenset_become_lists() -> None:
    assert sorted(json_default({"a", "b"})) == ["a", "b"]
    assert sorted(json_default(frozenset({1, 2}))) == [1, 2]


def test_dataclass_is_converted_to_dict() -> None:
    @dataclasses.dataclass
    class _Point:
        x: int
        y: int

    assert json_default(_Point(1, 2)) == {"x": 1, "y": 2}


def test_object_with_model_dump_uses_it() -> None:
    class _Shim:
        def model_dump(self) -> dict[str, int]:
            return {"v": 42}

    assert json_default(_Shim()) == {"v": 42}


def test_object_with_dunder_json_uses_it() -> None:
    class _Shim:
        def __json__(self) -> dict[str, str]:
            return {"kind": "custom"}

    assert json_default(_Shim()) == {"kind": "custom"}


def test_object_with_to_dict_uses_it() -> None:
    class _Shim:
        def to_dict(self) -> dict[str, int]:
            return {"n": 7}

    assert json_default(_Shim()) == {"n": 7}


def test_unknown_type_raises_type_error() -> None:
    class _Unknown:
        pass

    with pytest.raises(TypeError, match="not JSON serializable"):
        json_default(_Unknown())


# ---------------------------------------------------------------------------
# safe_dumps — end-to-end behaviour via json.dumps
# ---------------------------------------------------------------------------


def test_safe_dumps_handles_the_FIX_017_regression_payload() -> None:
    """The exact shape that used to crash the WebSocket v2 ``complete``
    event: a dict with records containing datetime and Decimal values."""
    payload = {
        "type": "complete",
        "answer": "Reservas del BCRA: USD 25.000 M al 2026-04-01.",
        "chart_data": {
            "type": "line_chart",
            "data": [
                {"fecha": _dt.date(2026, 4, 1), "valor": decimal.Decimal("25000.50")},
                {"fecha": _dt.date(2026, 4, 2), "valor": decimal.Decimal("25100.75")},
            ],
        },
    }

    serialized = safe_dumps(payload, ensure_ascii=False)
    parsed = json.loads(serialized)

    assert parsed["type"] == "complete"
    assert parsed["chart_data"]["data"][0]["fecha"] == "2026-04-01"
    assert parsed["chart_data"]["data"][0]["valor"] == "25000.50"


def test_safe_dumps_preserves_caller_default_and_falls_through() -> None:
    sentinel = object()

    def _custom_default(obj: object) -> str:
        if obj is sentinel:
            return "SENTINEL"
        raise TypeError("not handled here")

    payload = {"a": sentinel, "b": _dt.date(2026, 1, 1)}
    serialized = safe_dumps(payload, default=_custom_default)
    parsed = json.loads(serialized)

    assert parsed == {"a": "SENTINEL", "b": "2026-01-01"}


def test_safe_dumps_raises_on_truly_unknown_type() -> None:
    class _Unknown:
        pass

    with pytest.raises(TypeError):
        safe_dumps({"bad": _Unknown()})


# ---------------------------------------------------------------------------
# to_json_safe — pre-normalisation for Starlette send_json
# ---------------------------------------------------------------------------


def test_to_json_safe_walks_nested_structures() -> None:
    payload = {
        "when": _dt.datetime(2026, 4, 12, tzinfo=_dt.UTC),
        "items": [
            {"id": uuid.UUID("12345678-1234-5678-1234-567812345678"), "n": decimal.Decimal("1.5")},
            {"path": pathlib.PurePosixPath("/tmp/x"), "tags": {"a", "b"}},
        ],
    }

    normalised = to_json_safe(payload)

    # The return value must be json.dumps-able without any default encoder.
    serialized = json.dumps(normalised)
    parsed = json.loads(serialized)

    assert parsed["when"].startswith("2026-04-12")
    assert parsed["items"][0]["id"] == "12345678-1234-5678-1234-567812345678"
    assert parsed["items"][0]["n"] == "1.5"
    assert parsed["items"][1]["path"] == "/tmp/x"
    assert sorted(parsed["items"][1]["tags"]) == ["a", "b"]


def test_to_json_safe_stringifies_unknown_types_instead_of_raising() -> None:
    """``to_json_safe`` is the pre-serialize-for-Starlette helper, so it
    must *never* raise — an unknown object falls back to ``str(obj)``
    rather than crashing the WebSocket send."""

    class _Weird:
        def __repr__(self) -> str:
            return "<weird>"

    normalised = to_json_safe({"x": _Weird()})
    json.dumps(normalised)  # must not raise
    assert normalised["x"] == "<weird>"


def test_to_json_safe_is_identity_on_primitives() -> None:
    assert to_json_safe(None) is None
    assert to_json_safe(True) is True
    assert to_json_safe(42) == 42
    assert to_json_safe(3.14) == pytest.approx(3.14)
    assert to_json_safe("hola") == "hola"


def test_to_json_safe_coerces_non_string_dict_keys() -> None:
    payload = {1: "one", _dt.date(2026, 1, 1): "new-year"}
    normalised = to_json_safe(payload)
    assert normalised == {"1": "one", "2026-01-01": "new-year"}
