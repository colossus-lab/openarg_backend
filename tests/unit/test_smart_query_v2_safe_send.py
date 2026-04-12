"""Router-level regression tests for the WebSocket v2 serialization path.

FIX-017 moved every ``ws.send_json(...)`` call in ``smart_query_v2_router``
through a local helper ``_safe_send_json`` that pre-serializes the payload
with the project-wide defensive encoder. These tests exercise that helper
against a minimal fake WebSocket and prove that the exact shapes observed
in the 2026-04-12 prod regression — a ``complete`` event carrying
``datetime`` / ``date`` / ``Decimal`` / ``UUID`` values in ``chart_data``
— reach the wire as valid JSON instead of crashing Starlette's internal
``json.dumps`` call.

These complement ``test_json_safe.py``: that file pins the helper in
isolation; this file pins the helper *at the integration point the user
sees* — the ``_safe_send_json`` coroutine in the smart-query router.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import json
import uuid
from typing import Any

import pytest

from app.presentation.http.controllers.query.smart_query_v2_router import (
    _safe_send_json,
)


class _FakeWebSocket:
    """Minimal stand-in for ``starlette.websockets.WebSocket``.

    We only need the ``send_text`` method — ``_safe_send_json`` routes
    everything through ``send_text`` after pre-serialization, so a
    fixture recording what text it received is enough to prove the
    payload survived the encoder.
    """

    def __init__(self) -> None:
        self.sent_text: list[str] = []
        self.sent_json_calls: int = 0  # must stay 0 — proves we bypass send_json

    async def send_text(self, text: str) -> None:
        self.sent_text.append(text)

    async def send_json(self, _data: Any) -> None:  # pragma: no cover
        # Defensive — if _safe_send_json ever routes through send_json,
        # we want the test to flag it loudly so the regression can't
        # come back silently. Starlette's send_json has no default=
        # hook, which is exactly the bug FIX-017 exists to prevent.
        self.sent_json_calls += 1
        raise AssertionError("_safe_send_json must use send_text, never send_json — FIX-017")


@pytest.fixture()
def ws() -> _FakeWebSocket:
    return _FakeWebSocket()


# ---------------------------------------------------------------------------
# The exact regression: "Mostrame la evolución de las reservas del BCRA"
# ---------------------------------------------------------------------------


async def test_safe_send_json_accepts_complete_event_with_datetime(
    ws: _FakeWebSocket,
) -> None:
    """The payload the browser receives when the pipeline finishes a
    fresh (non-cached) reservas-BCRA query. Before FIX-017 this exact
    dict crashed Starlette's ``send_json`` because the series-tiempo /
    sandbox path had slipped a ``date`` into the chart series."""
    payload = {
        "type": "complete",
        "answer": "Reservas internacionales del BCRA: USD 25.100 M al 2026-04-12.",
        "sources": [{"id": "bcra", "name": "Banco Central de la República Argentina"}],
        "chart_data": {
            "type": "line_chart",
            "title": "Evolución reservas BCRA",
            "xKey": "fecha",
            "yKeys": ["valor"],
            "data": [
                {"fecha": _dt.date(2026, 4, 1), "valor": decimal.Decimal("25000.50")},
                {"fecha": _dt.date(2026, 4, 5), "valor": decimal.Decimal("25050.10")},
                {"fecha": _dt.date(2026, 4, 12), "valor": decimal.Decimal("25100.75")},
            ],
        },
        "map_data": None,
        "confidence": 0.92,
        "citations": [],
        "documents": None,
        "warnings": [],
    }

    await _safe_send_json(ws, payload)  # type: ignore[arg-type]

    assert len(ws.sent_text) == 1
    sent = json.loads(ws.sent_text[0])
    assert sent["type"] == "complete"
    assert sent["chart_data"]["data"][0]["fecha"] == "2026-04-01"
    assert sent["chart_data"]["data"][0]["valor"] == "25000.50"
    assert sent["confidence"] == 0.92


async def test_safe_send_json_handles_aware_datetime(
    ws: _FakeWebSocket,
) -> None:
    """Timezone-aware ``datetime`` values (e.g. ``datetime.now(UTC)`` from
    a connector's ``fetched_at`` metadata) survive the round trip."""
    payload = {
        "type": "complete",
        "answer": "ok",
        "metadata": {"fetched_at": _dt.datetime(2026, 4, 12, 1, 49, 0, tzinfo=_dt.UTC)},
    }

    await _safe_send_json(ws, payload)  # type: ignore[arg-type]

    parsed = json.loads(ws.sent_text[0])
    assert parsed["metadata"]["fetched_at"].startswith("2026-04-12T01:49:00")


async def test_safe_send_json_handles_uuid_in_citations(
    ws: _FakeWebSocket,
) -> None:
    """A ``UUID`` from ``query_dataset_links`` rows reaching the
    ``citations`` field used to crash the complete event. Now handled."""
    payload = {
        "type": "complete",
        "answer": "ok",
        "citations": [{"id": uuid.UUID("12345678-1234-5678-1234-567812345678"), "source": "bcra"}],
    }

    await _safe_send_json(ws, payload)  # type: ignore[arg-type]

    parsed = json.loads(ws.sent_text[0])
    assert parsed["citations"][0]["id"] == "12345678-1234-5678-1234-567812345678"


async def test_safe_send_json_falls_back_to_to_json_safe_on_unknown_type(
    ws: _FakeWebSocket,
) -> None:
    """If some exotic object slips past ``json_default``, the helper's
    second try uses ``to_json_safe`` which ``str()``-coerces unknowns.
    The WebSocket send must never raise — the ``complete`` event is the
    user-visible reply and a crash here blanks the chat bubble."""

    class _Weird:
        def __repr__(self) -> str:
            return "<weird-object>"

    payload = {"type": "complete", "answer": "ok", "meta": {"weird": _Weird()}}

    await _safe_send_json(ws, payload)  # type: ignore[arg-type]

    parsed = json.loads(ws.sent_text[0])
    assert parsed["meta"]["weird"] == "<weird-object>"


async def test_safe_send_json_does_not_touch_send_json(
    ws: _FakeWebSocket,
) -> None:
    """Regression guard: the helper must never delegate to ``send_json``,
    even on purely-primitive payloads, because a future maintainer
    reintroducing ``send_json`` would silently bring back the FIX-017
    bug. The fake WebSocket's ``send_json`` raises to catch any slip."""
    await _safe_send_json(ws, {"type": "status", "step": "planning"})  # type: ignore[arg-type]

    assert ws.sent_json_calls == 0
    assert len(ws.sent_text) == 1
