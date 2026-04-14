from __future__ import annotations

from app.presentation.http.controllers.conversations.conversations_router import (
    MessageCreate,
    MessageResponse,
)


def test_message_create_accepts_confidence() -> None:
    payload = MessageCreate(
        role="assistant",
        content="respuesta",
        confidence=0.72,
        ui_trace={"quality": {"confidence": 0.72, "sourceCount": 2, "portalCount": 1}},
    )

    assert payload.confidence == 0.72
    assert payload.ui_trace == {"quality": {"confidence": 0.72, "sourceCount": 2, "portalCount": 1}}


def test_message_response_exposes_confidence() -> None:
    response = MessageResponse(
        id="msg-1",
        conversation_id="conv-1",
        role="assistant",
        content="respuesta",
        sources=[],
        confidence=0.72,
        ui_trace={"pipeline": {"phases": ["planning"], "thinking": []}},
        created_at="2026-04-12T00:00:00Z",
    )

    dumped = response.model_dump()

    assert dumped["confidence"] == 0.72
    assert dumped["ui_trace"] == {"pipeline": {"phases": ["planning"], "thinking": []}}
