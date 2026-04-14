from __future__ import annotations

import pytest

from app.application.pipeline.nodes.cache import cache_reply_node


@pytest.mark.asyncio
async def test_cache_reply_node_preserves_cached_confidence_citations_and_warnings() -> None:
    result = await cache_reply_node(
        {
            "cached_result": {
                "answer": "Respuesta cacheada",
                "sources": [{"name": "IPC", "url": "https://datos.gob.ar", "portal": "datos.gob.ar"}],
                "confidence": 0.42,
                "citations": [{"claim": "inflación 2025", "source": "IPC"}],
                "warnings": ["Cifra no totalmente verificada"],
            }
        }
    )

    assert result["confidence"] == 0.42
    assert result["citations"] == [{"claim": "inflación 2025", "source": "IPC"}]
    assert result["warnings"] == ["Cifra no totalmente verificada"]


@pytest.mark.asyncio
async def test_cache_reply_node_defaults_when_cached_result_has_no_confidence() -> None:
    result = await cache_reply_node({"cached_result": {"answer": "Respuesta cacheada"}})

    assert result["confidence"] == 1.0
    assert result["citations"] == []
    assert result["warnings"] == []
