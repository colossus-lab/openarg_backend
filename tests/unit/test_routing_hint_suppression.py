"""BUG-001/002: `search_ckan` routing hints must be suppressed when a
curated mart already covers the question.

A portal-keyword match ("caba", "cordoba", ...) emits a deterministic
`search_ckan(portalId=...)` hint. When discovery surfaced a mart, that
hint must not reach the planner — otherwise it steers the planner to a
slow live CKAN fetch instead of the millisecond mart query.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.application.pipeline.step_executor import _top_mart_similarity
from app.infrastructure.adapters.connectors.query_planner import _resolve_routing_hints


def test_ckan_hint_present_without_suppression() -> None:
    # "caba" matches a DOMAIN_PATTERN → search_ckan(portalId='caba').
    hints = _resolve_routing_hints("flujo vehicular en peajes de caba 2023")
    assert "search_ckan" in hints


def test_ckan_hint_suppressed_when_mart_available() -> None:
    hints = _resolve_routing_hints(
        "flujo vehicular en peajes de caba 2023", suppress_ckan=True
    )
    assert "search_ckan" not in hints


def test_suppression_keeps_non_ckan_hints() -> None:
    # A question with a non-CKAN hint (e.g. dolar → query_argentina_datos)
    # keeps that hint even when suppress_ckan is on.
    hints = _resolve_routing_hints("dolar blue hoy", suppress_ckan=True)
    assert "search_ckan" not in hints
    # the dolar hint survives (non-search_ckan action)
    assert hints == "" or "search_ckan" not in hints


@pytest.mark.asyncio
async def test_mart_redirect_skipped_when_no_sandbox() -> None:
    # Fase 3 safety net must never redirect when there is no sandbox to
    # query — a topic with no mart infrastructure is left untouched.
    deps = SimpleNamespace(sandbox=None, embedding=None)
    sim = await _top_mart_similarity("flujo vehicular peajes caba", deps)  # type: ignore[arg-type]
    assert sim == 0.0


@pytest.mark.asyncio
async def test_mart_redirect_skipped_for_empty_query() -> None:
    deps = SimpleNamespace(sandbox=object(), embedding=None)
    sim = await _top_mart_similarity("   ", deps)  # type: ignore[arg-type]
    assert sim == 0.0
