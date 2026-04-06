from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, MemoryContext
from app.infrastructure.adapters.connectors.memory_agent import (
    _dedupe_preserve_order,
    update_memory,
)


class TestDedupePreserveOrder:
    def test_preserves_first_seen_order(self):
        assert _dedupe_preserve_order(["b", "a", "b", "c", "a"]) == ["b", "a", "c"]


@pytest.mark.asyncio
class TestUpdateMemory:
    async def test_update_memory_preserves_dataset_order_with_llm_response(self):
        llm = AsyncMock()
        llm.chat.return_value.content = json.dumps(
            {
                "summary": "Resumen",
                "keyFindings": ["Hallazgo"],
                "datasetsUsed": ["Dataset B", "Dataset A", "Dataset B", "Dataset C"],
                "suggestedFollowups": ["Siguiente"],
            }
        )
        current_memory = MemoryContext(
            turn_number=1,
            summaries=["Previo"],
            key_findings=[],
            datasets_used=["Dataset A", "Dataset B"],
            pending_questions=[],
        )
        plan = ExecutionPlan(query="consulta", intent="analizar", steps=[])

        updated = await update_memory(llm, current_memory, plan, [], "analisis")

        assert updated.datasets_used == ["Dataset A", "Dataset B", "Dataset C"]

    async def test_update_memory_preserves_dataset_order_in_fallback(self):
        llm = AsyncMock()
        llm.chat.side_effect = ValueError("boom")
        current_memory = MemoryContext(
            turn_number=1,
            summaries=["Previo"],
            key_findings=[],
            datasets_used=["Dataset A", "Dataset B"],
            pending_questions=[],
        )
        plan = ExecutionPlan(query="consulta", intent="analizar", steps=[])
        results = [
            DataResult(
                source="test:1",
                portal_name="Portal",
                portal_url="https://example.com",
                dataset_title="Dataset B",
                format="json",
                records=[],
                metadata={},
            ),
            DataResult(
                source="test:2",
                portal_name="Portal",
                portal_url="https://example.com",
                dataset_title="Dataset C",
                format="json",
                records=[],
                metadata={},
            ),
        ]

        updated = await update_memory(llm, current_memory, plan, results, "analisis")

        assert updated.datasets_used == ["Dataset A", "Dataset B", "Dataset C"]
