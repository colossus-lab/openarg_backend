from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.application.pipeline.connectors.cache_table_selection import (
    prefer_consolidated_table,
    table_base_name,
    table_priority,
)
from app.application.pipeline.connectors.ckan import search_cached_tables
from app.application.pipeline.connectors.sandbox import discover_tables_by_vector_search
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo


class TestCacheTableSelection:
    def test_table_base_name_strips_resource_schema_and_group_suffixes(self):
        assert table_base_name("cache_energia_canon_r1234567890") == "cache_energia_canon"
        assert table_base_name("cache_energia_canon_sdeadbeef") == "cache_energia_canon"
        assert table_base_name("cache_energia_canon_gdeadbeef") == "cache_energia_canon"

    def test_prefer_consolidated_table_picks_group_before_resource(self):
        available = [
            "cache_energia_canon_r1234567890",
            "cache_energia_canon_gdeadbeef",
        ]
        assert (
            prefer_consolidated_table("cache_energia_canon_r1234567890", available)
            == "cache_energia_canon_gdeadbeef"
        )
        assert table_priority("cache_energia_canon_gdeadbeef") < table_priority(
            "cache_energia_canon_r1234567890"
        )


class TestCachedTablePreferenceInConnectors:
    @pytest.mark.asyncio
    async def test_search_cached_tables_prefers_consolidated_match(self):
        sandbox = SimpleNamespace()
        sandbox.list_cached_tables = AsyncMock(
            return_value=[
                CachedTableInfo(
                    table_name="cache_energia_canon_r1234567890",
                    dataset_id="ds-1",
                    row_count=10,
                    columns=["a"],
                ),
                CachedTableInfo(
                    table_name="cache_energia_canon_gdeadbeef",
                    dataset_id="",
                    row_count=20,
                    columns=["a"],
                ),
            ]
        )
        sandbox.execute_readonly = AsyncMock(
            return_value=SimpleNamespace(error=None, rows=[{"a": 1}])
        )

        results = await search_cached_tables("canon", sandbox, limit=5)

        assert len(results) == 1
        assert results[0].source == "cache:cache_energia_canon_gdeadbeef"

    @pytest.mark.asyncio
    async def test_discover_tables_by_vector_search_prefers_consolidated_name(self):
        cached_tables = [
            CachedTableInfo(
                table_name="cache_energia_canon_r1234567890",
                dataset_id="ds-1",
                row_count=10,
                columns=["a"],
            ),
            CachedTableInfo(
                table_name="cache_energia_canon_gdeadbeef",
                dataset_id="",
                row_count=20,
                columns=["a"],
            ),
        ]
        embedding = SimpleNamespace(embed=AsyncMock(return_value=[0.1, 0.2]))
        vector_search = SimpleNamespace(
            search_datasets=AsyncMock(return_value=[SimpleNamespace(dataset_id="ds-1")])
        )

        tables = await discover_tables_by_vector_search(
            "canon",
            cached_tables,
            embedding,
            vector_search,
        )

        assert tables == ["cache_energia_canon_gdeadbeef"]
