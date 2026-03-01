from __future__ import annotations

import logging
from datetime import UTC, datetime

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.infrastructure.mcp.mcp_client import MCPClient

logger = logging.getLogger(__name__)


class CKANSearchAdapter(ICKANSearchConnector):
    def __init__(self, mcp_client: MCPClient) -> None:
        self._mcp = mcp_client

    async def search_datasets(
        self,
        query: str,
        portal_id: str | None = None,
        rows: int = 10,
    ) -> list[DataResult]:
        try:
            params: dict = {"query": query, "rows": rows}
            if portal_id:
                params["portal_id"] = portal_id

            result = await self._mcp.call_tool(
                "ckan", "search_datasets", params
            )

            items = result.get("results", [])
            now = datetime.now(UTC).isoformat()
            data_results: list[DataResult] = []
            for item in items:
                data_results.append(
                    DataResult(
                        source=f"ckan:{item.get('portal_id', '')}",
                        portal_name=item.get("portal", ""),
                        portal_url=item.get("url", ""),
                        dataset_title=item.get("title", ""),
                        format="json",
                        records=item.get("records", []),
                        metadata={
                            "total_records": item.get("record_count", len(item.get("records", []))),
                            "fetched_at": now,
                            "description": item.get("description", ""),
                        },
                    )
                )
            return data_results
        except Exception:
            logger.warning("CKAN search_datasets failed", exc_info=True)
            return []

    async def query_datastore(
        self,
        portal_id: str,
        resource_id: str,
        q: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        try:
            params: dict = {
                "portal_id": portal_id,
                "resource_id": resource_id,
                "limit": limit,
            }
            if q:
                params["q"] = q

            result = await self._mcp.call_tool(
                "ckan", "query_datastore", params
            )
            return result.get("records", [])
        except Exception:
            logger.warning("CKAN query_datastore failed", exc_info=True)
            return []
