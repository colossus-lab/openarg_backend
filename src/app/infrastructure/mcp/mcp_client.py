"""HTTP client for calling remote MCP servers."""
from __future__ import annotations

import logging
from typing import Any

import httpx

from app.infrastructure.mcp.exceptions import MCPNetworkError, classify_http_error

logger = logging.getLogger(__name__)


class MCPClient:
    """
    Calls tools on remote MCP servers via HTTP.

    MCP servers expose:
    - GET  /tools              — list available tools
    - POST /execute/{tool_name} — execute a tool
    - GET  /health             — health check
    """

    def __init__(self, servers: dict[str, str], timeout: float = 30.0) -> None:
        """
        Args:
            servers: Mapping of server_name → base_url, e.g.
                     {"series_tiempo": "http://mcp-series:8091"}
            timeout: HTTP request timeout in seconds.
        """
        self._servers = servers
        self._client = httpx.AsyncClient(timeout=timeout)

    async def call_tool(
        self,
        server_name: str,
        tool_name: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        base_url = self._servers.get(server_name)
        if not base_url:
            raise ValueError(f"Unknown MCP server: {server_name}")

        url = f"{base_url.rstrip('/')}/execute/{tool_name}"

        try:
            resp = await self._client.post(url, json={"params": params})
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict) and data.get("success") is False:
                raise ValueError(f"MCP tool '{server_name}.{tool_name}' failed: {data.get('error')}")

            return data.get("result") if isinstance(data, dict) and "result" in data else data

        except httpx.HTTPStatusError as e:
            raise classify_http_error(
                e.response.status_code,
                f"{server_name}.{tool_name}: HTTP {e.response.status_code}",
                server_name,
            ) from e
        except httpx.RequestError as e:
            raise MCPNetworkError(
                f"{server_name}.{tool_name}: {e}", server_name
            ) from e

    async def list_tools(self, server_name: str | None = None) -> list[dict]:
        """List tools from one server or all servers."""
        targets = (
            {server_name: self._servers[server_name]}
            if server_name and server_name in self._servers
            else self._servers
        )
        all_tools: list[dict] = []
        for name, base_url in targets.items():
            try:
                resp = await self._client.get(f"{base_url.rstrip('/')}/tools")
                resp.raise_for_status()
                data = resp.json()
                tools = data.get("tools", []) if isinstance(data, dict) else []
                all_tools.extend(tools)
            except Exception:
                logger.warning("Failed to list tools from %s (%s)", name, base_url)
        return all_tools

    async def health_check(self, server_name: str) -> dict:
        base_url = self._servers.get(server_name)
        if not base_url:
            return {"status": "unknown", "error": f"No server: {server_name}"}
        try:
            resp = await self._client.get(f"{base_url.rstrip('/')}/health")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def close(self) -> None:
        await self._client.aclose()
