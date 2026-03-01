"""
Base MCP Server for OpenArg.

Standalone FastAPI-based MCP tool server.
Each MCP server runs as an independent Docker container.
"""
from __future__ import annotations

import logging
import os
import secrets
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _verify_mcp_token(request: Request) -> None:
    """Validate MCP_AUTH_TOKEN header for inter-service auth."""
    expected = os.getenv("MCP_AUTH_TOKEN", "")
    if not expected:
        return  # No token configured — allow all (dev mode)
    provided = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid or missing MCP auth token")


class MCPExecuteRequest(BaseModel):
    params: dict[str, Any] = {}


class MCPExecuteResponse(BaseModel):
    success: bool
    result: dict[str, Any] | None = None
    error: str | None = None


class MCPServer:
    """
    Base MCP Server — each subclass runs as a standalone FastAPI process.

    Usage:
        server = MCPServer("series_tiempo", "Series de Tiempo API tools", port=8091)
        server.register_tool("search_series", "Search time series", schema, handler)
        server.run()  # starts uvicorn
    """

    def __init__(self, server_name: str, description: str, port: int = 8080) -> None:
        self.server_name = server_name
        self.description = description
        self.port = port
        self._tools: dict[str, dict] = {}
        self.app = FastAPI(
            title=f"{server_name} MCP Server",
            description=description,
            version="1.0.0",
        )
        self._setup_routes()

    def register_tool(
        self,
        name: str,
        description: str,
        parameters: dict[str, Any],
        handler: Callable,
    ) -> None:
        self._tools[name] = {
            "name": name,
            "description": description,
            "parameters": parameters,
            "handler": handler,
        }
        logger.info("[%s] Registered tool: %s", self.server_name, name)

    def _setup_routes(self) -> None:
        @self.app.get("/")
        async def root():
            return {
                "server": self.server_name,
                "description": self.description,
                "version": "1.0.0",
            }

        @self.app.get("/health")
        async def health():
            return {
                "status": "healthy",
                "server": self.server_name,
                "tools_registered": len(self._tools),
            }

        @self.app.get("/tools")
        async def list_tools():
            return {
                "server": self.server_name,
                "tools": [
                    {
                        "name": t["name"],
                        "qualified_name": f"{self.server_name}.{t['name']}",
                        "description": t["description"],
                        "parameters": t["parameters"],
                    }
                    for t in self._tools.values()
                ],
            }

        @self.app.post("/execute/{tool_name}", response_model=MCPExecuteResponse, dependencies=[Depends(_verify_mcp_token)])
        async def execute_tool(tool_name: str, request: MCPExecuteRequest):
            if tool_name not in self._tools:
                raise HTTPException(
                    status_code=404,
                    detail=f"Tool '{tool_name}' not found on '{self.server_name}'",
                )
            tool = self._tools[tool_name]
            try:
                result = await tool["handler"](**request.params)
                return MCPExecuteResponse(success=True, result=result)
            except Exception as e:
                logger.exception("[%s] Error executing %s", self.server_name, tool_name)
                return MCPExecuteResponse(success=False, error=str(e))

    def run(self, host: str = "0.0.0.0") -> None:
        import uvicorn

        logger.info("Starting %s MCP Server on %s:%d", self.server_name, host, self.port)
        uvicorn.run(self.app, host=host, port=self.port)
