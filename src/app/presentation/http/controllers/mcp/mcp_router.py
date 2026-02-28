"""MCP proxy router — forwards requests to standalone MCP server containers."""
from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter
from pydantic import BaseModel

from app.infrastructure.mcp.mcp_client import MCPClient

router = APIRouter(prefix="/mcp", tags=["mcp"])


class ToolCallRequest(BaseModel):
    params: dict = {}


@router.get("/tools")
@inject
async def list_tools(
    client: FromDishka[MCPClient],
) -> list[dict]:
    return await client.list_tools()


@router.post("/tools/{server_name}/{tool_name}")
@inject
async def call_tool(
    server_name: str,
    tool_name: str,
    body: ToolCallRequest,
    client: FromDishka[MCPClient],
) -> dict:
    result = await client.call_tool(server_name, tool_name, body.params)
    return result if isinstance(result, dict) else {"result": result}


@router.get("/health/{server_name}")
@inject
async def server_health(
    server_name: str,
    client: FromDishka[MCPClient],
) -> dict:
    return await client.health_check(server_name)
