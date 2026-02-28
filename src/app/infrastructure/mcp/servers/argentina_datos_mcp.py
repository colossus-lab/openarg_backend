"""Standalone MCP Server for ArgentinaDatos API."""
from __future__ import annotations

import logging
import os

import httpx

from app.infrastructure.mcp.base_server import MCPServer

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("ARGENTINA_DATOS_BASE_URL", "https://api.argentinadatos.com/v1")

_client = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "OpenArg-MCP/1.0"})


async def get_dolar(casa: str | None = None) -> dict:
    path = f"/cotizaciones/dolares/{casa}" if casa else "/cotizaciones/dolares"
    resp = await _client.get(f"{BASE_URL}{path}")
    resp.raise_for_status()
    data = resp.json()

    if not data or not isinstance(data, list):
        return {"result": None}

    recent = data[-60:]
    return {
        "records": [
            {
                "fecha": d["fecha"],
                "casa": d.get("casa", casa or ""),
                "compra": d.get("compra"),
                "venta": d.get("venta"),
            }
            for d in recent
        ],
        "total_records": len(recent),
    }


async def get_riesgo_pais(ultimo: bool = False) -> dict:
    path = "/finanzas/indices/riesgo-pais/ultimo" if ultimo else "/finanzas/indices/riesgo-pais"
    resp = await _client.get(f"{BASE_URL}{path}")
    resp.raise_for_status()
    data = resp.json()

    items = [data] if isinstance(data, dict) else data
    if not items:
        return {"result": None}

    recent = items[-60:]
    return {
        "records": [{"fecha": d["fecha"], "riesgo_pais": d["valor"]} for d in recent],
        "total_records": len(recent),
    }


async def get_inflacion() -> dict:
    resp = await _client.get(f"{BASE_URL}/finanzas/indices/inflacion")
    resp.raise_for_status()
    data = resp.json()

    if not data or not isinstance(data, list):
        return {"result": None}

    return {
        "records": [{"fecha": d["fecha"], "inflacion": d["valor"]} for d in data],
        "total_records": len(data),
    }


def create_server() -> MCPServer:
    server = MCPServer(
        server_name="argentina_datos",
        description="ArgentinaDatos API — Dollar rates, country risk, inflation",
        port=int(os.getenv("MCP_PORT", "8093")),
    )

    server.register_tool(
        name="get_dolar",
        description="Get Argentine dollar exchange rates (blue, oficial, cripto, etc.)",
        parameters={
            "type": "object",
            "properties": {
                "casa": {"type": "string", "description": "e.g. blue, oficial, cripto"},
            },
            "required": [],
        },
        handler=get_dolar,
    )

    server.register_tool(
        name="get_riesgo_pais",
        description="Get Argentina's country risk index (EMBI+)",
        parameters={
            "type": "object",
            "properties": {
                "ultimo": {"type": "boolean", "default": False},
            },
            "required": [],
        },
        handler=get_riesgo_pais,
    )

    server.register_tool(
        name="get_inflacion",
        description="Get monthly inflation data for Argentina",
        parameters={"type": "object", "properties": {}, "required": []},
        handler=get_inflacion,
    )

    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    srv = create_server()
    srv.run()
