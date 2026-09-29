"""El MCP público de punta a punta por HTTP, con el backend falso.

Necesita `mcp` (no es dependencia del proyecto; va en la imagen del MCP),
así que sin él se saltea. Para correrlo:
`uv pip install -r mcp_publico/requirements.txt` y pytest.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("mcp")

import httpx  # noqa: E402
import httpx2  # noqa: E402
from mcp.client import Client  # noqa: E402
from mcp.client.streamable_http import streamable_http_client  # noqa: E402
from mcp_publico import server as mcp_server  # noqa: E402

KEY = "oarg_sk_" + "a" * 43


class FakeBackend:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.status = 200
        self.body: dict[str, Any] = {
            "answer": "La tasa de desempleo fue 7,6 %.",
            "sources": [
                {"name": "EPH", "url": "https://datos.gob.ar/eph", "portal": "datos_gob_ar"}
            ],
            "warnings": [],
            "usage": {"requests_remaining_today": 9},
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/api/v1/fuentes":
            return httpx.Response(
                200, json={"total_datasets": 10, "fuentes": [{"portal": "caba", "datasets": 10}]}
            )
        return httpx.Response(self.status, json=self.body)


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend()
    monkeypatch.setattr(
        mcp_server,
        "_client_factory",
        lambda: httpx.AsyncClient(
            base_url="http://backend", transport=httpx.MockTransport(fake.handler)
        ),
    )
    return fake


async def _session(headers: dict[str, str]):  # type: ignore[no-untyped-def]
    app = mcp_server.create_app()
    http = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app),
        base_url="http://localhost:8000",
        headers={"host": "localhost:8000", **headers},
    )
    return app, http


async def _call(tool: str, args: dict[str, Any], headers: dict[str, str]) -> Any:
    app, http = await _session(headers)
    async with mcp_server.server.session_manager.run(), http:
        transport = streamable_http_client(
            "http://localhost:8000/mcp", http_client=http, terminate_on_close=False
        )
        async with Client(transport) as client:
            if tool == "__list__":
                return await client.list_tools()
            return await client.call_tool(tool, args)


def _text(result: Any) -> str:
    return "\n".join(c.text for c in result.content if getattr(c, "text", None))


async def test_tools_are_listed_without_a_key(backend: FakeBackend) -> None:
    result = await _call("__list__", {}, {})
    names = {t.name for t in result.tools}
    assert names == {"consultar_datos_publicos", "listar_fuentes"}
    for tool in result.tools:
        assert "\n    " not in (tool.description or ""), f"{tool.name}: descripción con sangría"
    assert backend.requests == []


async def test_question_without_key_explains_how_to_get_one(backend: FakeBackend) -> None:
    result = await _call("consultar_datos_publicos", {"pregunta": "desempleo"}, {})
    assert result.is_error
    assert "openarg.org/desarrolladores" in _text(result)
    assert backend.requests == []


async def test_question_forwards_key_and_client_ip(backend: FakeBackend) -> None:
    result = await _call(
        "consultar_datos_publicos",
        {"pregunta": "tasa de desempleo"},
        {"Authorization": f"Bearer {KEY}", "X-Forwarded-For": "181.1.2.3"},
    )
    assert not result.is_error, _text(result)
    text = _text(result)
    assert "7,6 %" in text and "datos.gob.ar/eph" in text and "restantes hoy: 9" in text
    sent = backend.requests[0]
    assert sent.url.path == "/api/v1/ask"
    assert sent.headers["authorization"] == f"Bearer {KEY}"
    assert sent.headers["x-forwarded-for"] == "181.1.2.3"
    assert json.loads(sent.content) == {"question": "tasa de desempleo"}


async def test_quota_exhausted_is_a_spanish_tool_error(backend: FakeBackend) -> None:
    backend.status = 429
    backend.body = {"detail": "Rate limit exceeded: 10 requests per day"}
    result = await _call(
        "consultar_datos_publicos", {"pregunta": "x"}, {"Authorization": f"Bearer {KEY}"}
    )
    assert result.is_error
    assert "10 consultas de hoy" in _text(result)


async def test_listar_fuentes(backend: FakeBackend) -> None:
    result = await _call("listar_fuentes", {}, {"Authorization": f"Bearer {KEY}"})
    assert "caba: 10 datasets" in _text(result)
    assert backend.requests[0].url.path == "/api/v1/fuentes"


async def test_unknown_host_is_rejected() -> None:
    app = mcp_server.create_app()
    async with mcp_server.server.session_manager.run():
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://evil.example"
        ) as http:
            r = await http.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers={"accept": "application/json, text/event-stream"},
            )
    assert r.status_code in (400, 421)


async def test_health_and_site() -> None:
    app = mcp_server.create_app()
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://localhost:8000"
    ) as http:
        assert (await http.get("/health")).json() == {"status": "ok"}
        home = await http.get("/")
    assert home.status_code == 200
    assert "OpenArg" in home.text
