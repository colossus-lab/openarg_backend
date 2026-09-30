"""`client_family`: de un `clientInfo` o un User-Agent a una familia corta."""

from __future__ import annotations

import pytest

from app.application.public_api_clients import FAMILIES, client_family


@pytest.mark.parametrize(
    ("raw", "family"),
    [
        ("claude-code/2.1.0", "claude-code"),
        ("claude-code/2.1.0 (external, cli)", "claude-code"),
        ("claude-ai/0.1.0", "claude-desktop"),
        ("Claude/0.14.3 Electron/37", "claude-desktop"),
        ("cursor-vscode/1.0.0", "cursor"),
        ("Cursor/1.7.2", "cursor"),
        ("Visual Studio Code/1.104.0", "vscode"),
        ("Windsurf/1.12", "windsurf"),
        ("openai-mcp/1.0.0", "chatgpt"),
        ("mcp-remote/0.1.29", "mcp-remote"),
        ("n8n", "n8n"),
        ("python-httpx/0.28.1", "python"),
        ("Python/3.12 aiohttp/3.9", "python"),
        ("node", "node"),
        ("undici", "node"),
        ("curl/8.7.1", "curl"),
        ("Mozilla/5.0 (algo raro)", "otro"),
    ],
)
def test_known_clients(raw: str, family: str) -> None:
    assert client_family(raw) == family


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_nothing_sent(raw: str | None) -> None:
    assert client_family(raw) is None


def test_claude_code_is_not_mistaken_for_desktop() -> None:
    # "claude-code" también contiene "claude": el orden de las reglas importa.
    assert client_family("claude-code/2.1.0 node") == "claude-code"


def test_every_result_fits_the_column() -> None:
    assert all(len(f) <= 32 for f in FAMILIES)
