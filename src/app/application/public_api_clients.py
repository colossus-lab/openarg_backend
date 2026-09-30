"""Qué programa hizo un pedido a la API pública, en una familia corta.

El tablero de uso agrupa por esto ("¿cuántos usan Claude Code y cuántos su
propio agente?"). La entrada es el `clientInfo.name` del protocolo MCP cuando
el cliente lo manda, o si no el `User-Agent`. Ninguno de los dos es confiable
—cualquiera puede mandar lo que quiera—, así que sirve para estadística y
nunca para decidir nada. El texto crudo se guarda aparte (`user_agent`) para
poder ajustar estas reglas mirando datos reales.

Sólo stdlib: se testea sin levantar nada.
"""

from __future__ import annotations

# Orden importante: lo más específico primero. "claude-code" contiene "claude",
# y un User-Agent de Cursor puede traer "node" o "electron" más adelante.
_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("claude-code", ("claude-code", "claude code", "claudecode")),
    ("claude-desktop", ("claude-desktop", "claude desktop", "claude-ai", "claude/")),
    ("cursor", ("cursor",)),
    ("vscode", ("visual studio code", "vscode", "vs code")),
    ("windsurf", ("windsurf", "codeium")),
    ("chatgpt", ("chatgpt", "openai-mcp", "openai")),
    ("mcp-remote", ("mcp-remote",)),
    ("n8n", ("n8n",)),
    ("python", ("python", "httpx", "aiohttp")),
    ("node", ("node", "undici", "axios", "got/")),
    ("curl", ("curl/",)),
)

FAMILIES: tuple[str, ...] = (*(family for family, _ in _RULES), "otro")


def client_family(raw: str | None) -> str | None:
    """Familia del cliente, `"otro"` si no se reconoce, `None` si no vino nada."""
    text = (raw or "").strip().lower()
    if not text:
        return None
    for family, needles in _RULES:
        if any(needle in text for needle in needles):
            return family
    return "otro"
