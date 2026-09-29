"""MCP público de datos de OpenArg (streamable HTTP) + la web mcp.openarg.org.

    BACKEND_URL=http://backend:8080 uvicorn mcp_publico.server:app --port 8000

Rutas:
    /mcp     servidor MCP (stateless, respuestas JSON)
    /health  para el healthcheck del contenedor y el monitoreo
    /        la web estática de `site/`

Listar las herramientas no pide clave, así los directorios de MCP pueden
inspeccionar el servidor. Llamarlas sí: la clave del usuario viaja en
`Authorization: Bearer oarg_sk_…` y se reenvía tal cual al backend, que la
valida y cobra la cuota. Este proceso no guarda nada.
"""

from __future__ import annotations

import inspect
import logging
import os
from pathlib import Path
from typing import Any

import httpx
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.staticfiles import StaticFiles

from mcp_publico import core

logger = logging.getLogger("openarg.mcp")

BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8080").rstrip("/")
# Margen sobre el timeout del pipeline del backend (PUBLIC_API_TIMEOUT_SECONDS).
_ASK_TIMEOUT = float(os.getenv("MCP_BACKEND_TIMEOUT_SECONDS", "75"))
_SITE_DIR = Path(__file__).parent / "site"

_READ_ONLY = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=False, open_world_hint=True
)

server = MCPServer(
    "openarg",
    title="OpenArg — Datos Públicos de Argentina",
    website_url=core.DOCS_URL,
    instructions=(
        "OpenArg responde preguntas sobre datos públicos oficiales de Argentina "
        "(INDEC, datos.gob.ar, provincias, municipios, Congreso, presupuesto, "
        "series económicas y más) consultando los datasets de los portales "
        "de datos abiertos. Usá `consultar_datos_publicos` con la pregunta en "
        "lenguaje natural, preferentemente en español y lo más concreta posible "
        "(indicador, período, jurisdicción). Cada llamada descuenta una de las "
        "10 consultas diarias del usuario: no la llames para repreguntar lo "
        "mismo ni para cosas que no sean datos públicos argentinos. Citá "
        "siempre las fuentes que devuelve y mostrá las advertencias. "
        "`listar_fuentes` muestra qué portales cubre y no descuenta cupo."
    ),
)

# Reemplazable en tests: devuelve un httpx.AsyncClient contra el backend.
_client_factory = lambda: httpx.AsyncClient(base_url=BACKEND_URL, timeout=_ASK_TIMEOUT)  # noqa: E731


def _key_and_ip(ctx: Context) -> tuple[str, str | None]:
    headers = ctx.headers
    try:
        return core.extract_key(headers), core.client_ip(headers)
    except core.UserFacingError as exc:
        raise ToolError(str(exc)) from None


async def _call_backend(
    method: str, path: str, key: str, ip: str | None, json: Any = None
) -> dict[str, Any]:
    try:
        async with _client_factory() as client:
            resp = await client.request(
                method, path, json=json, headers=core.backend_headers(key, ip)
            )
    except httpx.TimeoutException:
        raise ToolError(core.error_message(408)) from None
    except httpx.HTTPError as exc:
        logger.error("backend unreachable: %s", core.redact(repr(exc)))
        raise ToolError(core.error_message(502)) from None

    if resp.status_code != 200:
        try:
            detail = str(resp.json().get("detail", ""))
        except ValueError:
            detail = ""
        logger.info("backend %s %s -> %s", method, path, resp.status_code)
        raise ToolError(core.error_message(resp.status_code, detail))
    return resp.json()


def _tool(title: str, annotations: ToolAnnotations):  # type: ignore[no-untyped-def]
    """`server.tool` con la descripción sacada del docstring, sin la sangría.

    El SDK manda `__doc__` tal cual, y cada línea llegaba al modelo con cuatro
    espacios adelante.
    """

    def register(fn):  # type: ignore[no-untyped-def]
        return server.tool(
            title=title, description=inspect.cleandoc(fn.__doc__ or ""), annotations=annotations
        )(fn)

    return register


@_tool("Consultar datos públicos de Argentina", _READ_ONLY)
async def consultar_datos_publicos(pregunta: str, ctx: Context) -> str:
    """Responde una pregunta sobre datos públicos oficiales de Argentina, con fuentes.

    Ejemplos: "¿Cuál fue la tasa de desempleo del último trimestre?",
    "Evolución del IPC en 2025", "¿Cuántos diputados tiene cada bloque?",
    "Presupuesto ejecutado por el Ministerio de Salud en 2024".
    La respuesta incluye los datasets usados (con link al portal oficial),
    advertencias sobre la calidad o cobertura del dato, y cuántas consultas
    le quedan hoy al usuario. Descuenta 1 de las 10 consultas diarias.
    """
    try:
        question = core.validate_question(pregunta)
    except core.UserFacingError as exc:
        raise ToolError(str(exc)) from None
    key, ip = _key_and_ip(ctx)
    payload = await _call_backend("POST", "/api/v1/ask", key, ip, json={"question": question})
    return core.format_answer(payload)


@_tool(
    "Listar fuentes de datos de OpenArg",
    ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True),
)
async def listar_fuentes(ctx: Context) -> str:
    """Lista los portales de datos abiertos que cubre OpenArg y cuántos datasets tiene cada uno.

    No descuenta consultas del cupo diario.
    """
    key, ip = _key_and_ip(ctx)
    payload = await _call_backend("GET", "/api/v1/fuentes", key, ip)
    return core.format_sources(payload)


@server.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


def _transport_security() -> TransportSecuritySettings:
    # Protección contra DNS rebinding: sólo se aceptan los Host de los dominios
    # propios (y localhost para desarrollo). Los clientes MCP no mandan Origin;
    # un navegador de otro sitio sí, y queda rechazado.
    hosts = os.getenv(
        "MCP_ALLOWED_HOSTS",
        "mcp.openarg.org,mcp.staging.openarg.org,localhost:*,127.0.0.1:*,mcp:8000",
    )
    allowed_hosts = [h.strip() for h in hosts.split(",") if h.strip()]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=[f"https://{h}" for h in allowed_hosts if "*" not in h]
        + ["http://localhost:*", "http://127.0.0.1:*"],
    )


def create_app():  # type: ignore[no-untyped-def]
    app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=_transport_security(),
        host="0.0.0.0",  # noqa: S104 - corre dentro de la red de docker, detrás de Caddy
    )
    if _SITE_DIR.is_dir():
        # Va después de /mcp y /health: Starlette resuelve en orden.
        app.mount("/", StaticFiles(directory=_SITE_DIR, html=True), name="site")
    return app


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
app = create_app()
