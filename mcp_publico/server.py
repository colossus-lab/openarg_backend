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
from starlette.responses import JSONResponse, PlainTextResponse, Response
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
        "OpenArg da acceso a datos públicos oficiales de Argentina (INDEC, "
        "datos.gob.ar, provincias, municipios, Congreso, presupuesto, series "
        "económicas y más) en dos modos.\n"
        "MODO DATOS (preferilo cuando puedas razonar vos con los datos): "
        "`buscar_datasets` encuentra datasets y sus tablas; `describir_tabla` "
        "muestra columnas, período, de cuándo son los datos y una muestra; "
        "`obtener_datos` trae filas filtradas por período, columnas y valores; "
        "`agregar_datos` calcula en la base totales, promedios, conteos, mínimos, "
        "máximos y rankings agrupados: usalo SIEMPRE que necesites una de esas "
        "cuentas, en vez de traer filas y sumarlas vos. No descuenta preguntas "
        "(tiene su propio cupo, más amplio).\n"
        "MODO RESPUESTAS: `consultar_datos_publicos` recibe una pregunta en "
        "lenguaje natural y OpenArg arma la respuesta con fuentes y advertencias. "
        "Cada respuesta descuenta 1 de las preguntas del mes del usuario; no descuentan "
        "los errores, los cortes por tiempo ni repetir la misma pregunta dentro de los "
        "5 minutos. Usala cuando la pregunta necesite cruzar tablas o cuando el modo "
        "datos no alcance.\n"
        f"{core.LIMITES}\n"
        "Citá siempre la fuente (título y link) de los datos que uses. "
        "`listar_fuentes` muestra qué portales cubre."
    ),
)

# Reemplazable en tests: devuelve un httpx.AsyncClient contra el backend.
_client_factory = lambda: httpx.AsyncClient(base_url=BACKEND_URL, timeout=_ASK_TIMEOUT)  # noqa: E731


def _client_name(ctx: Context) -> str | None:
    """`clientInfo` del protocolo, si el cliente lo mandó en este pedido."""
    # En modo stateless, `client_params` es None salvo en clientes que mandan
    # `clientInfo` en cada pedido (protocolo 2026-07-28). Nunca debe romper.
    try:
        info = ctx.session.client_params.client_info  # type: ignore[union-attr]
    except Exception:
        return None
    name = (getattr(info, "name", "") or "").strip()
    version = (getattr(info, "version", "") or "").strip()
    return f"{name}/{version}" if name and version else name or None


def _caller(ctx: Context) -> tuple[str, str | None, str | None]:
    """Clave, IP real y programa cliente de quien llama a la herramienta."""
    headers = ctx.headers
    try:
        key, ip = core.extract_key(headers), core.client_ip(headers)
    except core.UserFacingError as exc:
        raise ToolError(str(exc)) from None
    return key, ip, core.client_label(headers, _client_name(ctx))


async def _call_backend(
    method: str,
    path: str,
    key: str,
    ip: str | None,
    caller: str | None,
    json: Any = None,
    params: dict[str, Any] | None = None,
    *,
    data_mode: bool = False,
) -> dict[str, Any]:
    try:
        async with _client_factory() as client:
            resp = await client.request(
                method,
                path,
                json=json,
                params=params,
                headers=core.backend_headers(key, ip, caller),
            )
    except httpx.TimeoutException:
        raise ToolError(core.error_message(408)) from None
    except httpx.HTTPError as exc:
        logger.error("backend unreachable: %s", core.redact(repr(exc)))
        raise ToolError(core.error_message(502)) from None

    if resp.status_code != 200:
        try:
            detail = core.error_detail(resp.json().get("detail", ""))
        except (ValueError, AttributeError):
            detail = ""
        logger.info("backend %s %s -> %s", method, path, resp.status_code)
        raise ToolError(
            core.error_message(
                resp.status_code,
                detail,
                data_mode=data_mode,
                retry_after=resp.headers.get("retry-after"),
            )
        )
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
    advertencias sobre la calidad o cobertura del dato, y cuántas preguntas
    le quedan este mes al usuario. Cada respuesta descuenta 1 de sus preguntas
    del mes (10 gratis); un error, un corte por tiempo o la misma pregunta
    repetida dentro de los 5 minutos no descuentan. Si podés responder con
    `buscar_datasets` + `obtener_datos` o `agregar_datos`, preferí esas.
    """
    try:
        question = core.validate_question(pregunta)
    except core.UserFacingError as exc:
        raise ToolError(str(exc)) from None
    key, ip, caller = _caller(ctx)
    payload = await _call_backend(
        "POST", "/api/v1/ask", key, ip, caller, json={"question": question}
    )
    return core.format_answer(payload)


@_tool(
    "Listar fuentes de datos de OpenArg",
    ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True),
)
async def listar_fuentes(ctx: Context) -> str:
    """Lista los portales de datos abiertos que cubre OpenArg y cuántos datasets tiene cada uno.

    No descuenta preguntas: cuenta como un pedido del modo datos.
    """
    key, ip, caller = _caller(ctx)
    payload = await _call_backend("GET", "/api/v1/fuentes", key, ip, caller)
    return core.format_sources(payload)


_IDEMPOTENT = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=True
)


@_tool("Buscar datasets públicos de Argentina", _IDEMPOTENT)
async def buscar_datasets(
    texto: str, ctx: Context, portal: str | None = None, limite: int = 10
) -> str:
    """Busca en el catálogo de OpenArg (más de 30.000 datasets de portales oficiales).

    Devuelve título, portal, descripción, link a la fuente oficial y las tablas
    consultables de cada dataset (usalas con `describir_tabla` y `obtener_datos`).
    `texto` es lo que buscás, en castellano ("tasas de interés BCRA", "matrícula
    escolar CABA"). `portal` filtra por portal (ver `listar_fuentes`).
    `limite` entre 1 y 25. No descuenta preguntas.
    """
    key, ip, caller = _caller(ctx)
    params: dict[str, Any] = {"q": texto.strip(), "limite": max(1, min(int(limite), 25))}
    if portal:
        params["portal"] = portal
    payload = await _call_backend(
        "GET", "/api/v1/catalogo/buscar", key, ip, caller, params=params, data_mode=True
    )
    return core.format_search(payload)


@_tool("Describir una tabla de OpenArg", _IDEMPOTENT)
async def describir_tabla(tabla: str, ctx: Context) -> str:
    """Muestra las columnas (con su tipo), cantidad de filas, período cubierto, frescura y una muestra de una tabla.

    `tabla` es el nombre que devuelve `buscar_datasets`. Usalo antes de
    `obtener_datos` o `agregar_datos` para saber qué columnas pedir y qué
    fechas existen. La frescura dice cuándo leyó OpenArg la tabla de su fuente,
    cuál es el último dato y, sólo si la tabla es una foto del período en curso,
    su fecha de corte. La fecha de lectura no es la de los datos: una tabla de
    un período pasado o sin columna de fecha no es vigente por haberse leído
    hoy. No presentes como actual un dato viejo. No descuenta preguntas.
    """
    key, ip, caller = _caller(ctx)
    payload = await _call_backend(
        "GET",
        "/api/v1/catalogo/tabla",
        key,
        ip,
        caller,
        params={"nombre": tabla.strip()},
        data_mode=True,
    )
    return core.format_table(payload)


@_tool("Obtener datos de una tabla de OpenArg", _IDEMPOTENT)
async def obtener_datos(
    tabla: str,
    ctx: Context,
    columnas: list[str] | None = None,
    desde: str | None = None,
    hasta: str | None = None,
    filtros: dict[str, str | int | float] | list[dict[str, Any]] | None = None,
    orden: str | None = None,
    limite: int = 100,
    columna_fecha: str | None = None,
    offset: int = 0,
) -> str:
    """Trae filas de una tabla, en CSV, con la fuente oficial.

    Para un total, promedio, conteo, mínimo, máximo o ranking NO traigas filas para
    sumarlas: usá `agregar_datos`.
    - `columnas`: las que quieras (por defecto, todas); nombres exactos de `describir_tabla`.
    - `desde` / `hasta`: período, como AAAA, AAAA-MM o AAAA-MM-DD, sobre la columna de
      fecha (o de año).
    - `filtros`: hasta 5. Igualdad: {"provincia": "Córdoba"}. Con operador, una lista:
      [{"columna": "monto", "operador": "mayor_que", "valor": "1000000"},
      {"columna": "provincia", "operador": "en", "valores": ["Salta", "Jujuy"]}].
      Operadores: =, !=, mayor_que, menor_que, >=, <=, contiene y en. La igualdad y
      `contiene` no distinguen mayúsculas ni acentos (en tablas de más de un millón de
      filas, sí: ahí la respuesta dice en `filtros_aplicados` qué se buscó tal cual).
    - `orden`: "asc" (por defecto: primero lo más viejo) o "desc" (primero lo más
      reciente), por fecha. Para el último dato de una serie, pedí "desc".
    - `limite`: 1 a 500 filas. Si hay más, la respuesta lo dice y da el `offset` de la
      página siguiente (hasta 10.000).
    - `columna_fecha`: opcional, otra columna de fecha para `desde`/`hasta` y el orden.
    Si ninguna fila cumple los filtros, la respuesta dice qué valores existen.
    No descuenta preguntas.
    """
    key, ip, caller = _caller(ctx)
    body: dict[str, Any] = {
        "tabla": tabla.strip(),
        "limite": max(1, min(int(limite), core.LIMITE_FILAS_POR_PEDIDO)),
    }
    for field, value in (
        ("columnas", columnas),
        ("desde", desde),
        ("hasta", hasta),
        ("filtros", filtros),
        # Sólo si lo eligió: sin `orden`, el backend sabe que trajo lo más viejo
        # por defecto y sugiere "desc".
        ("orden", orden),
        ("columna_fecha", columna_fecha),
        ("offset", max(0, int(offset or 0))),
    ):
        if value:
            body[field] = value
    payload = await _call_backend(
        "POST", "/api/v1/catalogo/datos", key, ip, caller, json=body, data_mode=True
    )
    return core.format_rows(payload)


@_tool("Calcular totales y rankings sobre una tabla de OpenArg", _IDEMPOTENT)
async def agregar_datos(
    tabla: str,
    operacion: str,
    ctx: Context,
    columna: str | None = None,
    agrupar_por: list[str] | None = None,
    filtros: dict[str, str | int | float] | list[dict[str, Any]] | None = None,
    desde: str | None = None,
    hasta: str | None = None,
    ordenar_por: str | None = None,
    orden: str = "desc",
    limite: int = 50,
    ponderar_por: str | None = None,
    columna_fecha: str | None = None,
) -> str:
    """Calcula en la base una suma, promedio, conteo, mínimo o máximo, opcionalmente agrupado.

    Usala SIEMPRE que necesites un total, un promedio, un conteo, un mínimo/máximo o un
    ranking, en vez de traer filas con `obtener_datos` y hacer la cuenta vos: es exacta,
    no tiene el tope de 500 filas y gasta muchos menos tokens.
    - `operacion`: "suma", "promedio", "conteo", "minimo" o "maximo".
    - `columna`: la que se suma/promedia/etc. (nombres exactos de `describir_tabla`); no
      va con "conteo", que cuenta filas (si la mandás, da error). Las columnas de texto
      con números se leen según su formato (1.234,5 o 1,234.5); si el formato es
      ambiguo, no calcula y lo dice.
    - `agrupar_por`: hasta 3 columnas, p. ej. ["jurisdiccion_desc"] para un ranking.
    - `filtros`, `desde`, `hasta`, `columna_fecha`: como en `obtener_datos` (hasta 6 filtros).
    - `ordenar_por`: "valor" (por defecto, para rankings) o una columna de `agrupar_por`
      (p. ej. el año, para una serie). `orden`: "desc" (por defecto) o "asc".
    - `limite`: grupos a devolver, 1 a 200.
    - `ponderar_por`: la columna de ponderación de una encuesta (con "conteo" da la
      población estimada, no la cantidad de filas de la muestra).
    La respuesta dice sobre cuántas filas se calculó (`filas_usadas`, de todos los
    grupos) y si hay más grupos que `limite`. Si ninguna fila cumple los filtros no hay
    valor: dice qué valores existen. No descuenta preguntas.
    """
    key, ip, caller = _caller(ctx)
    body: dict[str, Any] = {
        "tabla": tabla.strip(),
        "operacion": (operacion or "").strip(),
        "orden": orden,
        "limite": max(1, min(int(limite), core.LIMITE_GRUPOS_AGREGAR)),
    }
    for field, value in (
        ("columna", columna),
        ("agrupar_por", agrupar_por),
        ("filtros", filtros),
        ("desde", desde),
        ("hasta", hasta),
        ("ordenar_por", ordenar_por),
        ("ponderar_por", ponderar_por),
        ("columna_fecha", columna_fecha),
    ):
        if value:
            body[field] = value
    payload = await _call_backend(
        "POST", "/api/v1/catalogo/agregar", key, ip, caller, json=body, data_mode=True
    )
    return core.format_aggregate(payload)


@server.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


@server.custom_route("/robots.txt", methods=["GET"])
async def robots(request: Request) -> Response:
    # Detrás de Caddy el Host llega tal cual; x-forwarded-host por si cambia.
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    return PlainTextResponse(core.robots_txt(host))


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
