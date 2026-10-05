"""Lógica pura del MCP público de OpenArg: sólo biblioteca estándar.

Todo lo que decide qué se le manda al backend y qué se le devuelve al
cliente vive acá, para poder testearlo sin tener `mcp` instalado (igual que
`scripts/ops_mcp/ops_core.py`). `server.py` sólo conecta esto con el SDK y
con HTTP.

El MCP no decide nada de autenticación ni de cuotas: reenvía la clave del
usuario a `POST /api/v1/ask` y el backend es la única fuente de verdad.
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

KEY_URL = "https://openarg.org/desarrolladores"
DOCS_URL = "https://mcp.openarg.org"
CONTACT_EMAIL = "devops@colossuslab.org"
SUPPORT_URL = "https://www.colossuslab.org/support"
# Sólo cuando se agota un cupo diario: es el momento en que alguien valora el
# servicio. Nunca en respuestas normales, que el modelo lee en cada llamada.
SUPPORT_LINE = (
    " OpenArg es gratis y se sostiene con aportes de quienes lo usan: si te sirve, "
    f"podés bancarlo en {SUPPORT_URL} (quienes lo sostienen como Fundadores tienen "
    "cupo ampliado)."
)
# Como en Tomi: el acceso no depende de poder pagar.
HARDSHIP_LINE = (
    " Si lo necesitás para periodismo, investigación o una organización y no podés "
    f"aportar, escribinos a {CONTACT_EMAIL}: el acceso no depende de poder pagar."
)


def _first_int(text: str) -> int | None:
    match = re.search(r"\d+", text or "")
    return int(match.group()) if match else None


def renewal_label(now: datetime | None = None) -> str:
    """Cuándo se renueva el cupo: el 1° del mes que viene a las 00:00 UTC,
    que en Argentina es el último día del mes a las 21:00."""
    now = now or datetime.now(UTC)
    first = datetime(now.year + (now.month == 12), now.month % 12 + 1, 1, tzinfo=UTC)
    last_day = first - timedelta(days=1)
    return f"{first.day}/{first.month} (el {last_day.day}/{last_day.month} a las 21:00, hora de Argentina)"


KEY_PREFIX = "oarg_sk_"
MAX_QUESTION_CHARS = 2000

_KEY_PATTERN = re.compile(r"oarg_sk_[A-Za-z0-9_\-]+")


class UserFacingError(Exception):
    """Error cuyo mensaje se le puede mostrar tal cual al usuario."""


def redact(text: str) -> str:
    """Tapa cualquier clave `oarg_sk_…` antes de que llegue a un log."""
    return _KEY_PATTERN.sub("oarg_sk_[REDACTED]", text)


def _header(headers: Mapping[str, str] | None, name: str) -> str:
    if not headers:
        return ""
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value or ""
    return ""


def extract_key(headers: Mapping[str, str] | None) -> str:
    """Devuelve la clave del header `Authorization: Bearer oarg_sk_…`.

    Levanta `UserFacingError` con instrucciones si falta o no tiene forma de
    clave de OpenArg. No valida contra la base: eso lo hace el backend.
    """
    auth = _header(headers, "authorization").strip()
    scheme, _, token = auth.partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        raise UserFacingError(
            "Falta la clave de OpenArg. Conseguí una gratis en "
            f"{KEY_URL} y configurala en tu cliente MCP como header "
            "`Authorization: Bearer oarg_sk_…`. Instrucciones por cliente: "
            f"{DOCS_URL}/empezar.html"
        )
    if not token.startswith(KEY_PREFIX):
        raise UserFacingError(
            "La clave no tiene el formato de OpenArg (empieza con `oarg_sk_`). "
            f"Revisá la configuración o generá una nueva en {KEY_URL}."
        )
    return token


def client_ip(headers: Mapping[str, str] | None) -> str | None:
    """IP del usuario según el `X-Forwarded-For` que pone Caddy.

    Caddy reescribe ese header con la IP real (no confía en el que manda el
    cliente), así que el primer valor es la IP de quien se conectó. Si no es
    una IP válida, no se reenvía nada.
    """
    first = _header(headers, "x-forwarded-for").split(",")[0].strip()
    if not first:
        return None
    try:
        return str(ipaddress.ip_address(first))
    except ValueError:
        return None


def validate_question(question: str) -> str:
    q = (question or "").strip()
    if not q:
        raise UserFacingError("La pregunta está vacía.")
    if len(q) > MAX_QUESTION_CHARS:
        raise UserFacingError(
            f"La pregunta es demasiado larga ({len(q)} caracteres; máximo "
            f"{MAX_QUESTION_CHARS}). Probá acotarla."
        )
    return q


_CLIENT_LABEL_CHARS = 160


def client_label(headers: Mapping[str, str] | None, client_name: str | None = None) -> str | None:
    """Qué programa usa la persona (Claude Code, Cursor, su propio agente).

    El `clientInfo` del protocolo es lo más preciso, pero en modo stateless sólo
    llega con los clientes nuevos; si no, queda el `User-Agent`. Es para el
    tablero de uso: nunca decide nada, y se limpia para que no pueda inyectar
    otro header.
    """
    raw = (client_name or "").strip() or _header(headers, "User-Agent").strip()
    clean = "".join(ch for ch in raw if 32 <= ord(ch) < 127)[:_CLIENT_LABEL_CHARS].strip()
    return clean or None


def backend_headers(key: str, ip: str | None, client: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        # Para que `api_usage` distinga el MCP de la API directa.
        "X-OpenArg-Via": "mcp",
    }
    if ip:
        headers["X-Forwarded-For"] = ip
    if client:
        headers["X-OpenArg-Client"] = client
    return headers


def error_message(status: int, detail: str = "", *, data_mode: bool = False) -> str:
    """Traduce una respuesta de error del backend a un mensaje para el usuario.

    En el modo datos (`/catalogo/*`) los 400/404/503 traen un `detail` que
    armamos nosotros en castellano ("Columnas que no existen en la tabla: …")
    y que le dice al modelo cómo corregir el pedido: se pasa tal cual. En el
    modo respuestas el detalle de esos códigos no es para el usuario.
    """
    detail_l = (detail or "").lower()
    if data_mode and status in (400, 404, 503) and detail:
        return detail
    if status == 401:
        return (
            "La clave de OpenArg es inválida o fue revocada. Generá una nueva en "
            f"{KEY_URL} y actualizala en tu cliente MCP."
        )
    if status == 402:
        # El cupo del mes se terminó y no quedan créditos. El número sale del
        # detalle del backend: no es el mismo para un Fundador.
        n = _first_int(detail)
        cuantas = f"las {n}" if n else "todas las"
        if "catalog" in detail_l:
            head = f"Usaste {cuantas} consultas del modo datos de este mes."
        else:
            head = (
                f"Usaste {cuantas} preguntas de este mes. Mientras tanto podés seguir con "
                "el modo datos (buscar_datasets, describir_tabla, obtener_datos), que "
                "tiene su propio cupo."
            )
        return f"{head} Se renuevan el {renewal_label()}." + SUPPORT_LINE + HARDSHIP_LINE
    if status == 429:
        if "minute" in detail_l:
            return "Demasiadas consultas seguidas: esperá un minuto y volvé a intentar."
        if "this ip" in detail_l:
            return (
                "Se alcanzó el límite diario de consultas desde esta conexión. "
                "Se renueva a las 21:00 (hora de Argentina)."
            )
        return "Demasiadas consultas: esperá un rato y volvé a intentar."
    if status == 503:
        # Dos 503 distintos: el tope global del día ("daily capacity") y el
        # servicio de cupos caído (Redis), o cualquier otro 503 sin ese
        # detalle (p. ej. el proxy sin backend). Sólo el primero es "cupo
        # agotado"; mostrar eso durante un incidente confundía a todos.
        if "capacity" in detail_l:
            return (
                "El cupo público de OpenArg para hoy está agotado. Se renueva a las "
                "21:00 (hora de Argentina). Los aportes son lo que nos permite ampliarlo."
            ) + SUPPORT_LINE
        return (
            "El servicio de cupos de OpenArg no responde en este momento. Es una "
            "falla nuestra, no tu cupo: probá de nuevo en unos minutos. Esta "
            "consulta no descontó ninguna pregunta."
        )
    if status == 408:
        return (
            "La consulta tardó demasiado y se cortó. Probá con una pregunta más "
            "acotada (un indicador, un período, una jurisdicción)."
        )
    if status == 400:
        return "OpenArg no puede procesar esa pregunta. Reformulala como una consulta sobre datos públicos."
    if status == 422:
        return "La consulta no tiene un formato válido."
    return "OpenArg no pudo responder en este momento. Probá de nuevo en unos minutos."


def _source_line(source: Mapping[str, Any]) -> str:
    name = str(source.get("name") or source.get("title") or "Fuente sin título").strip()
    portal = str(source.get("portal") or "").strip()
    url = str(source.get("url") or "").strip()
    label = f"{name} ({portal})" if portal else name
    return f"- [{label}]({url})" if url.startswith(("http://", "https://")) else f"- {label}"


def format_answer(payload: Mapping[str, Any]) -> str:
    """Arma la respuesta en markdown: texto, fuentes, advertencias y cupo."""
    parts: list[str] = [str(payload.get("answer") or "").strip() or "(OpenArg no devolvió texto)"]

    sources = [s for s in payload.get("sources") or [] if isinstance(s, Mapping)]
    if sources:
        seen: set[str] = set()
        lines = []
        for s in sources:
            line = _source_line(s)
            if line not in seen:
                seen.add(line)
                lines.append(line)
        parts.append("**Fuentes**\n" + "\n".join(lines))

    warnings = [str(w).strip() for w in payload.get("warnings") or [] if str(w).strip()]
    if warnings:
        parts.append("**Advertencias**\n" + "\n".join(f"- {w}" for w in warnings))

    usage = payload.get("usage") or {}
    if isinstance(usage, Mapping):
        remaining = usage.get("requests_remaining_month")
        if not isinstance(remaining, int):
            remaining = usage.get("requests_remaining_today")
        if isinstance(remaining, int):
            # `charged` es False cuando la respuesta no descontó (repetida,
            # del caché, un saludo). Un backend viejo no lo manda: no se dice nada.
            free = " (esta no se descontó)" if usage.get("charged") is False else ""
            parts.append(
                f"_Preguntas que te quedan este mes: {remaining}{free}. "
                "Datos: OpenArg (openarg.org)._"
            )
    return "\n\n".join(parts)


def format_sources(payload: Mapping[str, Any]) -> str:
    fuentes = [f for f in payload.get("fuentes") or [] if isinstance(f, Mapping)]
    if not fuentes:
        return "OpenArg no devolvió fuentes en este momento."
    total = payload.get("total_datasets")
    head = f"OpenArg indexa {total} datasets de {len(fuentes)} portales:" if total else "Portales:"
    lines = [f"- {f.get('portal')}: {f.get('datasets')} datasets" for f in fuentes]
    return head + "\n" + "\n".join(lines)


# ── Modo datos ───────────────────────────────────────────────────────────────

_MAX_CELL = 120


def _cell(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", " ").replace("\n", " ")
    if len(text) > _MAX_CELL:
        text = text[: _MAX_CELL - 1] + "…"
    if any(ch in text for ch in (",", '"')):
        text = '"' + text.replace('"', '""') + '"'
    return text


def rows_to_csv(columns: list[str], rows: list[Mapping[str, Any]]) -> str:
    """CSV compacto para el modelo: menos tokens que JSON para lo mismo."""
    lines = [",".join(_cell(c) for c in columns)]
    lines += [",".join(_cell(row.get(c)) for c in columns) for row in rows]
    return "\n".join(lines)


def _link(title: str, url: str) -> str:
    return f"[{title}]({url})" if url.startswith(("http://", "https://")) else title


def format_search(payload: Mapping[str, Any]) -> str:
    results = [r for r in payload.get("resultados") or [] if isinstance(r, Mapping)]
    if not results:
        return "No encontré datasets para esa búsqueda. Probá con otras palabras o sin filtrar por portal."
    parts = []
    for i, r in enumerate(results, 1):
        lines = [f"{i}. **{r.get('titulo') or 'Sin título'}** ({r.get('portal', '')})"]
        # Varios recursos de un mismo package comparten título y descripción
        # ("Votaciones Nominales": cabecera y detalle, período 137 y 129-137):
        # el archivo es lo único que los distingue.
        archivo = str(r.get("archivo") or "").strip()
        formato = str(r.get("formato") or "").strip()
        if archivo or formato:
            lines.append(
                "   Archivo: "
                + (f"`{archivo}`" if archivo else "")
                + (f" ({formato})" if formato and archivo else formato)
            )
        desc = str(r.get("descripcion") or "").strip()
        if desc:
            lines.append(f"   {desc}")
        tables = [t for t in r.get("tablas") or [] if isinstance(t, Mapping)]
        if tables:
            listed = ", ".join(
                f"`{t.get('tabla')}`"
                + (f" ({t.get('filas')} filas)" if t.get("filas") is not None else "")
                for t in tables
            )
            lines.append(f"   Tablas consultables: {listed}")
        else:
            lines.append("   Sin tabla consultable en OpenArg")
        url = str(r.get("url") or "")
        if url:
            lines.append(f"   Fuente: {_link('descarga oficial', url)}")
        parts.append("\n".join(lines))
    return (
        "\n\n".join(parts)
        + "\n\nUsá `describir_tabla` con el nombre de una tabla para ver sus columnas y período."
    )


def format_table(payload: Mapping[str, Any]) -> str:
    title = str(payload.get("titulo") or payload.get("tabla") or "")
    lines = [f"**{_link(title, str(payload.get('url') or ''))}** — tabla `{payload.get('tabla')}`"]
    if payload.get("filas") is not None:
        lines.append(f"Filas: {payload.get('filas')}")
    if payload.get("columna_fecha"):
        lines.append(
            f"Período ({payload.get('columna_fecha')}): {payload.get('desde')} a {payload.get('hasta')}"
        )
    if payload.get("aviso_fecha"):
        lines.append(f"Aviso: {payload.get('aviso_fecha')}")
    cols = [c for c in payload.get("columnas") or [] if isinstance(c, Mapping)]
    lines.append("Columnas:\n" + "\n".join(f"- {c.get('nombre')} ({c.get('tipo')})" for c in cols))
    sample = [r for r in payload.get("muestra") or [] if isinstance(r, Mapping)]
    if sample:
        names = [str(c.get("nombre")) for c in cols] or list(sample[0].keys())
        lines.append("Muestra:\n```csv\n" + rows_to_csv(names, sample) + "\n```")
    lines.append("Pedí las filas con `obtener_datos` (columnas, desde, hasta, filtros, limite).")
    return "\n".join(lines)


def format_rows(payload: Mapping[str, Any]) -> str:
    rows = [r for r in payload.get("filas") or [] if isinstance(r, Mapping)]
    columns = [str(c) for c in payload.get("columnas") or []]
    source = _link(
        str(payload.get("fuente") or payload.get("tabla") or ""), str(payload.get("url") or "")
    )
    applied = [str(n) for n in payload.get("filtros_aplicados") or []]
    if not rows:
        # El backend explica por qué no hubo filas y qué valores existen.
        # Antes este mensaje fijo era todo lo que veía el modelo cliente.
        lines = [f"La consulta no devolvió filas. Fuente: {source}"]
        if payload.get("aviso"):
            lines.append(str(payload["aviso"]))
        lines.extend(applied)
        return "\n".join(lines)
    head = f"{len(rows)} filas de `{payload.get('tabla')}`. Fuente: {source}"
    if applied:
        head += "\n" + "\n".join(applied)
    if payload.get("truncado"):
        head += (
            "\nHay más filas: acotá con `desde`/`hasta` o `filtros`, o subí `limite` (máximo 500)."
        )
    return head + "\n```csv\n" + rows_to_csv(columns, rows) + "\n```"


# Sólo el dominio de producción se indexa: la misma imagen sirve la web en
# mcp.staging.openarg.org, que no debe competir con la real en los buscadores.
INDEXABLE_HOST = "mcp.openarg.org"


def robots_txt(host: str | None) -> str:
    """robots.txt según el host que pidió la página."""
    if (host or "").split(":")[0].lower() != INDEXABLE_HOST:
        return "User-agent: *\nDisallow: /\n"
    # Buscadores y agentes de IA entran por la regla general. /mcp es el
    # protocolo, no una página.
    return f"User-agent: *\nAllow: /\nDisallow: /mcp\n\nSitemap: {DOCS_URL}/sitemap.xml\n"
