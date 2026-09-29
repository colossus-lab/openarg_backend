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
from typing import Any

KEY_URL = "https://openarg.org/desarrolladores"
DOCS_URL = "https://mcp.openarg.org"
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


def backend_headers(key: str, ip: str | None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if ip:
        headers["X-Forwarded-For"] = ip
    return headers


def error_message(status: int, detail: str = "") -> str:
    """Traduce una respuesta de error del backend a un mensaje para el usuario."""
    detail_l = (detail or "").lower()
    if status == 401:
        return (
            "La clave de OpenArg es inválida o fue revocada. Generá una nueva en "
            f"{KEY_URL} y actualizala en tu cliente MCP."
        )
    if status == 429:
        if "minute" in detail_l:
            return "Demasiadas consultas seguidas: esperá un minuto y volvé a intentar."
        if "this ip" in detail_l:
            return (
                "Se alcanzó el límite diario de consultas desde esta conexión. "
                "Se renueva a las 21:00 (hora de Argentina)."
            )
        if "catalog" in detail_l:
            return "Se alcanzó el límite diario de consultas al catálogo. Se renueva a las 21:00 (hora de Argentina)."
        return (
            "Usaste las 10 consultas de hoy. Se renuevan a las 21:00 (hora de "
            "Argentina). Listar las fuentes no descuenta cupo."
        )
    if status == 503:
        return (
            "El cupo público de OpenArg para hoy está agotado. Se renueva a las "
            "21:00 (hora de Argentina)."
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
    remaining = usage.get("requests_remaining_today") if isinstance(usage, Mapping) else None
    if isinstance(remaining, int):
        parts.append(f"_Consultas restantes hoy: {remaining}. Datos: OpenArg (openarg.org)._")
    return "\n\n".join(parts)


def format_sources(payload: Mapping[str, Any]) -> str:
    fuentes = [f for f in payload.get("fuentes") or [] if isinstance(f, Mapping)]
    if not fuentes:
        return "OpenArg no devolvió fuentes en este momento."
    total = payload.get("total_datasets")
    head = f"OpenArg indexa {total} datasets de {len(fuentes)} portales:" if total else "Portales:"
    lines = [f"- {f.get('portal')}: {f.get('datasets')} datasets" for f in fuentes]
    return head + "\n" + "\n".join(lines)
