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
from decimal import Decimal
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

# Los límites, en un solo lugar: van en las `instructions` del servidor, que es
# lo que leen los clientes al conectarse. Antes no estaban en ningún lado que
# el modelo viera, y la integración de n8n reintentaba la misma pregunta cada
# 21 s hasta agotar el mes (auditoría 3.5). Los números son los del backend
# (`api_key_service`, `public_quota`); un test los compara.
LIMITE_DATOS_POR_MINUTO = 30
LIMITE_DATOS_POR_MES = 200
LIMITE_DATOS_FUNDADOR = 2000
LIMITE_PREGUNTAS_POR_MINUTO = 2
LIMITE_PREGUNTAS_POR_MES = 10
LIMITE_PREGUNTAS_FUNDADOR = 100
# La misma pregunta repetida dentro de los 5 minutos devuelve la respuesta
# guardada sin cobrarla ni contar las 2 por minuto, con su propio tope.
LIMITE_REPETICIONES_POR_MINUTO = 10
LIMITE_FILAS_POR_PEDIDO = 500
# Grupos por pedido de `agregar_datos`.
LIMITE_GRUPOS_AGREGAR = 200


def _miles(n: int) -> str:
    return f"{n:,}".replace(",", ".")


LIMITES = (
    "LÍMITES (por persona, sumando todas sus claves): "
    f"modo datos {LIMITE_DATOS_POR_MINUTO} pedidos por minuto y {LIMITE_DATOS_POR_MES} por mes "
    f"(Fundadores: {_miles(LIMITE_DATOS_FUNDADOR)}); cada llamada a una herramienta del modo "
    f"datos cuenta 1. Modo respuestas {LIMITE_PREGUNTAS_POR_MINUTO} preguntas por minuto y "
    f"{LIMITE_PREGUNTAS_POR_MES} por mes (Fundadores: {LIMITE_PREGUNTAS_FUNDADOR}). "
    f"obtener_datos trae hasta {LIMITE_FILAS_POR_PEDIDO} filas por pedido. Los cupos del mes "
    "se renuevan el 1° a las 00:00 UTC. Si te frena el límite por minuto, el error dice "
    "cuántos segundos esperar: esperá eso y reintentá una sola vez. En el modo respuestas "
    "se descuenta la respuesta, no el intento: un error o un corte por tiempo no descuentan, "
    "y la misma pregunta repetida dentro de los 5 minutos devuelve la respuesta ya calculada "
    f"sin descontar (hasta {LIMITE_REPETICIONES_POR_MINUTO} repeticiones por minuto). "
    "Igual no repitas la misma pregunta en bucle."
)

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


def _segundos(retry_after: str | int | None) -> int | None:
    """El `Retry-After` del backend en segundos (1 a 3600), o None si no sirve."""
    try:
        seconds = int(str(retry_after).strip())
    except (TypeError, ValueError):
        return None
    return seconds if 0 < seconds <= 3600 else None


_MAX_DETAIL_422 = 3


def error_detail(detail: Any) -> str:
    """El `detail` de una respuesta de error del backend, como texto.

    Un 400/404 del modo datos trae un texto nuestro. Un 422 es la validación
    de FastAPI: una lista de errores de Pydantic (`loc`, `msg`). Antes se
    pasaba como `str(lista)` y se tiraba: el modelo veía «La consulta no
    tiene un formato válido.» sin saber qué corregir, por ejemplo un
    `offset` arriba del tope o 4 columnas en `agrupar_por` (revisión del PR
    #139). Ahora sale «`offset`: Input should be less than or equal to 10000».
    """
    if isinstance(detail, list):
        partes = []
        for err in detail[:_MAX_DETAIL_422]:
            if not isinstance(err, Mapping):
                continue
            loc = [str(p) for p in err.get("loc") or [] if p not in ("body", "query")]
            msg = str(err.get("msg") or "").strip()
            if msg:
                partes.append(f"`{'.'.join(loc)}`: {msg}" if loc else msg)
        return "; ".join(partes)
    return "" if detail is None else str(detail)


def error_message(
    status: int,
    detail: str = "",
    *,
    data_mode: bool = False,
    retry_after: str | int | None = None,
) -> str:
    """Traduce una respuesta de error del backend a un mensaje para el usuario.

    En el modo datos (`/catalogo/*`) los 400/404/503 traen un `detail` que
    armamos nosotros en castellano ("Columnas que no existen en la tabla: …")
    y que le dice al modelo cómo corregir el pedido: se pasa tal cual. En el
    modo respuestas el detalle de esos códigos no es para el usuario.

    `retry_after` es el header del backend: en un 429 por minuto son los
    segundos que faltan para que se abra la ventana (antes el mensaje decía
    siempre "esperá un minuto" y el backend mandaba 60 fijo).
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
                "el modo datos (buscar_datasets, describir_tabla, obtener_datos, "
                "agregar_datos), que tiene su propio cupo."
            )
        return f"{head} Se renuevan el {renewal_label()}." + SUPPORT_LINE + HARDSHIP_LINE
    if status == 429:
        if "minute" in detail_l:
            n = _first_int(detail)
            limite = f" (máximo {n} por minuto)" if n else ""
            seconds = _segundos(retry_after)
            espera = (
                f"esperá {seconds} segundo{'s' if seconds != 1 else ''}"
                if seconds
                else "esperá un minuto"
            )
            return f"Demasiadas consultas seguidas{limite}: {espera} y volvé a intentar una vez."
        if "this ip" in detail_l:
            return (
                "Se alcanzó el límite diario de consultas desde esta conexión. "
                "Se renueva a las 21:00 (hora de Argentina)."
            )
        if "unbilled" in detail_l:
            # Tope diario por persona de corridas que no se cobran (H108).
            return (
                "Se alcanzó el límite diario de preguntas que no terminaron en una "
                "respuesta (tiempos agotados, errores o pedidos de aclaración). Se "
                "renueva a las 21:00 (hora de Argentina). Mientras tanto podés seguir "
                "con el modo datos (buscar_datasets, describir_tabla, obtener_datos, "
                "agregar_datos), que tiene su propio cupo."
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
        if data_mode and detail:
            # Qué parámetro y por qué (`error_detail`): el modelo puede corregirlo.
            return f"La consulta no tiene un formato válido: {detail}."
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


def _number_text(value: float) -> str:
    """Un float sin notación científica: `str(1.2e16)` es "1.2e+16".

    Los totales de `agregar_datos` llegan como número (antes, como texto) y
    un monto en pesos pasa fácil de 10^16.
    """
    text = repr(value)
    if "e" not in text or value != value or value in (float("inf"), float("-inf")):
        return text
    return format(Decimal(text), "f")


def _cell(value: Any) -> str:
    if isinstance(value, float):
        text = _number_text(value)
    else:
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
            # El backend ya los manda al fondo; acá queda claro que no sirven
            # para las otras herramientas (QW11).
            lines.append(
                "   Sin tabla consultable en OpenArg: sólo el link de descarga (no sirve para "
                "describir_tabla, obtener_datos ni agregar_datos)"
            )
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
    if payload.get("aviso"):
        lines.append(f"Aviso: {payload.get('aviso')}")
    frescura = _freshness_line(payload.get("frescura"))
    if frescura:
        lines.append(frescura)
    cols = [c for c in payload.get("columnas") or [] if isinstance(c, Mapping)]
    lines.append("Columnas:\n" + "\n".join(f"- {c.get('nombre')} ({c.get('tipo')})" for c in cols))
    sample = [r for r in payload.get("muestra") or [] if isinstance(r, Mapping)]
    if sample:
        names = [str(c.get("nombre")) for c in cols] or list(sample[0].keys())
        lines.append("Muestra:\n```csv\n" + rows_to_csv(names, sample) + "\n```")
    lines.append(
        "Pedí las filas con `obtener_datos` (columnas, desde, hasta, filtros, limite) o un "
        "total, promedio, conteo o ranking con `agregar_datos`."
    )
    return "\n".join(lines)


def _freshness_line(frescura: Any) -> str | None:
    """La línea «Último dato: … · Leída de la fuente por OpenArg: …» (auditoría 3.4)."""
    if not isinstance(frescura, Mapping):
        return None
    parts: list[str] = []
    if frescura.get("ultimo_dato") and frescura.get("serie"):
        # De una muestra de la tabla: puede haber datos posteriores.
        aprox = " (aproximado)" if frescura.get("aproximado") else ""
        parts.append(f"Último dato{aprox}: {frescura.get('ultimo_dato')}")
    if frescura.get("fecha_corte"):
        parts.append(f"Fecha de corte: {frescura.get('fecha_corte')}")
    if frescura.get("actualizada"):
        parts.append(f"Leída de la fuente por OpenArg: {frescura.get('actualizada')}")
    nota = str(frescura.get("nota") or "").strip()
    if not parts and not nota:
        return None
    lines = ["Frescura: " + " · ".join(parts)] if parts else []
    if nota:
        # Que es una foto, o que hace meses que no se relee: cambia lo que se
        # puede afirmar con la tabla.
        lines.append(f"Nota: {nota}")
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
        siguiente = payload.get("siguiente_offset")
        pagina = f" o pedí las siguientes con `offset={siguiente}`" if siguiente else ""
        head += (
            "\nHay más filas: acotá con `desde`/`hasta` o `filtros`, subí `limite` (máximo 500)"
            f"{pagina}. Para un total, un promedio o un ranking usá `agregar_datos` en vez de "
            "sumar filas."
        )
    return head + "\n```csv\n" + rows_to_csv(columns, rows) + "\n```"


def format_aggregate(payload: Mapping[str, Any]) -> str:
    """El resultado de `agregar_datos`: sobre cuántas filas, los grupos en CSV y la fuente."""
    source = _link(
        str(payload.get("fuente") or payload.get("tabla") or ""), str(payload.get("url") or "")
    )
    calculo = str(payload.get("calculo") or "cálculo")
    tabla = payload.get("tabla")
    notas = [str(n) for n in payload.get("notas") or [] if str(n).strip()]
    rows = [r for r in payload.get("filas") or [] if isinstance(r, Mapping)]
    if not rows:
        # Ningún valor que citar: cero filas cumplen los filtros o ninguna
        # tiene un número. Nunca un "0" que parezca un dato.
        lines = [f"Sin resultado para {calculo} en `{tabla}`. Fuente: {source}"]
        if payload.get("aviso"):
            lines.append(str(payload["aviso"]))
        lines.extend(notas)
        return "\n".join(lines)
    usadas = payload.get("filas_usadas")
    sobre = f" sobre {_miles(int(usadas))} filas" if isinstance(usadas, int) else ""
    head = f"{calculo} en `{tabla}`{sobre}, calculado por OpenArg. Fuente: {source}"
    if notas:
        head += "\n" + "\n".join(notas)
    columns = [str(c) for c in payload.get("columnas") or []] or list(rows[0].keys())
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
