"""Registro de uso de la API pública en `api_usage`, para el tablero de admin.

Tres entradas:

- `log_usage`: una fila por pedido que llegó a ejecutarse (`/ask` y el modo
  datos). En el modo datos nunca se guarda `question`: qué buscó alguien o qué
  tabla leyó no queda escrito en ningún lado.
- `track_usage`: envuelve un handler del modo datos y registra lo que termine
  pasando (200, 400, 404, 503…) con su duración.
- `log_rejection`: los 429/503 del cupo, que antes se lanzaban sin dejar
  rastro. Son la mejor señal de demanda, pero un cliente que insiste puede
  generar miles; se registra a lo sumo uno por clave, modo y minuto.

Nada de esto puede romper la respuesta al usuario: si la base falla, se loguea
en debug y sigue.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import HTTPException

from app.application.public_api_clients import client_family
from app.domain.entities.api_key.api_key import ApiKey, ApiUsage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fastapi import Request

    from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
    from app.domain.ports.cache.cache_port import ICacheService

logger = logging.getLogger(__name__)

# Headers que agrega el servidor MCP público (mcp_publico/core.py). Cualquiera
# los puede mandar a mano: sirven para estadística, no para decidir nada.
VIA_HEADER = "X-OpenArg-Via"
CLIENT_HEADER = "X-OpenArg-Client"

_QUESTION_CHARS = 200
_USER_AGENT_CHARS = 160
_REJECTION_LOG_TTL = 60


def request_origin(request: Request) -> tuple[str, str | None, str | None]:
    """`(via, client, user_agent)` de un pedido.

    Por MCP, el programa que importa es el del usuario (Claude Code, Cursor),
    que el servidor MCP informa en `X-OpenArg-Client`; el `User-Agent` de ese
    pedido es el de httpx del propio servidor MCP y no dice nada.
    """
    via = "mcp" if request.headers.get(VIA_HEADER, "").strip().lower() == "mcp" else "api"
    raw = request.headers.get(CLIENT_HEADER) if via == "mcp" else request.headers.get("User-Agent")
    raw = (raw or "").strip()[:_USER_AGENT_CHARS] or None
    return via, client_family(raw), raw


async def log_usage(
    repo: IApiKeyRepository,
    api_key: ApiKey,
    request: Request,
    *,
    endpoint: str,
    mode: str,
    tool: str,
    status_code: int,
    question: str | None = None,
    tokens_used: int = 0,
    duration_ms: int = 0,
    model: str | None = None,
    cost_usd: float | None = None,
) -> None:
    via, client, user_agent = request_origin(request)
    try:
        await repo.record_usage(
            ApiUsage(
                api_key_id=api_key.id,
                endpoint=endpoint,
                question=question[:_QUESTION_CHARS] if question and mode == "respuestas" else None,
                status_code=status_code,
                tokens_used=tokens_used,
                duration_ms=duration_ms,
                mode=mode,
                tool=tool,
                via=via,
                client=client,
                user_agent=user_agent,
                model=model or None,
                cost_usd=cost_usd,
            )
        )
    except Exception:
        logger.debug("Failed to log API usage", exc_info=True)


@asynccontextmanager
async def track_usage(
    repo: IApiKeyRepository,
    api_key: ApiKey,
    request: Request,
    *,
    endpoint: str,
    tool: str,
) -> AsyncIterator[None]:
    """Registra un pedido del modo datos con el estado con el que termine."""
    start = time.monotonic()
    status_code = 200
    try:
        yield
    except HTTPException as exc:
        status_code = exc.status_code
        raise
    except Exception:
        status_code = 500
        raise
    finally:
        await log_usage(
            repo,
            api_key,
            request,
            endpoint=endpoint,
            mode="datos",
            tool=tool,
            status_code=status_code,
            duration_ms=int((time.monotonic() - start) * 1000),
        )
        if status_code == 200:
            try:
                await repo.update_last_used(api_key.id)
            except Exception:
                logger.debug("Failed to update last_used_at", exc_info=True)


async def log_rejection(
    repo: IApiKeyRepository,
    cache: ICacheService,
    api_key: ApiKey,
    request: Request,
    *,
    endpoint: str,
    mode: str,
    tool: str,
    status_code: int,
    question: str | None = None,
) -> None:
    """Registra un 429/503 del cupo, uno por clave, modo y minuto como mucho."""
    minute = datetime.now(UTC).strftime("%Y%m%d%H%M")
    try:
        seen = await cache.increment_with_ttl(
            f"rl:logged:{api_key.id}:{mode}:{minute}", _REJECTION_LOG_TTL
        )
    except Exception:
        # Sin Redis no hay forma de acotar la cantidad: mejor no registrar.
        logger.debug("Rejection log throttle unavailable", exc_info=True)
        return
    if seen != 1:
        return
    await log_usage(
        repo,
        api_key,
        request,
        endpoint=endpoint,
        mode=mode,
        tool=tool,
        status_code=status_code,
        question=question,
    )
