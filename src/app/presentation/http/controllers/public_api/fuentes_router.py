"""Catálogo público: qué portales cubre OpenArg y cuántos datasets tiene cada uno.

Lo usa el MCP público (herramienta ``listar_fuentes``). No pasa por el LLM,
así que no descuenta del cupo de preguntas de ``/ask``; tiene su propio
límite diario por clave (``check_catalog_rate_limit``).
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text

from app.application.api_key_service import check_catalog_rate_limit
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession  # noqa: TC001
from app.presentation.http.controllers.public_api.ask_router import authenticate_bearer
from app.presentation.http.controllers.public_api.usage_log import log_rejection, track_usage

router = APIRouter(tags=["public-api"])

_ENDPOINT = "/api/v1/fuentes"
_TOOL = "listar_fuentes"


class Fuente(BaseModel):
    portal: str
    datasets: int


class FuentesResponse(BaseModel):
    total_datasets: int
    fuentes: list[Fuente]


@router.get("/fuentes", response_model=FuentesResponse)
@inject  # type: ignore[untyped-decorator]
async def listar_fuentes(
    request: Request,
    session: FromDishka[MainAsyncSession],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
) -> FuentesResponse:
    """List the portals OpenArg indexes, with their dataset counts.

    Auth: ``Authorization: Bearer oarg_sk_xxx``. Same query as
    ``GET /datasets/stats``, which is not reachable with a user key.
    """
    api_key = await authenticate_bearer(request, api_key_repo)
    try:
        await check_catalog_rate_limit(api_key, cache, credits)
    except HTTPException as exc:
        await log_rejection(
            api_key_repo,
            cache,
            api_key,
            request,
            endpoint=_ENDPOINT,
            mode="datos",
            tool=_TOOL,
            status_code=exc.status_code,
        )
        raise

    async with track_usage(api_key_repo, api_key, request, endpoint=_ENDPOINT, tool=_TOOL):
        result = await session.execute(
            text(
                "SELECT portal, COUNT(*) AS count FROM datasets GROUP BY portal ORDER BY count DESC"
            )
        )
        fuentes = [Fuente(portal=row.portal, datasets=row.count) for row in result.fetchall()]
        return FuentesResponse(total_datasets=sum(f.datasets for f in fuentes), fuentes=fuentes)
