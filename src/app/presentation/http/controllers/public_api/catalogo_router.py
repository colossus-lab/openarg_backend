"""Modo datos de la API pública: buscar datasets, describir y leer tablas.

Lo usa el MCP público (herramientas `buscar_datasets`, `describir_tabla`,
`obtener_datos`). Nada de esto pasa por el LLM: el modelo del usuario hace el
razonamiento y OpenArg sólo sirve catálogo y filas. Auth con la clave
`oarg_sk_` y cupo propio (`check_catalog_rate_limit`), separado de las 10
preguntas diarias de `/ask`.

Las consultas se arman en `app.application.public_catalog` —nunca con SQL del
usuario— y corren por `ISQLSandbox.execute_readonly`: rol de sólo lectura,
timeout, tope de filas y el mismo validador que NL2SQL.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.application.api_key_service import check_catalog_rate_limit
from app.application.catalog.collapse import collapse_hits
from app.application.catalog.national_prior import national_prior
from app.application.public_catalog import (
    DEFAULT_LIMIT,
    MAX_COLUMNS,
    MAX_FILTERS,
    MAX_LIMIT,
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_date_range_query,
    build_sample_query,
    date_column,
    is_internal_column,
    resolve_table,
)
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.domain.ports.search.vector_search import IVectorSearch
from app.domain.value_objects.table_reference import bare_name
from app.presentation.http.controllers.public_api.ask_router import authenticate_bearer
from app.presentation.http.controllers.public_api.usage_log import log_rejection, track_usage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/catalogo", tags=["public-api"])

_MAX_TABLES_PER_DATASET = 5
_DESCRIPTION_CHARS = 400
# Mismo umbral que `/data/search`.
_MIN_SIMILARITY = 0.40
# Datasets pedidos por resultado mostrado: varios son el mismo archivo
# (gemelos, espejos, CSV y JSON) y se juntan en `collapse_hits`.
_CANDIDATES_PER_RESULT = 4


class TablaResumen(BaseModel):
    tabla: str
    filas: int | None


class DatasetEncontrado(BaseModel):
    dataset_id: str
    titulo: str
    descripcion: str
    portal: str
    url: str
    tablas: list[TablaResumen]
    # Varios recursos de un package comparten título y descripción; el
    # archivo es lo que los distingue ("actas-cabecera-137-2.0.csv").
    archivo: str | None = None
    formato: str | None = None


class BuscarResponse(BaseModel):
    resultados: list[DatasetEncontrado]


class Columna(BaseModel):
    nombre: str
    tipo: str


class TablaResponse(BaseModel):
    tabla: str
    titulo: str
    portal: str
    url: str
    filas: int | None
    columnas: list[Columna]
    columna_fecha: str | None
    desde: str | None
    hasta: str | None
    muestra: list[dict[str, Any]]


class DatosRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tabla: str = Field(..., min_length=1, max_length=200)
    columnas: list[str] | None = Field(default=None, max_length=MAX_COLUMNS)
    desde: str | None = Field(default=None, max_length=10)
    hasta: str | None = Field(default=None, max_length=10)
    filtros: dict[str, str] | None = Field(default=None, max_length=MAX_FILTERS)
    orden: str = "asc"
    limite: int = Field(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT)


class DatosResponse(BaseModel):
    tabla: str
    columnas: list[str]
    filas: list[dict[str, Any]]
    cantidad: int
    truncado: bool
    fuente: str
    url: str


async def _authorize(
    request: Request,
    api_key_repo: IApiKeyRepository,
    cache: ICacheService,
    credits: ICreditRepository,
    *,
    endpoint: str,
    tool: str,
) -> ApiKey:
    api_key = await authenticate_bearer(request, api_key_repo)
    try:
        await check_catalog_rate_limit(api_key, cache, credits)
    except HTTPException as exc:
        await log_rejection(
            api_key_repo,
            cache,
            api_key,
            request,
            endpoint=endpoint,
            mode="datos",
            tool=tool,
            status_code=exc.status_code,
        )
        raise
    return api_key


async def _run(sandbox: ISQLSandbox, sql: str) -> list[dict[str, Any]]:
    result = await sandbox.execute_readonly(sql)
    if result.error:
        # El validador rechaza, p. ej., un valor de filtro con una palabra
        # reservada ("do", "set"). No es un error del servidor.
        logger.info("catalogo: consulta rechazada: %s", result.error[:200])
        raise HTTPException(
            status_code=400,
            detail="La consulta no se pudo ejecutar. Probá con otros filtros o menos columnas.",
        )
    return result.rows


@router.get("/buscar", response_model=BuscarResponse)
@inject  # type: ignore[untyped-decorator]
async def buscar(
    request: Request,
    sandbox: FromDishka[ISQLSandbox],
    vector_search: FromDishka[IVectorSearch],
    embedding: FromDishka[IEmbeddingProvider],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
    q: str = Query(..., min_length=2, max_length=300),
    portal: str | None = Query(default=None, max_length=80),
    limite: int = Query(default=10, ge=1, le=25),
) -> BuscarResponse:
    """Búsqueda semántica en el catálogo (un embedding, sin LLM).

    Semántica y no híbrida: en staging la híbrida tardaba 3,5 s por pedido.
    Por `search_datasets_ann`: el índice HNSW con la búsqueda exacta detrás
    cuando su respuesta no convence. Se piden varios datasets por resultado
    porque muchos son el mismo archivo y `collapse_hits` los junta en uno,
    mostrando una sola copia; los que no tienen tabla consultable van al
    fondo. Loguea cuánto tardó cada etapa (sin la consulta).
    """
    endpoint, tool = "/api/v1/catalogo/buscar", "buscar_datasets"
    api_key = await _authorize(request, api_key_repo, cache, credits, endpoint=endpoint, tool=tool)
    async with track_usage(api_key_repo, api_key, request, endpoint=endpoint, tool=tool):
        t0 = time.perf_counter()
        try:
            vector = await embedding.embed(q)
        except Exception:
            logger.exception("catalogo/buscar: embedding falló")
            raise HTTPException(status_code=503, detail="La búsqueda no está disponible ahora.")
        t_embed = time.perf_counter()
        hits = await vector_search.search_datasets_ann(
            query_embedding=vector,
            limit=limite * _CANDIDATES_PER_RESULT,
            portal_filter=portal,
            min_similarity=_MIN_SIMILARITY,
        )
        t_search = time.perf_counter()
        if portal and not hits:
            # Un portal que no existe vacía el filtro: antes salía una lista
            # vacía con 200 y el MCP decía "no encontré datasets".
            portals = await vector_search.known_portals()
            if portals and portal not in portals:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"No existe el portal {portal!r}. Portales válidos: "
                        + ", ".join(portals)
                        + ". O buscá sin filtrar por portal."
                    ),
                )
        tables = await sandbox.find_tables(dataset_ids=[str(h.dataset_id) for h in hits])
        profiles = await sandbox.table_profiles([t.table_name for t in tables])
        t_tables = time.perf_counter()

        collapsed = collapse_hits(hits, tables, profiles, prior=national_prior(q))
        # Sin tabla consultable al fondo (orden estable): ocupaban lugares con
        # "Sin tabla consultable en OpenArg" y el modelo no puede usarlos.
        collapsed.sort(key=lambda c: not c.tables)
        results = [
            DatasetEncontrado(
                dataset_id=str(c.hit.dataset_id),
                titulo=c.hit.title,
                descripcion=(c.hit.description or "")[:_DESCRIPTION_CHARS],
                portal=c.hit.portal,
                url=c.hit.download_url or "",
                tablas=[
                    TablaResumen(tabla=t.table_name, filas=t.row_count)
                    for t in c.tables[:_MAX_TABLES_PER_DATASET]
                ],
                archivo=c.archivo,
                formato=c.formato,
            )
            for c in collapsed[:limite]
        ]
        logger.info(
            "catalogo/buscar: embed_ms=%.0f busqueda_ms=%.0f tablas_ms=%.0f total_ms=%.0f "
            "hits=%d archivos=%d resultados=%d",
            (t_embed - t0) * 1000,
            (t_search - t_embed) * 1000,
            (t_tables - t_search) * 1000,
            (time.perf_counter() - t0) * 1000,
            len(hits),
            len(collapsed),
            len(results),
        )
        return BuscarResponse(resultados=results)


@router.get("/tabla", response_model=TablaResponse)
@inject  # type: ignore[untyped-decorator]
async def describir_tabla(
    request: Request,
    sandbox: FromDishka[ISQLSandbox],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
    nombre: str = Query(..., min_length=1, max_length=200),
) -> TablaResponse:
    """Columnas con su tipo, filas, período cubierto y una muestra."""
    endpoint, tool = "/api/v1/catalogo/tabla", "describir_tabla"
    api_key = await _authorize(request, api_key_repo, cache, credits, endpoint=endpoint, tool=tool)
    async with track_usage(api_key_repo, api_key, request, endpoint=endpoint, tool=tool):
        table = resolve_table(nombre, await sandbox.find_tables(table_names=[nombre]))
        if table is None:
            raise HTTPException(
                status_code=404,
                detail="No existe esa tabla en el catálogo. Usá buscar_datasets para encontrar una.",
            )

        types = (await sandbox.get_column_types([table.table_name])).get(table.table_name, [])
        columns = [(c, t) for c, t in types if not is_internal_column(c)]
        names = [c for c, _ in columns]
        fecha = date_column(names)

        desde = hasta = None
        if fecha:
            rows = await _run(sandbox, build_date_range_query(table.table_name, fecha))
            if rows:
                desde, hasta = rows[0].get("desde"), rows[0].get("hasta")
        muestra = await _run(sandbox, build_sample_query(table.table_name, names))
        source = (await sandbox.get_table_sources([table.table_name])).get(
            bare_name(table.table_name)
        )

        return TablaResponse(
            tabla=table.table_name,
            titulo=source.title if source else "",
            portal=source.portal if source else "",
            url=source.url if source else "",
            filas=table.row_count,
            columnas=[Columna(nombre=c, tipo=t) for c, t in columns],
            columna_fecha=fecha,
            desde=desde,
            hasta=hasta,
            muestra=muestra,
        )


@router.post("/datos", response_model=DatosResponse)
@inject  # type: ignore[untyped-decorator]
async def obtener_datos(
    request: Request,
    body: DatosRequest,
    sandbox: FromDishka[ISQLSandbox],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
) -> DatosResponse:
    """Filas de una tabla: columnas, período, filtros de igualdad y orden por fecha."""
    endpoint, tool = "/api/v1/catalogo/datos", "obtener_datos"
    api_key = await _authorize(request, api_key_repo, cache, credits, endpoint=endpoint, tool=tool)
    async with track_usage(api_key_repo, api_key, request, endpoint=endpoint, tool=tool):
        table = resolve_table(body.tabla, await sandbox.find_tables(table_names=[body.tabla]))
        if table is None:
            raise HTTPException(
                status_code=404,
                detail="No existe esa tabla en el catálogo. Usá buscar_datasets para encontrar una.",
            )
        types = (await sandbox.get_column_types([table.table_name])).get(table.table_name, [])
        try:
            sql, columns = build_data_query(
                DataRequest(
                    table=table.table_name,
                    available_columns=[c for c, _ in types],
                    columns=body.columnas,
                    desde=body.desde,
                    hasta=body.hasta,
                    filtros=body.filtros,
                    orden=body.orden,
                    limite=body.limite,
                )
            )
        except CatalogRequestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

        rows = await _run(sandbox, sql)
        source = (await sandbox.get_table_sources([table.table_name])).get(
            bare_name(table.table_name)
        )
        return DatosResponse(
            tabla=table.table_name,
            columnas=columns,
            filas=rows,
            cantidad=len(rows),
            truncado=len(rows) >= body.limite,
            fuente=source.title if source else "",
            url=source.url if source else "",
        )
