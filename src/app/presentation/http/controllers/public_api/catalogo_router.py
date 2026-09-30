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
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.application.api_key_service import check_catalog_rate_limit
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
    *,
    endpoint: str,
    tool: str,
) -> ApiKey:
    api_key = await authenticate_bearer(request, api_key_repo)
    try:
        await check_catalog_rate_limit(api_key, cache)
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
    q: str = Query(..., min_length=2, max_length=300),
    portal: str | None = Query(default=None, max_length=80),
    limite: int = Query(default=10, ge=1, le=25),
) -> BuscarResponse:
    """Búsqueda semántica en el catálogo (un embedding, sin LLM).

    Semántica y no híbrida: en staging la híbrida tardaba 3,5 s por pedido y
    la semántica 0,5 s, que es la misma que usa `/data/search`. Se pide el
    doble de resultados porque después se agrupan los duplicados.
    """
    endpoint, tool = "/api/v1/catalogo/buscar", "buscar_datasets"
    api_key = await _authorize(request, api_key_repo, cache, endpoint=endpoint, tool=tool)
    async with track_usage(api_key_repo, api_key, request, endpoint=endpoint, tool=tool):
        try:
            vector = await embedding.embed(q)
        except Exception:
            logger.exception("catalogo/buscar: embedding falló")
            raise HTTPException(status_code=503, detail="La búsqueda no está disponible ahora.")
        hits = await vector_search.search_datasets(
            query_embedding=vector,
            limit=limite * 2,
            portal_filter=portal,
            min_similarity=_MIN_SIMILARITY,
        )

        tables_by_dataset: dict[str, list[TablaResumen]] = {}
        for t in await sandbox.find_tables(dataset_ids=[str(h.dataset_id) for h in hits]):
            # Una tabla con 0 filas es una versión vieja o una descarga fallida:
            # no sirve para consultar y confunde al modelo.
            if t.dataset_id and t.row_count != 0:
                tables_by_dataset.setdefault(str(t.dataset_id), []).append(
                    TablaResumen(tabla=t.table_name, filas=t.row_count)
                )

        # El catálogo tiene el mismo dataset varias veces (la migración de
        # datos.gob.ar regeneró IDs). Mismo título + misma URL = el mismo dataset:
        # se muestra una vez, con todas sus tablas.
        merged: dict[tuple[str, str], DatasetEncontrado] = {}
        for h in hits:
            key = (h.title.strip().lower(), (h.download_url or "").strip())
            tables = tables_by_dataset.get(str(h.dataset_id), [])
            if key in merged:
                known = {t.tabla for t in merged[key].tablas}
                merged[key].tablas.extend(t for t in tables if t.tabla not in known)
                continue
            merged[key] = DatasetEncontrado(
                dataset_id=str(h.dataset_id),
                titulo=h.title,
                descripcion=(h.description or "")[:_DESCRIPTION_CHARS],
                portal=h.portal,
                url=h.download_url or "",
                tablas=list(tables),
            )
        results = list(merged.values())[:limite]
        for r in results:
            r.tablas = sorted(r.tablas, key=lambda t: -(t.filas or 0))[:_MAX_TABLES_PER_DATASET]
        return BuscarResponse(resultados=results)


@router.get("/tabla", response_model=TablaResponse)
@inject  # type: ignore[untyped-decorator]
async def describir_tabla(
    request: Request,
    sandbox: FromDishka[ISQLSandbox],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    nombre: str = Query(..., min_length=1, max_length=200),
) -> TablaResponse:
    """Columnas con su tipo, filas, período cubierto y una muestra."""
    endpoint, tool = "/api/v1/catalogo/tabla", "describir_tabla"
    api_key = await _authorize(request, api_key_repo, cache, endpoint=endpoint, tool=tool)
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
) -> DatosResponse:
    """Filas de una tabla: columnas, período, filtros de igualdad y orden por fecha."""
    endpoint, tool = "/api/v1/catalogo/datos", "obtener_datos"
    api_key = await _authorize(request, api_key_repo, cache, endpoint=endpoint, tool=tool)
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
