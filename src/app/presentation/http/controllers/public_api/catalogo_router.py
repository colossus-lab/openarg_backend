"""Modo datos de la API pública: buscar datasets, describir y leer tablas.

Lo usa el MCP público (herramientas `buscar_datasets`, `describir_tabla`,
`obtener_datos`). Nada de esto pasa por el LLM: el modelo del usuario hace el
razonamiento y OpenArg sólo sirve catálogo y filas. Auth con la clave
`oarg_sk_` y cupo propio (`check_catalog_rate_limit`), separado de las 10
preguntas diarias de `/ask`.

Las consultas se arman en `app.application.public_catalog` —nunca con SQL del
usuario— y corren por `ISQLSandbox.execute_readonly` con los valores como
parámetros ligados: rol de sólo lectura, timeout, tope de filas y el mismo
validador que NL2SQL. Si un pedido no devuelve filas, la respuesta dice por
qué y qué valores existen (`app.application.consultas.sugerencias`).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.application.api_key_service import check_catalog_rate_limit
from app.application.consultas.fechas import aviso_formato_guardado
from app.application.consultas.filtros import notas_de_filtros
from app.application.consultas.preparar import Preparado, describir_periodo, ejecutar, preparar
from app.application.consultas.sugerencias import diagnosticar_vacio
from app.application.public_catalog import (
    DEFAULT_LIMIT,
    MAX_COLUMNS,
    MAX_FILTERS,
    MAX_LIMIT,
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_sample_query,
    is_internal_column,
    resolve_date_column,
    resolve_table,
)
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox, SandboxResult
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
    # Si el período no se pudo calcular o la columna de fecha tiene un formato
    # que no se sabe filtrar.
    aviso_fecha: str | None = None


class FiltroItem(BaseModel):
    """Un filtro con operador: =, !=, >, >=, <, <=, contiene o en."""

    model_config = ConfigDict(extra="forbid")
    columna: str = Field(..., min_length=1, max_length=200)
    operador: str = Field(default="=", max_length=20)
    valor: str | int | float | None = None
    # Números también: un cliente manda `en [2020, 2021]` sobre una columna de
    # año. Con list[str] era un 422 genérico ("Input should be a valid string").
    valores: list[str | int | float] | None = Field(default=None, max_length=50)


class DatosRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tabla: str = Field(..., min_length=1, max_length=200)
    columnas: list[str] | None = Field(default=None, max_length=MAX_COLUMNS)
    desde: str | None = Field(default=None, max_length=10)
    hasta: str | None = Field(default=None, max_length=10)
    # {columna: valor} (igualdad, la forma de siempre) o una lista de filtros
    # con operador.
    filtros: dict[str, str | int | float] | list[FiltroItem] | None = Field(
        default=None, max_length=MAX_FILTERS
    )
    columna_fecha: str | None = Field(default=None, max_length=200)
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
    # Con cero filas: por qué, y qué valores existen ({columna: [{valor, filas}]}).
    aviso: str | None = None
    sugerencias: dict[str, list[dict[str, Any]]] | None = None
    # Cómo se aplicaron los filtros y el período: con qué valores de la tabla
    # se comparó lo pedido, qué se buscó tal cual en una tabla grande, y si
    # las fechas se leyeron sólo en su forma dominante.
    filtros_aplicados: list[str] | None = None


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


def _error_detail(result: SandboxResult) -> str:
    """Un mensaje que le dice al modelo cliente qué hacer, según qué falló.

    Antes todo era "Probá con otros filtros o menos columnas", también un
    timeout en una tabla de millones de filas o una tabla bloqueada por un
    problema de calidad, donde el consejo lleva por mal camino.
    """
    if result.error_kind == "timeout":
        return (
            "La consulta tardó demasiado: la tabla es muy grande para ese pedido. Acotá con "
            "`desde`/`hasta` o con `filtros` más específicos."
        )
    if result.error_kind == "blocked" and result.error:
        return result.error
    return "La consulta no se pudo ejecutar. Probá con otros filtros o menos columnas."


async def _run(
    sandbox: ISQLSandbox, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    result = await ejecutar(sandbox, sql, params or {})
    if result.error:
        # No es un error del servidor: un timeout, una tabla bloqueada o un
        # rechazo del validador.
        logger.info("catalogo: consulta rechazada (%s): %s", result.error_kind, result.error[:200])
        raise HTTPException(status_code=400, detail=_error_detail(result))
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

    Semántica y no híbrida: en staging la híbrida tardaba 3,5 s por pedido y
    la semántica 0,5 s, que es la misma que usa `/data/search`. Por el índice
    HNSW (`search_datasets_ann`): la exacta recorría todos los chunks y
    tardaba de 1 a más de 60 s. Se pide el doble de resultados porque después
    se agrupan los duplicados.
    """
    endpoint, tool = "/api/v1/catalogo/buscar", "buscar_datasets"
    api_key = await _authorize(request, api_key_repo, cache, credits, endpoint=endpoint, tool=tool)
    async with track_usage(api_key_repo, api_key, request, endpoint=endpoint, tool=tool):
        try:
            vector = await embedding.embed(q)
        except Exception:
            logger.exception("catalogo/buscar: embedding falló")
            raise HTTPException(status_code=503, detail="La búsqueda no está disponible ahora.")
        hits = await vector_search.search_datasets_ann(
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
        fecha = resolve_date_column(columns)

        # Ni el período ni la muestra tiran la descripción: si una de esas
        # consultas falla (timeout, una columna llamada "Set."), se describe
        # la tabla igual. Antes era un 400 genérico.
        periodo = await describir_periodo(sandbox, table.table_name, fecha)
        sample = await ejecutar(sandbox, build_sample_query(table.table_name, names), {})
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
            columna_fecha=fecha.nombre if fecha else None,
            desde=periodo.desde,
            hasta=periodo.hasta,
            muestra=[] if sample.error else sample.rows,
            aviso_fecha=periodo.aviso,
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
    """Filas de una tabla: columnas, período, filtros con operador y orden por fecha."""
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
        raw_filters: Any = body.filtros
        if isinstance(body.filtros, list):
            raw_filters = [f.model_dump(exclude_none=True) for f in body.filtros]
        try:
            req = DataRequest(
                table=table.table_name,
                available_columns=[c for c, _ in types],
                column_types=types,
                columns=body.columnas,
                desde=body.desde,
                hasta=body.hasta,
                filtros=raw_filters,
                orden=body.orden,
                limite=body.limite,
                columna_fecha=body.columna_fecha,
            )
            # Primero sólo valida (puro, antes de tocar la base); con filtros o
            # fecha, lee estadísticas y formatos y arma la consulta definitiva.
            query = build_data_query(req)
            prep = Preparado(filtros=query.filtros, filas_estimadas=table.row_count or None)
            if query.filtros or query.fecha:
                prep = await preparar(
                    sandbox,
                    table.table_name,
                    query.tipos,
                    query.filtros,
                    row_count=table.row_count,
                    fecha=query.fecha,
                )
                query = build_data_query(
                    replace(
                        req,
                        filtros=prep.filtros,
                        tolerante=prep.tolerante,
                        formatos=prep.formatos,
                        formato_fecha=prep.formato_fecha,
                    )
                )
        except CatalogRequestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

        rows = await _run(sandbox, query.sql, query.params)
        aviso: str | None = None
        sugerencias: dict[str, list[dict[str, Any]]] | None = None
        if not rows and (query.filtros or query.desde or query.hasta):
            try:
                diag = await diagnosticar_vacio(
                    sandbox,
                    tabla=table.table_name,
                    tipos=query.tipos,
                    filtros=query.filtros,
                    fecha=query.fecha,
                    desde=query.desde,
                    hasta=query.hasta,
                    tolerante=prep.tolerante,
                    formatos=prep.formatos,
                    stats=prep.stats,
                    filas_estimadas=prep.filas_estimadas,
                )
            except CatalogRequestError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from None
            aviso, sugerencias = diag.aviso, diag.sugerencias or None
        notas = notas_de_filtros(query.filtros, query.tipos, tolerante=prep.tolerante)
        aviso_fecha = aviso_formato_guardado(query.fecha)
        if aviso_fecha:
            notas.append(aviso_fecha)
        source = (await sandbox.get_table_sources([table.table_name])).get(
            bare_name(table.table_name)
        )
        return DatosResponse(
            tabla=table.table_name,
            columnas=query.columns,
            filas=rows,
            cantidad=len(rows),
            truncado=len(rows) >= body.limite,
            fuente=source.title if source else "",
            url=source.url if source else "",
            aviso=aviso,
            sugerencias=sugerencias,
            filtros_aplicados=notas or None,
        )
