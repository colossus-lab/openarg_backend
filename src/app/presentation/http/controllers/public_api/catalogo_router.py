"""Modo datos de la API pública: buscar datasets, describir, leer y agregar tablas.

Lo usa el MCP público (herramientas `buscar_datasets`, `describir_tabla`,
`obtener_datos`, `agregar_datos`). Nada de esto pasa por el LLM: el modelo del
usuario hace el razonamiento y OpenArg sólo sirve catálogo, filas y cuentas
armadas por nuestro código. Auth con la clave `oarg_sk_` y cupo propio
(`check_catalog_rate_limit`), separado de las preguntas de `/ask`.

Las consultas se arman en `app.application.public_catalog` y
`app.application.answers.aggregates` —nunca con SQL del usuario— y corren por
`ISQLSandbox.execute_readonly` con los valores como parámetros ligados: rol de
sólo lectura, timeout, tope de filas y el mismo validador que NL2SQL. Si un
pedido no devuelve filas, la respuesta dice por qué y qué valores existen
(`app.application.consultas.sugerencias`). Si el sandbox rechaza la consulta,
el mensaje dice cuál de los rechazos fue (`_error_detail`) y el header
`X-OpenArg-Error` lo da en una palabra.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from app.application.answers.aggregates import MAX_FILTERS as MAX_AGG_FILTERS
from app.application.answers.aggregates import MAX_GROUP_BY
from app.application.answers.aggregates import MAX_LIMIT as MAX_AGG_LIMIT
from app.application.api_key_service import check_catalog_rate_limit
from app.application.catalog.collapse import collapse_hits
from app.application.catalog.national_prior import national_prior
from app.application.consultas.agregar import PedidoAgregado, agregar
from app.application.consultas.fechas import aviso_formato_guardado
from app.application.consultas.filtros import notas_de_filtros
from app.application.consultas.preparar import (
    Preparado,
    describir_periodo,
    ejecutar,
    estadisticas,
    filas_estimadas,
    preparar,
)
from app.application.consultas.sugerencias import diagnosticar_vacio
from app.application.public_catalog import (
    DEFAULT_LIMIT,
    MAX_COLUMNS,
    MAX_FILTERS,
    MAX_LIMIT,
    MAX_OFFSET,
    ORDEN_FISICO_MAX_FILAS,
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_sample_query,
    is_internal_column,
    json_rows,
    resolve_date_column,
    resolve_table,
)
from app.application.quality.data_age import DataAge, data_age_for, table_freshness
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


class Frescura(BaseModel):
    """De cuándo es lo que tiene la tabla (auditoría 3.4)."""

    # Última vez que OpenArg leyó la tabla de su fuente (AAAA-MM-DD).
    actualizada: str | None = None
    dias_desde_actualizacion: int | None = None
    # Último período con datos según la columna de fecha.
    ultimo_dato: str | None = None
    # Sólo las fotos: un único período, el de la lectura o el anterior (el día
    # al que corresponden los datos). Sin columna de fecha o con un período
    # pasado no hay fecha de corte: la de lectura no es la de los datos.
    fecha_corte: str | None = None
    # None: tiene columna de fecha pero no se pudo calcular qué período cubre
    # (ni serie ni foto: no se sabe cuál es el último dato).
    serie: bool | None = False
    # `ultimo_dato` sale de una muestra de la tabla: puede haber posteriores.
    aproximado: bool = False
    nota: str | None = None


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
    frescura: Frescura | None = None
    # Si la tabla no se puede consultar (un problema de calidad sin resolver):
    # antes, sin columna de fecha, sólo se notaba en que la muestra venía vacía.
    aviso: str | None = None


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
    # Filas a saltear: la página siguiente es `offset = siguiente_offset`.
    offset: int = Field(default=0, ge=0, le=MAX_OFFSET)


class DatosResponse(BaseModel):
    tabla: str
    columnas: list[str]
    filas: list[dict[str, Any]]
    cantidad: int
    # Hay más filas que las devueltas (se piden `limite + 1`).
    truncado: bool
    fuente: str
    url: str
    # Con cero filas: por qué, y qué valores existen ({columna: [{valor, filas}]}).
    aviso: str | None = None
    sugerencias: dict[str, list[dict[str, Any]]] | None = None
    # Cómo se aplicaron los filtros y el período: con qué valores de la tabla
    # se comparó lo pedido, qué se buscó tal cual en una tabla grande, y si
    # las fechas se leyeron sólo en su forma dominante. También cómo quedaron
    # ordenadas las filas si hace falta saberlo para seguir.
    filtros_aplicados: list[str] | None = None
    # Con `truncado`: el `offset` de la página siguiente.
    siguiente_offset: int | None = None


class AgregarRequest(BaseModel):
    """Un total, promedio, conteo, mínimo o máximo, opcionalmente agrupado."""

    model_config = ConfigDict(extra="forbid")
    tabla: str = Field(..., min_length=1, max_length=200)
    # suma, promedio, conteo, minimo, maximo (o min, max, sum, avg, count).
    operacion: str = Field(..., min_length=1, max_length=20)
    columna: str | None = Field(default=None, max_length=200)
    ponderar_por: str | None = Field(default=None, max_length=200)
    agrupar_por: list[str] | None = Field(default=None, max_length=MAX_GROUP_BY)
    filtros: dict[str, str | int | float] | list[FiltroItem] | None = Field(
        default=None, max_length=MAX_AGG_FILTERS
    )
    desde: str | None = Field(default=None, max_length=10)
    hasta: str | None = Field(default=None, max_length=10)
    columna_fecha: str | None = Field(default=None, max_length=200)
    # "valor" (por defecto: ranking) o una columna de `agrupar_por` (una serie).
    ordenar_por: str | None = Field(default=None, max_length=200)
    orden: str = "desc"
    limite: int = Field(default=50, ge=1, le=MAX_AGG_LIMIT)


class AgregarResponse(BaseModel):
    tabla: str
    # "suma de credito_devengado", "conteo ponderado por pondera".
    calculo: str
    agrupado_por: list[str]
    # Las de `agrupado_por`, "valor" y "filas_usadas" (filas de cada grupo).
    columnas: list[str]
    filas: list[dict[str, Any]]
    # Grupos devueltos.
    cantidad: int
    # Filas de la tabla que entraron en el cálculo, de TODOS los grupos
    # (también los que `limite` dejó afuera). 0 si ninguna cumplió los filtros.
    filas_usadas: int | None
    # Cuántas de ellas tenían un número reconocible en la columna.
    filas_con_valor: int | None = None
    # Hay más grupos que `limite`.
    truncado: bool
    fuente: str
    url: str
    # Sin valor que informar: por qué (y con cero filas, qué valores existen).
    aviso: str | None = None
    sugerencias: dict[str, list[dict[str, Any]]] | None = None
    # Sobre el cálculo y los filtros: filas sin número, grupos que no
    # entraron, con qué valores de la tabla se comparó lo pedido.
    notas: list[str] | None = None


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


# Qué rechazo fue, en una palabra, para un cliente que no quiera leer el texto.
ERROR_HEADER = "X-OpenArg-Error"


def _error_detail(result: SandboxResult, *, agregado: bool = False) -> tuple[int, str, str]:
    """``(status, código, mensaje)`` según qué falló: el mensaje dice qué hacer.

    Antes todo era un 400 con "Probá con otros filtros o menos columnas",
    también un timeout en una tabla de millones de filas, una tabla bloqueada
    por un problema de calidad o el rechazo del validador, donde el consejo
    lleva por mal camino. ``agregado``: el pedido ya era ``agregar_datos``, y
    sugerirle esa misma herramienta lo hacía reintentar en bucle.
    """
    kind = result.error_kind
    error = (result.error or "").strip()
    if kind == "timeout":
        consejo = (
            "Acotá con `desde`/`hasta` o con `filtros` más específicos, o agrupá por menos "
            "columnas: sobre la tabla entera la cuenta no entra en el tiempo límite."
            if agregado
            else "Acotá con `desde`/`hasta` o con `filtros` más específicos; para un total o un "
            "ranking usá agregar_datos, que no trae las filas."
        )
        return (
            400,
            "timeout",
            "La consulta tardó demasiado y se cortó: la tabla es muy grande para ese pedido. "
            + consejo,
        )
    if kind == "blocked":
        return (
            400,
            "tabla_bloqueada",
            (error or "La tabla tiene un problema de calidad sin resolver.")
            + " Está oculta hasta que se corrija: buscá otra fuente con buscar_datasets.",
        )
    if kind == "validation":
        return (
            400,
            "validador",
            f"El validador de seguridad rechazó la consulta armada para este pedido ({error}). "
            "No es un error de los parámetros: probá con otras columnas o filtros, y si se "
            "repite avisanos a devops@colossuslab.org.",
        )
    if kind == "missing_column":
        return (
            400,
            "columna_inexistente",
            "Alguna de las columnas pedidas ya no existe en la tabla (cambió desde que se "
            "describió). Volvé a llamar a describir_tabla y usá las columnas que lista.",
        )
    if kind == "missing_table":
        return (
            404,
            "tabla_inexistente",
            "La tabla ya no está disponible: se está reemplazando por una versión nueva. "
            "Volvé a buscarla con buscar_datasets.",
        )
    detalle = f" ({error})" if error else ""
    return (
        400,
        "ejecucion",
        f"La consulta no se pudo ejecutar{detalle}. Probá con otros filtros o menos columnas.",
    )


async def _run(
    sandbox: ISQLSandbox,
    sql: str,
    params: Mapping[str, Any] | None = None,
    *,
    agregado: bool = False,
) -> list[dict[str, Any]]:
    result = await ejecutar(sandbox, sql, params or {})
    if result.error:
        # No es un error del servidor: un timeout, una tabla bloqueada, un
        # rechazo del validador o una tabla que cambió en el medio.
        logger.info("catalogo: consulta rechazada (%s): %s", result.error_kind, result.error[:200])
        status, code, detail = _error_detail(result, agregado=agregado)
        raise HTTPException(status_code=status, detail=detail, headers={ERROR_HEADER: code})
    return result.rows


async def _edad_de_los_datos(table_name: str) -> DataAge | None:
    """Cuándo leyó OpenArg la tabla de su fuente (``data_age_for``). Nunca falla.

    La misma función que usa el aviso de atraso del motor de respuestas, así
    las dos dicen la misma fecha.
    """
    try:
        from app.infrastructure.celery.tasks._db import get_sync_engine

        return await asyncio.to_thread(data_age_for, get_sync_engine(), table_name)
    except Exception:
        logger.debug("catalogo: no se pudo fechar %s", table_name, exc_info=True)
        return None


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
    """Columnas con su tipo, filas, período cubierto, frescura y una muestra."""
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
        bloqueada = sample.error_kind == "blocked"
        frescura = table_freshness(
            None if bloqueada else await _edad_de_los_datos(table.table_name),
            columna_fecha=fecha.nombre if fecha else None,
            desde=periodo.desde,
            hasta=periodo.hasta,
            aproximado=periodo.aproximado,
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
            muestra=[] if sample.error else json_rows(sample.rows),
            aviso_fecha=periodo.aviso,
            frescura=None
            if bloqueada
            else Frescura(
                actualizada=frescura.actualizada.isoformat() if frescura.actualizada else None,
                dias_desde_actualizacion=frescura.dias_desde_actualizacion,
                ultimo_dato=frescura.ultimo_dato,
                fecha_corte=frescura.fecha_corte.isoformat() if frescura.fecha_corte else None,
                serie=frescura.serie,
                aproximado=frescura.aproximado,
                nota=frescura.nota,
            ),
            aviso=(sample.error or "La tabla no se puede consultar.") if bloqueada else None,
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
    """Filas de una tabla: columnas, período, filtros con operador, orden estable y offset."""
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
                offset=body.offset,
                una_de_mas=True,
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
            orden_fisico = False
            filas_tabla: int | None = None
            if query.fecha is None:
                # Sin fecha, el orden estable es la posición física, que sólo
                # es barata en una tabla chica (ver ORDEN_FISICO_MAX_FILAS).
                # Sin saber el tamaño no se arriesga: el catálogo dice 0 filas
                # en tablas que tienen millones.
                stats = prep.stats or await estadisticas(sandbox, table.table_name, [])
                filas_tabla = filas_estimadas(stats, table.row_count)
                orden_fisico = filas_tabla is not None and filas_tabla <= ORDEN_FISICO_MAX_FILAS
            query = build_data_query(
                replace(
                    req,
                    filtros=prep.filtros,
                    tolerante=prep.tolerante,
                    formatos=prep.formatos,
                    formato_fecha=prep.formato_fecha,
                    orden_fisico=orden_fisico,
                )
            )
        except CatalogRequestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

        rows = json_rows(await _run(sandbox, query.sql, query.params))
        truncado = len(rows) > query.limite
        rows = rows[: query.limite]
        aviso: str | None = None
        sugerencias: dict[str, list[dict[str, Any]]] | None = None
        if not rows and body.offset:
            # La página pedida está más allá del final: no es que nada
            # cumpla los filtros.
            aviso = (
                f"No hay filas desde offset={body.offset}: el pedido tiene menos. Pedí con "
                "un offset menor (o sin offset)."
            )
        elif not rows and (query.filtros or query.desde or query.hasta):
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
        notas.extend(_notas_de_orden(query.orden, body, truncado, filas_tabla))
        siguiente: int | None = body.offset + len(rows) if truncado else None
        if siguiente is not None and siguiente > MAX_OFFSET:
            # Esa página la rechazaría la validación del pedido (422): no se
            # la ofrece.
            siguiente = None
            notas.append(
                f"Hay más filas, pero no se pagina más allá de offset={MAX_OFFSET}: para "
                "seguir acotá con `desde`/`hasta` o `filtros`, o calculá totales con "
                "agregar_datos."
            )
        source = (await sandbox.get_table_sources([table.table_name])).get(
            bare_name(table.table_name)
        )
        return DatosResponse(
            tabla=table.table_name,
            columnas=query.columns,
            filas=rows,
            cantidad=len(rows),
            truncado=truncado,
            fuente=source.title if source else "",
            url=source.url if source else "",
            aviso=aviso,
            sugerencias=sugerencias,
            filtros_aplicados=notas or None,
            siguiente_offset=siguiente,
        )


def _notas_de_orden(
    orden: str | None, body: DatosRequest, truncado: bool, filas_tabla: int | None = None
) -> list[str]:
    """Lo que hay que saber del orden para no sacar una conclusión equivocada.

    El orden por defecto es ascendente: en una serie larga, las primeras 100
    filas son las más viejas, y un modelo que pidió "los datos" sin `orden`
    leía 2003 como lo último (auditoría ok.3).
    """
    if orden == "fecha":
        # Sólo si no eligió el orden y no está paginando: ahí ya sabe.
        pidio_orden = "orden" in body.model_fields_set
        if truncado and not body.offset and not pidio_orden and body.orden.lower() == "asc":
            return [
                "Las filas van de la más vieja a la más nueva y hay más: para ver las más "
                'recientes pedí `orden="desc"`.'
            ]
        return []
    if orden is None and (truncado or body.offset):
        # Sin tamaño conocido (catálogo en 0 y sin estadísticas) no se puede
        # decir que sea grande: no se ordena porque no se sabe.
        por_que = (
            "y es demasiado grande para ordenarla entera"
            if filas_tabla
            else "y no se sabe cuántas filas tiene, así que no se la ordena entera (en una "
            "tabla grande eso pasa el tiempo límite)"
        )
        return [
            f"La tabla no tiene columna de fecha {por_que}: las filas salen en el orden en "
            "que Postgres las lea, que no está garantizado entre pedidos (una página "
            "siguiente puede repetir o saltear filas). Para recorrerla con `offset` acotá "
            "antes con `filtros`; para totales usá agregar_datos."
        ]
    return []


@router.post("/agregar", response_model=AgregarResponse)
@inject  # type: ignore[untyped-decorator]
async def agregar_datos(
    request: Request,
    body: AgregarRequest,
    sandbox: FromDishka[ISQLSandbox],
    cache: FromDishka[ICacheService],
    api_key_repo: FromDishka[IApiKeyRepository],
    credits: FromDishka[ICreditRepository],
) -> AgregarResponse:
    """Suma, promedio, conteo, mínimo o máximo, agrupado y filtrado, sin traer las filas.

    La misma cuenta que la herramienta `calcular` del agente
    (`consultas.agregar`): el modelo del cliente pide el total y OpenArg lo
    calcula en la base, en vez de traer 500 filas en CSV y sumarlas a mano
    (auditoría 3.1). Mismo cupo que el resto del modo datos.
    """
    endpoint, tool = "/api/v1/catalogo/agregar", "agregar_datos"
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

        async def run_sql(sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
            return await _run(sandbox, sql, params, agregado=True)

        try:
            res = await agregar(
                sandbox,
                PedidoAgregado(
                    tabla=table.table_name,
                    tipos=types,
                    operacion=body.operacion,
                    columna=body.columna,
                    ponderar_por=body.ponderar_por,
                    agrupar_por=list(body.agrupar_por or []),
                    filtros=raw_filters,
                    desde=body.desde,
                    hasta=body.hasta,
                    orden=body.orden,
                    limite=body.limite,
                    columna_fecha=body.columna_fecha,
                    ordenar_por=body.ordenar_por,
                    filas_tabla=table.row_count,
                ),
                run_sql,
            )
        except CatalogRequestError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

        source = (await sandbox.get_table_sources([table.table_name])).get(
            bare_name(table.table_name)
        )
        # `valor` llega como Decimal (los números se leen como ::numeric): sin
        # `json_rows` salía en el JSON como texto.
        grupos = [
            {**g, "filas_usadas": n}
            for g, n in zip(json_rows(res.grupos), res.filas_por_grupo, strict=True)
        ]
        notas = [*res.avisos, *res.notas]
        if res.parcial:
            # Sólo con un sandbox que no devuelve el total de todos los grupos.
            notas.insert(0, "`filas_usadas` no incluye los grupos que quedaron afuera.")
        return AgregarResponse(
            tabla=table.table_name,
            calculo=res.calculo,
            agrupado_por=res.req.agrupar_por,
            columnas=[*res.req.agrupar_por, "valor", "filas_usadas"],
            filas=grupos,
            cantidad=len(grupos),
            filas_usadas=res.filas_usadas,
            filas_con_valor=None if res.parcial else res.filas_con_valor,
            truncado=res.truncado,
            fuente=source.title if source else "",
            url=source.url if source else "",
            aviso=res.aviso,
            sugerencias=res.sugerencias or None,
            notas=notas or None,
        )
