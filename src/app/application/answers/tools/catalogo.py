"""Buscar, describir, leer y calcular sobre las tablas de OpenArg.

Es el modo datos del MCP (``public_catalog``, ``catalogo_router``), que ya
funciona bien con modelos capaces, más dos cosas que el agente necesita:

- **los marts**: tablas curadas que juntan y normalizan varios datasets. El
  modo datos no los ve; acá aparecen primero porque son la versión revisada
  del dato.
- **``calcular``**: sumas, conteos y promedios armados por nuestro código, con
  ponderador para las encuestas. Nunca SQL escrito por el modelo.

``describir_tabla`` además señala lo que el pipeline viejo no miraba y le
costó respuestas equivocadas: la columna de ponderación de una encuesta, las
columnas geográficas (o su ausencia: un dato nacional no es un dato de
Pinamar) y las unidades que declara el mart.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.application.answers.aggregates import (
    FILTER_OPERATORS,
    OPERATIONS,
    AggregateQuery,
    AggregateRequest,
)
from app.application.answers.aggregates import (
    MAX_FILTERS as MAX_AGG_FILTERS,
)
from app.application.answers.tools.base import (
    MAX_ROWS_FOR_MODEL,
    ToolContext,
    ToolInputError,
    ToolOutcome,
    count,
    int_arg,
    plain_rows,
    quoted,
    str_arg,
    to_json,
)
from app.application.catalog.collapse import _ROW_CAPS, collapse_hits, content_fingerprints
from app.application.catalog.national_prior import NATIONAL_PORTALS, national_prior
from app.application.consultas.agregar import PedidoAgregado, agregar
from app.application.consultas.fechas import (
    ColumnaFecha,
    abarca_rango,
    aviso_lectura_fecha,
    descarta_valores_de_periodo,
    es_nombre_de_periodo,
    es_valor_de_periodo,
    resolver_columna_fecha,
    tiene_valores_de_periodo,
)
from app.application.consultas.filtros import notas_de_filtros
from app.application.consultas.preparar import (
    TIMEOUT_CONFIRMAR_S,
    Periodo,
    Preparado,
    describir_periodo,
    ejecutar,
    es_tolerante,
    estadisticas,
    filas_estimadas,
    preparar,
    rango_de_muestra,
)
from app.application.consultas.sugerencias import diagnosticar_vacio
from app.application.public_catalog import (
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_sample_query,
    is_internal_column,
    resolve_date_column,
    resolve_table,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.agent_llm import AgentTool
from app.domain.ports.sandbox.sql_sandbox import MartInfo, TableProfile, TableValueStats
from app.domain.value_objects.table_reference import bare_name, quote_qualified

logger = logging.getLogger(__name__)

# Mismo umbral que el modo datos y `/data/search`.
_MIN_SIMILARITY = 0.40
_MAX_DATASETS = 8
_MAX_TABLES_PER_DATASET = 4
_MAX_MARTS = 5
# Con Cohere Embed Multilingual v3 casi todo el catálogo pasa 0,40 (hasta
# "receta de empanadas" trae 8 datasets y 5 marts), así que el umbral fijo no
# cortaba nada y siempre se llenaban los topes. Se queda lo que está cerca del
# mejor resultado. Calibrado en prod el 09-oct-2026 con el gold de búsqueda
# (52 consultas) y el de ruteo a marts (49): el dataset correcto queda a
# 0,015 o menos del primero, el mart correcto a 0,05 o menos; con 0,05 no se
# pierde ninguno y los marts por consulta bajan de 4,9 a 2,3.
_DELTA_DATASETS = 0.05
_DELTA_MARTS = 0.05
_TITULO_RESUMEN_CHARS = 70


def _cerca_del_mejor(puntajes: list[float], delta: float) -> float:
    """El puntaje mínimo para quedar: el umbral fijo o el mejor menos `delta`."""
    return max(_MIN_SIMILARITY, max(puntajes) - delta) if puntajes else _MIN_SIMILARITY


_DESCRIPTION_CHARS = 300
_MART_PORTAL = "OpenArg (tabla curada)"

# Columnas de ponderación de las encuestas del INDEC y similares. Si una tabla
# tiene una, "cuántas personas" es la suma de esa columna, no la cantidad de
# filas.
_WEIGHT_RE = re.compile(
    r"^(pondera|pondih|pondii|pondiio|ponderador|ponderacion|factor_exp\w*|fexp\w*|peso|weight)$",
    re.IGNORECASE,
)
_GEO_RE = re.compile(
    r"(provincia|departamento|partido|municipio|localidad|aglomerado|region|jurisdiccion"
    r"|nivel_geografico|comuna|barrio|distrito|cod_prov|codprov|id_prov|seccion)",
    re.IGNORECASE,
)
# De qué lugar es el dato de una tabla sin columnas geográficas, según el
# portal provincial o municipal que la publica (`datasets.portal`). El aviso
# decía "normalmente el total nacional" también acá: en staging, al menos 444
# tablas de estos portales (contadas sólo entre las que tienen `columns_json`,
# 06-oct), entre ellas la Tarifa Social Eléctrica de Mendoza (nueva_16).
_NIVEL_DEL_PORTAL = {
    "caba": "la Ciudad de Buenos Aires",
    "legislatura_caba": "la Ciudad de Buenos Aires",
    "bac": "la Ciudad de Buenos Aires",
    "buenos_aires_prov": "la provincia de Buenos Aires",
    "cordoba_prov": "la provincia de Córdoba",
    "cordoba_estadistica": "la provincia de Córdoba",
    "mendoza": "la provincia de Mendoza",
    "entre_rios": "la provincia de Entre Ríos",
    "neuquen_legislatura": "la provincia de Neuquén",
    "tucuman": "la provincia de Tucumán",
    "chaco": "la provincia del Chaco",
    "misiones": "la provincia de Misiones",
    "jujuy_dkan": "la provincia de Jujuy",
    "ciudad_mendoza": "la ciudad de Mendoza",
    "corrientes": "la ciudad de Corrientes",
    "rosario_dkan": "la ciudad de Rosario",
    "acumar": "la cuenca Matanza-Riachuelo",
}
# Lo que pone `resolve` cuando la tabla no tiene fuente.
_DEFAULT_PORTAL = "datos.gob.ar"


# ── resolver una tabla ─────────────────────────────────────


@dataclass
class ResolvedTable:
    name: str  # calificado, tal como se ejecuta
    title: str
    portal: str
    url: str
    row_count: int | None
    mart: MartInfo | None = None


async def resolve(sandbox: Any, requested: str) -> ResolvedTable:
    """La tabla del catálogo que nombra ``requested``, o un error para el modelo."""
    requested = (requested or "").strip()
    if requested.lower().startswith("mart."):
        marts = await sandbox.describe_marts([requested])
        mart = marts.get(requested) or next(
            (m for name, m in marts.items() if name.lower() == requested.lower()), None
        )
        if mart is None:
            raise ToolInputError(
                f"No existe el mart {requested!r} o está retirado. Usá buscar_datos."
            )
        return ResolvedTable(
            name=mart.table_name,
            title=_mart_title(mart),
            portal=_MART_PORTAL,
            url="",
            row_count=mart.row_count,
            mart=mart,
        )
    table = resolve_table(requested, await sandbox.find_tables(table_names=[requested]))
    if table is None:
        raise ToolInputError(f"No existe la tabla {requested!r} en el catálogo. Usá buscar_datos.")
    source = (await sandbox.get_table_sources([table.table_name])).get(bare_name(table.table_name))
    return ResolvedTable(
        name=table.table_name,
        title=source.title if source else table.table_name,
        portal=source.portal if source else _DEFAULT_PORTAL,
        url=source.url if source else "",
        row_count=table.row_count,
    )


def _mart_title(mart: MartInfo) -> str:
    first = next((line.strip() for line in mart.description.splitlines() if line.strip()), "")
    return first[:160] or mart.mart_id


def _sandbox_error(result: Any) -> str:
    kind = getattr(result, "error_kind", None)
    if kind == "timeout":
        return (
            "La consulta tardó demasiado: la tabla es muy grande para ese pedido. Acotá con "
            "desde/hasta o con filtros más específicos."
        )
    if kind == "blocked":
        return str(result.error)
    return (
        f"La consulta no se pudo ejecutar ({str(result.error)[:160]}). "
        "Probá con otros filtros, menos columnas o otra tabla."
    )


async def _run_sql(
    sandbox: Any, sql: str, params: Mapping[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Corre SQL armado por nuestro código (valores ligados en ``params``)."""
    result = await ejecutar(sandbox, sql, params or {})
    if result.error:
        raise ToolInputError(_sandbox_error(result))
    return plain_rows(result.rows)


def _printable(params: Mapping[str, Any]) -> dict[str, Any]:
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in params.items()}


def _data_result(
    table: ResolvedTable,
    rows: list[dict[str, Any]],
    sql: str,
    title: str,
    params: Mapping[str, Any] | None = None,
) -> DataResult:
    return DataResult(
        source=f"sandbox:{table.name}",
        portal_name=table.portal,
        portal_url=table.url,
        dataset_title=title,
        format="json",
        records=rows,
        metadata={
            "served_table": table.name,
            "total_records": len(rows),
            "generated_sql": sql,
            **({"sql_params": _printable(params)} if params else {}),
            "fetched_at": datetime.now(UTC).isoformat(),
            **({"description": table.mart.description} if table.mart else {}),
        },
    )


# Columnas que se muestran de cada tabla en la búsqueda. Un dataset puede traer
# varias tablas con la misma cantidad de filas: el Estudio de Discapacidad
# trae la de microdatos (pondera, dificultad_total…) y otra sólo con 40 pesos
# replicados. Batería del 02-oct: sin ver las columnas, Sonnet abrió la de
# pesos replicados y concluyó que el estudio "estaba incompleto".
_PREVIEW_COLUMNS = 12


def _table_summary(t: Any) -> dict[str, Any]:
    visible = [c for c in (t.columns or []) if not is_internal_column(str(c))]
    summary: dict[str, Any] = {"tabla": t.table_name, "filas": t.row_count}
    if visible:
        summary["columnas"] = len(visible)
        summary["primeras_columnas"] = [str(c) for c in visible[:_PREVIEW_COLUMNS]]
    return summary


# ── buscar_datos ───────────────────────────────────────────


class BuscarDatos:
    status = "Buscando en el catálogo..."

    def describe(self, args: dict[str, Any]) -> str:
        return f"Buscando {quoted(args.get('texto'))} en el catálogo"

    spec = AgentTool(
        name="buscar_datos",
        description=(
            "Busca en el catálogo de OpenArg (unos 30.000 recursos de portales de datos abiertos "
            "argentinos) las tablas que pueden responder la pregunta. Devuelve tablas curadas "
            "(`mart.*`, preferilas: ya están revisadas y normalizadas) y datasets publicados con "
            "sus tablas consultables y cantidad de filas. No devuelve datos: después hay que "
            "llamar a describir_tabla. Para indicadores macro oficiales (inflación, PBI, EMAE, "
            "desempleo, reservas, tipo de cambio) usá primero series_tiempo."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "texto": {
                    "type": "string",
                    "description": "Qué dato se busca, en pocas palabras (tema, lugar, período).",
                },
                "portal": {
                    "type": "string",
                    "description": "Opcional: limitar a un portal (p. ej. datos_gob_ar, caba).",
                },
            },
            "required": ["texto"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        texto = str_arg(args, "texto", required=True)
        portal = str_arg(args, "portal", max_len=80)
        deps = ctx.deps
        t0 = time.perf_counter()
        vector = await deps.embedding.embed(texto)
        t_embed = time.perf_counter()
        async with ctx.db_lock:
            try:
                # Varios datasets por resultado: muchos son el mismo archivo
                # (gemelos, espejos, CSV y JSON) y `collapse_hits` los junta.
                hits = await deps.vector_search.search_datasets_ann(
                    query_embedding=vector,
                    limit=_MAX_DATASETS * 4,
                    portal_filter=portal,
                    min_similarity=_MIN_SIMILARITY,
                )
            except BaseException:
                # Una búsqueda cortada por el tope de la herramienta dejaba la
                # sesión con la transacción rota, y todas las búsquedas
                # siguientes del turno fallaban (staging, 01-oct).
                await deps.vector_search.reset()
                raise
        t_search = time.perf_counter()
        found_tables = await deps.sandbox.find_tables(dataset_ids=[str(h.dataset_id) for h in hits])
        profiles = await deps.sandbox.table_profiles([t.table_name for t in found_tables])
        # Las hojas de un .xls con la misma forma y otro título se juntan sólo
        # con el mismo contenido (ISAC 3.1 y 4.1 son series distintas).
        fingerprints = await content_fingerprints(deps.sandbox, hits, found_tables, profiles)
        # Una entrada por archivo, con la copia de más filas reales y
        # encabezado sano; prioridad chica a lo nacional si no nombra lugar.
        colapsados = [
            c
            for c in collapse_hits(
                hits,
                found_tables,
                profiles,
                prior=national_prior(texto),
                fingerprints=fingerprints,
            )
            if c.tables
        ]
        piso = _cerca_del_mejor([c.hit.score for c in colapsados], _DELTA_DATASETS)
        elegidos = [c for c in colapsados if c.hit.score >= piso][:_MAX_DATASETS]
        datasets = [
            {
                "titulo": c.hit.title,
                "portal": c.hit.portal,
                "descripcion": (c.hit.description or "")[:_DESCRIPTION_CHARS],
                **({"archivo": c.archivo} if c.archivo else {}),
                "tablas": [_table_summary(t) for t in c.tables[:_MAX_TABLES_PER_DATASET]],
            }
            for c in elegidos
        ]
        logger.info(
            "buscar_datos: embed_ms=%.0f busqueda_ms=%.0f tablas_ms=%.0f hits=%d datasets=%d",
            (t_embed - t0) * 1000,
            (t_search - t_embed) * 1000,
            (time.perf_counter() - t_search) * 1000,
            len(hits),
            len(datasets),
        )

        marts = await deps.sandbox.find_marts(vector, limit=_MAX_MARTS) if not portal else []
        piso_marts = _cerca_del_mejor([m.score for m in marts], _DELTA_MARTS)
        curated = [
            {
                "tabla": m.table_name,
                "descripcion": m.description[:_DESCRIPTION_CHARS],
                "filas": m.row_count,
                "similitud": round(m.score, 2),
            }
            for m in marts
            if m.score >= piso_marts
        ]
        if not datasets and not curated:
            nota = "Nada parecido en el catálogo."
            if portal and not hits:
                # Un portal inexistente ("INDEC") vacía el filtro y parecía
                # que el dato no estaba.
                async with ctx.db_lock:
                    portals = await deps.vector_search.known_portals()
                if portals and portal not in portals:
                    nota = (
                        f"No existe el portal {portal!r}. Portales válidos: "
                        f"{', '.join(portals)}. Probá sin portal."
                    )
            return ToolOutcome(
                to_json({"resultados": [], "nota": nota}),
                summary="No encontró nada parecido en el catálogo",
            )
        found = count(len(datasets), "dataset", "datasets")
        if curated:
            found += f" y {count(len(curated), 'tabla curada', 'tablas curadas')}"
        if elegidos:
            # El conteo solo se repetía igual en cada pregunta: el título del
            # mejor dice si la búsqueda encontró lo que se pedía. No es siempre
            # el primero de la lista: el agrupado le da prioridad a lo nacional.
            titulo = max(elegidos, key=lambda c: c.hit.score).hit.title or ""
            if len(titulo) > _TITULO_RESUMEN_CHARS:
                titulo = titulo[: _TITULO_RESUMEN_CHARS - 1].rstrip() + "…"
            found += f"; el más parecido: «{titulo}»"
        return ToolOutcome(
            to_json({"tablas_curadas": curated, "datasets": datasets}),
            summary=f"Encontró {found}",
        )


# ── describir_tabla ────────────────────────────────────────


def _nivel_del_portal(portal: str) -> str | None:
    """De qué lugar es una tabla sin columnas geográficas; None si es nacional."""
    if portal in _NIVEL_DEL_PORTAL:
        return _NIVEL_DEL_PORTAL[portal]
    if not portal or portal in NATIONAL_PORTALS or portal in (_MART_PORTAL, _DEFAULT_PORTAL):
        return None
    return f"lo que cubre el portal «{portal}»"


async def _perfil(sandbox: Any, tabla: str) -> TableProfile | None:
    """Lo que registró el colector de la versión viva (filas, si quedó
    cortada), o None si el sandbox no lo da o falla: describe igual."""
    getter = getattr(sandbox, "table_profiles", None)
    if getter is None:
        return None
    try:
        perfiles: dict[str, TableProfile] = await getter([tabla])
    except Exception:
        logger.warning("describir_tabla: no se pudo leer el perfil de %s", tabla, exc_info=True)
        return None
    return perfiles.get(bare_name(tabla))


def _aviso_geografico(portal: str) -> str:
    nivel = _nivel_del_portal(portal)
    if nivel is None:
        # Batería del 02-oct: con el aviso sin la segunda oración, Sonnet y
        # Haiku explicaban bien que no había dato de Pinamar pero no daban
        # el total nacional, que la tabla sí tiene.
        return (
            "Sin columnas geográficas: el dato es de un solo nivel (normalmente el total "
            "nacional). No lo presentes como dato de una provincia, partido o ciudad. Si te "
            "preguntaron por un lugar, decí que no hay dato a ese nivel y DÁ IGUAL la cifra "
            "nacional, calculándola con calcular (con el ponderador si lo hay) y aclarando "
            "que es nacional."
        )
    return (
        f"Sin columnas geográficas: el dato es de un solo nivel, normalmente el de quien lo "
        f"publica ({nivel}). No lo presentes como dato de otro lugar. Si te preguntaron por un "
        f"lugar más chico (un departamento, una localidad, un barrio), decí que no hay dato a "
        f"ese nivel y DÁ IGUAL la cifra de {nivel}, calculándola con calcular (con el "
        "ponderador si lo hay) y aclarando a qué nivel corresponde."
    )


class DescribirTabla:
    status = "Revisando la tabla..."

    def describe(self, args: dict[str, Any]) -> str:
        return "Revisando cómo es la tabla"

    spec = AgentTool(
        name="describir_tabla",
        description=(
            "Muestra cómo es una tabla antes de consultarla: columnas con su tipo, cantidad de "
            "filas, período cubierto, 5 filas de muestra, la fuente, y señales para no "
            "equivocarse: `ponderador` (si es una encuesta, para contar personas hay que sumar "
            "esa columna, no contar filas), `columnas_geograficas` (si no hay, el dato es de un "
            "solo nivel —el de quien lo publica: el país, una provincia o una ciudad— y no "
            "sirve para un lugar más chico) y, en "
            "las tablas curadas, la descripción y la unidad de cada columna. Usala SIEMPRE antes "
            "de obtener_datos o calcular sobre una tabla nueva."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "tabla": {
                    "type": "string",
                    "description": "Nombre de la tabla, tal como lo dio buscar_datos.",
                }
            },
            "required": ["tabla"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        sandbox = ctx.deps.sandbox
        table = await resolve(sandbox, str_arg(args, "tabla", required=True, max_len=200) or "")
        types = (await sandbox.get_column_types([table.name])).get(table.name, [])
        columns = [(c, t) for c, t in types if not is_internal_column(c)]
        names = [c for c, _ in columns]
        fecha = resolve_date_column(columns, tabla=table.name)
        periodo = await describir_periodo(sandbox, table.name, fecha)
        # La muestra orienta: si falla (tabla bloqueada, timeout), se describe
        # igual sin ella en vez de tirar la herramienta entera.
        sample_result = await ejecutar(sandbox, build_sample_query(table.name, names), {})
        sample = [] if sample_result.error else plain_rows(sample_result.rows)

        described: dict[str, str] = {}
        if table.mart and table.mart.columns:
            for col in table.mart.columns:
                if isinstance(col, dict) and col.get("name") and col.get("description"):
                    described[str(col["name"])] = str(col["description"])[:200]

        # El conteo del catálogo no es confiable. Un `row_count` 0 no es una
        # tabla vacía: en staging lo tienen 18.134 de 31.236 tablas listas con
        # filas (chequeo del 06-oct). Y uno distinto de 0 puede ser viejo:
        # 1.482 de 11.491 no coinciden con Postgres, y la Tarifa Social
        # Eléctrica de Mendoza 2019 anuncia 107.067 filas y tiene 7.065
        # (revisión de #165). En una muestra de 45 que no coincidían,
        # `reltuples` era el `count(*)` en 44. Así que el del catálogo va como
        # conteo sólo si Postgres dice lo mismo (o no dice nada); si no, la
        # estimación de Postgres, dicha como tal.
        estimada = filas_estimadas(await estadisticas(sandbox, table.name, []), None)
        catalogo = table.row_count or None
        if catalogo and estimada in (None, catalogo):
            filas, estimadas = catalogo, None
        else:
            filas, estimadas = None, estimada
        # Una tabla que el colector cortó en el tope: contarla da el tope, no
        # el total de la fuente. En staging, 499 tablas listas tienen
        # `reltuples` en un tope y 303 están marcadas como cortadas, 42 de
        # ellas con `reltuples` fuera de los topes (revisión de #165).
        # Sólo cuentan los números de la tabla viva: un conteo viejo del
        # catálogo en el tope no dice que la de hoy esté cortada.
        perfil = None if table.mart else await _perfil(sandbox, table.name)
        en_tope = (filas, perfil.rows if perfil else None, estimada)
        tope = next((n for n in en_tope if n in _ROW_CAPS), None)
        cortada = tope is not None or bool(perfil and perfil.truncated)
        if tope:
            filas, estimadas = tope, None  # lo guardado es justo el tope

        weights = [c for c in names if _WEIGHT_RE.match(c)]
        geo = [c for c in names if _GEO_RE.search(c)]
        payload: dict[str, Any] = {
            "tabla": table.name,
            "titulo": table.title,
            "fuente": table.portal,
            "filas": filas,
            "columnas": [
                {
                    "nombre": c,
                    "tipo": t,
                    **({"descripcion": described[c]} if c in described else {}),
                }
                for c, t in columns
            ],
            "columna_fecha": fecha.nombre if fecha else None,
            "desde": periodo.desde,
            "hasta": periodo.hasta,
            "muestra": sample,
            "ponderador": weights[0] if weights else None,
            "columnas_geograficas": geo,
        }
        if estimadas:
            payload["filas_estimadas"] = estimadas
        if cortada:
            en = f" en {count(tope, 'fila', 'filas')}" if tope else ""
            payload["aviso_filas"] = (
                f"La tabla está cortada{en}: OpenArg guardó sólo las primeras filas del archivo "
                "de la fuente. Un conteo o una suma sobre esta tabla da lo guardado, no el total "
                "de la fuente: no lo presentes como total."
            )
        elif estimadas:
            payload["aviso_filas"] = (
                "El catálogo no tiene la cantidad exacta de filas: filas_estimadas es una "
                "estimación de la base. Para un total, contalo con calcular (operacion=conteo)."
            )
        elif not filas:
            payload["aviso_filas"] = (
                "El catálogo no tiene la cantidad de filas de esta tabla. Para un total, contalo "
                "con calcular (operacion=conteo)."
            )
        if periodo.aviso:
            payload["aviso_fecha"] = periodo.aviso
        if sample_result.error:
            payload["aviso_muestra"] = "No pude leer filas de muestra de esta tabla."
        if table.mart:
            payload["descripcion"] = table.mart.description[:1200]
        if weights:
            payload["aviso_ponderador"] = (
                f"Es una encuesta: cada fila representa a muchas personas. Para 'cuántos' usá "
                f"calcular con operacion=conteo y ponderar_por={weights[0]!r}; contar filas da el "
                "tamaño de la muestra, no la población."
            )
        if not geo:
            payload["aviso_geografico"] = _aviso_geografico(table.portal)
        if cortada and (tope or filas or estimadas):
            tamano = f" (al menos {count(tope or filas or estimadas, 'fila', 'filas')})"
        elif filas:
            tamano = f" ({count(filas, 'fila', 'filas')})"
        elif estimadas:
            tamano = f" (unas {count(estimadas, 'fila', 'filas')})"
        else:
            tamano = ""
        return ToolOutcome(to_json(payload), summary=f"Revisó {quoted(table.title, 80)}{tamano}")


# ── obtener_datos ──────────────────────────────────────────


def _filters_schema(max_items: int) -> dict[str, Any]:
    """La misma gramática de filtros para obtener_datos y calcular."""
    return {
        "type": "array",
        "maxItems": max_items,
        "description": (
            "Filtros {columna, operador, valor}. Operadores: = y != (texto o número), >, >=, "
            "<, <= (números), contiene (parte del texto) y en (lista, en `valores`). = y "
            "contiene no distinguen mayúsculas ni acentos, salvo en tablas de más de un millón "
            "de filas (ahí `filtros_aplicados` dice qué se buscó tal cual)."
        ),
        "items": {
            "type": "object",
            "properties": {
                "columna": {"type": "string"},
                "operador": {"type": "string", "enum": list(FILTER_OPERATORS)},
                "valor": {"type": "string"},
                "valores": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 50,
                    "description": "Sólo para el operador en.",
                },
            },
            "required": ["columna", "operador"],
        },
    }


_FILTERS_SCHEMA = _filters_schema(5)


class ObtenerDatos:
    status = "Leyendo datos..."

    def describe(self, args: dict[str, Any]) -> str:
        return "Leyendo los datos de la tabla"

    spec = AgentTool(
        name="obtener_datos",
        description=(
            "Lee filas de una tabla: columnas elegidas, período (desde/hasta sobre la columna de "
            "fecha o de año), filtros y orden por fecha. Para series y listados cortos. Si hay "
            "que sumar, contar o promediar muchas filas, usá calcular. Los filtros de igualdad y "
            "`contiene` no distinguen mayúsculas ni acentos; si ninguna fila cumple, la "
            "respuesta trae `aviso` y `sugerencias` con los valores que sí existen."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "tabla": {"type": "string"},
                "columnas": {"type": "array", "items": {"type": "string"}, "maxItems": 30},
                "desde": {"type": "string", "description": "AAAA, AAAA-MM o AAAA-MM-DD"},
                "hasta": {"type": "string", "description": "AAAA, AAAA-MM o AAAA-MM-DD"},
                "columna_fecha": {
                    "type": "string",
                    "description": (
                        "Opcional: la columna de fecha para desde/hasta y el orden, si no es la "
                        "que informa describir_tabla (p. ej. fecha_fin en vez de fecha_inicio)."
                    ),
                },
                "filtros": _FILTERS_SCHEMA,
                "orden": {"type": "string", "enum": ["asc", "desc"]},
                "limite": {"type": "integer", "minimum": 1, "maximum": 200},
            },
            "required": ["tabla"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        sandbox = ctx.deps.sandbox
        table = await resolve(sandbox, str_arg(args, "tabla", required=True, max_len=200) or "")
        types = (await sandbox.get_column_types([table.name])).get(table.name, [])
        columnas = args.get("columnas")
        if columnas is not None and not isinstance(columnas, list):
            raise ToolInputError("`columnas` es una lista de nombres.")
        try:
            req = DataRequest(
                table=table.name,
                available_columns=[c for c, _ in types],
                column_types=types,
                columns=[str(c) for c in columnas] if columnas else None,
                desde=str_arg(args, "desde", max_len=10),
                hasta=str_arg(args, "hasta", max_len=10),
                filtros=args.get("filtros") or None,
                orden=str_arg(args, "orden", max_len=4) or "asc",
                limite=int_arg(args, "limite", 100, 1, 200),
                columna_fecha=str_arg(args, "columna_fecha", max_len=200),
            )
            # Primero sólo valida (puro, sin tocar la base); después se leen
            # estadísticas y formatos (filtros y fecha), y se arma de nuevo.
            query = build_data_query(req)
            prep = Preparado(filtros=query.filtros, filas_estimadas=table.row_count or None)
            if query.filtros or query.fecha:
                prep = await preparar(
                    sandbox,
                    table.name,
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
            raise ToolInputError(str(exc)) from None
        rows = await _run_sql(sandbox, query.sql, query.params)
        payload: dict[str, Any] = {
            "tabla": table.name,
            "titulo": table.title,
            "columnas": query.columns,
            "cantidad": len(rows),
            "filas": rows[:MAX_ROWS_FOR_MODEL],
        }
        if len(rows) > MAX_ROWS_FOR_MODEL:
            payload["nota"] = f"Se muestran {MAX_ROWS_FOR_MODEL} de {len(rows)} filas."
        notas = notas_de_filtros(query.filtros, query.tipos, tolerante=prep.tolerante)
        aviso_fecha = aviso_lectura_fecha(query.fecha)
        if aviso_fecha:
            notas.append(aviso_fecha)
        if notas:
            payload["filtros_aplicados"] = notas
        if not rows and (query.filtros or query.desde or query.hasta):
            try:
                diag = await diagnosticar_vacio(
                    sandbox,
                    tabla=table.name,
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
                raise ToolInputError(str(exc)) from None
            payload["aviso"] = diag.aviso
            if diag.sugerencias:
                payload["sugerencias"] = diag.sugerencias
        result = _data_result(table, rows, query.sql, table.title, query.params)
        return ToolOutcome(to_json(payload), results=[result] if rows else [])


# ── calcular ───────────────────────────────────────────────


async def _filas_de_la_tabla(
    sandbox: Any, tabla: str, estimada: int | None
) -> tuple[int | None, int | None]:
    """Las filas de toda la tabla: (conteo exacto, estimación de Postgres).

    El conteo exacto sólo en las tablas chicas (``es_tolerante``): en una de
    millones de filas el ``count(*)`` pasaría el tope. Ahí, o si el conteo
    falla, queda sólo la estimación (None si tampoco la hay).
    """
    if not es_tolerante(estimada):
        return None, estimada
    sql = f"SELECT count(*) AS filas_tabla FROM {quote_qualified(tabla)}"
    try:
        result = await ejecutar(sandbox, sql, {}, timeout_seconds=TIMEOUT_CONFIRMAR_S)
    except Exception:
        # El cálculo ya está hecho: no se lo pierde por un dato del aviso.
        logger.warning("calcular: no se pudo contar %s", tabla, exc_info=True)
        return None, estimada
    if result.error or not result.rows or result.rows[0].get("filas_tabla") is None:
        logger.warning("calcular: no se pudo contar %s: %s", tabla, result.error)
        return None, estimada
    return int(result.rows[0]["filas_tabla"]), estimada


@dataclass(frozen=True)
class _Forma:
    """Si la tabla es una sola foto o apila períodos, para el aviso de un conteo filtrado.

    ``clase``:

    - ``foto``: no apila períodos (``motivo`` dice por qué). Su total es el
      conteo sin filtros, y un filtro elige una parte de esa foto.
    - ``abarca``: tiene varios períodos en su columna de fecha (``columna``, de
      ``desde`` a ``hasta``), pero el pedido sobre esa columna los abarca a todos.
    - ``apila``: apila períodos en ``columna`` (de ``desde`` a ``hasta``): sus
      filas juntan todos y no son el total de uno solo.
    - ``dudosa``: no se sabe; ``dudosas`` son las columnas que podrían
      distinguir períodos (ninguna si la tabla no tiene fecha y no hay
      muestra de sus valores).
    """

    clase: str
    motivo: str = ""
    columna: str = ""
    desde: str | None = None
    hasta: str | None = None
    aproximado: bool = False
    # Valores de la columna de fecha que no se leyeron: el rango no los cuenta.
    sin_reconocer: int = 0
    dudosas: tuple[str, ...] = ()


def _lista(columnas: list[str], y: str = "y") -> str:
    """«A» / «A» y «B» / «A», «B» y «C»."""
    citadas = [f"«{c}»" for c in dict.fromkeys(columnas)]
    if len(citadas) == 1:
        return citadas[0]
    return f"{', '.join(citadas[:-1])} {y} {citadas[-1]}"


# Desde qué parte de las filas un solo valor con forma de período no deja
# repartir la tabla en períodos (ver `_forma_sin_fecha`). Si la columna sí
# los separara, ese período tendría el 90 % de las filas o más, y el conteo
# sin filtros se pasaría de su total en un 11 % como mucho.
_DOMINANTE_FOTO = 0.9
# Y con cuántos valores distintos en la columna. Con pocos, uno con el 90 %
# puede ser el único período completo y otro a medio cargar: en dos tablas
# de salud de ACUMAR en staging, 2023 y 2022 tienen el 93 % y el 7 %, y la
# tabla apila además semanas. Con diez o más, los demás se reparten el 10 %
# que queda, alrededor de un 1 % cada uno: no son fotos de una misma población.
_MIN_VALORES_DOMINANTE = 10


def _valores_de_muestra(stats: TableValueStats | None, columna: str) -> list[str]:
    st = stats.columns.get(columna) if stats else None
    return [*st.most_common_vals, *st.histogram_bounds] if st else []


def _dominante(stats: TableValueStats | None, columna: str) -> tuple[str, float] | None:
    """El valor con forma de período que tiene ``_DOMINANTE_FOTO`` de las filas, en
    una columna con ``_MIN_VALORES_DOMINANTE`` valores distintos o más; o None."""
    st = stats.columns.get(columna) if stats else None
    if not st or not st.most_common_vals or not st.most_common_freqs or st.n_distinct is None:
        return None
    # `n_distinct` negativo es una fracción de las filas (convención de Postgres).
    filas = (stats.estimated_rows if stats else None) or 0
    distintos = st.n_distinct if st.n_distinct > 0 else -st.n_distinct * filas
    valor, parte = st.most_common_vals[0], st.most_common_freqs[0]
    if (
        parte < _DOMINANTE_FOTO
        or distintos < _MIN_VALORES_DOMINANTE
        or not es_valor_de_periodo(valor)
    ):
        return None
    return valor, parte


async def _forma_sin_fecha(
    sandbox: Any, tabla: str, query: AggregateQuery, propia: ColumnaFecha | None
) -> _Forma:
    """La forma de una tabla sin fecha propia (``propia`` es None o de alta,
    nacimiento…).

    No haber encontrado la fecha no alcanza para decir que la tabla no apila
    períodos (verificación de #171): en staging, 118 tablas sin fecha tenían
    una columna con 2 a 60 años, AAAAMM o campañas, y unas 40 de las primeras
    60 apilaban de verdad (`medicion` en el índice de reciclabilidad, 3 años;
    `ciclo_lectivo`, `campania`, `eleccion`, `a±o`). Queda la duda si una
    columna puede distinguir períodos:

    - por el nombre (`mes`, `corte`, `campania`, `ciclo_lectivo`), o una fecha
      de alta o de nacimiento, que describe a cada fila y puede no separarlos;
    - por los valores de la muestra de ``pg_stats`` (``tiene_valores_de_periodo``).

    Por los valores no cuentan las fechas de carga o auditoría
    (`fecha_modificacion`, `proceso_fecha`), igual que en
    ``resolver_columna_fecha``, ni los identificadores (`E0079_ID`), con
    números que caen entre 1800 y 2099 (``descarta_valores_de_periodo``).

    Salvo que un solo valor de la columna tenga el 90 % de las filas o más, y
    la columna diez valores distintos o más: en la Tarifa Social de Mendoza
    (nueva_16), `PADRON` es el mes de alta en el padrón, con 32 valores, y
    202207 tiene el 90 % de las 110.179 filas. Una columna que separara fotos
    de una misma población no se reparte así, y aun si las separara, el
    conteo sin filtros se pasaría del total de esa foto en un 11 % como mucho.
    La prueba que proponía la verificación (una columna con un valor distinto
    por fila) no sirve: `SUMINISTRO` tiene un 19 % de valores distintos (Excel
    los pasó a «1,31603E+14»), y en tablas que apilan sí hay columnas así (la
    producción de tabaco por campaña, los montos del comercio de minerales).

    Sin muestra (una tabla que nunca se analizó) no se sabe.
    """
    dudosas = [propia.nombre] if propia else []
    dudosas += [c for c in query.tipos if es_nombre_de_periodo(c)]
    stats = await estadisticas(sandbox, tabla, list(query.tipos))
    con_muestra = stats is not None and bool(stats.columns)
    dudosas += [
        c
        for c in query.tipos
        if not descarta_valores_de_periodo(c)
        and tiene_valores_de_periodo(_valores_de_muestra(stats, c))
    ]
    dudosas = list(dict.fromkeys(dudosas))
    if not dudosas:
        if not con_muestra:
            return _Forma("dudosa")
        return _Forma("foto", motivo="no tiene columna de fecha ni otra con valores de período")
    dominantes = {c: d for c in dudosas if (d := _dominante(stats, c))}
    if len(dominantes) == len(dudosas):
        columna, (valor, parte) = next(iter(dominantes.items()))
        return _Forma(
            "foto",
            motivo=(
                f"el {round(100 * parte)} % de sus filas tiene el mismo «{columna}», {valor}: "
                "esa columna describe a cada fila, no separa fotos"
            ),
        )
    return _Forma("dudosa", dudosas=tuple(c for c in dudosas if c not in dominantes)[:3])


async def _forma_de_la_tabla(
    sandbox: Any,
    tabla: str,
    query: AggregateQuery,
    propia: ColumnaFecha | None,
    periodo_propio: bool,
    *,
    con_muestra: bool = False,
    recorrer: bool = True,
) -> _Forma:
    """Si la tabla apila períodos, según su propia columna de fecha.

    ``propia`` es la que reconoce ``resolver_columna_fecha`` sin lo que haya
    pedido el modelo en ``columna_fecha``; ``periodo_propio``, si
    ``desde``/``hasta`` va sobre ella.

    ``con_muestra``: si la muestra de ``pg_stats`` ya tiene dos períodos y el
    pedido no la abarca, la tabla apila períodos y el pedido no los abarca a
    todos (el rango real contiene al de la muestra): se decide sin recorrer
    la tabla. Lo usa el pedido con un período de la tabla, donde ``apila`` no
    avisa (en las mediaciones, 914 mil filas, el rango exacto tarda 1,1 s).

    ``recorrer``: False en una tabla de un millón de filas o más
    (``es_tolerante``, el mismo corte que el conteo exacto de
    ``_filas_de_la_tabla``). Ahí el rango exacto tarda segundos, y muchas
    veces llega al tope de 5 s para terminar en el de la muestra
    (verificación de #171: los movimientos pecuarios del SENASA, 10,8 M de
    filas, y las transferencias del registro automotor, 9,4 M; de 8 tablas de
    1,2 a 3 M de filas en staging, 4 llegaron al tope y las otras tardaron de
    1,5 a 2,5 s). Se usa directamente el de la muestra, como aproximado.
    """
    if propia is None or propia.atributo:
        return await _forma_sin_fecha(sandbox, tabla, query, propia)
    nombre = _lista([propia.nombre, propia.mes] if propia.mes else [propia.nombre])
    muestra: tuple[str, str] | None = None
    if con_muestra or not recorrer:
        try:
            muestra = await rango_de_muestra(sandbox, tabla, propia)
        except Exception:
            logger.warning("calcular: no se pudo leer la muestra de %s", tabla, exc_info=True)
        if (
            con_muestra
            and muestra is not None
            and muestra[0] != muestra[1]
            and not (periodo_propio and abarca_rango(query.desde, query.hasta, *muestra))
        ):
            return _Forma(
                "apila", columna=nombre, desde=muestra[0], hasta=muestra[1], aproximado=True
            )
    if not recorrer:
        periodo = (
            Periodo(desde=muestra[0], hasta=muestra[1], aproximado=True) if muestra else Periodo()
        )
    else:
        try:
            periodo = await describir_periodo(
                sandbox, tabla, propia, timeout_seconds=TIMEOUT_CONFIRMAR_S
            )
        except Exception:
            # El cálculo ya está hecho: no se lo pierde por un dato del aviso.
            logger.warning("calcular: no se pudo leer el período de %s", tabla, exc_info=True)
            periodo = Periodo()
    if periodo.desde is None or periodo.hasta is None:
        return _Forma("dudosa", dudosas=(propia.nombre,))
    # Un rango de una muestra, o con valores que no se leyeron como fecha, no
    # alcanza para decir que la tabla tiene un solo período ni que el pedido
    # los abarca a todos: los que faltan pueden ser otros.
    rango_completo = not periodo.aproximado and not periodo.sin_reconocer
    if periodo.desde == periodo.hasta:
        if not rango_completo:
            return _Forma("dudosa", dudosas=(propia.nombre,))
        return _Forma("foto", motivo=f"toda la tabla es de {periodo.desde} según {nombre}")
    if (
        periodo_propio
        and rango_completo
        and abarca_rango(query.desde, query.hasta, periodo.desde, periodo.hasta)
    ):
        return _Forma("abarca", columna=nombre, desde=periodo.desde, hasta=periodo.hasta)
    return _Forma(
        "apila",
        columna=nombre,
        desde=periodo.desde,
        hasta=periodo.hasta,
        aproximado=periodo.aproximado,
        sin_reconocer=periodo.sin_reconocer,
    )


def _texto_aviso_parte(
    forma: _Forma,
    *,
    total: int,
    exacto: int | None,
    filas: int,
    filtradas: list[str],
    fecha_pedida: str | None,
    con_filtros: bool,
) -> str:
    """El aviso de un conteo filtrado: qué dejó afuera y qué es la tabla."""
    if forma.clase == "abarca" or fecha_pedida is None:
        # En `abarca` el período no dejó ninguna fila afuera: fueron los filtros.
        quien = "Los filtros dejaron"
    elif con_filtros:
        quien = f"Los filtros y el período pedido sobre «{fecha_pedida}» dejaron"
    else:
        quien = f"El período pedido sobre «{fecha_pedida}» dejó"
    if exacto is not None:
        cabeza = (
            f"{quien} afuera {exacto - total} filas: la tabla entera tiene {exacto}, y {total} "
            "es sólo la parte que queda."
        )
        filas_tabla, conteo_sin_filtros = str(exacto), f"el conteo sin filtros, {exacto}"
    else:
        cabeza = (
            f"{quien} afuera unas {filas - total} filas: la tabla entera tiene unas {filas} "
            f"(una estimación de la base, no un conteo), y {total} es sólo la parte que queda."
        )
        filas_tabla = f"unas {filas}"
        conteo_sin_filtros = "el conteo sin filtros (contalo con calcular, sin filtros)"
    if forma.clase == "foto":
        cola = (
            f"La tabla no apila períodos ({forma.motivo}): es una sola foto, así que su total es "
            f"{conteo_sin_filtros}. Filtrar por {_lista(filtradas)} no elige otro período ni "
            "otra foto: elige una parte de esa foto."
        )
    elif forma.clase == "abarca":
        cola = (
            f"El período pedido abarca toda la tabla (de {forma.desde} a {forma.hasta} según "
            f"{forma.columna}), así que el total de ese período es {conteo_sin_filtros}."
        )
    elif forma.clase == "apila":
        rango = f"de {forma.desde} a {forma.hasta}"
        if forma.aproximado:
            rango = f"aproximadamente {rango}, según una muestra"
        elif forma.sin_reconocer:
            rango += f", sin contar {forma.sin_reconocer} valores que no reconozco como fecha"
        cola = (
            f"La tabla apila períodos en {forma.columna} ({rango}): sus {filas_tabla} filas "
            "juntan todos esos períodos, no son el total de uno solo. Para un período, filtralo "
            "con `desde`/`hasta`."
        )
    elif forma.dudosas:
        puede = "pueden" if len(forma.dudosas) > 1 else "puede"
        cola = (
            f"No sé si la tabla apila períodos: {_lista(list(forma.dudosas), 'o')} {puede} "
            f"distinguirlos. Si los distingue, sus {filas_tabla} filas juntan todos y no son el "
            "total de uno solo; si no, la tabla es una sola foto y su total es el conteo sin "
            "filtros."
        )
    else:
        cola = (
            "No sé si la tabla apila períodos: no tiene columna de fecha, y sin una muestra de "
            f"sus valores no puedo ver si otra los distingue. Si los apila, sus {filas_tabla} "
            "filas juntan todos y no son el total de uno solo; si no, la tabla es una sola foto "
            "y su total es el conteo sin filtros."
        )
    return f"{cabeza} {cola}"


async def _aviso_parte(
    sandbox: Any, tabla: str, req: AggregateRequest, query: AggregateQuery, total: int
) -> tuple[int | None, str | None]:
    """``(filas_tabla, aviso_parte)`` de un conteo filtrado, o ``(None, None)``.

    Prueba de calidad del 07-oct (nueva_16): con la Tarifa Social de Mendoza,
    el agente filtró ``PADRON = 202207`` (el mes de alta en el padrón) y
    presentó esas 99.558 filas como el padrón de 2022, que tiene 110.179. La
    tabla es un solo archivo: no apila períodos.

    El aviso dice cuántas filas dejó afuera el filtro y cuántas tiene la tabla,
    y después qué es la tabla, sin dejarle al modelo una lectura que justifique
    la parte. El de antes decía «si el filtro elige un período, una foto…, la
    tabla los suma a todos», y ``PADRON = 202207`` parece justo una foto
    mensual (verificación de la ola 5):

    - una sola foto (sin columna de fecha ni otra que pueda distinguir
      períodos, o con un solo período): su total es el conteo sin filtros, y
      filtrar elige una parte de esa foto (ver `_forma_sin_fecha`);
    - ``desde``/``hasta`` sobre su columna de fecha abarca todos sus períodos:
      el total de ese período es el conteo sin filtros;
    - apila períodos: lo dice con la columna que los distingue, y sus filas no
      son el total de uno solo (homicidios del SNIC: 38.126 filas con
      ``fecha_hecho`` de varios años, y además imputados y víctimas);
    - no se sabe: lo dice, con la columna que podría distinguirlos.

    Cubre también ``desde``/``hasta`` sobre una columna que no es la fecha de
    la tabla (``columna_fecha=PADRON``, «en 2022»: los mismos 99.558, que antes
    salían sin aviso). Un período sobre la fecha de una tabla que los apila no
    avisa: contar sin él suma todos, y no hay total que dar.
    """
    propia = resolver_columna_fecha(list(query.tipos.items()), None, tabla)
    # Una fecha de alta o de nacimiento no separa los períodos de la tabla: un
    # filtro sobre ella no es «el período de la tabla».
    de_periodo = propia if propia is not None and not propia.atributo else None
    columnas_propias = (
        {c for c in (de_periodo.nombre, de_periodo.mes) if c} if de_periodo else set()
    )
    fecha_pedida = query.fecha.nombre if (query.desde or query.hasta) and query.fecha else None
    periodo_propio = (
        fecha_pedida is not None and de_periodo is not None and fecha_pedida == de_periodo.nombre
    )
    filtro_propio = any(f.columna in columnas_propias for f in req.filtros)
    if all(f.columna in columnas_propias for f in req.filtros) and (
        periodo_propio or fecha_pedida is None
    ):
        # Sólo el período de la tabla: si dejó filas afuera, son de otros
        # períodos, y si no, no hay nada que avisar. Sin consultas de más.
        return None, None
    estimada = filas_estimadas(await estadisticas(sandbox, tabla, []), None)
    # En una tabla grande no se recorre la tabla para el aviso: ni el conteo
    # ni el rango de fechas (ver `_forma_de_la_tabla`).
    recorrer = es_tolerante(estimada)
    forma: _Forma | None = None
    if periodo_propio or filtro_propio:
        # Un período de una tabla que los apila, más otros filtros: contar sin
        # el período tampoco da un total. Se mira antes de contar la tabla.
        forma = await _forma_de_la_tabla(
            sandbox, tabla, query, propia, periodo_propio, con_muestra=True, recorrer=recorrer
        )
        if forma.clase in ("apila", "dudosa"):
            return None, None
    exacto, estimada = await _filas_de_la_tabla(sandbox, tabla, estimada)
    filas = exacto if exacto is not None else estimada
    if not filas or filas <= total:
        return None, None
    if forma is None:
        forma = await _forma_de_la_tabla(
            sandbox, tabla, query, propia, periodo_propio, recorrer=recorrer
        )
    filtradas = [f.columna for f in req.filtros] + ([fecha_pedida] if fecha_pedida else [])
    aviso = _texto_aviso_parte(
        forma,
        total=total,
        exacto=exacto,
        filas=filas,
        filtradas=filtradas,
        fecha_pedida=fecha_pedida,
        con_filtros=bool(req.filtros),
    )
    return exacto, aviso


class Calcular:
    status = "Calculando..."

    def describe(self, args: dict[str, Any]) -> str:
        what = {
            "conteo": "Contando",
            "suma": "Sumando",
            "promedio": "Promediando",
            "minimo": "Buscando el mínimo",
            "maximo": "Buscando el máximo",
        }.get(str(args.get("operacion") or ""), "Calculando")
        if args.get("ponderar_por"):
            what += " con el factor de expansión de la encuesta"
        return what

    spec = AgentTool(
        name="calcular",
        description=(
            "Suma, cuenta, promedia o busca mínimo/máximo sobre una tabla, opcionalmente "
            "agrupando y filtrando. La consulta la arma OpenArg: no se escribe SQL. Para "
            "encuestas, pasá la columna de ponderación en `ponderar_por`: con operacion=conteo "
            "devuelve la población estimada (suma de pesos), no la cantidad de filas de la "
            "muestra. Las columnas de texto se convierten a número según su formato (argentino "
            "1.234,5 o inglés 1,234.5, decidido con una muestra de la columna); si el formato es "
            "ambiguo, no calcula y lo dice. Cada resultado trae `filas_usadas` (sobre cuántas "
            "filas se calculó). Un conteo con filtros que dejan filas afuera trae además "
            "`filas_tabla` (las filas de toda la tabla) y `aviso_parte`, que dice si la tabla es "
            "una sola foto (su total es el conteo sin filtros) o apila períodos, y en qué columna; "
            "no viene si el único filtro es un período de la columna de fecha de la tabla. "
            "Si ninguna fila cumple los filtros no hay valor: viene `aviso` con los valores que sí "
            "existen."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "tabla": {"type": "string"},
                "operacion": {"type": "string", "enum": list(OPERATIONS)},
                "columna": {
                    "type": "string",
                    "description": "La columna a sumar/promediar/etc. No va con conteo.",
                },
                "ponderar_por": {
                    "type": "string",
                    "description": "Columna de ponderación (encuestas).",
                },
                "agrupar_por": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
                "filtros": _filters_schema(MAX_AGG_FILTERS),
                "desde": {"type": "string"},
                "hasta": {"type": "string"},
                "columna_fecha": {
                    "type": "string",
                    "description": "Opcional: la columna de fecha para desde/hasta.",
                },
                "ordenar_por": {
                    "type": "string",
                    "description": (
                        "'valor' (por defecto) o una columna de agrupar_por (p. ej. el año, "
                        "para una serie)."
                    ),
                },
                "orden": {"type": "string", "enum": ["asc", "desc"]},
                "limite": {"type": "integer", "minimum": 1, "maximum": 200},
            },
            "required": ["tabla", "operacion"],
        },
    )

    async def run(self, args: dict[str, Any], ctx: ToolContext) -> ToolOutcome:
        sandbox = ctx.deps.sandbox
        table = await resolve(sandbox, str_arg(args, "tabla", required=True, max_len=200) or "")
        types = (await sandbox.get_column_types([table.name])).get(table.name, [])
        raw_filters = args.get("filtros") or []
        groups = args.get("agrupar_por") or []
        if not isinstance(raw_filters, list) or not isinstance(groups, list):
            raise ToolInputError("`filtros` y `agrupar_por` son listas.")

        async def run_sql(sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
            return await _run_sql(sandbox, sql, params)

        try:
            res = await agregar(
                sandbox,
                PedidoAgregado(
                    tabla=table.name,
                    tipos=types,
                    operacion=str_arg(args, "operacion", required=True, max_len=20) or "",
                    columna=str_arg(args, "columna", max_len=200),
                    ponderar_por=str_arg(args, "ponderar_por", max_len=200),
                    agrupar_por=[str(g) for g in groups],
                    filtros=raw_filters,
                    desde=str_arg(args, "desde", max_len=10),
                    hasta=str_arg(args, "hasta", max_len=10),
                    orden=str_arg(args, "orden", max_len=4) or "desc",
                    limite=int_arg(args, "limite", 50, 1, 200),
                    columna_fecha=str_arg(args, "columna_fecha", max_len=200),
                    ordenar_por=str_arg(args, "ordenar_por", max_len=200),
                    filas_tabla=table.row_count,
                ),
                run_sql,
            )
        except CatalogRequestError as exc:
            raise ToolInputError(str(exc)) from None
        req, query, what = res.req, res.query, res.calculo
        title = f"{table.title} — {what}"
        base: dict[str, Any] = {
            "tabla": table.name,
            "calculo": what,
            "agrupado_por": req.agrupar_por,
        }
        total, parcial = res.filas_usadas, res.parcial
        clave_filas = "filas_usadas_en_grupos_mostrados" if parcial else "filas_usadas"

        if res.vacio:
            payload = {**base, "filas_usadas": 0, "resultado": None, "aviso": res.aviso}
            if res.sugerencias:
                payload["sugerencias"] = res.sugerencias
            if res.notas:
                payload["notas"] = res.notas
            return ToolOutcome(
                to_json(payload),
                summary=f"Ninguna fila cumplió los filtros en {quoted(table.title, 80)}",
            )

        if res.sin_numeros:
            payload = {**base, clave_filas: total, "resultado": None, "aviso": res.aviso}
            return ToolOutcome(
                to_json(payload),
                summary=f"{res.columna_valorada} no tiene números en {quoted(table.title, 80)}",
            )

        clean = plain_rows(res.grupos)
        controlado = total is not None
        for_model = [
            {**c, "filas_usadas": n} if controlado else c
            for c, n in zip(clean, res.filas_por_grupo, strict=True)
        ]
        avisos = list(res.avisos)
        if len(for_model) > MAX_ROWS_FOR_MODEL:
            avisos.append(f"Se muestran {MAX_ROWS_FOR_MODEL} de {len(for_model)} grupos.")
        notas = [*avisos, *res.notas]

        result = _data_result(table, clean, query.sql, title, query.params)
        if total is not None:
            result.metadata[clave_filas] = total
        if res.filas_con_valor is not None and not parcial:
            result.metadata["filas_con_valor"] = res.filas_con_valor
        # Contrato de metadatos (lo lee la verificación de cifras).
        result.metadata["truncada"] = res.truncado
        payload = {
            **base,
            "columnas": [*req.agrupar_por, "valor", *(["filas_usadas"] if controlado else [])],
            "filas": for_model[:MAX_ROWS_FOR_MODEL],
        }
        if total is not None:
            payload[clave_filas] = total
        # Un conteo filtrado es una parte de la tabla, y el resultado no lo
        # decía (nueva_16, ver `_aviso_parte`). Sólo el conteo sin ponderador:
        # ahí la tabla se mide en filas.
        #
        # Revisión de #171: las filas de la tabla no se citan como evidencia
        # (van sólo en lo que lee el modelo). Citadas, el verificador daba por
        # respaldado un total inflado, y en la batería la fuente de complex_003
        # dejaba de ser «sólo conteos chicos».
        if (
            req.operacion == "conteo"
            and not req.ponderar_por
            and (req.filtros or query.desde or query.hasta)
            and total
            and not parcial
        ):
            filas_tabla, aviso_parte = await _aviso_parte(sandbox, table.name, req, query, total)
            if filas_tabla is not None:
                payload["filas_tabla"] = filas_tabla
            if aviso_parte:
                payload["aviso_parte"] = aviso_parte
        if notas:
            payload["notas"] = notas
        return ToolOutcome(
            to_json(payload),
            results=[result],
            summary=f"Calculó {what} en {quoted(table.title, 80)}",
        )
