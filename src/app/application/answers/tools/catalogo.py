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
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.application.answers.aggregates import (
    FILTER_OPERATORS,
    OPERATIONS,
    AggregateRequest,
    Filter,
    build_aggregate_query,
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
from app.application.catalog.collapse import collapse_hits
from app.application.catalog.national_prior import national_prior
from app.application.public_catalog import (
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_date_range_query,
    build_sample_query,
    date_column,
    is_internal_column,
    resolve_table,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.agent_llm import AgentTool
from app.domain.ports.sandbox.sql_sandbox import MartInfo
from app.domain.value_objects.table_reference import bare_name

logger = logging.getLogger(__name__)

# Mismo umbral que el modo datos y `/data/search`.
_MIN_SIMILARITY = 0.40
_MAX_DATASETS = 8
_MAX_TABLES_PER_DATASET = 4
_MAX_MARTS = 5
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
        portal=source.portal if source else "datos.gob.ar",
        url=source.url if source else "",
        row_count=table.row_count,
    )


def _mart_title(mart: MartInfo) -> str:
    first = next((line.strip() for line in mart.description.splitlines() if line.strip()), "")
    return first[:160] or mart.mart_id


async def _run_sql(sandbox: Any, sql: str) -> list[dict[str, Any]]:
    result = await sandbox.execute_readonly(sql)
    if result.error:
        raise ToolInputError(
            f"La consulta no se pudo ejecutar ({result.error[:160]}). "
            "Probá con otros filtros, menos columnas o otra tabla."
        )
    return plain_rows(result.rows)


def _data_result(
    table: ResolvedTable, rows: list[dict[str, Any]], sql: str, title: str
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
        # Una entrada por archivo, con la copia de más filas reales y
        # encabezado sano; prioridad chica a lo nacional si no nombra lugar.
        datasets = [
            {
                "titulo": c.hit.title,
                "portal": c.hit.portal,
                "descripcion": (c.hit.description or "")[:_DESCRIPTION_CHARS],
                **({"archivo": c.archivo} if c.archivo else {}),
                "tablas": [_table_summary(t) for t in c.tables[:_MAX_TABLES_PER_DATASET]],
            }
            for c in collapse_hits(hits, found_tables, profiles, prior=national_prior(texto))
            if c.tables
        ][:_MAX_DATASETS]
        logger.info(
            "buscar_datos: embed_ms=%.0f busqueda_ms=%.0f tablas_ms=%.0f hits=%d datasets=%d",
            (t_embed - t0) * 1000,
            (t_search - t_embed) * 1000,
            (time.perf_counter() - t_search) * 1000,
            len(hits),
            len(datasets),
        )

        marts = await deps.sandbox.find_marts(vector, limit=_MAX_MARTS) if not portal else []
        curated = [
            {
                "tabla": m.table_name,
                "descripcion": m.description[:_DESCRIPTION_CHARS],
                "filas": m.row_count,
                "similitud": round(m.score, 2),
            }
            for m in marts
            if m.score >= _MIN_SIMILARITY
        ]
        if not datasets and not curated:
            return ToolOutcome(
                to_json({"resultados": [], "nota": "Nada parecido en el catálogo."}),
                summary="No encontró nada parecido en el catálogo",
            )
        found = count(len(datasets), "dataset", "datasets")
        if curated:
            found += f" y {count(len(curated), 'tabla curada', 'tablas curadas')}"
        return ToolOutcome(
            to_json({"tablas_curadas": curated, "datasets": datasets}),
            summary=f"Encontró {found}",
        )


# ── describir_tabla ────────────────────────────────────────


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
            "solo nivel —normalmente nacional— y no sirve para una provincia o un partido) y, en "
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
        fecha = date_column(names)
        desde = hasta = None
        if fecha:
            rows = await _run_sql(sandbox, build_date_range_query(table.name, fecha))
            if rows:
                desde, hasta = rows[0].get("desde"), rows[0].get("hasta")
        sample = await _run_sql(sandbox, build_sample_query(table.name, names))

        described: dict[str, str] = {}
        if table.mart and table.mart.columns:
            for col in table.mart.columns:
                if isinstance(col, dict) and col.get("name") and col.get("description"):
                    described[str(col["name"])] = str(col["description"])[:200]

        weights = [c for c in names if _WEIGHT_RE.match(c)]
        geo = [c for c in names if _GEO_RE.search(c)]
        payload: dict[str, Any] = {
            "tabla": table.name,
            "titulo": table.title,
            "fuente": table.portal,
            "filas": table.row_count,
            "columnas": [
                {
                    "nombre": c,
                    "tipo": t,
                    **({"descripcion": described[c]} if c in described else {}),
                }
                for c, t in columns
            ],
            "columna_fecha": fecha,
            "desde": desde,
            "hasta": hasta,
            "muestra": sample,
            "ponderador": weights[0] if weights else None,
            "columnas_geograficas": geo,
        }
        if table.mart:
            payload["descripcion"] = table.mart.description[:1200]
        if weights:
            payload["aviso_ponderador"] = (
                f"Es una encuesta: cada fila representa a muchas personas. Para 'cuántos' usá "
                f"calcular con operacion=conteo y ponderar_por={weights[0]!r}; contar filas da el "
                "tamaño de la muestra, no la población."
            )
        if not geo:
            # Batería del 02-oct: con el aviso sin la segunda oración, Sonnet y
            # Haiku explicaban bien que no había dato de Pinamar pero no daban
            # el total nacional, que la tabla sí tiene.
            payload["aviso_geografico"] = (
                "Sin columnas geográficas: el dato es de un solo nivel (normalmente el total "
                "nacional). No lo presentes como dato de una provincia, partido o ciudad. Si te "
                "preguntaron por un lugar, decí que no hay dato a ese nivel y DÁ IGUAL la cifra "
                "nacional, calculándola con calcular (con el ponderador si lo hay) y aclarando "
                "que es nacional."
            )
        return ToolOutcome(
            to_json(payload),
            summary=f"Revisó {quoted(table.title, 80)} ({count(table.row_count, 'fila', 'filas')})",
        )


# ── obtener_datos ──────────────────────────────────────────


class ObtenerDatos:
    status = "Leyendo datos..."

    def describe(self, args: dict[str, Any]) -> str:
        return "Leyendo los datos de la tabla"

    spec = AgentTool(
        name="obtener_datos",
        description=(
            "Lee filas de una tabla: columnas elegidas, período (desde/hasta sobre la columna de "
            "fecha), filtros de igualdad y orden por fecha. Para series y listados cortos. Si hay "
            "que sumar, contar o promediar muchas filas, usá calcular."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "tabla": {"type": "string"},
                "columnas": {"type": "array", "items": {"type": "string"}, "maxItems": 30},
                "desde": {"type": "string", "description": "AAAA, AAAA-MM o AAAA-MM-DD"},
                "hasta": {"type": "string", "description": "AAAA, AAAA-MM o AAAA-MM-DD"},
                "filtros": {
                    "type": "object",
                    "description": "{columna: valor exacto}. Hasta 5.",
                    "additionalProperties": {"type": "string"},
                },
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
        filtros = args.get("filtros")
        if columnas is not None and not isinstance(columnas, list):
            raise ToolInputError("`columnas` es una lista de nombres.")
        if filtros is not None and not isinstance(filtros, dict):
            raise ToolInputError("`filtros` es un objeto {columna: valor}.")
        try:
            sql, cols = build_data_query(
                DataRequest(
                    table=table.name,
                    available_columns=[c for c, _ in types],
                    columns=[str(c) for c in columnas] if columnas else None,
                    desde=str_arg(args, "desde", max_len=10),
                    hasta=str_arg(args, "hasta", max_len=10),
                    filtros={str(k): str(v) for k, v in (filtros or {}).items()} or None,
                    orden=str_arg(args, "orden", max_len=4) or "asc",
                    limite=int_arg(args, "limite", 100, 1, 200),
                )
            )
        except CatalogRequestError as exc:
            raise ToolInputError(str(exc)) from None
        rows = await _run_sql(sandbox, sql)
        result = _data_result(table, rows, sql, table.title)
        return ToolOutcome(
            to_json(
                {
                    "tabla": table.name,
                    "titulo": table.title,
                    "columnas": cols,
                    "cantidad": len(rows),
                    "filas": rows[:MAX_ROWS_FOR_MODEL],
                    **(
                        {"nota": f"Se muestran {MAX_ROWS_FOR_MODEL} de {len(rows)} filas."}
                        if len(rows) > MAX_ROWS_FOR_MODEL
                        else {}
                    ),
                }
            ),
            results=[result] if rows else [],
        )


# ── calcular ───────────────────────────────────────────────


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
            "muestra. Las columnas de texto se convierten a número sólo si el valor es un número "
            "limpio (1234.5)."
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
                "filtros": {
                    "type": "array",
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "properties": {
                            "columna": {"type": "string"},
                            "operador": {"type": "string", "enum": list(FILTER_OPERATORS)},
                            "valor": {"type": "string"},
                        },
                        "required": ["columna", "operador", "valor"],
                    },
                },
                "desde": {"type": "string"},
                "hasta": {"type": "string"},
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
        try:
            filters = [
                Filter(str(f["columna"]), str(f["operador"]), str(f["valor"]))
                for f in raw_filters
                if isinstance(f, dict)
            ]
            req = AggregateRequest(
                table=table.name,
                column_types=types,
                operacion=str_arg(args, "operacion", required=True, max_len=20) or "",
                columna=str_arg(args, "columna", max_len=200),
                ponderar_por=str_arg(args, "ponderar_por", max_len=200),
                agrupar_por=[str(g) for g in groups],
                filtros=filters,
                desde=str_arg(args, "desde", max_len=10),
                hasta=str_arg(args, "hasta", max_len=10),
                orden=str_arg(args, "orden", max_len=4) or "desc",
                limite=int_arg(args, "limite", 50, 1, 200),
            )
            sql, cols = build_aggregate_query(req)
        except (CatalogRequestError, KeyError) as exc:
            raise ToolInputError(str(exc)) from None
        rows = await _run_sql(sandbox, sql)
        what = req.operacion + (f" de {req.columna}" if req.columna else "")
        if req.ponderar_por:
            what += f" ponderado por {req.ponderar_por}"
        title = f"{table.title} — {what}"
        return ToolOutcome(
            to_json(
                {
                    "tabla": table.name,
                    "calculo": what,
                    "agrupado_por": req.agrupar_por,
                    "columnas": cols,
                    "filas": rows[:MAX_ROWS_FOR_MODEL],
                }
            ),
            results=[_data_result(table, rows, sql, title)] if rows else [],
            summary=f"Calculó {what} en {quoted(table.title, 80)}",
        )
