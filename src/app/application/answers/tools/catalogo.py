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

import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.application.answers.aggregates import (
    COLUMNAS_DE_CONTROL,
    FILAS,
    FILAS_CON_VALOR,
    FILAS_CON_VALOR_TOTAL,
    FILAS_TOTAL,
    FILTER_OPERATORS,
    OPERATIONS,
    AggregateRequest,
    build_aggregate_query,
    numeric_columns,
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
from app.application.consultas.fechas import aviso_formato_guardado
from app.application.consultas.filtros import leer_filtros, notas_de_filtros, validar_filtros
from app.application.consultas.preparar import Preparado, describir_periodo, ejecutar, preparar
from app.application.consultas.sugerencias import diagnosticar_vacio
from app.application.public_catalog import (
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_sample_query,
    is_internal_column,
    resolve_date_column,
    resolve_table,
    visible_types,
)
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.agent_llm import AgentTool
from app.domain.ports.sandbox.sql_sandbox import MartInfo
from app.domain.value_objects.table_reference import bare_name

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


def _filas_del_calculo(rows: list[dict[str, Any]], por_grupo: str, de_todos: str) -> int:
    """Sobre cuántas filas se calculó: el total de todos los grupos si la consulta
    lo trae (``aggregates.FILAS_TOTAL``), si no la suma de las filas recibidas."""
    if rows[0].get(de_todos) is not None:
        return int(rows[0][de_todos])
    return sum(int(r.get(por_grupo) or 0) for r in rows)


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
        vector = await deps.embedding.embed(texto)
        async with ctx.db_lock:
            try:
                hits = await deps.vector_search.search_datasets_ann(
                    query_embedding=vector,
                    limit=_MAX_DATASETS * 2,
                    portal_filter=portal,
                    min_similarity=_MIN_SIMILARITY,
                )
            except BaseException:
                # Una búsqueda cortada por el tope de la herramienta dejaba la
                # sesión con la transacción rota, y todas las búsquedas
                # siguientes del turno fallaban (staging, 01-oct).
                await deps.vector_search.reset()
                raise
        tables: dict[str, list[dict[str, Any]]] = {}
        for t in await deps.sandbox.find_tables(dataset_ids=[str(h.dataset_id) for h in hits]):
            # Una tabla con 0 filas es una versión vieja o una descarga fallida.
            if t.dataset_id and t.row_count != 0:
                tables.setdefault(str(t.dataset_id), []).append(_table_summary(t))

        # El catálogo tiene datasets repetidos (la migración de datos.gob.ar
        # regeneró IDs): mismo título y URL = el mismo dataset.
        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for h in hits:
            key = (h.title.strip().lower(), (h.download_url or "").strip())
            found = tables.get(str(h.dataset_id), [])
            if key in merged:
                known = {t["tabla"] for t in merged[key]["tablas"]}
                merged[key]["tablas"].extend(t for t in found if t["tabla"] not in known)
                continue
            merged[key] = {
                "titulo": h.title,
                "portal": h.portal,
                "descripcion": (h.description or "")[:_DESCRIPTION_CHARS],
                "tablas": list(found),
            }
        datasets = [d for d in merged.values() if d["tablas"]][:_MAX_DATASETS]
        for d in datasets:
            d["tablas"] = sorted(d["tablas"], key=lambda t: -(t["filas"] or 0))[
                :_MAX_TABLES_PER_DATASET
            ]

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
        fecha = resolve_date_column(columns)
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
            "columna_fecha": fecha.nombre if fecha else None,
            "desde": periodo.desde,
            "hasta": periodo.hasta,
            "muestra": sample,
            "ponderador": weights[0] if weights else None,
            "columnas_geograficas": geo,
        }
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
        aviso_fecha = aviso_formato_guardado(query.fecha)
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
            "filas se calculó). Si ninguna fila cumple los filtros no hay valor: viene `aviso` "
            "con los valores que sí existen."
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
        try:
            filters = validar_filtros(
                leer_filtros(raw_filters, MAX_AGG_FILTERS),
                visible_types([c for c, _ in types], types),
            )
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
                columna_fecha=str_arg(args, "columna_fecha", max_len=200),
                ordenar_por=str_arg(args, "ordenar_por", max_len=200),
            )
            # Primero valida (puro); después mira el formato de los números y
            # las estadísticas de los filtros, y arma la consulta definitiva.
            query = build_aggregate_query(req)
            prep = await preparar(
                sandbox,
                table.name,
                query.tipos,
                query.filtros,
                numericas=numeric_columns(req),
                row_count=table.row_count,
                fecha=query.fecha if (query.desde or query.hasta) else None,
            )
            query = build_aggregate_query(
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
        what = req.operacion + (f" de {req.columna}" if req.columna else "")
        if req.ponderar_por:
            what += f" ponderado por {req.ponderar_por}"
        title = f"{table.title} — {what}"
        base: dict[str, Any] = {
            "tabla": table.name,
            "calculo": what,
            "agrupado_por": req.agrupar_por,
        }

        truncado = len(rows) > query.limite
        rows = rows[: query.limite]
        # Sin las columnas de control (un sandbox de prueba que no las
        # devuelve) se sigue como antes: no se sabe cuántas filas entraron.
        controlado = bool(rows) and FILAS in rows[0]
        total = _filas_del_calculo(rows, FILAS, FILAS_TOTAL) if controlado else None
        con_valor = (
            _filas_del_calculo(rows, FILAS_CON_VALOR, FILAS_CON_VALOR_TOTAL)
            if controlado and FILAS_CON_VALOR in rows[0]
            else None
        )
        # Con más grupos que `limite` y sin el total de todos los grupos (un
        # sandbox que no lo devuelve), la suma es sólo de los grupos
        # mostrados: no se la presenta como el total del cálculo.
        parcial = truncado and not (rows and rows[0].get(FILAS_TOTAL) is not None)
        clave_filas = "filas_usadas_en_grupos_mostrados" if parcial else "filas_usadas"
        notas: list[str] = notas_de_filtros(query.filtros, query.tipos, tolerante=prep.tolerante)
        aviso_fecha = aviso_formato_guardado(query.fecha) if (query.desde or query.hasta) else None
        if aviso_fecha:
            notas.append(aviso_fecha)

        if not rows or total == 0:
            # Ninguna fila cumplió los filtros: no hay valor que citar. Antes
            # salía `valor: 0` (conteo) o `None` (suma) como un dato más.
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
            payload = {**base, "filas_usadas": 0, "resultado": None, "aviso": diag.aviso}
            if diag.sugerencias:
                payload["sugerencias"] = diag.sugerencias
            if notas:
                payload["notas"] = notas
            return ToolOutcome(
                to_json(payload),
                summary=f"Ninguna fila cumplió los filtros en {quoted(table.title, 80)}",
            )

        valued = req.ponderar_por if req.operacion == "conteo" else req.columna
        if con_valor == 0:
            payload = {
                **base,
                clave_filas: total,
                "resultado": None,
                "aviso": (
                    f"Ninguna de las {total} filas que cumplen los filtros tiene un número "
                    f"reconocible en {valued!r}: no hay valor que informar."
                ),
            }
            return ToolOutcome(
                to_json(payload),
                summary=f"{valued} no tiene números en {quoted(table.title, 80)}",
            )

        clean = [{k: v for k, v in r.items() if k not in COLUMNAS_DE_CONTROL} for r in rows]
        for_model = [
            {**c, "filas_usadas": r.get(FILAS)} if controlado else c
            for c, r in zip(clean, rows, strict=True)
        ]
        avisos: list[str] = []
        if con_valor is not None and total is not None and con_valor < total:
            donde = " de los grupos mostrados" if parcial else ""
            avisos.append(
                f"Se calculó sobre {con_valor} de {total} filas{donde}: las otras "
                f"{total - con_valor} no tienen un número reconocible en {valued!r}."
            )
        if truncado:
            avisos.append(
                f"Hay más de {query.limite} grupos: se muestran los primeros {query.limite} "
                "según el orden pedido."
                + ("" if parcial else f" `filas_usadas` ({total}) es de todos los grupos.")
            )
        if len(for_model) > MAX_ROWS_FOR_MODEL:
            avisos.append(f"Se muestran {MAX_ROWS_FOR_MODEL} de {len(for_model)} grupos.")
        notas = [*avisos, *notas]

        result = _data_result(table, clean, query.sql, title, query.params)
        if total is not None:
            result.metadata[clave_filas] = total
        if con_valor is not None and not parcial:
            result.metadata["filas_con_valor"] = con_valor
        # Contrato de metadatos (lo lee la verificación de cifras).
        result.metadata["truncada"] = truncado
        payload = {
            **base,
            "columnas": [*req.agrupar_por, "valor", *(["filas_usadas"] if controlado else [])],
            "filas": for_model[:MAX_ROWS_FOR_MODEL],
        }
        if total is not None:
            payload[clave_filas] = total
        if notas:
            payload["notas"] = notas
        return ToolOutcome(
            to_json(payload),
            results=[result],
            summary=f"Calculó {what} en {quoted(table.title, 80)}",
        )
