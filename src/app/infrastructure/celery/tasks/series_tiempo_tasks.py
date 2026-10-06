"""
Series de Tiempo — ETL de las series curadas a `raw.cache_series_*`.

Consulta la API de Series de Tiempo (apis.datos.gob.ar/series/api) y deja cada
serie en una tabla de `raw`, que es lo que ven el modo datos del MCP y el
NL2SQL. El agente no lee estas tablas: consulta la API en vivo.

Por qué está escrito así (auditoría verificada del 04-oct-2026):

- **Nunca refrescaba.** Desde el commit original, toda serie con fila `ready`
  en `raw.cached_datasets` se salteaba, así que una serie que llegaba a `ready`
  quedaba congelada para siempre. Las 12 tablas estaban paradas desde el
  06-may mientras la tarea corría todos los meses y terminaba en "success" en
  medio segundo. Ahora se compara el último dato de la API (`time_index_end`)
  contra `max(fecha)` de la tabla y se reescribe sólo si hay algo nuevo (o si
  la tabla falta, cambió la serie o la cantidad de filas no coincide).
- **Truncaba.** Un solo GET con `limit=1000` ascendente y sin leer `count`:
  tipo de cambio (8.643 observaciones) terminaba en 2005-09-27 y reservas
  (1.036) en 2023-04. Ahora se pagina con `start` hasta cubrir `count` (el tope
  de la API es 5.000 por pedido) y se exige que lo bajado coincida.
- **Chocaba con el registro.** Registraba `series_tiempo::<clave>` cuando el
  registro ya tenía `series_tiempo::series-tiempo-<clave>` para la misma tabla,
  y el índice UNIQUE `(schema_name, table_name)` rechazaba el INSERT en un
  `except` que se lo tragaba. Ahora la identidad es la del registro, se
  verifica antes de escribir y un registro fallido cuenta como falla.
- **Escribía antes de validar.** `to_sql(replace)` corría antes de la puerta
  WS0 (`_finalize_cached_dataset`): un rechazo dejaba la tabla reescrita y la
  serie fuera del catálogo. Ahora WS0 decide primero (mira columnas y cantidad
  de filas, no el contenido), la carga va a una tabla `__nueva` y el reemplazo
  son dos RENAME en la misma transacción: la vieja pasa a `<tabla>__previa`
  (volver atrás es otro RENAME) y la nueva ocupa su lugar. Un guardián se
  niega a reemplazar por menos filas o por una fecha máxima anterior (salvo
  `permitir_menos_filas`), y por otra serie salvo que el cambio esté en
  `CAMBIOS_DE_SERIE_APROBADOS`.
- **Decía "al día" con el catálogo roto.** El reemplazo se confirma solo y lo
  que viene después (dataset, catálogo, registro) va en transacciones
  propias. Si la corrida se cortaba en el medio, la siguiente veía la tabla
  igual a la API, latía sana y no reparaba nada. Ahora "al día" también mira
  `raw.cached_datasets`, `raw_table_versions` y `datasets`, y si no coinciden
  rehace esos pasos sin bajar ni reescribir la tabla (`reconciliada`).
- **Tenía un catálogo propio.** Los ids salen del `SERIES_CATALOG` del
  adaptador (el mismo que usa el agente); acá sólo queda qué clave del
  catálogo alimenta qué tabla. Los títulos salen de la metadata de la API, que
  dice qué serie es de verdad.
- **El latido mentía.** Ahora la tarea cuenta cuántas series escribió, cuántas
  estaban al día, cuántas rechazó y cuántas fallaron; cada serie verificada
  late con su propia identidad, y si no pudo consultar la API para ninguna
  serie la tarea falla en vez de registrar un éxito.

`check_series_freshness` es la alarma: distingue `series_cache_stale` (la API
tiene datos más nuevos que la tabla: bug nuestro) de `series_source_stale` (la
fuente misma está atrasada para su frecuencia).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.infrastructure.adapters.connectors import series_tiempo_adapter
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine, register_via_b_table
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

API_URL = "https://apis.datos.gob.ar/series/api/series"
PORTAL = "series_tiempo"
ORGANIZACION = "Ministerio de Economia — Series de Tiempo"

# Tope de la API: `limit` por encima de 5000 responde 400.
PAGINA = 5000
_TIMEOUT_S = 30.0
_ESPERAS_REINTENTO_S = (2.0, 5.0)
_ZONA = ZoneInfo("America/Argentina/Buenos_Aires")
# Postgres trunca los identificadores a 63 bytes; mejor hacerlo acá que dejar
# que la tabla tenga una columna distinta de la que registramos.
_MAX_BYTES_IDENTIFICADOR = 63

# Qué clave del `SERIES_CATALOG` del adaptador alimenta cada tabla
# `raw.cache_series_<clave>`. Los nombres de tabla no cambian: el NL2SQL, el
# planner y el registro (`series_tiempo::series-tiempo-<clave>`) ya los
# conocen. Los ids y las descripciones viven en el adaptador; si una clave deja
# de existir allá, la serie se reporta como `sin_catalogo` en vez de romper.
SERIES_TABLAS: dict[str, str] = {
    "inflacion_ipc": "inflacion",
    "tipo_cambio": "tipo_cambio",
    "emae": "emae",
    "desempleo": "desempleo",
    "gasto_publico": "presupuesto",
    "reservas_internacionales": "reservas",
    "base_monetaria": "base_monetaria",
    "salarios": "salarios",
    "canasta_basica": "canasta_basica",
    "exportaciones": "exportaciones",
    "importaciones": "importaciones",
    "actividad_industrial": "actividad_industrial",
}

# Los cambios de id que una persona aprobó, como (tabla, id viejo, id nuevo).
# El id de cada tabla sale del `SERIES_CATALOG` del adaptador, que comparten el
# agente y otros PRs: sin esta lista, cualquier edición del catálogo
# reemplazaba la tabla raw sin comparar filas ni fechas. Un cambio que no está
# acá se rechaza y avisa; para aprobarlo se agrega la tupla en un PR.
CAMBIOS_DE_SERIE_APROBADOS: frozenset[tuple[str, str, str]] = frozenset(
    {
        # "Actividad industrial" era EMAE Comercio (265 meses desde 2004) desde
        # 2026-02; el catálogo la corrigió al IPI manufacturero del INDEC (127
        # meses desde 2016). Se pierde 2004-2015 de una serie que no era la
        # que decía ser, y cambia el nombre de la columna.
        ("actividad_industrial", "11.3_AGCS_2004_M_41", "453.1_SERIE_ORIGNAL_0_0_14_46"),
    }
)

_FRECUENCIAS = {
    # field.frequency (ISO 8601) y meta[0].frequency de la API
    "R/P1D": "diaria",
    "R/P1W": "semanal",
    "R/P1M": "mensual",
    "R/P3M": "trimestral",
    "R/P6M": "semestral",
    "R/P1Y": "anual",
    "day": "diaria",
    "week": "semanal",
    "month": "mensual",
    "quarter": "trimestral",
    "semester": "semestral",
    "year": "anual",
}

# Cuánto puede tener de antigüedad el último dato (contado desde el inicio de
# su período) antes de llamar atrasada a la fuente, cuando la API no dice
# `is_updated`. Período + el siguiente + el rezago de publicación: el EMAE de
# julio sale a fines de septiembre y el de agosto a fines de octubre, así que
# un mensual sano llega a ~115 días; la EPH trimestral, a ~265.
_MARGEN_FUENTE_DIAS = {
    "diaria": 10,
    "semanal": 30,
    "mensual": 130,
    "trimestral": 285,
    "semestral": 490,
    "anual": 950,
}


# ── catálogo ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SerieETL:
    """Una tabla del ETL y la serie del catálogo del adaptador que la alimenta."""

    clave: str
    clave_catalogo: str
    serie_id: str
    descripcion_catalogo: str = ""

    @property
    def tabla(self) -> str:
        return f"cache_series_{self.clave}"

    @property
    def source_id(self) -> str:
        return f"series-tiempo-{self.clave}"

    @property
    def identidad(self) -> str:
        # La que ya tiene el registro (backfill del 09-may). Con otra, el INSERT
        # choca con `uq_raw_table_versions_table_name`.
        return f"{PORTAL}::{self.source_id}"

    @property
    def url(self) -> str:
        return f"{API_URL}?ids={self.serie_id}"


def cambio_de_serie_aprobado(serie: SerieETL, serie_id_previa: str | None) -> bool:
    """Si pasar la tabla de `serie_id_previa` al id actual está en la lista aprobada."""
    return (serie.clave, serie_id_previa or "", serie.serie_id) in CAMBIOS_DE_SERIE_APROBADOS


def series_del_catalogo(
    catalogo: Mapping[str, Any] | None = None,
    claves: Iterable[str] | None = None,
) -> tuple[list[SerieETL], dict[str, str]]:
    """Resuelve `SERIES_TABLAS` contra el catálogo del adaptador.

    Devuelve las series resolubles y, por clave de tabla, por qué no se pudo
    resolver el resto (clave ausente, entrada con varias series o sin id).
    """
    cat = series_tiempo_adapter.SERIES_CATALOG if catalogo is None else catalogo
    pedidas = set(claves) if claves else None
    series: list[SerieETL] = []
    problemas: dict[str, str] = {}
    for clave, clave_catalogo in SERIES_TABLAS.items():
        if pedidas is not None and clave not in pedidas:
            continue
        entrada = cat.get(clave_catalogo)
        if not isinstance(entrada, Mapping):
            problemas[clave] = f"la clave '{clave_catalogo}' no está en el catálogo del adaptador"
            continue
        ids = entrada.get("ids") or []
        if isinstance(ids, str):
            ids = [ids]
        if len(ids) != 1 or not ids[0]:
            problemas[clave] = (
                f"la entrada '{clave_catalogo}' tiene {len(ids)} series; la tabla necesita una"
            )
            continue
        series.append(
            SerieETL(
                clave=clave,
                clave_catalogo=clave_catalogo,
                serie_id=str(ids[0]),
                descripcion_catalogo=str(entrada.get("description") or ""),
            )
        )
    if pedidas is not None:
        for clave in sorted(pedidas - set(SERIES_TABLAS)):
            problemas[clave] = "no es una tabla del ETL de series"
    return series, problemas


# ── API ──────────────────────────────────────────────────────────────────────


class _SerieFallida(Exception):
    """No se pudo completar la serie (API, base). Se reintenta en la próxima corrida."""

    def __init__(self, motivo: str, detalle: str = "") -> None:
        super().__init__(f"{motivo}: {detalle}" if detalle else motivo)
        self.motivo = motivo
        self.detalle = detalle


@dataclass(frozen=True)
class MetadatosAPI:
    """Lo que la API dice de una serie, con `metadata=full`."""

    serie_id: str
    total: int
    fin: date | None
    actualizada: bool | None
    frecuencia: str | None
    descripcion: str = ""
    unidades: str = ""
    titulo_dataset: str = ""
    fuente: str = ""


def _fecha(valor: Any) -> date | None:
    if valor is None or valor == "":
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError:
        return None


def _booleano(valor: Any) -> bool | None:
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, str):
        bajo = valor.strip().lower()
        if bajo in {"true", "t", "1", "si", "sí"}:
            return True
        if bajo in {"false", "f", "0", "no"}:
            return False
    return None


def _get(client: httpx.Client, params: dict[str, Any]) -> dict[str, Any]:
    """Un GET a la API con dos reintentos ante errores de red, 429 y 5xx."""
    ultimo: Exception | None = None
    for intento in range(len(_ESPERAS_REINTENTO_S) + 1):
        try:
            resp = client.get(API_URL, params=params)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp
                )
            resp.raise_for_status()
            cuerpo = resp.json()
            if not isinstance(cuerpo, dict):
                raise _SerieFallida("api", "respuesta que no es un objeto JSON")
            return cuerpo
        except httpx.HTTPStatusError as exc:
            ultimo = exc
            codigo = exc.response.status_code
            if codigo != 429 and codigo < 500:
                # Un 4xx no se arregla reintentando; el cuerpo dice qué rechazó.
                raise _SerieFallida("api", f"HTTP {codigo}: {exc.response.text[:200]}") from exc
        except (httpx.TransportError, ValueError) as exc:
            ultimo = exc
        if intento < len(_ESPERAS_REINTENTO_S):
            time.sleep(_ESPERAS_REINTENTO_S[intento])
    raise _SerieFallida("api", str(ultimo)[:300])


def _frecuencia(field: Mapping[str, Any], meta0: Mapping[str, Any]) -> str | None:
    for valor in (field.get("frequency"), meta0.get("frequency")):
        if valor and str(valor) in _FRECUENCIAS:
            return _FRECUENCIAS[str(valor)]
    return None


def consultar_metadatos(client: httpx.Client, serie_id: str) -> MetadatosAPI:
    """Una sola fila con `metadata=full`: alcanza para saber si hay algo nuevo."""
    cuerpo = _get(client, {"ids": serie_id, "limit": 1, "metadata": "full", "format": "json"})
    meta = cuerpo.get("meta") or []
    if len(meta) < 2 or not isinstance(meta[1], Mapping):
        raise _SerieFallida("api", f"la respuesta de {serie_id} no trae la metadata de la serie")
    meta0 = meta[0] if isinstance(meta[0], Mapping) else {}
    field = meta[1].get("field") or {}
    dataset = meta[1].get("dataset") or {}
    try:
        total = int(cuerpo.get("count"))  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise _SerieFallida("api", f"la respuesta de {serie_id} no trae 'count'") from exc
    return MetadatosAPI(
        serie_id=serie_id,
        total=total,
        fin=_fecha(field.get("time_index_end")),
        actualizada=_booleano(field.get("is_updated")),
        frecuencia=_frecuencia(field, meta0),
        descripcion=str(field.get("description") or field.get("title") or "").strip(),
        unidades=str(field.get("units") or "").strip(),
        titulo_dataset=str(dataset.get("title") or "").strip(),
        fuente=str(dataset.get("source") or "").strip(),
    )


def descargar_serie(client: httpx.Client, serie_id: str) -> list[list[Any]]:
    """La serie entera, ascendente, paginando con `start` hasta cubrir `count`.

    Ascendente y no `sort=desc`: es el orden en que se guarda, y con
    transformaciones la API corre la ventana. Si `count` cambia entre páginas
    (la API se actualizó en medio de la descarga) o lo bajado no coincide, se
    falla: mejor la versión de ayer completa que una mezcla.
    """
    filas: list[list[Any]] = []
    total: int | None = None
    inicio = 0
    while True:
        cuerpo = _get(
            client,
            {"ids": serie_id, "limit": PAGINA, "start": inicio, "format": "json"},
        )
        pagina = cuerpo.get("data") or []
        try:
            cuenta = int(cuerpo.get("count"))  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise _SerieFallida("api", f"página sin 'count' en {serie_id}") from exc
        if total is None:
            total = cuenta
        elif cuenta != total:
            raise _SerieFallida(
                "api_cambio", f"count pasó de {total} a {cuenta} durante la descarga"
            )
        filas.extend(pagina)
        inicio += PAGINA
        if not pagina or len(filas) >= total or inicio >= total:
            break

    if total is None or len(filas) != total:
        raise _SerieFallida(
            "descarga_incompleta", f"bajé {len(filas)} filas y la API anuncia {total}"
        )
    fechas = [str(f[0]) for f in filas if f]
    if len(set(fechas)) != len(fechas):
        raise _SerieFallida("descarga_incompleta", "fechas repetidas entre páginas")
    if fechas != sorted(fechas):
        raise _SerieFallida("descarga_incompleta", "las páginas no vinieron en orden")
    return filas


# ── estado de la tabla ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class EstadoTabla:
    existe: bool
    filas: int = 0
    max_fecha: date | None = None
    columnas: tuple[str, ...] = ()
    dataset_id: str | None = None
    titulo: str | None = None
    descripcion: str | None = None
    columnas_dataset: tuple[str, ...] = ()
    serie_id_previa: str | None = None
    dueno_registro: str | None = None
    # Lo que acompaña a la tabla, para ver si una corrida cortada lo dejó atrás.
    dataset_filas: int | None = None
    dataset_cacheado: bool | None = None
    catalogo_estado: str | None = None
    catalogo_filas: int | None = None
    registro_filas: int | None = None


def _serie_id_de_url(url: str | None) -> str | None:
    if not url:
        return None
    ids = parse_qs(urlparse(url).query).get("ids")
    return ids[0] if ids else None


def _columnas_de_json(valor: Any) -> tuple[str, ...]:
    if not valor:
        return ()
    try:
        lista = json.loads(valor) if isinstance(valor, str) else list(valor)
    except (TypeError, ValueError):
        return ()
    return tuple(str(c) for c in lista)


def _entero(valor: Any) -> int | None:
    return None if valor is None else int(valor)


def estado_tabla(engine: Engine, serie: SerieETL) -> EstadoTabla:
    """Lo que hay hoy: la tabla (filas, última fecha, columnas), el dataset, el catálogo
    (`raw.cached_datasets`) y el registro."""
    with engine.connect() as conn:
        existe = bool(
            conn.execute(
                text("SELECT to_regclass(:q) IS NOT NULL"), {"q": f'raw."{serie.tabla}"'}
            ).scalar()
        )
        filas = 0
        max_fecha: date | None = None
        columnas: tuple[str, ...] = ()
        if existe:
            fila = conn.execute(
                text(f'SELECT count(*) AS n, max(fecha) AS fin FROM raw."{serie.tabla}"')  # noqa: S608
            ).one()
            filas = int(fila.n)
            max_fecha = _fecha(fila.fin)
            columnas = tuple(
                str(r[0])
                for r in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'raw' AND table_name = :tn "
                        "ORDER BY ordinal_position"
                    ),
                    {"tn": serie.tabla},
                )
            )
        ds = conn.execute(
            text(
                "SELECT CAST(id AS text) AS id, title, description, url, columns, "
                "row_count, is_cached "
                "FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": serie.source_id, "portal": PORTAL},
        ).first()
        registro = conn.execute(
            text(
                "SELECT resource_identity, row_count FROM public.raw_table_versions "
                "WHERE schema_name = 'raw' AND table_name = :tn LIMIT 1"
            ),
            {"tn": serie.tabla},
        ).first()
        catalogo = conn.execute(
            text("SELECT status, row_count FROM raw.cached_datasets WHERE table_name = :tn"),
            {"tn": serie.tabla},
        ).first()
        conn.rollback()
    return EstadoTabla(
        existe=existe,
        filas=filas,
        max_fecha=max_fecha,
        columnas=columnas,
        dataset_id=ds.id if ds else None,
        titulo=ds.title if ds else None,
        descripcion=ds.description if ds else None,
        columnas_dataset=_columnas_de_json(ds.columns) if ds else (),
        serie_id_previa=_serie_id_de_url(ds.url) if ds else None,
        dueno_registro=str(registro.resource_identity)
        if registro and registro.resource_identity
        else None,
        dataset_filas=_entero(ds.row_count) if ds else None,
        dataset_cacheado=bool(ds.is_cached) if ds and ds.is_cached is not None else None,
        catalogo_estado=str(catalogo.status) if catalogo and catalogo.status else None,
        catalogo_filas=_entero(catalogo.row_count) if catalogo else None,
        registro_filas=_entero(registro.row_count) if registro else None,
    )


# ── decisiones (puras) ───────────────────────────────────────────────────────


def nombre_columna(descripcion: str, respaldo: str) -> str:
    """La columna de valores se llama como la serie en la API, como siempre."""
    nombre = (descripcion or respaldo).strip() or respaldo
    crudo = nombre.encode("utf-8")
    if len(crudo) <= _MAX_BYTES_IDENTIFICADOR:
        return nombre
    return crudo[:_MAX_BYTES_IDENTIFICADOR].decode("utf-8", errors="ignore").rstrip()


def motivo_para_escribir(
    meta: MetadatosAPI,
    estado: EstadoTabla,
    columnas: list[str],
    *,
    forzar: bool = False,
) -> str | None:
    """Por qué hay que reescribir la tabla, o `None` si ya está al día."""
    if forzar:
        return "forzada"
    if not estado.existe:
        return "tabla_inexistente"
    if estado.serie_id_previa and estado.serie_id_previa != meta.serie_id:
        return "cambio_de_serie"
    if estado.max_fecha is None:
        return "tabla_vacia"
    if meta.fin is not None and meta.fin > estado.max_fecha:
        return "fuente_mas_nueva"
    if meta.total != estado.filas:
        return "cantidad_distinta"
    if tuple(columnas) != estado.columnas:
        return "columnas_distintas"
    return None


def motivo_para_rechazar(
    *,
    filas_nuevas: int,
    fin_nuevo: date | None,
    estado: EstadoTabla,
    misma_serie: bool,
    cambio_aprobado: bool = False,
    permitir_menos_filas: bool = False,
    serie_id: str = "",
) -> str | None:
    """El guardián: lo que no se reemplaza sin que una persona lo pida.

    Otra serie en la misma tabla sólo con el cambio aprobado
    (`CAMBIOS_DE_SERIE_APROBADOS`); con él, comparar filas y fechas contra la
    serie anterior no dice nada. `permitir_menos_filas` no lo reemplaza: achicar
    la misma serie no es cambiarla.
    """
    if filas_nuevas <= 0:
        return "sin_filas"
    if not estado.existe:
        return None
    if not misma_serie:
        if cambio_aprobado:
            return None
        return (
            f"cambio de serie sin aprobar ({estado.serie_id_previa} → {serie_id or 'otro id'}): "
            "agregarlo a CAMBIOS_DE_SERIE_APROBADOS si es intencional"
        )
    if permitir_menos_filas:
        return None
    if estado.max_fecha and (fin_nuevo is None or fin_nuevo < estado.max_fecha):
        return f"la fecha máxima retrocede ({estado.max_fecha} → {fin_nuevo})"
    if filas_nuevas < estado.filas:
        return f"menos filas que las que hay ({estado.filas} → {filas_nuevas})"
    return None


def motivo_para_reconciliar(
    serie: SerieETL, meta: MetadatosAPI, estado: EstadoTabla, columnas: list[str]
) -> str | None:
    """Con la tabla al día, qué de lo que la acompaña quedó atrás, o `None`.

    El reemplazo se confirma solo; el dataset, el catálogo y el registro van
    después, cada uno en su transacción. Un corte en el medio (límite de
    tiempo, redespacho por deploy, la conexión) dejaba la tabla nueva con el
    resto viejo, y la corrida siguiente, que miraba sólo la tabla, decía "al
    día" y latía sana para siempre.
    """
    if estado.dataset_id is None:
        return "no hay fila en datasets"
    if (
        estado.titulo != _titulo(serie, meta)
        or estado.descripcion != _descripcion(serie, meta)
        or estado.columnas_dataset != tuple(columnas)
        or estado.dataset_filas != estado.filas
        or estado.dataset_cacheado is not True
    ):
        return "datasets no describe la tabla (título, descripción, columnas, filas o is_cached)"
    if estado.catalogo_estado != "ready" or estado.catalogo_filas != estado.filas:
        return (
            f"raw.cached_datasets en {estado.catalogo_estado or 'ninguna fila'} con "
            f"{estado.catalogo_filas} filas; la tabla tiene {estado.filas}"
        )
    if estado.dueno_registro is None:
        return "la tabla no está en raw_table_versions"
    if estado.registro_filas != estado.filas:
        return (
            f"raw_table_versions con {estado.registro_filas} filas; la tabla tiene {estado.filas}"
        )
    return None


def clasificar_frescura(meta: MetadatosAPI, estado: EstadoTabla, hoy: date) -> list[str]:
    """`series_cache_stale` y/o `series_source_stale` para una serie.

    - **cache**: la API tiene algo que la tabla no (o la tabla falta, o tiene
      otra serie). Es un bug nuestro: el ETL no la refrescó.
    - **fuente**: la API misma está atrasada. Manda `is_updated` cuando la API
      lo da; si no, un margen por frecuencia. No es un bug de la caché, pero
      el dato que servimos es viejo y alguien tiene que decidir qué hacer.
    """
    hallazgos: list[str] = []
    if (
        not estado.existe
        or estado.max_fecha is None
        or (estado.serie_id_previa and estado.serie_id_previa != meta.serie_id)
        or (meta.fin is not None and meta.fin > estado.max_fecha)
    ):
        hallazgos.append("series_cache_stale")
    if meta.actualizada is False:
        hallazgos.append("series_source_stale")
    elif meta.actualizada is None and meta.fin is not None and meta.frecuencia:
        margen = _MARGEN_FUENTE_DIAS.get(meta.frecuencia)
        if margen is not None and (hoy - meta.fin).days > margen:
            hallazgos.append("series_source_stale")
    return hallazgos


# ── escritura ────────────────────────────────────────────────────────────────


def armar_dataframe(
    filas: list[list[Any]], columna_valor: str, *, serie_id: str = ""
) -> pd.DataFrame:
    df = pd.DataFrame([f[:2] for f in filas], columns=["fecha", columna_valor])
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    if df["fecha"].isna().any():
        raise _SerieFallida("datos", "la API devolvió fechas que no se pueden leer")
    df[columna_valor] = pd.to_numeric(df[columna_valor], errors="coerce")
    if serie_id in series_tiempo_adapter.FRACTION_PERCENT_IDS:
        # Desempleo y compañía: la API dice «Porcentaje» y manda la fracción
        # (0,079 = 7,9 %). La tabla guardaba 0,079 bajo la columna "… En
        # porcentaje." y el conector en vivo da 7,9: misma regla y mismo
        # redondeo que el conector, para que los dos caminos den lo mismo.
        df[columna_valor] = df[columna_valor].map(lambda v: v if pd.isna(v) else round(v * 100, 2))
    return df


def _titulo(serie: SerieETL, meta: MetadatosAPI) -> str:
    nombre = meta.descripcion or serie.descripcion_catalogo.split(". ")[0] or serie.clave
    return f"Series de Tiempo — {nombre}"[:1000]


def _descripcion(serie: SerieETL, meta: MetadatosAPI) -> str:
    partes = [f"Serie temporal: {meta.descripcion or serie.clave}."]
    if meta.titulo_dataset:
        partes.append(meta.titulo_dataset.rstrip(".") + ".")
    if meta.fuente:
        partes.append(f"Fuente: {meta.fuente}.")
    if meta.unidades:
        partes.append(f"Unidad: {meta.unidades}.")
    if meta.frecuencia:
        partes.append(f"Frecuencia: {meta.frecuencia}.")
    partes.append(
        f"Serie {serie.serie_id} de la API de Series de Tiempo (APIs de Datos Argentina)."
    )
    return " ".join(partes)


def _asegurar_dataset(
    engine: Engine, serie: SerieETL, meta: MetadatosAPI, columnas: list[str], filas: int
) -> str:
    """El id del dataset, creándolo si no existe. No toca uno existente:
    eso se hace recién después del reemplazo (`_actualizar_dataset`)."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'json', :cols, :tags,
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO NOTHING
                """
            ),
            {
                "sid": serie.source_id,
                "title": _titulo(serie, meta),
                "desc": _descripcion(serie, meta),
                "org": ORGANIZACION,
                "portal": PORTAL,
                "url": serie.url,
                "cols": json.dumps(columnas),
                "tags": f"series de tiempo,{serie.clave.replace('_', ' ')},economia,indicadores",
                "now": datetime.now(UTC),
                "rows": filas,
            },
        )
        fila = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": serie.source_id, "portal": PORTAL},
        ).fetchone()
    if not fila:
        raise _SerieFallida("dataset", "no se pudo crear la fila de datasets")
    return str(fila[0])


def veredicto_ws0(
    engine: Engine, *, dataset_id: str, serie: SerieETL, columnas: list[str], filas: int
) -> str | None:
    """La misma puerta WS0 que aplica `_finalize_cached_dataset`, pero ANTES de escribir.

    WS0 mira columnas y cantidad de filas, no el contenido de la tabla, así que
    puede decidir sin tocar nada. `_finalize_cached_dataset` la vuelve a correr
    después del reemplazo (mismas entradas, mismo veredicto) y es la que deja
    el estado `ready`. Devuelve el motivo del rechazo, o `None`.
    """
    from app.infrastructure.celery.tasks import collector_tasks as ct

    finding = ct._ws0_validate_post_parse(
        engine,
        dataset_id=dataset_id,
        portal=PORTAL,
        source_id=serie.source_id,
        download_url=serie.url,
        declared_format="json",
        table_name=serie.tabla,
        materialized_columns=columnas,
        materialized_row_count=filas,
        declared_size_bytes=None,
        columns_json=json.dumps(columnas),
    )
    layout = (
        ct._LAYOUT_WIDE if len(columnas) > ct._WIDE_LAYOUT_COLUMN_THRESHOLD else ct._LAYOUT_SIMPLE
    )
    resultado = ct._materialization_outcome_for_ready(
        portal=PORTAL,
        declared_format="json",
        table_name=serie.tabla,
        row_count=filas,
        columns=columnas,
        layout_profile=layout,
        header_quality=ct._header_quality_label(columnas),
        ws0_finding=finding,
    )
    if resultado.cached_status == "ready":
        return None
    return resultado.error_message or resultado.result_kind


_SQL_PERMISOS = text(
    """
    SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS rol,
           a.privilege_type AS privilegio, a.is_grantable AS con_grant
    FROM pg_class c
    CROSS JOIN LATERAL aclexplode(c.relacl) AS a
    LEFT JOIN pg_roles r ON r.oid = a.grantee
    WHERE c.oid = to_regclass(:q)
      AND a.grantee <> c.relowner
      AND (a.grantee = 0 OR r.oid IS NOT NULL)
    """
)

_SQL_DEPENDIENTES = text(
    """
    SELECT DISTINCT v.oid::regclass::text
    FROM pg_depend d
    JOIN pg_rewrite w ON w.oid = d.objid
    JOIN pg_class v ON v.oid = w.ev_class
    WHERE d.classid = 'pg_rewrite'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND d.refobjid = to_regclass(:q)
      AND v.oid <> d.refobjid
    """
)


def _igualar_permisos(conn: Any, *, desde: str, hacia: str) -> None:
    """`raw.<hacia>` queda con los permisos de `raw.<desde>` (los del dueño no cuentan).

    La tabla nueva nace con los privilegios por defecto del esquema, que no
    tienen por qué ser los de la viva: le faltaría un GRANT hecho a mano, o
    tendría uno que a la viva le sacaron. En staging y prod hoy son iguales
    (`openarg_sandbox_ro=r`), así que normalmente no se ejecuta nada.
    """

    def permisos(tabla: str) -> set[tuple[str, str, bool]]:
        filas = conn.execute(_SQL_PERMISOS, {"q": f'raw."{tabla}"'}).all()
        return {(str(f.rol), str(f.privilegio), bool(f.con_grant)) for f in filas}

    viejos, nuevos = permisos(desde), permisos(hacia)
    a_limpiar = sorted({rol for rol, _, _ in nuevos - viejos})
    for rol in a_limpiar:
        conn.execute(text(f'REVOKE ALL ON raw."{hacia}" FROM {rol}'))
    for rol, privilegio, con_grant in sorted(viejos):
        if (rol, privilegio, con_grant) in nuevos and rol not in a_limpiar:
            continue
        opcion = " WITH GRANT OPTION" if con_grant else ""
        conn.execute(text(f'GRANT {privilegio} ON raw."{hacia}" TO {rol}{opcion}'))


def escribir_atomico(engine: Engine, tabla: str, df: pd.DataFrame) -> None:
    """Carga en `raw.<tabla>__nueva` y la pone en lugar de la vieja, en una transacción.

    La vieja no se borra: pasa a `raw.<tabla>__previa` (borrando la previa
    anterior en la misma transacción), con sus tipos, sus filas y sus
    permisos, así que volver atrás es un RENAME (ver docs/runbook.md). Antes
    era un DROP y el único retorno era un dump JSON que no se podía
    restaurar. La nueva toma los permisos de la viva antes del cambio.

    Si algo falla, el rollback deja la tabla de antes intacta y no queda una
    `__nueva` huérfana. Una vista sobre la tabla frena el reemplazo: con el
    RENAME se iría con la vieja a `__previa` y serviría datos viejos sin
    avisar (con el DROP de antes, fallaba). El lock exclusivo sobre la tabla
    viva dura sólo los RENAME del final; `lock_timeout` evita quedarse
    esperando detrás de una consulta larga mientras se encolan las demás.
    """
    nueva = f"{tabla}__nueva"
    previa = f"{tabla}__previa"
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '15s'"))
        df.to_sql(nueva, conn, schema="raw", if_exists="replace", index=False)
        existe = conn.execute(
            text("SELECT to_regclass(:q) IS NOT NULL"), {"q": f'raw."{tabla}"'}
        ).scalar()
        if existe:
            vistas = list(conn.execute(_SQL_DEPENDIENTES, {"q": f'raw."{tabla}"'}).scalars().all())
            if vistas:
                raise _SerieFallida(
                    "reemplazo", f"dependen de raw.{tabla}: {', '.join(map(str, vistas))}"
                )
            _igualar_permisos(conn, desde=tabla, hacia=nueva)
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{previa}"'))
            conn.execute(text(f'ALTER TABLE raw."{tabla}" RENAME TO "{previa}"'))
        conn.execute(text(f'ALTER TABLE raw."{nueva}" RENAME TO "{tabla}"'))


def _actualizar_dataset(
    engine: Engine,
    *,
    dataset_id: str,
    serie: SerieETL,
    meta: MetadatosAPI,
    columnas: list[str],
    filas: int,
) -> None:
    ahora = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE datasets SET
                    title = :title, description = :desc, organization = :org, url = :url,
                    columns = :cols, row_count = :rows, last_updated_at = :now, updated_at = :now
                WHERE id = CAST(:did AS uuid)
                """
            ),
            {
                "did": dataset_id,
                "title": _titulo(serie, meta),
                "desc": _descripcion(serie, meta),
                "org": ORGANIZACION,
                "url": serie.url,
                "cols": json.dumps(columnas),
                "rows": filas,
                "now": ahora,
            },
        )


def _completar_metadatos(
    engine: Engine,
    *,
    serie: SerieETL,
    meta: MetadatosAPI,
    dataset_id: str,
    columnas: list[str],
    filas: int,
    reembeber: bool,
) -> None:
    """Lo que acompaña a la tabla: dataset, catálogo (`ready`), registro y embedding.

    Lo usan la escritura, después del reemplazo, y la reconciliación, que lo
    repite sin tocar la tabla cuando una corrida anterior se cortó en el medio.
    Son upserts idempotentes. Una excepción acá deja la tabla nueva con la
    metadata vieja: sale como motivo `metadatos`, que alerta (como `error` no
    alertaba), y la corrida siguiente lo repara.
    """
    try:
        _actualizar_dataset(
            engine, dataset_id=dataset_id, serie=serie, meta=meta, columnas=columnas, filas=filas
        )
        final = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=PORTAL,
            source_id=serie.source_id,
            table_name=serie.tabla,
            row_count=filas,
            columns=columnas,
            declared_format="json",
            download_url=serie.url,
        )
        if not final.get("ok"):
            raise _SerieFallida("finalizacion", str(final.get("error") or final.get("status")))
        if not register_via_b_table(
            engine,
            resource_identity=serie.identidad,
            table_name=serie.tabla,
            schema_name="raw",
            row_count=filas,
        ):
            raise _SerieFallida("registro", f"no se pudo registrar {serie.identidad}")
    except (SoftTimeLimitExceeded, _SerieFallida):
        raise
    except Exception as exc:
        logger.exception("Series %s: falló la metadata de la tabla", serie.clave)
        raise _SerieFallida("metadatos", str(exc)[:300]) from exc

    if reembeber:
        # Re-embeber sólo si cambia lo que se embebe: con el beat diario, hacerlo
        # en cada escritura sumaría churn al índice sin cambiar nada buscable.
        try:
            from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

            index_dataset_embedding.delay(dataset_id)
        except Exception:
            logger.warning("Series %s: no se pudo encolar el embedding", serie.clave, exc_info=True)


# ── una serie ────────────────────────────────────────────────────────────────


@dataclass
class ResultadoSerie:
    estado: str  # escrita | reconciliada | al_dia | rechazada | fallida | simulada
    motivo: str = ""
    detalle: str = ""
    filas: int | None = None
    fin_api: str | None = None
    fin_tabla: str | None = None
    consulto_api: bool = False

    def como_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v not in (None, "")}


def procesar_serie(
    engine: Engine,
    client: httpx.Client,
    serie: SerieETL,
    *,
    forzar: bool = False,
    permitir_menos_filas: bool = False,
    dry_run: bool = False,
) -> ResultadoSerie:
    """Decide, baja, valida y reemplaza una serie.

    Si la API no contesta la metadata, `_SerieFallida` sube al llamador (no se
    miró nada). De ahí en adelante toda falla vuelve como resultado `fallida`
    con `consulto_api=True`.
    """
    meta = consultar_metadatos(client, serie.serie_id)
    try:
        return _procesar_con_metadatos(
            engine,
            client,
            serie,
            meta,
            forzar=forzar,
            permitir_menos_filas=permitir_menos_filas,
            dry_run=dry_run,
        )
    except SoftTimeLimitExceeded:
        raise
    except _SerieFallida as exc:
        return ResultadoSerie(
            estado="fallida",
            motivo=exc.motivo,
            detalle=exc.detalle,
            fin_api=meta.fin.isoformat() if meta.fin else None,
            consulto_api=True,
        )
    except Exception as exc:
        logger.exception("Series %s: falló", serie.clave)
        return ResultadoSerie(
            estado="fallida",
            motivo="error",
            detalle=str(exc)[:300],
            fin_api=meta.fin.isoformat() if meta.fin else None,
            consulto_api=True,
        )


def _procesar_con_metadatos(
    engine: Engine,
    client: httpx.Client,
    serie: SerieETL,
    meta: MetadatosAPI,
    *,
    forzar: bool,
    permitir_menos_filas: bool,
    dry_run: bool,
) -> ResultadoSerie:
    estado = estado_tabla(engine, serie)
    res = ResultadoSerie(
        estado="al_dia",
        fin_api=meta.fin.isoformat() if meta.fin else None,
        fin_tabla=estado.max_fecha.isoformat() if estado.max_fecha else None,
        consulto_api=True,
    )

    if estado.dueno_registro and estado.dueno_registro != serie.identidad:
        # Escribir igual dejaría la tabla sin registro (el INSERT choca con el
        # UNIQUE) y sin latido. Una persona tiene que decidir cuál identidad vale.
        res.estado, res.motivo = "rechazada", "registro_ajeno"
        res.detalle = f"{serie.tabla} está registrada como {estado.dueno_registro}"
        return res

    columna_valor = nombre_columna(meta.descripcion, serie.clave)
    columnas = ["fecha", columna_valor]
    motivo = motivo_para_escribir(meta, estado, columnas, forzar=forzar)
    if motivo is None:
        res.filas = estado.filas
        pendiente = motivo_para_reconciliar(serie, meta, estado, columnas)
        if pendiente is None:
            if not dry_run:
                # Mirado contra la API y al día, con todo lo que la acompaña en
                # orden: eso también es "llegó".
                from app.application.quality.heartbeat import record_ingest

                record_ingest(engine, serie.identidad)
            return res
        res.motivo, res.detalle = "reconciliar", pendiente
        if dry_run:
            res.estado = "simulada"
            res.detalle = f"repararía sin reescribir la tabla: {pendiente}"
            return res
        # La tabla está bien y lo de alrededor no: una corrida anterior se
        # cortó después del reemplazo. Se rehacen esos pasos (el registro es
        # el que late) y se re-embebe, porque no se sabe si la corrida cortada
        # llegó a encolar el embedding.
        dataset_id = estado.dataset_id or _asegurar_dataset(
            engine, serie, meta, columnas, estado.filas
        )
        _completar_metadatos(
            engine,
            serie=serie,
            meta=meta,
            dataset_id=dataset_id,
            columnas=columnas,
            filas=estado.filas,
            reembeber=True,
        )
        res.estado = "reconciliada"
        return res

    df = armar_dataframe(
        descargar_serie(client, serie.serie_id), columna_valor, serie_id=serie.serie_id
    )
    fin_nuevo = _fecha(df["fecha"].max())
    res.filas, res.motivo = len(df), motivo
    misma_serie = estado.serie_id_previa in (None, serie.serie_id)
    rechazo = motivo_para_rechazar(
        filas_nuevas=len(df),
        fin_nuevo=fin_nuevo,
        estado=estado,
        misma_serie=misma_serie,
        cambio_aprobado=not misma_serie and cambio_de_serie_aprobado(serie, estado.serie_id_previa),
        permitir_menos_filas=permitir_menos_filas,
        serie_id=serie.serie_id,
    )
    if rechazo:
        res.estado, res.detalle = "rechazada", rechazo
        return res
    if dry_run:
        res.estado = "simulada"
        res.detalle = f"escribiría {len(df)} filas hasta {fin_nuevo}"
        return res

    dataset_id = estado.dataset_id or _asegurar_dataset(engine, serie, meta, columnas, len(df))
    ws0 = veredicto_ws0(
        engine, dataset_id=dataset_id, serie=serie, columnas=columnas, filas=len(df)
    )
    if ws0:
        res.estado, res.detalle = "rechazada", f"WS0: {ws0}"
        return res

    escribir_atomico(engine, serie.tabla, df)
    cambio_lo_buscable = (
        estado.dataset_id is None
        or estado.titulo != _titulo(serie, meta)
        or estado.descripcion != _descripcion(serie, meta)
        or estado.columnas_dataset != tuple(columnas)
    )
    _completar_metadatos(
        engine,
        serie=serie,
        meta=meta,
        dataset_id=dataset_id,
        columnas=columnas,
        filas=len(df),
        reembeber=cambio_lo_buscable,
    )

    res.estado = "escrita"
    res.fin_tabla = fin_nuevo.isoformat() if fin_nuevo else None
    return res


def _alertas_de_ingesta(
    series: list[SerieETL], resultados: Mapping[str, ResultadoSerie]
) -> list[Any]:
    from app.application.quality.alerting import Alert

    por_clave = {s.clave: s for s in series}
    alertas = []
    for clave, res in resultados.items():
        serie = por_clave.get(clave)
        if serie is None:
            continue
        if res.estado == "rechazada":
            # Misma clase y clave que usa `check_series_freshness` para la tabla
            # atrasada: es el mismo problema visto antes y con el motivo, y la
            # deduplicación de `alert_log` evita el segundo mensaje.
            alertas.append(
                Alert(
                    kind="series_cache_stale",
                    key=f"{serie.identidad}@{res.fin_api}",
                    title=f"{serie.tabla}: el ETL se negó a escribir",
                    detail=f"{res.motivo or 'rechazo'} — {res.detalle}"[:300],
                )
            )
        elif res.estado == "fallida" and res.motivo in {
            "registro",
            "finalizacion",
            "dataset",
            "metadatos",
            "reemplazo",
        }:
            # Las de red se reintentan solas mañana; éstas no (o dejaron la
            # tabla nueva con la metadata vieja hasta la próxima corrida).
            alertas.append(
                Alert(
                    kind="series_ingest_failed",
                    key=f"{serie.identidad}:{res.motivo}",
                    title=f"{serie.tabla}: falló {res.motivo}",
                    detail=res.detalle[:300],
                )
            )
    return alertas


# ── tareas ───────────────────────────────────────────────────────────────────


@celery_app.task(
    name="openarg.ingest_series_tiempo",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def ingest_series_tiempo(
    self,
    *,
    forzar: bool = False,
    permitir_menos_filas: bool = False,
    claves: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Refresca las tablas `raw.cache_series_*` cuyas series tienen datos nuevos.

    - `forzar`: reescribe aunque la tabla esté al día (pasa igual por el guardián).
    - `permitir_menos_filas`: deja reemplazar por menos filas o por una fecha
      máxima anterior. Sólo a mano, sabiendo por qué. Ninguno de los dos
      habilita otra serie en la tabla: eso es `CAMBIOS_DE_SERIE_APROBADOS`.
    - `claves`: limita a esas tablas (p. ej. ``["tipo_cambio"]``).
    - `dry_run`: consulta la API y la base y dice qué haría, sin escribir nada
      (ni findings, ni latidos, ni alertas).
    """
    engine = get_sync_engine()
    series, problemas = series_del_catalogo(claves=claves)
    resultados: dict[str, ResultadoSerie] = {}
    for clave, problema in problemas.items():
        logger.error("Series %s: %s", clave, problema)
        resultados[clave] = ResultadoSerie(
            estado="fallida", motivo="sin_catalogo", detalle=problema
        )

    try:
        with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=True) as client:
            for serie in series:
                try:
                    res = procesar_serie(
                        engine,
                        client,
                        serie,
                        forzar=forzar,
                        permitir_menos_filas=permitir_menos_filas,
                        dry_run=dry_run,
                    )
                except SoftTimeLimitExceeded:
                    raise
                except _SerieFallida as exc:
                    # Sólo llega acá si la API no contestó la metadata.
                    res = ResultadoSerie(estado="fallida", motivo=exc.motivo, detalle=exc.detalle)
                except Exception as exc:
                    logger.exception("Series %s: falló", serie.clave)
                    res = ResultadoSerie(estado="fallida", motivo="error", detalle=str(exc)[:300])
                resultados[serie.clave] = res
                log = logger.warning if res.estado in {"rechazada", "fallida"} else logger.info
                log("Series %s: %s", serie.clave, res.como_dict())

        resumen = _resumen(resultados)
        cuentas = {k: v for k, v in resumen.items() if k != "series"}
        logger.info("Series de Tiempo: %s", cuentas)

        if not dry_run:
            alertas = _alertas_de_ingesta(series, resultados)
            if alertas:
                try:
                    from app.application.quality.alerting import notify

                    resumen["alertas"] = notify(
                        engine, alertas, heading="OpenArg · ETL de series de tiempo"
                    )
                except Exception:
                    logger.warning("Series de Tiempo: no se pudo alertar", exc_info=True)

        if resumen["resueltas"] == 0:
            # Ni una serie verificada, escrita o rechazada con motivo: no hubo
            # corrida. Terminar en "success" es lo que hizo pasar por sana
            # durante meses a una tarea que no miraba nada. Que falle, que
            # reintente y, si sigue, que el latido de la tarea se atrase.
            raise RuntimeError(
                "Series de Tiempo: ninguna serie se pudo verificar contra la API; "
                f"no cuenta como corrida ({cuentas})"
            )
        return resumen

    except SoftTimeLimitExceeded:
        logger.error("Series de Tiempo ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("Series de Tiempo ingestion failed")
        raise self.retry(exc=exc, countdown=120)


def _resumen(resultados: Mapping[str, ResultadoSerie]) -> dict[str, Any]:
    cuenta = {
        "escritas": 0,
        "reconciliadas": 0,
        "al_dia": 0,
        "rechazadas": 0,
        "fallidas": 0,
        "simuladas": 0,
    }
    nombres = {
        "escrita": "escritas",
        "reconciliada": "reconciliadas",
        "al_dia": "al_dia",
        "rechazada": "rechazadas",
        "fallida": "fallidas",
        "simulada": "simuladas",
    }
    for res in resultados.values():
        cuenta[nombres.get(res.estado, "fallidas")] += 1
    return {
        **cuenta,
        "resueltas": cuenta["escritas"]
        + cuenta["reconciliadas"]
        + cuenta["al_dia"]
        + cuenta["rechazadas"]
        + cuenta["simuladas"],
        "consultadas_api": sum(1 for r in resultados.values() if r.consulto_api),
        "series": {k: r.como_dict() for k, r in resultados.items()},
    }


@celery_app.task(
    name="openarg.check_series_freshness",
    bind=True,
    soft_time_limit=180,
    time_limit=240,
)
def check_series_freshness(self, *, dry_run: bool = False) -> dict[str, Any]:
    """La alarma de frescura de las series: ¿la tabla está atrás de la API, o la API atrás del mundo?

    Corre 45 minutos después de la ingesta diaria: si la tabla sigue atrás de la
    API, la ingesta ya tuvo su oportunidad y no la aprovechó. `dry_run` no
    alerta ni escribe; sólo devuelve el informe.
    """
    engine = get_sync_engine()
    series, problemas = series_del_catalogo()
    hoy = datetime.now(_ZONA).date()
    informe: dict[str, Any] = {
        "series_cache_stale": [],
        "series_source_stale": [],
        "al_dia": [],
        "sin_api": [],
        "sin_catalogo": sorted(problemas),
    }
    alertas = []
    from app.application.quality.alerting import Alert

    with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=True) as client:
        for serie in series:
            try:
                meta = consultar_metadatos(client, serie.serie_id)
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                informe["sin_api"].append(serie.clave)
                logger.warning(
                    "frescura de series: %s sin respuesta de la API: %s", serie.clave, exc
                )
                continue
            estado = estado_tabla(engine, serie)
            hallazgos = clasificar_frescura(meta, estado, hoy)
            fila = {
                "serie": serie.clave,
                "fin_api": meta.fin.isoformat() if meta.fin else None,
                "fin_tabla": estado.max_fecha.isoformat() if estado.max_fecha else None,
                "frecuencia": meta.frecuencia,
                "actualizada_en_fuente": meta.actualizada,
            }
            if not hallazgos:
                informe["al_dia"].append(serie.clave)
                continue
            for kind in hallazgos:
                informe[kind].append(fila)
                if kind == "series_cache_stale":
                    logger.error("frescura de series: tabla atrasada (bug nuestro): %s", fila)
                    otra = (
                        estado.serie_id_previa
                        if estado.serie_id_previa and estado.serie_id_previa != meta.serie_id
                        else None
                    )
                    detalle = (
                        f"la tabla tiene otra serie ({otra}, el catálogo dice {meta.serie_id})"
                        if otra
                        else f"la API llega a {fila['fin_api']} y la tabla a "
                        f"{fila['fin_tabla'] or 'nada (no existe)'}: el ETL no la refrescó"
                    )
                    alertas.append(
                        Alert(
                            kind="series_cache_stale",
                            key=f"{serie.identidad}@{fila['fin_api']}",
                            title=f"{serie.tabla} está atrasada respecto de la API",
                            detail=detalle,
                        )
                    )
                else:
                    logger.warning("frescura de series: la fuente está atrasada: %s", fila)
                    marca = (
                        "is_updated=False"
                        if meta.actualizada is False
                        else f"más de {_MARGEN_FUENTE_DIAS.get(meta.frecuencia or '', '?')} días"
                    )
                    alertas.append(
                        Alert(
                            kind="series_source_stale",
                            # Con la fecha de la fuente: si avanza y vuelve a
                            # trabarse, es un episodio nuevo y se avisa de nuevo.
                            key=f"{serie.identidad}@{fila['fin_api']}",
                            title=f"{serie.clave}: la fuente está atrasada",
                            detail=(
                                f"la API de Series de Tiempo llega a {fila['fin_api']} "
                                f"(serie {meta.frecuencia or 'de frecuencia desconocida'}, {marca}). "
                                "No es un bug de la caché: servimos lo último que hay."
                            ),
                        )
                    )

    if series and len(informe["sin_api"]) == len(series):
        raise RuntimeError("frescura de series: la API no respondió para ninguna serie")

    resumen = {k: (len(v) if isinstance(v, list) else v) for k, v in informe.items()}
    logger.info("frescura de series: %s", resumen)
    if alertas and not dry_run:
        try:
            from app.application.quality.alerting import notify

            informe["alertas"] = notify(
                engine, alertas, heading="OpenArg · frescura de las series de tiempo"
            )
        except Exception:
            logger.warning("frescura de series: no se pudo alertar", exc_info=True)
    return informe
