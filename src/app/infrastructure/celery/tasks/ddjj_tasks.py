"""DDJJ — carga de las declaraciones juradas patrimoniales a `raw.cache_ddjj_*`.

Reemplaza dos caminos que estaban mal:

- **El conector leía un JSON fijo.** `infrastructure/data/ddjj_dataset.json` tiene
  195 diputados de 2024, cargados a mano desde PDFs en febrero y nunca
  actualizados.
- **El colector bajaba la fuente rota.** Las DDJJ de la Oficina Anticorrupción
  estaban en ~60 tablas `raw.*` (portal `justicia` y su copia en
  `datos_gob_ar`), con años cortados en 500.000 filas, y dos marts que leían
  la mitad cada uno.

Acá se cargan tres tablas propias:

- `cache_ddjj_declaraciones`: una fila por declaración, de todas las fuentes
  (columna `fuente`).
- `cache_ddjj_bienes` y `cache_ddjj_deudas`: el detalle de la OA, ligado por
  `dj_id`.

Cómo se elige qué archivo de la OA usar está en
`application/ddjj/oficina_anticorrupcion.py`. Lo que se mide al cargar, sobre
cada declaración que tiene detalle:

- **Corrección ×10.** Si un total es exactamente 10 veces la suma de su detalle,
  es el error de publicación del corte 20251222: se divide por 10 y se anota en
  `corregido_x10`. Con la elección del corte casi no queda ninguno; es la red
  para el año que sólo viene en un corte malo.
- **Inconsistencias (H005).** `inconsistente` es el total que no cierra con su
  propio detalle; `ingresos_inconsistentes`, el ahorro imposible. Los dos
  vienen del conector viejo, donde la regla está explicada.

El reemplazo es como el de las series (`series_tiempo_tasks.escribir_atomico`):

- Se carga en `<tabla>__nueva` y la vieja pasa a `<tabla>__previa`, todo en una
  transacción. Volver atrás es un RENAME.
- Un guardián se niega si desaparece un año o alguno cae más de 10 %.
- Las filas de otras fuentes se copian de la tabla viva, así que cada carga
  reemplaza sólo lo suyo.
- Un lock advisory evita que dos cargas pisen la misma tabla.

Si la OA no cambió nada desde la última carga escrita (mismos recursos, misma
fecha de modificación), no se baja nada.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from app.application.ddjj import caba
from app.application.ddjj import oficina_anticorrupcion as oa
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine, register_via_b_table
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset
from app.infrastructure.celery.tasks.series_tiempo_tasks import (
    _SQL_DEPENDIENTES,
    _igualar_permisos,
)

logger = logging.getLogger(__name__)

PORTAL = "ddjj"
API_PAQUETE = "https://datos.jus.gob.ar/api/3/action/package_show"
PAQUETE_OA = "declaraciones-juradas-patrimoniales-integrales"

TABLA_DECLARACIONES = "cache_ddjj_declaraciones"
TABLA_BIENES = "cache_ddjj_bienes"
TABLA_DEUDAS = "cache_ddjj_deudas"

_TIMEOUT_S = 120.0
# Un número fijo para `pg_advisory_xact_lock`: las cargas de todas las fuentes
# escriben `cache_ddjj_declaraciones`.
_LOCK_DDJJ = 7_310_251_208
# Cuánto puede caer un año (o el detalle entero) contra la carga anterior.
_CAIDA_MAXIMA = 0.10

_DDL: dict[str, str] = {
    TABLA_DECLARACIONES: """
        fuente text NOT NULL,
        dj_id bigint NOT NULL,
        jurisdiccion text NOT NULL,
        poder text NOT NULL,
        cuit text,
        nombre text,
        anio integer NOT NULL,
        tipo text,
        rectificativa integer,
        sector text,
        organismo text,
        cargo text,
        en_funciones_desde date,
        bienes_inicio numeric,
        deudas_inicio numeric,
        bienes_cierre numeric,
        deudas_cierre numeric,
        ingresos_netos numeric,
        ingresos_trabajo_alquileres_rentas numeric,
        ingresos_no_alcanzados numeric,
        gastos_personales numeric,
        bienes_heredados numeric,
        corte date,
        archivo_fuente text,
        url_fuente text,
        bienes numeric,
        deudas numeric,
        patrimonio numeric,
        variacion_patrimonial numeric,
        detalle_bienes_inicio numeric,
        detalle_bienes_cierre numeric,
        detalle_deudas_inicio numeric,
        detalle_deudas_cierre numeric,
        corregido_x10 text[],
        inconsistente boolean NOT NULL DEFAULT false,
        ingresos_inconsistentes boolean NOT NULL DEFAULT false,
        bienes_por_tipo jsonb,
        fecha_presentacion date
    """,
    TABLA_BIENES: """
        dj_id bigint NOT NULL,
        periodo text NOT NULL,
        tipo text,
        descripcion text,
        origen_fondos text,
        titularidad numeric,
        importe numeric
    """,
    TABLA_DEUDAS: """
        dj_id bigint NOT NULL,
        periodo text NOT NULL,
        tipo text,
        descripcion text,
        radicacion text,
        clasificacion text,
        importe numeric
    """,
}

# (sufijo del nombre, definición). El primero de declaraciones es la clave.
_INDICES: dict[str, tuple[tuple[str, str], ...]] = {
    TABLA_DECLARACIONES: (
        ("pkey", "PRIMARY KEY (fuente, dj_id)"),
        ("cuit_idx", "(cuit)"),
        ("anio_idx", "(anio)"),
    ),
    TABLA_BIENES: (("dj_id_idx", "(dj_id)"),),
    TABLA_DEUDAS: (("dj_id_idx", "(dj_id)"),),
}

FUENTES_LEGIBLES = {
    oa.FUENTE: "Oficina Anticorrupción",
    caba.FUENTE: "Ciudad de Buenos Aires",
}
PODERES_LEGIBLES = {
    "ejecutivo": "Poder Ejecutivo",
    "legislativo": "Poder Legislativo (diputados y senadores)",
    "judicial": "Poder Judicial",
    "ministerio_publico": "Ministerio Público",
}


class _Rechazo(Exception):
    """El guardián o WS0 se negaron: no se reemplaza nada."""


class _Falla(Exception):
    def __init__(self, motivo: str, detalle: str) -> None:
        super().__init__(f"{motivo}: {detalle}")
        self.motivo = motivo
        self.detalle = detalle


# ── la fuente ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Recurso:
    id: str
    url: str
    formato: str
    modificado: str

    def clave(self) -> dict[str, str]:
        return {"id": self.id, "url": self.url, "modificado": self.modificado}


def consultar_paquete(
    client: httpx.Client, paquete: str = PAQUETE_OA, api: str = API_PAQUETE
) -> list[Recurso]:
    r = client.get(api, params={"id": paquete})
    r.raise_for_status()
    cuerpo = r.json()
    if not cuerpo.get("success"):
        raise _Falla("paquete", f"CKAN no devolvió {paquete}")
    recursos = []
    for res in cuerpo["result"].get("resources") or []:
        url = str(res.get("url") or "")
        formato = str(res.get("format") or "").strip().lower()
        if formato not in {"zip", "csv"} or not url:
            continue
        recursos.append(
            Recurso(
                id=str(res.get("id") or url),
                url=url,
                formato=formato,
                modificado=str(res.get("last_modified") or res.get("metadata_modified") or ""),
            )
        )
    if not recursos:
        raise _Falla("paquete", f"{paquete} no tiene recursos ZIP ni CSV")
    return recursos


def manifiesto(recursos: Iterable[Recurso]) -> list[dict[str, str]]:
    return sorted((r.clave() for r in recursos), key=lambda c: c["id"])


def _bajar(client: httpx.Client, url: str, destino: str) -> None:
    with client.stream("GET", url) as r:
        r.raise_for_status()
        with open(destino, "wb") as f:
            for parte in r.iter_bytes(1 << 20):
                f.write(parte)


def bajar_archivos(
    client: httpx.Client, recursos: Iterable[Recurso], carpeta: str
) -> tuple[list[oa.Archivo], list[str]]:
    """Baja los recursos y devuelve los CSV que traen. El grupo familiar ni se baja."""
    archivos: list[oa.Archivo] = []
    salteados: list[str] = []
    for i, rec in enumerate(recursos):
        clase = oa.clasificar(rec.url)
        if clase is not None and clase[0] == oa.TIPO_GRUPO_FAMILIAR:
            continue
        destino = os.path.join(carpeta, f"{i:02d}.{rec.formato}")
        _bajar(client, rec.url, destino)
        if rec.formato == "zip":
            del_zip, salt = oa.archivos_del_zip(destino, rec.url)
            archivos += del_zip
            salteados += salt
        else:
            suelto = oa.archivo_suelto(destino, rec.url)
            if suelto is not None:
                archivos.append(suelto)
    return archivos, salteados


@dataclass
class Plan:
    principal: dict[int, oa.Eleccion] = field(default_factory=dict)
    bienes: dict[int, oa.Eleccion] = field(default_factory=dict)
    deudas: dict[int, oa.Eleccion] = field(default_factory=dict)

    def como_dict(self) -> dict[str, Any]:
        def uno(e: oa.Eleccion) -> dict[str, Any]:
            d: dict[str, Any] = {"archivo": e.archivo.describir()}
            if e.descartados:
                d["descartados"] = [f"{a.describir()}: {m}" for a, m in e.descartados]
            return d

        return {
            tipo: {str(anio): uno(e) for anio, e in sorted(getattr(self, tipo).items())}
            for tipo in ("principal", "bienes", "deudas")
        }


def armar_plan(archivos: Iterable[oa.Archivo]) -> Plan:
    grupos = oa.agrupar(archivos)
    plan = Plan()
    for (tipo, anio), candidatos in sorted(grupos.items()):
        if tipo != oa.TIPO_PRINCIPAL:
            continue
        eleccion = oa.elegir_principal(candidatos)
        if eleccion is None:
            continue
        plan.principal[anio] = eleccion
        for tipo_detalle, destino in ((oa.TIPO_BIENES, plan.bienes), (oa.TIPO_DEUDAS, plan.deudas)):
            detalle = oa.elegir_detalle(
                grupos.get((tipo_detalle, anio), []), eleccion.archivo.corte
            )
            if detalle is not None:
                destino[anio] = detalle
    return plan


# ── escritura ────────────────────────────────────────────────────────────────


def _copiar(
    conn: Connection, destino: str, columnas: tuple[str, ...], filas: Iterable[tuple]
) -> int:
    """COPY a `destino` (un nombre ya calificado) en la transacción de `conn`."""
    lista = ", ".join(columnas)
    n = 0
    dbapi = conn.connection.driver_connection
    if dbapi is None:
        raise _Falla("copia", "la conexión no tiene driver (¿se cerró?)")
    with dbapi.cursor() as cur, cur.copy(f"COPY {destino} ({lista}) FROM STDIN") as cp:
        for fila in filas:
            cp.write_row(fila)
            n += 1
    return n


@dataclass
class Cuentas:
    leidas: Counter = field(default_factory=Counter)
    invalidas: Counter = field(default_factory=Counter)
    repetidas: Counter = field(default_factory=Counter)
    huerfanas: Counter = field(default_factory=Counter)
    vistos: set[int] = field(default_factory=set)


def filas_declaraciones(plan: Plan, cuentas: Cuentas) -> Iterator[tuple]:
    vistos = cuentas.vistos
    for anio, eleccion in sorted(plan.principal.items()):
        archivo = eleccion.archivo
        with oa.abrir(archivo) as lector:
            for crudo in lector:
                cuentas.leidas[anio] += 1
                fila = oa.fila_declaracion(crudo, archivo)
                if fila is None:
                    cuentas.invalidas[anio] += 1
                    continue
                dj_id = fila[1]
                if dj_id in vistos:
                    cuentas.repetidas[anio] += 1
                    continue
                vistos.add(dj_id)
                yield fila


def filas_detalle(
    elecciones: Mapping[int, oa.Eleccion], tipo: str, cuentas: Cuentas
) -> Iterator[tuple]:
    """El detalle de las declaraciones que quedaron. El de una que no quedó (otro
    corte, una rectificativa) no tiene a quién sumarse y se cuenta como huérfano."""
    convertir = oa.fila_bien if tipo == oa.TIPO_BIENES else oa.fila_deuda
    for _anio, eleccion in sorted(elecciones.items()):
        with oa.abrir(eleccion.archivo) as lector:
            for crudo in lector:
                fila = convertir(crudo)
                if fila is None:
                    continue
                if fila[0] not in cuentas.vistos:
                    cuentas.huerfanas[tipo] += 1
                    continue
                yield fila


def _existe(conn: Connection, tabla: str) -> bool:
    return bool(
        conn.execute(text("SELECT to_regclass(:q) IS NOT NULL"), {"q": f'raw."{tabla}"'}).scalar()
    )


def _crear_nueva(conn: Connection, tabla: str) -> None:
    conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}__nueva"'))
    conn.execute(text(f'CREATE TABLE raw."{tabla}__nueva" ({_DDL[tabla]})'))


def _indexar(conn: Connection, tabla: str) -> None:
    for sufijo, definicion in _INDICES[tabla]:
        nombre = f"{tabla}__nueva_{sufijo}"
        if definicion.startswith("PRIMARY KEY"):
            conn.execute(
                text(f'ALTER TABLE raw."{tabla}__nueva" ADD CONSTRAINT "{nombre}" {definicion}')
            )
        else:
            conn.execute(text(f'CREATE INDEX "{nombre}" ON raw."{tabla}__nueva" {definicion}'))
    conn.execute(text(f'ANALYZE raw."{tabla}__nueva"'))


def _conservar_otras_fuentes(conn: Connection, fuente: str) -> int:
    """Copia a la tabla nueva las declaraciones de las otras fuentes."""
    if not _existe(conn, TABLA_DECLARACIONES):
        return 0
    columnas = [
        r[0]
        for r in conn.execute(
            text(
                """
                SELECT n.column_name FROM information_schema.columns n
                JOIN information_schema.columns v
                  ON v.table_schema = 'raw' AND v.table_name = :viva
                 AND v.column_name = n.column_name
                WHERE n.table_schema = 'raw' AND n.table_name = :nueva
                ORDER BY n.ordinal_position
                """
            ),
            {"viva": TABLA_DECLARACIONES, "nueva": f"{TABLA_DECLARACIONES}__nueva"},
        )
    ]
    lista = ", ".join(f'"{c}"' for c in columnas)
    res = conn.execute(
        text(
            f'INSERT INTO raw."{TABLA_DECLARACIONES}__nueva" ({lista}) '
            f'SELECT {lista} FROM raw."{TABLA_DECLARACIONES}" WHERE fuente <> :f'
        ),
        {"f": fuente},
    )
    return int(res.rowcount or 0)


# Las declaraciones se arman en una sola pasada desde la tabla temporal de la
# carga: con UPDATE por columna, cada pasada dejaba una copia muerta de las
# 720.000 filas (1,2 GB en vez de ~250 MB, medido el 08-oct).
#
# - **Sumas del detalle**, por período, de `cache_ddjj_bienes` y `_deudas`.
# - **Corrección ×10**: un total que es exactamente 10 veces su detalle se
#   divide por 10 y se anota en `corregido_x10`.
# - **Lo declarado**: el inicio en una declaración inicial (no tiene cierre:
#   8.581 de 9.719 en 2024 traen bienes al inicio y cero al cierre) y el cierre
#   en las demás. La variación sólo tiene sentido en las anuales y las bajas.
# - **H005**, las mismas reglas que el conector viejo
#   (`ddjj_adapter._inconsistency` e `_income_inconsistency`), sobre lo
#   declarado y el detalle del mismo período.
_SQL_ARMAR = """
    INSERT INTO raw."{decl}__nueva" ({columnas})
    WITH sb AS (
        SELECT dj_id,
               sum(importe) FILTER (WHERE periodo = 'inicio') AS i,
               sum(importe) FILTER (WHERE periodo = 'cierre') AS c
        FROM raw."{bienes}__nueva" GROUP BY dj_id
    ), sd AS (
        SELECT dj_id,
               sum(importe) FILTER (WHERE periodo = 'inicio') AS i,
               sum(importe) FILTER (WHERE periodo = 'cierre') AS c
        FROM raw."{deudas}__nueva" GROUP BY dj_id
    ), x AS (
        SELECT c.*,
               sb.i AS dbi, sb.c AS dbc, sd.i AS ddi, sd.c AS ddc,
               coalesce(sb.i > 0 AND c.bienes_inicio > 0
                        AND abs(c.bienes_inicio / sb.i - 10) < 0.05, false) AS xbi,
               coalesce(sb.c > 0 AND c.bienes_cierre > 0
                        AND abs(c.bienes_cierre / sb.c - 10) < 0.05, false) AS xbc,
               coalesce(sd.i > 0 AND c.deudas_inicio > 0
                        AND abs(c.deudas_inicio / sd.i - 10) < 0.05, false) AS xdi,
               coalesce(sd.c > 0 AND c.deudas_cierre > 0
                        AND abs(c.deudas_cierre / sd.c - 10) < 0.05, false) AS xdc
        FROM pg_temp.{carga} c
        LEFT JOIN sb ON sb.dj_id = c.dj_id
        LEFT JOIN sd ON sd.dj_id = c.dj_id
    ), k AS (
        SELECT x.*,
               CASE WHEN xbi THEN bienes_inicio / 10 ELSE bienes_inicio END AS bi,
               CASE WHEN xbc THEN bienes_cierre / 10 ELSE bienes_cierre END AS bc,
               CASE WHEN xdi THEN deudas_inicio / 10 ELSE deudas_inicio END AS di,
               CASE WHEN xdc THEN deudas_cierre / 10 ELSE deudas_cierre END AS dc,
               CASE WHEN tipo = 'Inicial' THEN dbi ELSE dbc END AS det
        FROM x
    ), m AS (
        SELECT k.*,
               CASE WHEN tipo = 'Inicial' THEN bi ELSE bc END AS bienes_decl,
               GREATEST(coalesce(bi, 0), coalesce(bc, 0), coalesce(di, 0)) AS base
        FROM k
    )
    SELECT {originales},
           bi, di, bc, dc,
           bienes_decl,
           CASE WHEN tipo = 'Inicial' THEN di ELSE dc END,
           CASE WHEN tipo = 'Inicial' THEN coalesce(bi, 0) - coalesce(di, 0)
                ELSE coalesce(bc, 0) - coalesce(dc, 0) END,
           CASE WHEN tipo = 'Inicial' THEN NULL
                ELSE (coalesce(bc, 0) - coalesce(dc, 0)) - (coalesce(bi, 0) - coalesce(di, 0)) END,
           dbi, dbc, ddi, ddc,
           NULLIF(array_remove(ARRAY[
               CASE WHEN xbi THEN 'bienes_inicio' END,
               CASE WHEN xbc THEN 'bienes_cierre' END,
               CASE WHEN xdi THEN 'deudas_inicio' END,
               CASE WHEN xdc THEN 'deudas_cierre' END
           ]::text[], NULL), '{{}}'::text[]),
           coalesce(det > 0 AND (
               coalesce(bienes_decl, 0) > 10 * GREATEST(
                   det, CASE WHEN tipo = 'Inicial' THEN 0 ELSE coalesce(bi, 0) END)
               OR coalesce(bienes_decl, 0) * 10 < det), false),
           coalesce(base > 0
                    AND coalesce(ingresos_netos, 0) - coalesce(gastos_personales, 0) > 10 * base,
                    false)
    FROM m
"""

# Las columnas de la carga que pasan tal cual; los cuatro totales salen
# corregidos (`bi`, `di`, `bc`, `dc`).
_TOTALES = ("bienes_inicio", "deudas_inicio", "bienes_cierre", "deudas_cierre")
_CALCULADAS = (
    "bienes",
    "deudas",
    "patrimonio",
    "variacion_patrimonial",
    "detalle_bienes_inicio",
    "detalle_bienes_cierre",
    "detalle_deudas_inicio",
    "detalle_deudas_cierre",
    "corregido_x10",
    "inconsistente",
    "ingresos_inconsistentes",
)
_CARGA = "ddjj_carga"
# Tipos de la tabla temporal; lo que no está acá es un monto (`numeric`).
_TIPO_CARGA = {
    "fuente": "text",
    "dj_id": "bigint",
    "jurisdiccion": "text",
    "poder": "text",
    "cuit": "text",
    "nombre": "text",
    "anio": "integer",
    "tipo": "text",
    "rectificativa": "integer",
    "sector": "text",
    "organismo": "text",
    "cargo": "text",
    "en_funciones_desde": "date",
    "corte": "date",
    "archivo_fuente": "text",
    "url_fuente": "text",
}


def armar_declaraciones(conn: Connection) -> int:
    """Pasa la carga temporal a `cache_ddjj_declaraciones__nueva`. Devuelve
    cuántas declaraciones tuvieron alguna corrección ×10."""
    originales = [c for c in oa.COLUMNAS_DECLARACION if c not in _TOTALES]
    destino = [*originales, *_TOTALES, *_CALCULADAS]
    conn.execute(
        text(
            _SQL_ARMAR.format(
                decl=TABLA_DECLARACIONES,
                bienes=TABLA_BIENES,
                deudas=TABLA_DEUDAS,
                carga=_CARGA,
                columnas=", ".join(destino),
                originales=", ".join(originales),
            )
        )
    )
    return int(
        conn.execute(
            text(
                f'SELECT count(*) FROM raw."{TABLA_DECLARACIONES}__nueva" '
                "WHERE fuente = :f AND corregido_x10 IS NOT NULL"
            ),
            {"f": oa.FUENTE},
        ).scalar()
        or 0
    )


def _por_anio(conn: Connection, tabla: str, fuente: str) -> dict[int, int]:
    if not _existe(conn, tabla):
        return {}
    filas = conn.execute(
        text(f'SELECT anio, count(*) FROM raw."{tabla}" WHERE fuente = :f GROUP BY anio'),
        {"f": fuente},
    ).all()
    return {int(a): int(n) for a, n in filas}


def _cantidad(conn: Connection, tabla: str) -> int | None:
    if not _existe(conn, tabla):
        return None
    return int(conn.execute(text(f'SELECT count(*) FROM raw."{tabla}"')).scalar() or 0)


def motivo_para_rechazar(
    *,
    nuevas: Mapping[int, int],
    vivas: Mapping[int, int],
    detalle_nuevo: Mapping[str, int],
    detalle_vivo: Mapping[str, int | None],
    permitir_menos_filas: bool,
) -> str | None:
    """Por qué no reemplazar, o `None`. `nuevas`/`vivas`: declaraciones por año."""
    if not nuevas:
        return "no quedó ninguna declaración"
    if permitir_menos_filas:
        return None
    faltan = sorted(set(vivas) - set(nuevas))
    if faltan:
        return f"desaparecen los años {', '.join(map(str, faltan))}"
    caidas = [
        f"{anio}: {nuevas[anio]} contra {vivas[anio]}"
        for anio in sorted(vivas)
        if nuevas[anio] < (1 - _CAIDA_MAXIMA) * vivas[anio]
    ]
    if caidas:
        return f"caen más de {_CAIDA_MAXIMA:.0%} las declaraciones de {'; '.join(caidas)}"
    for tabla, viva in detalle_vivo.items():
        nueva = detalle_nuevo.get(tabla, 0)
        if viva and nueva < (1 - _CAIDA_MAXIMA) * viva:
            return f"{tabla} cae de {viva} a {nueva} filas"
    return None


def reemplazar(conn: Connection, tabla: str) -> None:
    """Pone `<tabla>__nueva` en lugar de la viva, que queda como `<tabla>__previa`.

    Los índices se renombran con la tabla: los de la viva pasan a
    `<tabla>__previa_<sufijo>` y los de la nueva toman los nombres canónicos.
    """
    nueva, previa = f"{tabla}__nueva", f"{tabla}__previa"
    if _existe(conn, tabla):
        vistas = list(conn.execute(_SQL_DEPENDIENTES, {"q": f'raw."{tabla}"'}).scalars().all())
        if vistas:
            raise _Rechazo(f"dependen de raw.{tabla}: {', '.join(map(str, vistas))}")
        _igualar_permisos(conn, desde=tabla, hacia=nueva)
        conn.execute(text(f'DROP TABLE IF EXISTS raw."{previa}"'))
        for sufijo, _ in _INDICES[tabla]:
            conn.execute(
                text(f'ALTER INDEX IF EXISTS raw."{tabla}_{sufijo}" RENAME TO "{previa}_{sufijo}"')
            )
        conn.execute(text(f'ALTER TABLE raw."{tabla}" RENAME TO "{previa}"'))
    for sufijo, _ in _INDICES[tabla]:
        conn.execute(
            text(f'ALTER INDEX IF EXISTS raw."{nueva}_{sufijo}" RENAME TO "{tabla}_{sufijo}"')
        )
    conn.execute(text(f'ALTER TABLE raw."{nueva}" RENAME TO "{tabla}"'))


# ── catálogo ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FichaTabla:
    tabla: str
    source_id: str
    titulo: str
    descripcion: str
    columnas: tuple[str, ...]
    filas: int
    tags: str
    bytes: int
    organizacion: str = "Oficina Anticorrupción"

    @property
    def identidad(self) -> str:
        return f"{PORTAL}::{self.source_id}"


def _miles(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def _bytes(conn: Connection, tabla: str) -> int:
    return int(
        conn.execute(
            text("SELECT pg_total_relation_size(to_regclass(:q))"), {"q": f'raw."{tabla}"'}
        ).scalar()
        or 0
    )


def _rango(anios: Iterable[int]) -> str:
    lista = sorted(set(anios))
    if not lista:
        return "sin años"
    return str(lista[0]) if len(lista) == 1 else f"{lista[0]}–{lista[-1]}"


def _columnas_de(conn: Connection, tabla: str) -> tuple[str, ...]:
    return tuple(
        r[0]
        for r in conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = :t ORDER BY ordinal_position"
            ),
            {"t": tabla},
        )
    )


_LICENCIAS = {
    oa.FUENTE: f"Oficina Anticorrupción, datos.jus.gob.ar, dataset {PAQUETE_OA} (CC-BY 4.0)",
    caba.FUENTE: (
        "Ciudad de Buenos Aires, Secretaría Legal y Técnica, data.buenosaires.gob.ar, "
        f"dataset {caba.PAQUETE} (CC-BY 2.5 AR)"
    ),
}
_NOTAS_FUENTE = {
    caba.FUENTE: (
        "Las de la Ciudad de Buenos Aires traen el total de bienes por tipo (bienes_por_tipo) "
        "sin deudas, así que no tienen patrimonio neto; el año es el de presentación y no "
        "traen CUIT, organismo ni tipo de declaración."
    ),
}


def ficha_declaraciones(conn: Connection) -> FichaTabla:
    """Título y descripción de `cache_ddjj_declaraciones`, con lo que tiene la carga nueva."""
    decl = f"{TABLA_DECLARACIONES}__nueva"
    por_fuente = conn.execute(
        text(f'SELECT fuente, min(anio), max(anio), count(*) FROM raw."{decl}" GROUP BY fuente')
    ).all()
    fuentes = sorted(str(f) for f, *_ in por_fuente)
    poderes = [
        r[0]
        for r in conn.execute(
            text(f"SELECT DISTINCT poder FROM raw.\"{decl}\" WHERE poder <> 'sin_dato'")
        )
    ]
    cobertura = "; ".join(
        f"{FUENTES_LEGIBLES.get(f, f)}, años {lo}–{hi} ({_miles(int(n))} declaraciones)"
        for f, lo, hi, n in sorted(por_fuente)
    )
    alcance = ", ".join(PODERES_LEGIBLES[p] for p in PODERES_LEGIBLES if p in poderes)
    partes = [
        "Una fila por declaración jurada patrimonial: funcionario, CUIT, organismo, cargo, "
        "poder del Estado, año, tipo (inicial, anual o baja), bienes y deudas al inicio y al "
        "cierre, patrimonio declarado, variación patrimonial, ingresos y gastos personales.",
        f"Cobertura: {cobertura}.",
        f"Incluye {alcance}. No incluye el grupo familiar.",
        "Los totales que la Oficina Anticorrupción publicó multiplicados por 10 se toman de "
        "un corte sano o se corrigen contra el detalle (columna corregido_x10).",
        *(_NOTAS_FUENTE[f] for f in fuentes if f in _NOTAS_FUENTE),
        f"Fuentes: {'; '.join(_LICENCIAS.get(f, f) for f in fuentes)}.",
    ]
    tags = [
        "declaraciones juradas,ddjj,patrimonio,funcionarios,bienes,deudas,"
        "oficina anticorrupcion,transparencia,diputados,senadores"
    ]
    if caba.FUENTE in fuentes:
        tags.append("ciudad de buenos aires,caba")
    return FichaTabla(
        tabla=TABLA_DECLARACIONES,
        source_id="ddjj-declaraciones",
        titulo="Declaraciones juradas patrimoniales de funcionarios públicos",
        descripcion=" ".join(partes),
        columnas=_columnas_de(conn, decl),
        filas=sum(int(n) for *_, n in por_fuente),
        tags=",".join(tags),
        bytes=_bytes(conn, decl),
        organizacion=" y ".join(FUENTES_LEGIBLES.get(f, f) for f in fuentes),
    )


def fichas_detalle(conn: Connection) -> list[FichaTabla]:
    """Título y descripción de `cache_ddjj_bienes` y `cache_ddjj_deudas` (sólo la OA)."""
    decl = f"{TABLA_DECLARACIONES}__nueva"

    def anios(tipo: str) -> list[int]:
        return [
            r[0]
            for r in conn.execute(
                text(
                    f'SELECT DISTINCT anio FROM raw."{decl}" '
                    f"WHERE detalle_{tipo}_inicio IS NOT NULL OR detalle_{tipo}_cierre IS NOT NULL"
                )
            )
        ]

    licencia = f"Fuente: {_LICENCIAS[oa.FUENTE]}."
    return [
        FichaTabla(
            tabla=TABLA_BIENES,
            source_id="ddjj-bienes",
            titulo="Bienes declarados en las declaraciones juradas de funcionarios públicos",
            descripcion=(
                "Detalle de los bienes de cada declaración jurada patrimonial de la Oficina "
                "Anticorrupción (inmuebles, automotores, depósitos, acciones, títulos, dinero "
                "en efectivo, bienes del hogar), al inicio y al cierre. "
                f"Años {_rango(anios('bienes'))} (los de 2016 y 2017 se publicaron sin "
                f"importe). Se une a {TABLA_DECLARACIONES} por dj_id. {licencia}"
            ),
            columnas=_columnas_de(conn, f"{TABLA_BIENES}__nueva"),
            filas=_cantidad(conn, f"{TABLA_BIENES}__nueva") or 0,
            tags="declaraciones juradas,ddjj,bienes,inmuebles,automotores,funcionarios",
            bytes=_bytes(conn, f"{TABLA_BIENES}__nueva"),
        ),
        FichaTabla(
            tabla=TABLA_DEUDAS,
            source_id="ddjj-deudas",
            titulo="Deudas declaradas en las declaraciones juradas de funcionarios públicos",
            descripcion=(
                "Detalle de las deudas de cada declaración jurada patrimonial de la Oficina "
                "Anticorrupción (hipotecarias, prendarias, comunes), con acreedor, al inicio y "
                f"al cierre. Años {_rango(anios('deudas'))}. Se une a {TABLA_DECLARACIONES} "
                f"por dj_id. {licencia}"
            ),
            columnas=_columnas_de(conn, f"{TABLA_DEUDAS}__nueva"),
            filas=_cantidad(conn, f"{TABLA_DEUDAS}__nueva") or 0,
            tags="declaraciones juradas,ddjj,deudas,funcionarios",
            bytes=_bytes(conn, f"{TABLA_DEUDAS}__nueva"),
        ),
    ]


def _asegurar_dataset(engine: Engine, ficha: FichaTabla) -> tuple[str, bool]:
    """El id del dataset (creándolo si falta) y si lo buscable cambió."""
    with engine.begin() as conn:
        previa = conn.execute(
            text(
                "SELECT CAST(id AS text), title, description, columns FROM datasets "
                "WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": ficha.source_id, "portal": PORTAL},
        ).fetchone()
        if previa is not None:
            cambio = (
                previa[1] != ficha.titulo
                or previa[2] != ficha.descripcion
                or previa[3] != json.dumps(list(ficha.columnas))
            )
            return str(previa[0]), cambio
        conn.execute(
            text(
                """
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'csv', :cols, :tags,
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO NOTHING
                """
            ),
            {
                "sid": ficha.source_id,
                "title": ficha.titulo,
                "desc": ficha.descripcion,
                "org": ficha.organizacion,
                "portal": PORTAL,
                "url": oa.URL_DATASET,
                "cols": json.dumps(list(ficha.columnas)),
                "tags": ficha.tags,
                "now": datetime.now(UTC),
                "rows": ficha.filas,
            },
        )
        fila = conn.execute(
            text("SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :p"),
            {"sid": ficha.source_id, "p": PORTAL},
        ).fetchone()
    if not fila:
        raise _Falla("dataset", f"no se pudo crear el dataset de {ficha.tabla}")
    return str(fila[0]), True


def veredicto_ws0(engine: Engine, *, dataset_id: str, ficha: FichaTabla) -> str | None:
    """La puerta WS0 de `_finalize_cached_dataset`, antes de reemplazar (como en las series)."""
    from app.infrastructure.celery.tasks import collector_tasks as ct

    columnas = list(ficha.columnas)
    finding = ct._ws0_validate_post_parse(
        engine,
        dataset_id=dataset_id,
        portal=PORTAL,
        source_id=ficha.source_id,
        download_url=oa.URL_DATASET,
        declared_format="csv",
        table_name=ficha.tabla,
        materialized_columns=columnas,
        materialized_row_count=ficha.filas,
        declared_size_bytes=ficha.bytes,
        columns_json=json.dumps(columnas),
    )
    layout = (
        ct._LAYOUT_WIDE if len(columnas) > ct._WIDE_LAYOUT_COLUMN_THRESHOLD else ct._LAYOUT_SIMPLE
    )
    resultado = ct._materialization_outcome_for_ready(
        portal=PORTAL,
        declared_format="csv",
        table_name=ficha.tabla,
        row_count=ficha.filas,
        columns=columnas,
        layout_profile=layout,
        header_quality=ct._header_quality_label(columnas),
        ws0_finding=finding,
    )
    if resultado.cached_status == "ready":
        return None
    return resultado.error_message or resultado.result_kind


def completar_metadatos(
    engine: Engine, *, dataset_id: str, ficha: FichaTabla, reembeber: bool
) -> None:
    """Dataset, catálogo (`ready`), registro y embedding de una tabla ya reemplazada."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE datasets SET
                        title = :title, description = :desc, organization = :org, url = :url,
                        columns = :cols, tags = :tags, row_count = :rows,
                        last_updated_at = :now, updated_at = :now
                    WHERE id = CAST(:did AS uuid)
                    """
                ),
                {
                    "did": dataset_id,
                    "title": ficha.titulo,
                    "desc": ficha.descripcion,
                    "org": ficha.organizacion,
                    "url": oa.URL_DATASET,
                    "cols": json.dumps(list(ficha.columnas)),
                    "tags": ficha.tags,
                    "rows": ficha.filas,
                    "now": datetime.now(UTC),
                },
            )
        final = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=PORTAL,
            source_id=ficha.source_id,
            table_name=ficha.tabla,
            row_count=ficha.filas,
            columns=list(ficha.columnas),
            declared_format="csv",
            download_url=oa.URL_DATASET,
            declared_size_bytes=ficha.bytes,
        )
        if not final.get("ok"):
            raise _Falla("finalizacion", str(final.get("error") or final.get("status")))
        if not register_via_b_table(
            engine,
            resource_identity=ficha.identidad,
            table_name=ficha.tabla,
            schema_name="raw",
            row_count=ficha.filas,
        ):
            raise _Falla("registro", f"no se pudo registrar {ficha.identidad}")
    except (SoftTimeLimitExceeded, _Falla):
        raise
    except Exception as exc:
        logger.exception("DDJJ %s: falló la metadata", ficha.tabla)
        raise _Falla("metadatos", str(exc)[:300]) from exc

    from app.application.quality.heartbeat import record_ingest

    record_ingest(engine, ficha.identidad)
    if reembeber:
        try:
            from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

            index_dataset_embedding.delay(dataset_id)
        except Exception:
            logger.warning("DDJJ %s: no se pudo encolar el embedding", ficha.tabla, exc_info=True)


# ── registro de cargas ───────────────────────────────────────────────────────


def ultima_carga(engine: Engine, fuente: str) -> list[dict[str, str]] | None:
    """El manifiesto de la última carga escrita de la fuente."""
    with engine.connect() as conn:
        valor = conn.execute(
            text(
                "SELECT manifiesto FROM public.ddjj_cargas WHERE fuente = :f AND estado = 'escrita' "
                "ORDER BY fin DESC LIMIT 1"
            ),
            {"f": fuente},
        ).scalar()
    if valor is None:
        return None
    return valor if isinstance(valor, list) else json.loads(valor)


def anotar_carga(
    engine: Engine,
    *,
    fuente: str,
    estado: str,
    inicio: datetime,
    manifiesto_: list[dict[str, str]] | None,
    resumen: Mapping[str, Any],
    detalle: str = "",
) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO public.ddjj_cargas
                        (fuente, estado, inicio, fin, manifiesto, resumen, detalle)
                    VALUES (:f, :e, :i, now(), CAST(:m AS jsonb), CAST(:r AS jsonb), :d)
                    """
                ),
                {
                    "f": fuente,
                    "e": estado,
                    "i": inicio,
                    "m": json.dumps(manifiesto_) if manifiesto_ is not None else None,
                    "r": json.dumps(resumen, default=str),
                    "d": detalle[:2000],
                },
            )
    except Exception:
        logger.warning("DDJJ: no se pudo anotar la carga %s", estado, exc_info=True)


def tablas_en_orden(
    engine: Engine, tablas: tuple[str, ...] = (TABLA_DECLARACIONES, TABLA_BIENES, TABLA_DEUDAS)
) -> bool:
    """Las tablas existen y están registradas: si no, hay que escribir."""
    with engine.connect() as conn:
        for tabla in tablas:
            if not _existe(conn, tabla):
                return False
            registrada = conn.execute(
                text(
                    "SELECT 1 FROM public.raw_table_versions "
                    "WHERE schema_name = 'raw' AND table_name = :t"
                ),
                {"t": tabla},
            ).first()
            if registrada is None:
                return False
    return True


def _alertar(engine: Engine, titulo: str, detalle: str, clave: str) -> None:
    try:
        from app.application.quality.alerting import Alert, notify

        notify(
            engine,
            [Alert(kind="ddjj_ingest_failed", key=clave, title=titulo, detail=detalle[:300])],
            heading="OpenArg · carga de declaraciones juradas",
        )
    except Exception:
        logger.warning("DDJJ: no se pudo alertar", exc_info=True)


# ── la carga ─────────────────────────────────────────────────────────────────


def _publicar(
    engine: Engine, conn: Connection, lista: list[FichaTabla]
) -> list[tuple[FichaTabla, str, bool]]:
    """WS0 y reemplazo de las tablas de `lista`, dentro de la transacción de la carga."""
    publicadas: list[tuple[FichaTabla, str, bool]] = []
    for ficha in lista:
        dataset_id, cambio = _asegurar_dataset(engine, ficha)
        ws0 = veredicto_ws0(engine, dataset_id=dataset_id, ficha=ficha)
        if ws0:
            raise _Rechazo(f"WS0 {ficha.tabla}: {ws0}")
        publicadas.append((ficha, dataset_id, cambio))
    conn.execute(text("SET LOCAL lock_timeout = '15s'"))
    for ficha, _, _ in publicadas:
        reemplazar(conn, ficha.tabla)
    return publicadas


def _completar(engine: Engine, publicadas: list[tuple[FichaTabla, str, bool]]) -> None:
    for ficha, dataset_id, cambio in publicadas:
        completar_metadatos(engine, dataset_id=dataset_id, ficha=ficha, reembeber=cambio)


def cargar_oa(
    engine: Engine,
    plan: Plan,
    *,
    permitir_menos_filas: bool,
) -> dict[str, Any]:
    """Escribe las tres tablas desde el plan. Lanza `_Rechazo` sin tocar nada."""
    cuentas = Cuentas()
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _LOCK_DDJJ})
        conn.execute(text("SET LOCAL statement_timeout = '45min'"))
        for tabla in (TABLA_DECLARACIONES, TABLA_BIENES, TABLA_DEUDAS):
            _crear_nueva(conn, tabla)
        # Las declaraciones pasan por una tabla temporal y se arman de una vez
        # con sus calculadas (`armar_declaraciones`); el detalle va directo.
        columnas_carga = ", ".join(
            f"{c} {_TIPO_CARGA.get(c, 'numeric')}" for c in oa.COLUMNAS_DECLARACION
        )
        conn.execute(text(f"CREATE TEMP TABLE {_CARGA} ({columnas_carga}) ON COMMIT DROP"))
        n_decl = _copiar(
            conn, f"pg_temp.{_CARGA}", oa.COLUMNAS_DECLARACION, filas_declaraciones(plan, cuentas)
        )
        n_bienes = _copiar(
            conn,
            f'raw."{TABLA_BIENES}__nueva"',
            oa.COLUMNAS_BIEN,
            filas_detalle(plan.bienes, oa.TIPO_BIENES, cuentas),
        )
        n_deudas = _copiar(
            conn,
            f'raw."{TABLA_DEUDAS}__nueva"',
            oa.COLUMNAS_DEUDA,
            filas_detalle(plan.deudas, oa.TIPO_DEUDAS, cuentas),
        )
        corregidas = armar_declaraciones(conn)
        otras = _conservar_otras_fuentes(conn, oa.FUENTE)
        for tabla in (TABLA_DECLARACIONES, TABLA_BIENES, TABLA_DEUDAS):
            _indexar(conn, tabla)

        nuevas = _por_anio(conn, f"{TABLA_DECLARACIONES}__nueva", oa.FUENTE)
        vivas = _por_anio(conn, TABLA_DECLARACIONES, oa.FUENTE)
        detalle_nuevo = {TABLA_BIENES: n_bienes, TABLA_DEUDAS: n_deudas}
        detalle_vivo = {t: _cantidad(conn, t) for t in (TABLA_BIENES, TABLA_DEUDAS)}
        rechazo = motivo_para_rechazar(
            nuevas=nuevas,
            vivas=vivas,
            detalle_nuevo=detalle_nuevo,
            detalle_vivo=detalle_vivo,
            permitir_menos_filas=permitir_menos_filas,
        )
        if rechazo:
            raise _Rechazo(rechazo)

        publicadas = _publicar(engine, conn, [ficha_declaraciones(conn), *fichas_detalle(conn)])

    _completar(engine, publicadas)

    return {
        "declaraciones": n_decl,
        "declaraciones_por_anio": dict(sorted(nuevas.items())),
        "bienes": detalle_nuevo[TABLA_BIENES],
        "deudas": detalle_nuevo[TABLA_DEUDAS],
        "detalle_huerfano": dict(cuentas.huerfanas),
        "corregidas_x10": corregidas,
        "otras_fuentes_conservadas": otras,
        "filas_invalidas": dict(cuentas.invalidas),
        "filas_repetidas": dict(cuentas.repetidas),
    }


# CABA no publica el detalle de los bienes, así que no hay contra qué cerrar un
# total, y algunos son imposibles: una controladora de faltas con $21,5 billones en
# inmuebles en 2025, cuando la mediana del año es $15 millones y la declaración
# más grande de la OA en 2024 es de $614.000 millones. Se marca inconsistente lo
# que pasa 1.000 veces la mediana de su año. Con 10.000 quedaban adentro un
# asesor con $150.672 millones en inmuebles (9.920 veces) y jefes de departamento
# con $90.000 millones; entre 1.000 y 10.000 veces no hay un corte natural, y
# todo lo de esa franja son cargos medios con cifras de fortuna. Queda afuera de
# rankings y promedios, como en la OA, y el ranking dice cuántas excluyó.
_VECES_MEDIANA_CABA = 1_000
_SQL_INCONSISTENTES_CABA = """
    UPDATE raw."{decl}__nueva" d SET inconsistente = true
    FROM (
        SELECT anio, percentile_cont(0.5) WITHIN GROUP (ORDER BY bienes) AS mediana
        FROM raw."{decl}__nueva" WHERE fuente = :f AND bienes > 0 GROUP BY anio
    ) m
    WHERE d.fuente = :f AND d.anio = m.anio AND d.bienes > :veces * m.mediana
"""


def cargar_caba(
    engine: Engine,
    archivos: list[caba.ArchivoCaba],
    *,
    permitir_menos_filas: bool,
) -> dict[str, Any]:
    """Reemplaza las declaraciones de CABA en `cache_ddjj_declaraciones` (las de las
    otras fuentes se copian tal cual). Lanza `_Rechazo` sin tocar nada."""
    invalidas: Counter = Counter()
    repetidas: Counter = Counter()
    salteados: list[str] = []

    def filas() -> Iterator[tuple]:
        vistos: set[int] = set()
        for archivo in archivos:
            columnas, lector = caba.leer(archivo.ruta)
            nombre = archivo.url.rsplit("/", 1)[-1]
            if not caba.es_formato_ancho(columnas):
                salteados.append(f"{nombre}: formato largo (campo/valor), queda para después")
                continue
            for crudo in lector:
                fila = caba.fila_declaracion(crudo, archivo)
                if fila is None:
                    invalidas[nombre] += 1
                    continue
                if fila[1] in vistos:
                    repetidas[nombre] += 1
                    continue
                vistos.add(fila[1])
                yield fila

    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _LOCK_DDJJ})
        conn.execute(text("SET LOCAL statement_timeout = '30min'"))
        _crear_nueva(conn, TABLA_DECLARACIONES)
        n = _copiar(conn, f'raw."{TABLA_DECLARACIONES}__nueva"', caba.COLUMNAS_DECLARACION, filas())
        inconsistentes = int(
            conn.execute(
                text(_SQL_INCONSISTENTES_CABA.format(decl=TABLA_DECLARACIONES)),
                {"f": caba.FUENTE, "veces": _VECES_MEDIANA_CABA},
            ).rowcount
            or 0
        )
        otras = _conservar_otras_fuentes(conn, caba.FUENTE)
        _indexar(conn, TABLA_DECLARACIONES)
        nuevas = _por_anio(conn, f"{TABLA_DECLARACIONES}__nueva", caba.FUENTE)
        vivas = _por_anio(conn, TABLA_DECLARACIONES, caba.FUENTE)
        rechazo = motivo_para_rechazar(
            nuevas=nuevas,
            vivas=vivas,
            detalle_nuevo={},
            detalle_vivo={},
            permitir_menos_filas=permitir_menos_filas,
        )
        if rechazo:
            raise _Rechazo(rechazo)
        publicadas = _publicar(engine, conn, [ficha_declaraciones(conn)])

    _completar(engine, publicadas)
    return {
        "declaraciones": n,
        "declaraciones_por_anio": dict(sorted(nuevas.items())),
        "inconsistentes": inconsistentes,
        "otras_fuentes_conservadas": otras,
        "salteados": salteados,
        "filas_invalidas": dict(invalidas),
        "filas_repetidas": dict(repetidas),
    }


def _fecha_iso(texto: str) -> date | None:
    try:
        return date.fromisoformat(texto[:10])
    except ValueError:
        return None


def _sha256(ruta: str) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for parte in iter(lambda: f.read(1 << 20), b""):
            h.update(parte)
    return h.hexdigest()


def _ejecutar(
    tarea: Any, engine: Engine, fuente: str, etiqueta: str, cuerpo: Any
) -> dict[str, Any]:
    """Corre una carga y deja anotado y alertado cómo terminó.

    `cuerpo()` devuelve `(estado, manifiesto, resumen)`. Una carga escrita queda en
    `ddjj_cargas` con su manifiesto, y es lo que la próxima corrida compara para
    no volver a escribir. Un rechazo no reintenta (la fuente no va a cambiar
    sola en dos minutos), una falla de red sí.
    """
    inicio = datetime.now(UTC)
    try:
        estado, mani, resumen = cuerpo()
        if estado == "escrita":
            anotar_carga(
                engine,
                fuente=fuente,
                estado=estado,
                inicio=inicio,
                manifiesto_=mani,
                resumen=resumen,
            )
            logger.info(
                "%s: escrita %s", etiqueta, {k: v for k, v in resumen.items() if k != "plan"}
            )
        return {"estado": estado, **resumen}
    except _Rechazo as exc:
        logger.warning("%s: rechazada — %s", etiqueta, exc)
        anotar_carga(
            engine,
            fuente=fuente,
            estado="rechazada",
            inicio=inicio,
            manifiesto_=None,
            resumen={},
            detalle=str(exc),
        )
        _alertar(
            engine,
            f"{etiqueta}: la carga se negó a reemplazar",
            str(exc),
            f"{fuente}:rechazo:{exc}",
        )
        return {"estado": "rechazada", "detalle": str(exc)}
    except _Falla as exc:
        logger.error("%s: falló %s — %s", etiqueta, exc.motivo, exc.detalle)
        anotar_carga(
            engine,
            fuente=fuente,
            estado="fallida",
            inicio=inicio,
            manifiesto_=None,
            resumen={},
            detalle=str(exc),
        )
        _alertar(
            engine, f"{etiqueta}: falló {exc.motivo}", exc.detalle, f"{fuente}:falla:{exc.motivo}"
        )
        raise
    except SoftTimeLimitExceeded:
        logger.error("%s: se pasó del tiempo", etiqueta)
        raise
    except httpx.HTTPError as exc:
        logger.warning("%s: la fuente no respondió (%s); reintento", etiqueta, exc)
        raise tarea.retry(exc=exc, countdown=600) from exc
    except Exception as exc:
        logger.exception("%s: falló", etiqueta)
        anotar_carga(
            engine,
            fuente=fuente,
            estado="fallida",
            inicio=inicio,
            manifiesto_=None,
            resumen={},
            detalle=f"{type(exc).__name__}: {exc}",
        )
        _alertar(
            engine, f"{etiqueta}: falló la carga", str(exc), f"{fuente}:error:{type(exc).__name__}"
        )
        raise


def _latir(engine: Engine, source_ids: Iterable[str]) -> None:
    from app.application.quality.heartbeat import record_ingest

    for source_id in source_ids:
        record_ingest(engine, f"{PORTAL}::{source_id}")


@celery_app.task(
    name="openarg.ingest_ddjj_oa",
    bind=True,
    max_retries=2,
    soft_time_limit=3600,
    time_limit=3900,
)
def ingest_ddjj_oa(
    self,
    *,
    forzar: bool = False,
    permitir_menos_filas: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Carga las DDJJ de la Oficina Anticorrupción si la fuente cambió.

    - `forzar`: baja y reescribe aunque el manifiesto sea el de la última carga.
    - `permitir_menos_filas`: deja reemplazar aunque desaparezca un año o caiga
      más de 10 %. Sólo a mano, sabiendo por qué.
    - `dry_run`: consulta la fuente, baja y elige los archivos, y dice qué
      cargaría sin escribir nada.
    """
    engine = get_sync_engine()

    def cuerpo() -> tuple[str, list[dict[str, str]] | None, dict[str, Any]]:
        with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=True) as client:
            recursos = consultar_paquete(client)
            mani = manifiesto(recursos)
            if (
                not forzar
                and not dry_run
                and mani == ultima_carga(engine, oa.FUENTE)
                and tablas_en_orden(engine)
            ):
                _latir(engine, ("ddjj-declaraciones", "ddjj-bienes", "ddjj-deudas"))
                logger.info("DDJJ OA: sin cambios en la fuente")
                return "al_dia", mani, {}
            with tempfile.TemporaryDirectory(prefix="ddjj_oa_") as carpeta:
                archivos, salteados = bajar_archivos(client, recursos, carpeta)
                plan = armar_plan(archivos)
                resumen: dict[str, Any] = {"plan": plan.como_dict(), "salteados": salteados}
                if not plan.principal:
                    raise _Falla("plan", "ningún archivo principal utilizable")
                if dry_run:
                    return "simulada", mani, resumen
                resumen.update(cargar_oa(engine, plan, permitir_menos_filas=permitir_menos_filas))
        if resumen.get("corregidas_x10"):
            # Con la elección del corte no debería quedar ninguna: si quedan, la
            # OA publicó un año sólo en un corte malo (o uno nuevo con el error).
            _alertar(
                engine,
                "DDJJ OA: totales corregidos ×10 contra el detalle",
                f"{resumen['corregidas_x10']} declaraciones; el plan está en ddjj_cargas",
                f"x10:{','.join(sorted(plan.como_dict()['principal']))}",
            )
        return "escrita", mani, resumen

    return _ejecutar(self, engine, oa.FUENTE, "DDJJ OA", cuerpo)


@celery_app.task(
    name="openarg.ingest_ddjj_caba",
    bind=True,
    max_retries=2,
    soft_time_limit=900,
    time_limit=1000,
)
def ingest_ddjj_caba(
    self,
    *,
    forzar: bool = False,
    permitir_menos_filas: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Carga las DDJJ de la Ciudad de Buenos Aires si algún CSV cambió.

    Los CSV son chicos (~12 MB todos), así que siempre se bajan y se compara su
    hash con el de la última carga escrita: el de 2026 crece durante el año y
    CKAN no siempre actualiza la fecha del recurso. Mismos parámetros que
    `ingest_ddjj_oa`.
    """
    engine = get_sync_engine()

    def cuerpo() -> tuple[str, list[dict[str, str]] | None, dict[str, Any]]:
        with httpx.Client(timeout=_TIMEOUT_S, follow_redirects=True) as client:
            recursos = [
                r
                for r in consultar_paquete(client, caba.PAQUETE, caba.API_PAQUETE)
                if r.formato == "csv" and caba.anio_de_url(r.url) is not None
            ]
            if not recursos:
                raise _Falla("paquete", f"{caba.PAQUETE} no tiene CSV anuales")
            recursos.sort(key=lambda r: caba.anio_de_url(r.url) or 0)
            with tempfile.TemporaryDirectory(prefix="ddjj_caba_") as carpeta:
                archivos: list[caba.ArchivoCaba] = []
                mani: list[dict[str, str]] = []
                for i, rec in enumerate(recursos):
                    destino = os.path.join(carpeta, f"{i:02d}.csv")
                    _bajar(client, rec.url, destino)
                    archivos.append(
                        caba.ArchivoCaba(
                            ruta=destino, url=rec.url, corte=_fecha_iso(rec.modificado)
                        )
                    )
                    mani.append({"url": rec.url, "sha256": _sha256(destino)})
                if (
                    not forzar
                    and not dry_run
                    and mani == ultima_carga(engine, caba.FUENTE)
                    and tablas_en_orden(engine, (TABLA_DECLARACIONES,))
                ):
                    _latir(engine, ("ddjj-declaraciones",))
                    logger.info("DDJJ CABA: sin cambios en la fuente")
                    return "al_dia", mani, {}
                if dry_run:
                    formatos = {
                        a.url.rsplit("/", 1)[-1]: (
                            "ancho" if caba.es_formato_ancho(caba.leer(a.ruta)[0]) else "largo"
                        )
                        for a in archivos
                    }
                    return "simulada", mani, {"archivos": formatos}
                resumen = cargar_caba(engine, archivos, permitir_menos_filas=permitir_menos_filas)
        return "escrita", mani, resumen

    return _ejecutar(self, engine, caba.FUENTE, "DDJJ CABA", cuerpo)


# ── retiro de las tablas del colector genérico ───────────────────────────────

# Sin esquema: `cache_drop_audit` está en `raw` o en `public` según la base, y el
# search_path del engine (public, raw) la encuentra en cualquiera de los dos.
_AUDITAR_RETIRO = text(
    """
    INSERT INTO cache_drop_audit (object_name, reason, actor, extra, dropped_at)
    VALUES (:obj, 'reemplazada_por_ddjj', 'ddjj_tasks.retirar_ddjj_genericas',
            CAST(:extra AS jsonb), now())
    """
)


@dataclass
class TablaVieja:
    esquema: str
    tabla: str
    filas: int
    por: str  # "catalogo" (dataset reemplazado) o "firma" (columnas de la OA)


def tablas_viejas(engine: Engine) -> tuple[list[TablaVieja], list[str]]:
    """Las tablas que armó el colector genérico con las DDJJ, y los ids de sus datasets.

    Dos caminos:

    - **Catálogo:** las que nombra `cached_datasets` para los datasets de
      `reemplazos.REEMPLAZADOS`.
    - **Firma:** las que tienen las columnas de la OA (`dj_id` y
      `funcionario_apellido_nombre`), aunque no estén en el catálogo. Son los
      pedazos de ZIP (`_s<hash>`) y los desbordes (`_r<id>`) que registró el pase
      de huérfanas.

    No alcanza con el nombre: `datos_gob_ar__declaraciones_juradas_patrimoniales__*`
    también es el dataset de ARSAT, que es otra planilla (Apellido, Nombre, Cargo,
    Tipo, Cumplimiento) y se queda. Nuestras `cache_ddjj_*` no tienen esas
    columnas y además se excluyen por nombre.
    """
    from app.application.ddjj.reemplazos import REEMPLAZADOS

    pares = [{"p": p, "t": t} for p, t in sorted(REEMPLAZADOS)]
    with engine.connect() as conn:
        por_catalogo: list[Any] = []
        for par in pares:
            por_catalogo += conn.execute(
                text(
                    "SELECT cd.table_name, CAST(cd.dataset_id AS text) AS dataset_id "
                    "FROM raw.cached_datasets cd JOIN datasets d ON d.id = cd.dataset_id "
                    "WHERE d.portal = :p AND trim(d.title) = :t"
                ),
                par,
            ).all()
        por_firma = conn.execute(
            text(
                """
                SELECT a.table_name FROM information_schema.columns a
                JOIN information_schema.columns b
                  ON b.table_schema = a.table_schema AND b.table_name = a.table_name
                 AND b.column_name = 'funcionario_apellido_nombre'
                WHERE a.column_name = 'dj_id' AND a.table_schema IN ('raw', 'public')
                """
            )
        ).all()
        origen: dict[str, str] = {}
        for fila in por_catalogo:
            origen[str(fila.table_name)] = "catalogo"
        for fila in por_firma:
            origen.setdefault(str(fila.table_name), "firma")
        tablas: list[TablaVieja] = []
        for nombre, por in sorted(origen.items()):
            if nombre.startswith("cache_ddjj_"):
                continue
            ubicada = conn.execute(
                text(
                    """
                    SELECT n.nspname AS esquema, GREATEST(c.reltuples, 0)::bigint AS filas
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = :t AND c.relkind = 'r' AND n.nspname IN ('raw', 'public')
                    ORDER BY n.nspname = 'raw' DESC LIMIT 1
                    """
                ),
                {"t": nombre},
            ).first()
            if ubicada is not None:
                tablas.append(TablaVieja(str(ubicada.esquema), nombre, int(ubicada.filas), por))
        dataset_ids = sorted({str(f.dataset_id) for f in por_catalogo})
    return tablas, dataset_ids


def retirar(engine: Engine, *, dry_run: bool) -> dict[str, Any]:
    """Borra las tablas viejas de DDJJ con su registro, o dice cuáles borraría."""
    from app.application.catalog.registry_reconcile import require_registry

    # El mismo piso que las demás limpiezas: sin registro no se borra nada.
    require_registry(engine, task="retirar_ddjj_genericas")
    with engine.connect() as conn:
        for tabla in (TABLA_DECLARACIONES, TABLA_BIENES, TABLA_DEUDAS):
            if not _existe(conn, tabla):
                raise _Falla("retiro", f"falta raw.{tabla}: primero cargar las DDJJ propias")
    tablas, dataset_ids = tablas_viejas(engine)
    resumen: dict[str, Any] = {
        "tablas": len(tablas),
        "filas": sum(t.filas for t in tablas),
        "datasets": len(dataset_ids),
        "por_catalogo": sum(1 for t in tablas if t.por == "catalogo"),
        "por_firma": sum(1 for t in tablas if t.por == "firma"),
    }
    if dry_run:
        resumen["borraria"] = [f"{t.esquema}.{t.tabla} ({t.filas} filas, {t.por})" for t in tablas]
        return resumen
    borradas: list[str] = []
    fallidas: list[str] = []
    for t in tablas:
        try:
            with engine.begin() as conn:
                # Sin CASCADE: si una vista (un mart) depende de la tabla, falla y
                # se deja, en vez de llevarse la vista.
                conn.execute(text(f'DROP TABLE "{t.esquema}"."{t.tabla}"'))
                conn.execute(
                    text(
                        "UPDATE public.raw_table_versions SET superseded_at = now() "
                        "WHERE table_name = :t AND superseded_at IS NULL"
                    ),
                    {"t": t.tabla},
                )
                conn.execute(
                    text("DELETE FROM raw.cached_datasets WHERE table_name = :t"), {"t": t.tabla}
                )
        except Exception as exc:
            fallidas.append(f"{t.tabla}: {type(exc).__name__}")
            logger.warning("DDJJ retiro: no se pudo borrar %s", t.tabla, exc_info=True)
            continue
        borradas.append(t.tabla)
        try:
            with engine.begin() as conn:
                conn.execute(
                    _AUDITAR_RETIRO,
                    {
                        "obj": f"{t.esquema}.{t.tabla}",
                        "extra": json.dumps({"row_count": t.filas, "por": t.por}),
                    },
                )
        except Exception:
            logger.warning("DDJJ retiro: no se pudo auditar %s", t.tabla, exc_info=True)
    with engine.begin() as conn:
        # Lo que queda de los datasets reemplazados: filas del catálogo sin tabla y
        # embeddings que compiten en la búsqueda sin nada detrás.
        conn.execute(
            text("DELETE FROM raw.cached_datasets WHERE CAST(dataset_id AS text) = ANY(:ids)"),
            {"ids": dataset_ids},
        )
        conn.execute(
            text("DELETE FROM dataset_chunks WHERE CAST(dataset_id AS text) = ANY(:ids)"),
            {"ids": dataset_ids},
        )
    resumen.update({"borradas": len(borradas), "fallidas": fallidas})
    return resumen


@celery_app.task(
    name="openarg.retirar_ddjj_genericas",
    bind=True,
    soft_time_limit=1800,
    time_limit=1900,
)
def retirar_ddjj_genericas(self, *, dry_run: bool = True) -> dict[str, Any]:
    """Borra las tablas de DDJJ del colector genérico, reemplazadas por `cache_ddjj_*`.

    Por defecto sólo dice qué borraría. No está en el beat: se corre a mano una
    vez, después de cargar las DDJJ propias y de la migración 0068 (que retira
    los marts que protegían a estas tablas).
    """
    engine = get_sync_engine()
    resumen = retirar(engine, dry_run=dry_run)
    hecho = "simulado" if dry_run else "hecho"
    logger.info(
        "DDJJ retiro (%s): %s", hecho, {k: v for k, v in resumen.items() if k != "borraria"}
    )
    return resumen
