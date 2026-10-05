"""
BCRA Snapshot — cotizaciones cambiarias diarias del BCRA, con fecha e historia.

Baja las cotizaciones del último día publicado (Estadísticas Cambiarias v1.0)
y las acumula en ``raw.cache_bcra_cotizaciones``, una fila por
``(fecha, codigoMoneda)``.

Hasta el 04-oct la tabla era una foto sin fecha: el adaptador descartaba
``results.fecha``, la tarea pisaba la tabla todos los días (``to_sql``
replace, que con el mart dependiente caía a TRUNCATE + append, con una ventana
de tabla vacía en el medio) y ``datasets.last_updated_at`` decía la hora de la
corrida aunque los datos fueran del viernes (ítem 2.4 de la auditoría). Ahora:

- la tabla tiene ``fecha`` y un índice único ``(fecha, "codigoMoneda")``;
  la tarea los crea de forma idempotente (la tabla nunca la creó Alembic);
- las filas entran con ``INSERT … ON CONFLICT DO NOTHING``: un sábado o un
  feriado la API repite el último día hábil y no se duplica nada;
- ``datasets.last_updated_at`` es la fecha del último dato, no la de la
  corrida;
- el mart ``series_economicas`` (v0.4) lee sólo la última fecha.

``snapshot_bcra(backfill_desde="AAAA-MM-DD")`` completa la historia de cada
moneda desde esa fecha con el endpoint histórico, sin pisar lo que ya está.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

TABLE_NAME = "cache_bcra_cotizaciones"
SCHEMA = "raw"
UNIQUE_INDEX = "uq_cache_bcra_cotizaciones_fecha_moneda"
# Las columnas que publica la API, en el orden en que `to_sql` creó la tabla.
COLUMNS = ["codigoMoneda", "descripcion", "tipoPase", "tipoCotizacion", "fecha"]
_DOWNLOAD_URL = "https://www.bcra.gob.ar/Estadisticas/Datos_Abiertos.asp"
# Argentina no tiene horario de verano desde 2009.
_AR = timezone(timedelta(hours=-3))


def _data_as_of(fecha: date | None) -> datetime | None:
    """La medianoche de Argentina del día del dato: la misma fecha en UTC y en ART."""
    if fecha is None:
        return None
    return datetime(fecha.year, fecha.month, fecha.day, tzinfo=_AR)


def _register_dataset(
    engine,
    source_id: str,
    title: str,
    table_name: str,
    df: pd.DataFrame,
    *,
    data_as_of: datetime | None = None,
    row_count: int | None = None,
):
    """Upsert into datasets and cached_datasets tables.

    ``df`` sólo aporta las columnas (y las filas si no se pasa ``row_count``).
    ``data_as_of`` es la fecha del último dato: va a ``last_updated_at`` en
    lugar de la hora de la corrida.
    """
    portal = "bcra"
    columns = [str(c) for c in df.columns]
    columns_json = json.dumps(columns)
    rows = len(df) if row_count is None else row_count
    now = datetime.now(UTC)
    last_updated = data_as_of or now

    # IMPORTANT: the INSERT must commit BEFORE `_finalize_cached_dataset`
    # runs. The latter opens its own `engine.begin()` and inserts into
    # `cached_datasets` with a FK to `datasets.id`. If we kept everything
    # under a single `with engine.begin()`, the FK insert would happen in
    # a sibling transaction that does not see the still-uncommitted
    # parent INSERT and the constraint would fire.
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, '', 'json', :cols, :tags,
                     :last_updated, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :last_updated,
                    updated_at = :now
            """),
            {
                "sid": source_id,
                "title": title,
                "desc": f"Datos del BCRA: {title}",
                "org": "Banco Central de la República Argentina",
                "portal": portal,
                "url": _DOWNLOAD_URL,
                "cols": columns_json,
                "tags": "bcra,monetario,cambiario,finanzas",
                "now": now,
                "last_updated": last_updated,
                "rows": rows,
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

    if dataset_id:
        finalized = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=portal,
            source_id=source_id,
            table_name=table_name,
            row_count=rows,
            columns=columns,
            declared_format="json",
            download_url=_DOWNLOAD_URL,
            now=now,
        )
        if not finalized["ok"]:
            return None

    return dataset_id


def _fetch_bcra_data():
    """Fetch BCRA cotizaciones synchronously using async adapter."""
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter

    adapter = BCRAAdapter()

    async def _run():
        return await adapter.get_cotizaciones()

    return asyncio.run(_run())


def _fetch_historicas(monedas: list[str], desde: str, hasta: str) -> list[dict[str, Any]]:
    """La historia de cada moneda entre ``desde`` y ``hasta``, en un solo loop."""
    from app.domain.exceptions.connector_errors import ConnectorError
    from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter

    adapter = BCRAAdapter()

    async def _run() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for moneda in monedas:
            try:
                result = await adapter.get_cotizaciones_historicas(moneda, desde, hasta)
            except ConnectorError:
                logger.warning("BCRA backfill: no pude bajar la historia de %s", moneda)
                continue
            out.extend(result.records)
        return out

    return asyncio.run(_run())


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _normalize(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filas listas para insertar. Sin fecha o sin moneda no entran: una
    cotización que no dice de qué día es era justamente el problema."""
    rows: list[dict[str, Any]] = []
    for r in records:
        fecha = _parse_date(r.get("fecha"))
        moneda = str(r.get("codigoMoneda") or "").strip()
        if fecha is None or not moneda:
            continue
        rows.append(
            {
                "fecha": fecha,
                "codigoMoneda": moneda,
                "descripcion": r.get("descripcion"),
                "tipoPase": _to_float(r.get("tipoPase")),
                "tipoCotizacion": _to_float(r.get("tipoCotizacion")),
            }
        )
    return rows


def _ensure_table(conn) -> None:
    """La tabla con ``fecha`` y su índice único, sin tocar lo que ya está.

    La creó ``to_sql`` (no Alembic), así que el esquema lo asegura la tarea.
    Antes de cada DDL se mira el catálogo: un ``ALTER TABLE`` toma un lock
    exclusivo aunque la columna ya exista, y la tabla la leen el modo datos y
    el mart. Las filas viejas sin fecha (la foto de antes) se borran en la
    misma transacción que inserta las nuevas: nunca queda vacía.
    """
    conn.execute(text("SET LOCAL lock_timeout = '15s'"))
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
                "codigoMoneda" text,
                descripcion text,
                "tipoPase" double precision,
                "tipoCotizacion" double precision,
                fecha date
            )
            """  # noqa: S608 — nombres constantes del módulo
        )
    )
    has_fecha = conn.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :s AND table_name = :t AND column_name = 'fecha'"
        ),
        {"s": SCHEMA, "t": TABLE_NAME},
    ).first()
    if not has_fecha:
        conn.execute(text(f"ALTER TABLE {SCHEMA}.{TABLE_NAME} ADD COLUMN fecha date"))
    conn.execute(text(f"DELETE FROM {SCHEMA}.{TABLE_NAME} WHERE fecha IS NULL"))  # noqa: S608
    has_index = conn.execute(
        text("SELECT 1 FROM pg_indexes WHERE schemaname = :s AND indexname = :i"),
        {"s": SCHEMA, "i": UNIQUE_INDEX},
    ).first()
    if not has_index:
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {UNIQUE_INDEX} "
                f'ON {SCHEMA}.{TABLE_NAME} (fecha, "codigoMoneda")'
            )
        )


def _upsert(engine, rows: list[dict[str, Any]]) -> tuple[int, int, date | None]:
    """Inserta las filas nuevas. Devuelve (insertadas, total, fecha del último dato)."""
    count_sql = text(f"SELECT count(*), max(fecha) FROM {SCHEMA}.{TABLE_NAME}")  # noqa: S608
    with engine.begin() as conn:
        _ensure_table(conn)
        before, _ = conn.execute(count_sql).one()
        if rows:
            # `rowcount` de un executemany depende del driver: se cuenta antes
            # y después, dentro de la misma transacción.
            conn.execute(
                text(
                    f"""
                    INSERT INTO {SCHEMA}.{TABLE_NAME}
                        (fecha, "codigoMoneda", descripcion, "tipoPase", "tipoCotizacion")
                    VALUES (:fecha, :codigoMoneda, :descripcion, :tipoPase, :tipoCotizacion)
                    ON CONFLICT (fecha, "codigoMoneda") DO NOTHING
                    """  # noqa: S608
                ),
                rows,
            )
        total, last = conn.execute(count_sql).one()
    return int(total or 0) - int(before or 0), int(total or 0), _parse_date(last)


@celery_app.task(
    name="openarg.snapshot_bcra",
    bind=True,
    max_retries=3,
    soft_time_limit=300,
    time_limit=360,
)
def snapshot_bcra(self, backfill_desde: str | None = None):
    """Cotizaciones del último día hábil (y, si se pide, la historia desde una fecha)."""
    engine = get_sync_engine()

    try:
        cotizaciones = _fetch_bcra_data()
        rows = _normalize(cotizaciones.records)
        results: dict[str, Any] = {"tables": []}
        if not rows:
            # Sin fecha no se escribe nada: mejor un día sin dato que una foto
            # que no dice de qué día es. La tabla queda como estaba.
            logger.warning(
                "BCRA cotizaciones: la API no devolvió filas con fecha (%d registros)",
                len(cotizaciones.records),
            )
            return results

        if backfill_desde:
            hasta = max(r["fecha"] for r in rows).isoformat()
            monedas = sorted({r["codigoMoneda"] for r in rows})
            historicas = _normalize(_fetch_historicas(monedas, backfill_desde, hasta))
            logger.info(
                "BCRA backfill desde %s: %d filas de %d monedas",
                backfill_desde,
                len(historicas),
                len(monedas),
            )
            rows = historicas + rows

        inserted, total, last = _upsert(engine, rows)
        logger.info(
            "BCRA cotizaciones: %d filas nuevas, %d en total, último dato %s",
            inserted,
            total,
            last,
        )

        dataset_id = _register_dataset(
            engine,
            "bcra-cotizaciones",
            "Cotizaciones Cambiarias BCRA",
            TABLE_NAME,
            pd.DataFrame(columns=COLUMNS),
            data_as_of=_data_as_of(last),
            row_count=total,
        )
        if dataset_id and inserted:
            from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

            index_dataset_embedding.delay(dataset_id)

        # Register in `raw_table_versions` so the `series_economicas` mart
        # finds it (and refreshes it right away).
        from app.infrastructure.celery.tasks._db import register_via_b_table

        register_via_b_table(
            engine,
            resource_identity="bcra::cotizaciones",
            table_name=TABLE_NAME,
            # Registering it as `public` made the `series_economicas` mart
            # resolve to `public.cache_bcra_cotizaciones`, which does not
            # exist → build_failed (BUG-004).
            schema_name=SCHEMA,
            row_count=total,
        )
        results["tables"].append(
            {
                "table": TABLE_NAME,
                "rows": total,
                "inserted": inserted,
                "last_date": last.isoformat() if last else None,
            }
        )
        return results

    except SoftTimeLimitExceeded:
        logger.error("BCRA snapshot timed out")
        raise
    except Exception as exc:
        logger.exception("BCRA snapshot failed")
        raise self.retry(exc=exc, countdown=60)
