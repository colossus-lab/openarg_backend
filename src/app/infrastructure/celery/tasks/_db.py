"""Shared sync engine for Celery tasks."""

from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db"

_engine: Engine | None = None

_logger = logging.getLogger(__name__)

_UPSTREAM_ERROR_MARKERS = (
    "timeout",
    "timed out",
    "service unavailable",
    "temporarily unavailable",
    "connection reset",
    "connection refused",
    "remoteprotocolerror",
    "connecterror",
    "readtimeout",
    "connecttimeout",
    "no route to host",
    "name or service not known",
    "dns",
    "ssl",
    "http",
    "403",
    "404",
    "429",
    "502",
    "503",
    "504",
)


def get_sync_engine() -> Engine:
    global _engine  # noqa: PLW0603
    if _engine is None:
        url = os.getenv("DATABASE_URL", _DEFAULT_DB_URL)
        from app.setup.config.constants import DB_STATEMENT_TIMEOUT_SECONDS

        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=5,
            max_overflow=3,
        )

        # Default `statement_timeout` protects every Celery task from
        # unbounded queries. We can't pass it through `connect_args.options`
        # because the staging deploy front-ends Postgres with PgBouncer in
        # transaction-pooling mode, and PgBouncer rejects unknown startup
        # parameters with `unsupported startup parameter in options:
        # statement_timeout`. Instead, fire a `SET statement_timeout` on
        # every freshly checked-out connection — that runs inside the
        # session and PgBouncer passes it through.
        #
        # Defensive: if the SET fails (network blip, server overload),
        # we re-raise so SQLAlchemy invalidates the connection and
        # discards it from the pool. The previous version swallowed the
        # error in a finally-only block, which left the conn in the
        # pool without timeout armed — next query would have unbounded
        # runtime, exactly the failure mode we're trying to prevent.
        from sqlalchemy import event

        timeout_ms = DB_STATEMENT_TIMEOUT_SECONDS * 1000

        @event.listens_for(_engine, "connect")
        def _set_default_timeout(dbapi_conn, _connection_record):
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute(f"SET statement_timeout = {timeout_ms}")
            except Exception:
                # Mark the connection invalid so SQLAlchemy throws it
                # away and the next checkout creates a fresh one.
                # Re-raising from a `connect` listener is exactly how
                # SQLAlchemy expects you to signal "this conn is bad".
                _logger.warning(
                    "Failed to set statement_timeout on new connection; "
                    "discarding so the pool retries fresh",
                    exc_info=True,
                )
                raise
            finally:
                cursor.close()
    return _engine


def register_via_b_table(
    engine: Engine,
    *,
    resource_identity: str,
    table_name: str,
    schema_name: str = "public",
    version: int = 1,
    row_count: int | None = None,
) -> None:
    """Register a vía-B table (transparency / senado / staff / bcra ingest)
    in `raw_table_versions` so the medallion mart layer can `live_table()`
    it and the Serving Port can find it.

    Vía-B tables are populated by specialized connectors (`staff_tasks`,
    `senado_tasks`, `presupuesto_tasks`, `bcra_tasks`...) that bypass the
    collector's raw layer. Without this registration the marts that target
    those portals always read 0 rows.

    Idempotent on `(resource_identity, version)`: re-running the parent
    task is a no-op for the registry but still updates `row_count` so a
    growing table reports the latest size.

    After a successful registration, dispatches `refresh_mart` for every
    mart whose `source_portals` contains the portal embedded in
    `resource_identity` (the substring before `::`). This closes the
    auto-refresh loop for vía-B writers — without it, marts like
    `series_economicas` (consuming `bcra::cotizaciones`) stay stale
    forever even though the underlying table updates daily.
    """
    if not resource_identity or not table_name:
        return
    registered = False
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO raw_table_versions (
                        resource_identity, version, schema_name, table_name,
                        row_count
                    ) VALUES (:rid, :v, :sch, :tn, :rc)
                    ON CONFLICT (resource_identity, version) DO UPDATE SET
                        row_count = COALESCE(EXCLUDED.row_count, raw_table_versions.row_count),
                        schema_name = EXCLUDED.schema_name,
                        table_name = EXCLUDED.table_name
                    """
                ),
                {
                    "rid": resource_identity,
                    "v": version,
                    "sch": schema_name,
                    "tn": table_name,
                    "rc": row_count,
                },
            )
            registered = True
    except Exception:
        # The auxiliary connector's main job is the data write; the
        # registry update is best-effort. Log but do not raise.
        _logger.warning(
            "Could not register vía-B table %s.%s as %s",
            schema_name,
            table_name,
            resource_identity,
            exc_info=True,
        )

    if registered:
        # Reconcile the canonical `catalog_resources` row.
        #
        # Vía-B writers (BCRA, presupuesto, senado) register the rtv under
        # a curated identity like `bcra::cotizaciones`, but the catalog
        # row created by the standard collector path uses the literal
        # `{portal}::{source_id}` from `datasets` (e.g.
        # `bcra::bcra-cotizaciones`). Both keys point at the SAME physical
        # table, so without this UPDATE the catalog's `materialized_table_name`
        # stays NULL or stale and the serving port can't resolve the row
        # back to a queryable table. Marts continue to use the rtv identity
        # directly via `live_table()`, so we don't rewrite either side's
        # identity — just sync `materialized_table_name` so both views
        # agree on what physical table the resource maps to.
        try:
            qualified = f'{schema_name}."{table_name}"'
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE catalog_resources cr
                        SET materialized_table_name = :qn,
                            materialization_status = 'ready',
                            updated_at = NOW()
                        FROM cached_datasets cd
                        JOIN datasets d ON d.id = cd.dataset_id
                        WHERE cd.table_name = :tn
                          AND cr.resource_identity = d.portal || '::' || d.source_id
                          AND (
                              cr.materialized_table_name IS DISTINCT FROM :qn
                              OR cr.materialization_status IS DISTINCT FROM 'ready'
                          )
                        """
                    ),
                    {"qn": qualified, "tn": table_name},
                )
        except Exception:
            _logger.debug(
                "Could not reconcile catalog_resources for vía-B table %s.%s",
                schema_name,
                table_name,
                exc_info=True,
            )

        _trigger_marts_for_portal(engine, resource_identity)


_BCRA_PROD_DEBOUNCE_SECONDS = 110


def _trigger_marts_for_portal(engine: Engine, resource_identity: str) -> None:
    """Dispatch `refresh_mart` for every mart whose `source_portals`
    contains the portal of this resource_identity.

    The dispatch is deduplicated by `task_id` bucketed in 110-second
    windows: 50 vía-B writes within the same window produce one
    `refresh_mart` per matching mart, not 50. Mirrors the debounce
    pattern used by `_apply_cached_outcome` for vía-A landings so both
    paths share the same refresh-frequency budget.
    """
    portal = resource_identity.split("::", 1)[0]
    if not portal:
        return

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT mart_id FROM mart_definitions "
                    "WHERE :portal = ANY(source_portals)"
                ),
                {"portal": portal},
            ).fetchall()
            conn.rollback()
    except Exception:
        _logger.debug(
            "Could not enumerate marts for portal %s; skipping auto-refresh",
            portal,
            exc_info=True,
        )
        return

    if not rows:
        return

    try:
        # Lazy import: refresh_mart lives in mart_tasks which imports
        # collector_tasks, which imports this module. Top-level import
        # would create a cycle.
        from app.infrastructure.celery.tasks.mart_tasks import refresh_mart
    except Exception:
        _logger.debug(
            "Could not import refresh_mart; skipping auto-refresh", exc_info=True
        )
        return

    import time as _time

    bucket = int(_time.time() / _BCRA_PROD_DEBOUNCE_SECONDS)
    for r in rows:
        mart_id = str(r.mart_id)
        try:
            refresh_mart.apply_async(
                args=[mart_id],
                task_id=f"refresh_mart:viab:{mart_id}:{bucket}",
            )
        except Exception:
            # Celery raises on duplicate task_id within same bucket — that
            # IS the debounce working. Log at debug, no operator action
            # needed.
            _logger.debug(
                "Skipped duplicate refresh_mart dispatch for %s (bucket=%d)",
                mart_id,
                bucket,
            )


_VALID_TABLE_NAME = __import__("re").compile(r"^[a-z_][a-z0-9_]{0,62}$")


def register_via_b_with_state(
    engine: Engine,
    *,
    dataset_id: str,
    table_name: str,
    columns: list[str],
    row_count: int,
    layout_profile: str | None = None,
    header_quality: str | None = None,
    size_bytes: int | None = None,
) -> None:
    """Persist a vía-B writer outcome through the canonical state machine.

    Vía-B writers (BCRA, presupuesto dimensions, senado, BAC) used to
    INSERT into `cached_datasets` directly with `status='ready'`,
    bypassing `_apply_cached_outcome`. That bypass meant those rows
    never got `error_category`, `layout_profile`, `header_quality`,
    `parser_version`, or any of the metadata the standard collector
    path produces — even though those connectors successfully cache
    real data. Result: ~55 presupuesto dimension tables + N BAC tables
    appear as second-class citizens in `/data/tables`, dashboards, and
    the `cleanup_invariants` sweep.

    This helper routes the success outcome through the canonical
    `_apply_cached_outcome` so the same metadata pipeline applies. The
    caller is still responsible for the actual `df.to_sql(...)` write
    AND for calling `register_via_b_table()` separately if it wants the
    row in `raw_table_versions` (most do).

    Lazy-imports collector_tasks to avoid an import cycle: that module
    imports back from `_db` for `get_sync_engine` and `safe_truncate_table`.
    """
    from app.infrastructure.celery.tasks import collector_tasks as _collector_tasks

    # Derive layout/header when the caller didn't supply them, mirroring
    # what `_finalize_cached_dataset` does for the canonical via-A path.
    derived_layout = layout_profile or (
        _collector_tasks._LAYOUT_WIDE
        if len(columns) > _collector_tasks._WIDE_LAYOUT_COLUMN_THRESHOLD
        else _collector_tasks._LAYOUT_SIMPLE
    )
    derived_header = header_quality or _collector_tasks._header_quality_label(columns)

    outcome = _collector_tasks._CollectorRunOutcome(
        result_kind=_collector_tasks._OUTCOME_MATERIALIZED_READY,
        cached_status="ready",
        error_message=None,
        retry_increment=0,
        should_prune_open=False,
    )

    _collector_tasks._apply_cached_outcome(
        engine,
        dataset_id=dataset_id,
        outcome=outcome,
        table_name=table_name,
        row_count=row_count,
        columns=columns,
        size_bytes=size_bytes,
        layout_profile=derived_layout,
        header_quality=derived_header,
        # Vía-B does NOT use the raw schema (writes to public.cache_*),
        # so raw_schema/raw_version stay None and the atomic raw
        # promotion path is skipped. The caller calls
        # `register_via_b_table()` separately for the rtv entry.
        raw_schema=None,
        raw_version=None,
        resource_identity=None,
    )


def _via_b_result_kind_for_error(error_message: str) -> str:
    normalized = (error_message or "").lower()
    if any(marker in normalized for marker in _UPSTREAM_ERROR_MARKERS):
        from app.infrastructure.celery.tasks import collector_tasks as _collector_tasks

        return _collector_tasks._OUTCOME_RETRYABLE_UPSTREAM
    from app.infrastructure.celery.tasks import collector_tasks as _collector_tasks

    return _collector_tasks._OUTCOME_RETRYABLE_MATERIALIZATION


def register_via_b_error(
    engine: Engine,
    *,
    dataset_id: str,
    table_name: str,
    error_message: str,
) -> None:
    """Persist a vía-B writer error through the canonical state machine.

    Sister of `register_via_b_with_state`: same intent (route to the
    canonical pipeline so retry_count, error_category, status
    transitions are handled centrally), but for failure outcomes. The
    earlier BAC bypass in `bac_tasks.py:281-293` issued a raw SQL
    UPDATE that re-implemented the retry+permanently_failed transition
    inline, which inevitably drifted from `_apply_cached_outcome` as
    the state machine evolved.
    """
    from app.infrastructure.celery.tasks import collector_tasks as _collector_tasks

    outcome = _collector_tasks._CollectorRunOutcome(
        result_kind=_via_b_result_kind_for_error(error_message),
        cached_status="error",
        error_message=error_message[:500],
        retry_increment=1,
        should_prune_open=False,
    )
    _collector_tasks._apply_cached_outcome(
        engine,
        dataset_id=dataset_id,
        outcome=outcome,
        table_name=table_name,
    )


def safe_truncate_table(engine: Engine, table_name: str) -> None:
    """Issue `TRUNCATE TABLE <table>` with a strict charset guard so the
    f-string we still need (Postgres TRUNCATE doesn't accept parameterized
    identifiers) cannot be turned into SQL injection by a poisoned name.

    Raises `ValueError` when `table_name` doesn't match
    `[a-z_][a-z0-9_]{0,62}`. All call sites generate names through
    `_sanitize_*` helpers that already produce that charset, so a failure
    here means upstream gave us something genuinely unsafe.
    """
    if not _VALID_TABLE_NAME.match(table_name):
        raise ValueError(
            f"Refusing to TRUNCATE {table_name!r}: name fails identifier guard"
        )
    with engine.begin() as conn:
        # quote_ident-equivalent: the regex above already rejects backticks
        # and double quotes, so the literal `"…"` wrapper is purely cosmetic
        # for parser consistency.
        conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))  # noqa: S608
