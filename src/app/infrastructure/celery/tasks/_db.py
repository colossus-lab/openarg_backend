"""Shared sync engine for Celery tasks."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

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

        @event.listens_for(_engine, "connect")
        def _set_default_search_path(dbapi_conn, _connection_record):
            # The DB-level `ALTER DATABASE openarg_staging SET search_path
            # = raw, public, "$user"` workaround was fragile — under
            # pool checkout / DISCARD ALL semantics SQLAlchemy can clear
            # session settings, leaving `cached_datasets` and other
            # raw-only tables unresolvable. Set it explicitly on every
            # new pool connection. Order `public, raw` keeps tables
            # that exist in BOTH schemas (mart_sample_queries 484 vs
            # 225, langgraph checkpoints 205 vs 78) resolving to public
            # canonical, with raw-only tables found via fallback.
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("SET search_path = public, raw")
            except Exception:
                _logger.warning(
                    "Failed to set search_path on new connection",
                    exc_info=True,
                )
            finally:
                cursor.close()

    return _engine


def _default_parser_version() -> str:
    from app.application.catalog.parser_fingerprint import parser_fingerprint

    return os.getenv("OPENARG_PARSER_VERSION") or parser_fingerprint()


def _default_normalization_version() -> str:
    from app.application.catalog.parser_fingerprint import normalization_fingerprint

    return os.getenv("OPENARG_NORMALIZATION_VERSION") or normalization_fingerprint()


def _report_if_degenerate(
    engine: Engine,
    *,
    resource_identity: str,
    schema_name: str,
    table_name: str,
    version: int,
    row_count: int | None,
) -> None:
    """Tell a person when a connector just published nothing. Never raises."""
    try:
        from app.application.collection.batch_guard import check_after_write

        verdict = check_after_write(
            engine,
            resource_identity=resource_identity,
            schema_name=schema_name,
            table_name=table_name,
            version=version,
            row_count=row_count,
        )
        if verdict.ok:
            return

        _logger.error("vía-B batch looks degenerate: %s — %s", verdict.table, verdict.reason)
        from app.application.quality.alerting import Alert, notify

        notify(
            engine,
            [
                Alert(
                    kind="via_b_degenerate",
                    # Identity of the resource, so a source that stays broken is
                    # reported once rather than on every ingest.
                    key=resource_identity,
                    title=f"{resource_identity[:60]} publicó algo vacío",
                    detail=verdict.reason[:300],
                )
            ],
            heading="OpenArg · un conector publicó algo vacío",
        )
    except Exception:
        _logger.debug("batch guard: skipped for %s", table_name, exc_info=True)


def register_via_b_table(
    engine: Engine,
    *,
    resource_identity: str,
    table_name: str,
    schema_name: str = "public",
    version: int = 1,
    row_count: int | None = None,
    parser_version: str | None = None,
    normalization_version: str | None = None,
) -> bool:
    """Register a vía-B table (transparency / senado / staff / bcra ingest)
    in `public.raw_table_versions` so the medallion mart layer can `live_table()`
    it and the Serving Port can find it.

    Returns whether the registry row was written. The failure is still only
    logged — for most connectors the data write is the job and the registry is
    best-effort — but a caller that cannot call itself done without the row
    (series de tiempo: no row means no per-resource heartbeat and no
    `catalog_resources` reconciliation) can now see it. Before this the
    `uq_raw_table_versions_table_name` clash of the series ETL — a second
    identity for an already-registered table — was swallowed here and the task
    reported success.

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
        return False
    registered = False
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO public.raw_table_versions (
                        resource_identity, version, schema_name, table_name,
                        row_count, parser_version, normalization_version
                    ) VALUES (:rid, :v, :sch, :tn, :rc, :pv, :nv)
                    ON CONFLICT (resource_identity, version) DO UPDATE SET
                        row_count = COALESCE(EXCLUDED.row_count, public.raw_table_versions.row_count),
                        schema_name = EXCLUDED.schema_name,
                        table_name = EXCLUDED.table_name,
                        parser_version = EXCLUDED.parser_version,
                        normalization_version = EXCLUDED.normalization_version
                    """
                ),
                {
                    # Vía-B connectors do not run the collector's parser, but
                    # they do run its normalisation, and a row with no
                    # provenance is a row G1 can say nothing about. Recording
                    # the fingerprints here is what stops those tables being
                    # permanently unattributable.
                    "pv": parser_version or _default_parser_version(),
                    "nv": normalization_version or _default_normalization_version(),
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
        # Mark that this resource arrived. Without it there is no record of a
        # successful ingest anywhere: this function is idempotent on
        # `(resource_identity, version)` and never touches `created_at`, so for
        # the connectors that overwrite in place the registry's date is the day
        # the resource was *first* seen. A source that quietly stops arriving —
        # or that keeps being refused by a guard — is invisible without this.
        try:
            from app.application.quality.heartbeat import record_ingest

            record_ingest(engine, resource_identity)
        except Exception:
            _logger.debug("heartbeat skipped for %s", resource_identity, exc_info=True)

        # Look at what was just published, for every vía-B connector at once.
        # The thirteen of them are too different to share a schema, but they all
        # end here — and by this point the registry knows what the previous
        # version held and the table can be sampled. Reports only: the write has
        # already happened, so this buys seconds instead of weeks, not a veto.
        _report_if_degenerate(
            engine,
            resource_identity=resource_identity,
            schema_name=schema_name,
            table_name=table_name,
            version=version,
            row_count=row_count,
        )

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
                        FROM raw.cached_datasets cd
                        JOIN datasets d ON d.id = cd.dataset_id
                        WHERE cd.table_name = :tn
                          AND cr.resource_identity = d.portal || '::' || d.source_id
                          -- Una cuarentena no se deshace sola. Esta
                          -- reconciliación y `repair/quarantine` escriben el
                          -- mismo campo con intenciones opuestas, y sin esto
                          -- gana siempre la que dice "está todo bien": las 5
                          -- tablas retiradas del servicio volvieron a `ready`
                          -- en minutos, sin que nada lo dijera. Sólo un arreglo
                          -- exitoso (`release`) o una persona la levantan.
                          AND cr.materialization_status <> 'materialization_corrupted'
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

    return registered


_BCRA_PROD_DEBOUNCE_SECONDS = 110
_RECENT_MART_REFRESH_SUCCESS_SECONDS = 110


def _mart_refresh_recently_succeeded(
    *,
    last_refresh_status: str | None,
    last_refreshed_at,
    recent_window_seconds: int = _RECENT_MART_REFRESH_SUCCESS_SECONDS,
) -> bool:
    """Return True when a mart was refreshed successfully very recently.

    Used as a second guard on top of the task-id bucket debounce: if the
    previous build/refresh already landed within the current short window,
    re-enqueuing another refresh is usually redundant churn.
    """
    if last_refresh_status not in {"built", "refreshed"}:
        return False
    if last_refreshed_at is None:
        return False
    refreshed_at = last_refreshed_at
    if refreshed_at.tzinfo is None:
        refreshed_at = refreshed_at.replace(tzinfo=UTC)
    return refreshed_at >= datetime.now(UTC) - timedelta(seconds=recent_window_seconds)


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
                    "SELECT mart_id, last_refresh_status, last_refreshed_at "
                    "FROM mart_definitions "
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
        _logger.debug("Could not import refresh_mart; skipping auto-refresh", exc_info=True)
        return

    import time as _time

    bucket = int(_time.time() / _BCRA_PROD_DEBOUNCE_SECONDS)
    for r in rows:
        mart_id = str(r.mart_id)
        if _mart_refresh_recently_succeeded(
            last_refresh_status=getattr(r, "last_refresh_status", None),
            last_refreshed_at=getattr(r, "last_refreshed_at", None),
        ):
            _logger.debug(
                "Skipped refresh_mart dispatch for %s (recent successful refresh)",
                mart_id,
            )
            continue
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
    row in `public.raw_table_versions` (most do).

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
        raise ValueError(f"Refusing to TRUNCATE {table_name!r}: name fails identifier guard")
    with engine.begin() as conn:
        # quote_ident-equivalent: the regex above already rejects backticks
        # and double quotes, so the literal `"…"` wrapper is purely cosmetic
        # for parser consistency.
        conn.execute(text(f'TRUNCATE TABLE "{table_name}"'))  # noqa: S608


class TableReplaceRefused(RuntimeError):
    """The new contents would break what reads the table, so nothing was changed."""


# Everything built on a table, however deep, shallowest first: a matview over a
# view over the table has to be refreshed too, and after whatever it reads.
_SQL_DEPENDENTS = text(
    """
    WITH RECURSIVE dep AS (
        SELECT to_regclass(:q)::oid AS oid, 0 AS depth
        UNION ALL
        SELECT v.oid, dep.depth + 1
        FROM dep
        JOIN pg_depend d ON d.refobjid = dep.oid
                        AND d.classid = 'pg_rewrite'::regclass
                        AND d.refclassid = 'pg_class'::regclass
        JOIN pg_rewrite w ON w.oid = d.objid
        JOIN pg_class v ON v.oid = w.ev_class
        WHERE v.oid <> dep.oid AND dep.depth < 10
    )
    SELECT c.oid::regclass::text AS name, c.relkind, c.relispopulated, max(dep.depth) AS depth
    FROM dep JOIN pg_class c ON c.oid = dep.oid
    WHERE dep.depth > 0
    GROUP BY c.oid, c.relkind, c.relispopulated
    ORDER BY depth, name
    """
)

# How far a monthly load may move a table with a mart on it. A series grows by a
# month at a time: the IPC, stale for six months, goes up 5 %.
_MIN_ROWS_KEPT = 0.9
_MAX_ROWS_GROWTH = 1.5
# How far a matview may move beyond what its table moved, as a share of the table.
_MATVIEW_SLACK = 0.1


def _refuse_if_matview_moved(
    source: str, matview: str, *, before: int, after: int, old_rows: int, new_rows: int
) -> None:
    """Refuse a refresh whose row count the change in the table does not explain.

    A matview over several tables does not go empty when it loses one of them.
    A `valor` that stops passing its filter took one source of three out of a
    test mart, from 300 rows to 200, with the table still full. The mart has no
    record of which row came from where, but rows it gained or lost beyond what
    the table gained or lost came from the new contents being read differently.
    """
    if before and not after:
        raise TableReplaceRefused(f"{source}: the new rows would empty {matview}")
    slack = old_rows * _MATVIEW_SLACK
    lost, gained = before - after, after - before
    if lost > max(old_rows - new_rows, 0) + slack:
        raise TableReplaceRefused(
            f"{source}: {matview} would lose {lost} of {before} rows "
            f"while the table goes from {old_rows} to {new_rows}"
        )
    if gained > max(new_rows - old_rows, 0) + slack:
        raise TableReplaceRefused(
            f"{source}: {matview} would gain {gained} over {before} rows "
            f"while the table goes from {old_rows} to {new_rows}"
        )


def replace_table_keeping_dependents(
    engine: Engine, df, table_name: str, *, schema: str = "raw"
) -> list[str]:
    """Put `df` in `<schema>.<table_name>` without losing what is built on it.

    `to_sql(if_exists="replace")` is a DROP, and Postgres refuses it while a
    matview reads the table. `ingest_indec` hit that on the first sheet of the
    IPC file every month after the mart was built, so `mart.inflacion_argentina`
    stayed at March 2026 and the failure went no further than a warning.

    With no dependents this is still that replace. With dependents the table
    keeps its identity: the rows change inside one transaction, which also
    refreshes every matview over it. Any failure rolls all of it back, so readers
    go on seeing the previous table and the previous mart. Refused, without
    touching anything:

    - a column change: a matview names its columns, and whether it can survive
      the change is for its definition to decide, not for a monthly load;
    - a row count outside 90-150 % of the current one, the signature of a
      truncated, misread or repeated file;
    - a refresh that empties a matview, or moves it further than the table moved
      (`_refuse_if_matview_moved`).

    A refusal is for a person to look at. If the new contents are right after
    all, the way past it is to drop the mart, load, and run `build_mart`.

    `DELETE`, not `TRUNCATE`. `TRUNCATE` takes ACCESS EXCLUSIVE on the table
    before the refresh locks the matview, while a concurrent `refresh_mart` locks
    the matview and then reads the table: that order deadlocks. It would also
    block every reader of the table for the whole transaction.

    Returns the matviews refreshed.
    """
    if df.empty:
        raise TableReplaceRefused(f"{schema}.{table_name}: refusing to replace with no rows")
    qualified = f'{schema}."{table_name}"'
    refreshed: list[str] = []
    with engine.begin() as conn:
        # Waiting behind a long query would queue every other reader behind us.
        conn.execute(text("SET LOCAL lock_timeout = '30s'"))
        dependents = conn.execute(_SQL_DEPENDENTS, {"q": qualified}).fetchall()
        if not dependents:
            df.to_sql(table_name, conn, schema=schema, if_exists="replace", index=False)
            return refreshed

        names = ", ".join(d.name for d in dependents)
        current = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = :t"
                ),
                {"s": schema, "t": table_name},
            )
        }
        incoming = {str(c) for c in df.columns}
        if incoming != current:
            raise TableReplaceRefused(
                f"{qualified}: columns changed under {names} "
                f"(gone: {sorted(current - incoming)}, new: {sorted(incoming - current)})"
            )
        previous_rows = conn.execute(text(f"SELECT count(*) FROM {qualified}")).scalar() or 0  # noqa: S608
        if previous_rows and not (
            previous_rows * _MIN_ROWS_KEPT <= len(df) <= previous_rows * _MAX_ROWS_GROWTH
        ):
            raise TableReplaceRefused(
                f"{qualified}: {len(df)} rows would replace {previous_rows} under {names}"
            )

        def rows_in(name: str) -> int:
            return conn.execute(text(f"SELECT count(*) FROM {name}")).scalar() or 0  # noqa: S608

        matviews = [d for d in dependents if d.relkind == "m"]
        rows_before = {m.name: rows_in(m.name) for m in matviews if m.relispopulated}
        conn.execute(text(f"DELETE FROM {qualified}"))  # noqa: S608
        df.to_sql(table_name, conn, schema=schema, if_exists="append", index=False)
        for m in matviews:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {m.name}"))
            if m.name in rows_before:
                _refuse_if_matview_moved(
                    qualified,
                    m.name,
                    before=rows_before[m.name],
                    after=rows_in(m.name),
                    old_rows=previous_rows,
                    new_rows=len(df),
                )
            refreshed.append(m.name)
    return refreshed
