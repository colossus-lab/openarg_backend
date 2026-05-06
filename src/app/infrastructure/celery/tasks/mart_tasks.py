"""MASTERPLAN Fase 3 — build + refresh marts.

Two tasks:
  - `openarg.build_mart(mart_id)` — DROP + CREATE the materialized view from
    its YAML, install canonical-column COMMENTs, register in `mart_definitions`.
  - `openarg.refresh_mart(mart_id)` — REFRESH MATERIALIZED VIEW [CONCURRENTLY].

Auto-trigger: `staging_promotion._record_state` enqueues `refresh_mart` for
every mart whose `sources` includes the contract that just landed, when
the env flag `OPENARG_AUTO_REFRESH_MARTS=1` is set. That makes the chain
raw → staging → mart fully reactive.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from pathlib import Path

from sqlalchemy import text

from app.application.marts import (
    Mart,
    build_comment_sql,
    build_create_view_sql,
    load_all_marts,
    load_mart,
)
from app.application.marts.builder import build_refresh_sql
from app.application.marts.sql_macros import (
    MacroResolutionError,
    resolve_macros,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_DEFAULT_MARTS_DIR = Path(
    os.getenv("OPENARG_MARTS_DIR")
    or Path(__file__).resolve().parents[5] / "config" / "marts"
)


def _find_mart_yaml(mart_id: str, marts_dir: Path) -> Path | None:
    # Defensive: if the marts dir is missing (overlay mishap, image build
    # without config/marts/, mounted-volume config), `iterdir()` raises
    # `FileNotFoundError` *before* the caller can return a clean
    # `{"status": "not_found"}`. Returning None here lets build_mart and
    # refresh_mart degrade with a meaningful status instead of crashing.
    if not marts_dir.exists() or not marts_dir.is_dir():
        logger.warning("Marts dir missing or not a directory: %s", marts_dir)
        return None
    for entry in marts_dir.iterdir():
        if entry.suffix.lower() not in {".yaml", ".yml"}:
            continue
        if entry.stem == mart_id:
            return entry
    return None


def _mart_lock_key(mart_id: str) -> int:
    """Stable 63-bit advisory-lock key for a mart_id.

    Postgres `pg_advisory_xact_lock` takes a `bigint`. We hash the mart_id
    via blake2b (8 bytes → int63) so two builders for the SAME mart wait
    on the same lock; different mart_ids never collide.
    """
    import hashlib

    digest = hashlib.blake2b(mart_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


def _normalize_sql(sql: str | None) -> str:
    """Collapse all whitespace to a single space so two SQLs that differ
    only in indentation, trailing newlines, or formatting are treated as
    equal. Used by `refresh_mart` to decide between a cheap REFRESH and a
    full DROP+CREATE — without normalization, a YAML edit that adjusts
    indentation triggers an expensive rebuild for nothing.
    """
    if not sql:
        return ""
    return " ".join(sql.split())


def _pg_array(items: Iterable[str]) -> str:
    """Build a Postgres `text[]` literal: `{a,b,c}`.

    Items containing ANY character that needs quoting (whitespace,
    structural literals, OR a backslash) are double-quoted with
    embedded backslashes/double-quotes escaped. The `\\` check is
    critical: an unquoted item with a literal backslash would be
    interpreted by Postgres as an escape sequence (e.g. `\\t` → tab),
    silently corrupting the value.
    """
    items = list(items)
    if not items:
        return "{}"
    parts = []
    for s in items:
        if any(c in s for c in (",", '"', "{", "}", " ", "\\")):
            esc = s.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{esc}"')
        else:
            parts.append(s)
    return "{" + ",".join(parts) + "}"


def _build_mart_embedding_text(mart: Mart) -> str:
    """Compose the text used to embed a mart for semantic discovery.

    Includes name, description, domain, portales fuente y columnas canónicas
    con sus descripciones — todo lo que ayuda al planner a decidir si un
    mart es relevante para una pregunta del usuario.
    """
    parts: list[str] = [f"Mart: {mart.id}"]
    if mart.description:
        parts.append(mart.description)
    if mart.domain:
        parts.append(f"Dominio: {mart.domain}")
    if mart.source_portals:
        parts.append(f"Portales: {', '.join(mart.source_portals)}")
    cols = []
    for c in mart.canonical_columns:
        if c.description:
            cols.append(f"{c.name} ({c.type}): {c.description}")
        else:
            cols.append(f"{c.name} ({c.type})")
    if cols:
        parts.append("Columnas: " + "; ".join(cols))
    return ". ".join(parts)


def _compute_mart_embedding(payload_text: str) -> list[float] | None:
    """Generate a 1024-dim Cohere embedding for a mart description.

    Returns None on any failure — the caller persists NULL and the mart
    remains discoverable by name, just not via semantic similarity. Build
    must not fail because of an embedding outage.
    """
    if not payload_text:
        return None
    try:
        import boto3

        client = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        body = json.dumps(
            {
                "texts": [payload_text[:2048]],
                "input_type": "search_document",
                "truncate": "END",
            }
        )
        response = client.invoke_model(
            modelId="cohere.embed-multilingual-v3",
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        result = json.loads(response["body"].read())
        return list(result["embeddings"][0])
    except Exception:
        logger.warning("Failed to compute mart embedding", exc_info=True)
        return None


def _upsert_mart_definition(
    engine,
    mart: Mart,
    *,
    status: str,
    error: str | None = None,
    last_row_count: int | None = None,
    resolved_sql: str | None = None,
) -> None:
    # Invariant: mart_id and mart_view_name must coincide. Several call
    # sites rely on this implicitly (planner discovery surfaces mart_id
    # while sandbox executor expects `mart.<view_name>`), so divergence
    # silently produces planner hints that the executor can't run. We
    # enforce it explicitly here — if a future change wants to diverge,
    # this assert points the author at every call site that needs
    # updating (legacy_serving_adapter._discover_marts,
    # sandbox._mart_semantic_block, sandbox.list_cached_tables).
    if mart.id != mart.view_name:
        raise ValueError(
            f"Mart id/view_name mismatch: id={mart.id!r} view_name={mart.view_name!r}; "
            "this invariant is required by planner↔executor coherence"
        )
    # Persist the macro-resolved SQL when available so refresh_mart can
    # detect "raw versions changed" by comparing this to the next
    # resolution. When None (e.g. build_failed before resolution), fall
    # back to the YAML SQL.
    sql_to_persist = resolved_sql if resolved_sql is not None else mart.sql
    canonical_json = json.dumps(
        [
            {
                "name": c.name,
                "type": c.type,
                "description": c.description,
            }
            for c in mart.canonical_columns
        ]
    )
    portals_array = _pg_array(mart.source_portals)
    unique_index_array = _pg_array(mart.refresh.unique_index)
    # Compute the discovery embedding only when the build landed successfully.
    # Failed builds keep their previous embedding (or NULL) — we don't index
    # broken marts but we don't wipe a working one either.
    embedding_vec: list[float] | None = None
    if status == "built":
        embedding_vec = _compute_mart_embedding(_build_mart_embedding_text(mart))
    embedding_literal = (
        "[" + ",".join(repr(float(v)) for v in embedding_vec) + "]"
        if embedding_vec is not None
        else None
    )
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO mart_definitions (
                    mart_id, mart_schema, mart_view_name, description, domain,
                    source_portals, sql_definition, canonical_columns_json,
                    refresh_policy, unique_index_columns,
                    last_refreshed_at, last_refresh_status, last_refresh_error,
                    last_row_count, yaml_version, embedding, updated_at
                ) VALUES (
                    :id, :sch, :vn, :desc, :dom,
                    CAST(:portals AS text[]), :sql, CAST(:canonical AS jsonb),
                    :rp, CAST(:uniq AS text[]),
                    NOW(), :st, :err,
                    :lrc, :yv, CAST(:emb AS vector), NOW()
                )
                ON CONFLICT (mart_id) DO UPDATE SET
                    mart_schema = EXCLUDED.mart_schema,
                    mart_view_name = EXCLUDED.mart_view_name,
                    description = EXCLUDED.description,
                    domain = EXCLUDED.domain,
                    source_portals = EXCLUDED.source_portals,
                    sql_definition = EXCLUDED.sql_definition,
                    canonical_columns_json = EXCLUDED.canonical_columns_json,
                    refresh_policy = EXCLUDED.refresh_policy,
                    unique_index_columns = EXCLUDED.unique_index_columns,
                    last_refreshed_at = NOW(),
                    last_refresh_status = EXCLUDED.last_refresh_status,
                    last_refresh_error = EXCLUDED.last_refresh_error,
                    last_row_count = COALESCE(EXCLUDED.last_row_count,
                                              mart_definitions.last_row_count),
                    yaml_version = EXCLUDED.yaml_version,
                    embedding = COALESCE(EXCLUDED.embedding, mart_definitions.embedding),
                    updated_at = NOW()
                """
            ),
            {
                "id": mart.id,
                "sch": mart.schema_name,
                "vn": mart.view_name,
                "desc": mart.description,
                "dom": mart.domain,
                "portals": portals_array,
                "sql": sql_to_persist,
                "canonical": canonical_json,
                "rp": mart.refresh.policy,
                "uniq": unique_index_array,
                "st": status,
                "err": error,
                "lrc": last_row_count,
                "yv": mart.version,
                "emb": embedding_literal,
            },
        )


@celery_app.task(
    name="openarg.build_mart",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def build_mart(self, mart_id: str, *, marts_dir: str | None = None) -> dict:
    """(Re)create the materialized view for `mart_id` from its YAML.

    Steps:
      1. Resolve `{{ live_table(...) }}` macros against current
         `raw_table_versions` to qualified `raw."<table>"` references.
      2. DROP + CREATE MATERIALIZED VIEW with the resolved SQL.
      3. Apply CANONICAL column COMMENT statements.
      4. Count rows and persist `last_row_count` so the Serving Port can
         hide empty marts from discovery.

    Idempotent: re-running on an unchanged YAML + unchanged raw versions
    produces the same view + indexes + row count.
    """
    engine = get_sync_engine()
    base_dir = Path(marts_dir) if marts_dir else _DEFAULT_MARTS_DIR
    yaml_path = _find_mart_yaml(mart_id, base_dir)
    if yaml_path is None:
        return {"status": "not_found", "mart_id": mart_id}

    mart = load_mart(yaml_path)

    # Serialize concurrent build_mart / refresh_mart for the same mart_id
    # via session-level advisory lock. `pg_try_advisory_lock` returns
    # immediately: if another worker holds the lock for this mart, this
    # call returns False and we exit with status `skipped_locked`. The
    # next refresh trigger will retry. Better than blocking workers on
    # contended marts.
    lock_key = _mart_lock_key(mart_id)
    lock_conn = engine.connect()
    try:
        acquired = lock_conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}
        ).scalar()
        if not acquired:
            lock_conn.close()
            logger.info(
                "build_mart %s: another worker holds the build lock; skipping",
                mart_id,
            )
            return {"status": "skipped_locked", "mart_id": mart_id}

        # Resolve macros BEFORE building the CREATE statement. We pass the
        # resolved SQL into a synthetic Mart so build_create_view_sql sees
        # the final string (Mart is frozen, so we use dataclasses.replace).
        try:
            resolved_sql = resolve_macros(mart.sql, engine)
        except MacroResolutionError as exc:
            logger.exception("build_mart macro resolution failed for %s", mart_id)
            try:
                _upsert_mart_definition(
                    engine, mart, status="build_failed", error=f"macro: {str(exc)[:480]}"
                )
            except Exception:
                pass
            return {"status": "build_failed", "mart_id": mart_id, "error": f"macro: {exc}"}

        from dataclasses import replace as _dc_replace

        resolved_mart = _dc_replace(mart, sql=resolved_sql)
        statements = build_create_view_sql(resolved_mart) + build_comment_sql(resolved_mart)

        try:
            with engine.begin() as conn:
                for stmt in statements:
                    conn.execute(text(stmt))
            # Count rows in the freshly built view so empty marts are filtered
            # from the planner's discovery surface.
            with engine.connect() as conn:
                row_count = int(
                    conn.execute(
                        text(f'SELECT COUNT(*) FROM {mart.qualified_name}')  # noqa: S608
                    ).scalar()
                    or 0
                )
            _upsert_mart_definition(
                engine,
                mart,
                status="built",
                last_row_count=row_count,
                resolved_sql=resolved_sql,
            )
        except Exception as exc:
            logger.exception("build_mart failed for %s", mart_id)
            try:
                # Persist `last_row_count=0` so the Serving Port discovery
                # filter (`COALESCE(last_row_count, 0) > 0`) hides this mart
                # from the planner. Without this, a previously-successful
                # build leaves a stale row_count and the planner keeps
                # suggesting a broken mart.
                _upsert_mart_definition(
                    engine,
                    mart,
                    status="build_failed",
                    error=str(exc)[:500],
                    last_row_count=0,
                )
            except Exception:
                pass
            return {"status": "build_failed", "mart_id": mart_id, "error": str(exc)[:200]}

        logger.info(
            "build_mart success: %s (%s statements, %s rows)",
            mart_id,
            len(statements),
            row_count,
        )
        return {"status": "built", "mart_id": mart_id, "statements": len(statements)}
    finally:
        try:
            lock_conn.execute(
                text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key}
            )
        except Exception:
            # Lock release failure leaves the session lock until the
            # conn is GC'd — surface it as a warning so it gets noticed.
            logger.warning("could not release advisory lock for %s", mart_id, exc_info=True)
        try:
            lock_conn.close()
        except Exception:
            pass


@celery_app.task(
    name="openarg.refresh_mart",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def refresh_mart(self, mart_id: str, *, marts_dir: str | None = None) -> dict:
    """Re-materialize `mart.<id>`.

    The B-rebuild design refreshes by re-resolving macros and rebuilding
    the materialized view (DROP + CREATE). Plain `REFRESH MATERIALIZED
    VIEW` would not pick up new raw versions, because the materialized
    view's SQL was frozen at last build. Re-resolving on every refresh is
    more expensive but uniform and predictable, and the per-portal
    debounce in `_apply_cached_outcome` (2-min bucket) keeps refresh
    frequency manageable.
    """
    engine = get_sync_engine()
    base_dir = Path(marts_dir) if marts_dir else _DEFAULT_MARTS_DIR
    yaml_path = _find_mart_yaml(mart_id, base_dir)
    if yaml_path is None:
        return {"status": "not_found", "mart_id": mart_id}

    # Optimization: when the resolved SQL has not changed since the last
    # build, a plain REFRESH (cheap) is enough. Otherwise full rebuild.
    mart = load_mart(yaml_path)

    # Same advisory-lock pattern as `build_mart`: serializes refresh and
    # rebuild against each other for the same mart_id. Without this, a
    # `build_mart` mid-DROP would race a concurrent `refresh_mart` REFRESH
    # → "matview does not exist" on the refresh path. Lock is session-
    # scoped (auto-released on connection close).
    lock_key = _mart_lock_key(mart_id)
    lock_conn = engine.connect()
    try:
        acquired = lock_conn.execute(
            text("SELECT pg_try_advisory_lock(:k)"), {"k": lock_key}
        ).scalar()
        if not acquired:
            lock_conn.close()
            logger.info(
                "refresh_mart %s: another worker holds the lock; skipping",
                mart_id,
            )
            return {"status": "skipped_locked", "mart_id": mart_id}

        try:
            resolved_sql = resolve_macros(mart.sql, engine)
        except MacroResolutionError as exc:
            logger.warning(
                "refresh_mart macro resolution failed for %s: %s; deferring to build_mart",
                mart_id,
                exc,
            )
            return build_mart.run(mart_id, marts_dir=marts_dir)

        with engine.connect() as conn:
            prior = conn.execute(
                text(
                    "SELECT sql_definition FROM mart_definitions WHERE mart_id = :id"
                ),
                {"id": mart_id},
            ).fetchone()
            view_exists = conn.execute(
                text(
                    "SELECT 1 FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind = 'm' AND n.nspname = :sch AND c.relname = :vn"
                ),
                {"sch": mart.schema_name, "vn": mart.view_name},
            ).fetchone()
            conn.rollback()

        if (
            not view_exists
            or prior is None
            or _normalize_sql(prior.sql_definition) != _normalize_sql(resolved_sql)
        ):
            logger.info(
                "refresh_mart %s: view missing or SQL changed → full build_mart", mart_id
            )
            # Release our lock before calling build_mart.run, since
            # build_mart will try to take its own lock on the same key.
            try:
                lock_conn.execute(
                    text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key}
                )
            except Exception:
                pass
            lock_conn.close()
            return build_mart.run(mart_id, marts_dir=marts_dir)

        sql = build_refresh_sql(mart)
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            # Count rows post-REFRESH so discovery filter sees fresh state.
            with engine.connect() as conn:
                row_count = int(
                    conn.execute(
                        text(f"SELECT COUNT(*) FROM {mart.qualified_name}")  # noqa: S608
                    ).scalar()
                    or 0
                )
            _upsert_mart_definition(
                engine,
                mart,
                status="refreshed",
                last_row_count=row_count,
                resolved_sql=resolved_sql,
            )
        except Exception as exc:
            logger.exception("refresh_mart failed for %s", mart_id)
            try:
                # Same reasoning as build_failed: hide from discovery so
                # the planner doesn't keep suggesting a mart whose
                # materialized view is corrupted (e.g. underlying raw
                # table dropped by retain_raw_versions race).
                _upsert_mart_definition(
                    engine,
                    mart,
                    status="refresh_failed",
                    error=str(exc)[:500],
                    last_row_count=0,
                )
            except Exception:
                pass
            return {"status": "refresh_failed", "mart_id": mart_id, "error": str(exc)[:200]}

        logger.info("refresh_mart success: %s (%s rows)", mart_id, row_count)
        return {"status": "refreshed", "mart_id": mart_id, "row_count": row_count}
    finally:
        try:
            lock_conn.execute(
                text("SELECT pg_advisory_unlock(:k)"), {"k": lock_key}
            )
        except Exception:
            pass
        try:
            lock_conn.close()
        except Exception:
            pass


def find_marts_for_portal(portal: str, *, marts_dir: str | None = None) -> list[str]:
    """Return the list of `mart_id`s whose YAML lists `portal` in
    `sources.portals`. Called by the collector after a raw landing to
    enqueue downstream refreshes.

    Loads YAMLs from disk on every call — fast (5 marts) and avoids a
    cache that could go stale across deploys. Returns an empty list if
    the marts directory is missing (deploy without `config/marts/`)
    instead of raising — keeps the caller's auto-refresh flow alive.
    """
    base_dir = Path(marts_dir) if marts_dir else _DEFAULT_MARTS_DIR
    if not base_dir.exists() or not base_dir.is_dir():
        logger.warning("Marts dir missing for find_marts_for_portal: %s", base_dir)
        return []
    return [m.id for m in load_all_marts(base_dir) if portal in m.source_portals]


@celery_app.task(
    name="openarg.refresh_via_b_marts",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def refresh_via_b_marts(self, *, marts_dir: str | None = None) -> dict:
    """Daily refresh of vía-B marts (`presupuesto_consolidado`,
    `staff_estado`, `series_economicas`).

    Vía-B writers (`ingest_presupuesto`, `staff_tasks`, `bcra_tasks`)
    run on monthly/weekly schedules, so without this cron those marts
    can stay stale up to 30 days even when the underlying tables are
    healthy. This task runs daily at 03:00 ART and forces a refresh
    of every mart whose `source_portals` includes a vía-B portal
    (those that don't aterrizan via collector.collect_data).

    Idempotent: refresh of an unchanged source produces the same
    matview content with `last_refreshed_at` bumped.
    """
    via_b_portals = {
        "presupuesto_abierto",
        "staff_hcdn",
        "bcra",
        "senado",
    }
    base_dir = Path(marts_dir) if marts_dir else _DEFAULT_MARTS_DIR
    try:
        marts = load_all_marts(base_dir)
    except Exception:
        logger.exception("refresh_via_b_marts: failed to load mart YAMLs")
        return {"status": "load_failed", "dispatched": 0}

    targets = [
        m.id for m in marts
        if any(p in via_b_portals for p in m.source_portals)
    ]

    dispatched = 0
    for mart_id in targets:
        try:
            refresh_mart.apply_async(
                args=[mart_id],
                # Bucketed task_id so a manual run minutes before the cron
                # doesn't get rejected on conflict but stays deduplicated
                # within the same hour.
                task_id=f"refresh_via_b:{mart_id}:{int(__import__('time').time() / 3600)}",
            )
            dispatched += 1
        except Exception:
            logger.warning(
                "Could not dispatch refresh for %s; skipping",
                mart_id,
                exc_info=True,
            )

    logger.info(
        "refresh_via_b_marts: dispatched %d/%d marts (%s)",
        dispatched,
        len(targets),
        ", ".join(targets),
    )
    return {
        "status": "ok",
        "dispatched": dispatched,
        "targets": targets,
    }


@celery_app.task(
    name="openarg.backfill_mart_embeddings",
    bind=True,
    soft_time_limit=300,
    time_limit=600,
)
def backfill_mart_embeddings(
    self,
    *,
    only_missing: bool = True,
    marts_dir: str | None = None,
) -> dict:
    """Compute and persist embeddings for marts that don't have one.

    Without this, marts are invisible to semantic discovery — the planner
    can only find them by name (fnmatch). Run once after migrating the
    column, then ongoing build_mart calls keep embeddings fresh.

    Parameters:
        only_missing: when True (default) skip marts that already have an
                      embedding. Set False to force regeneration of all.
    """
    engine = get_sync_engine()
    base_dir = Path(marts_dir) if marts_dir else _DEFAULT_MARTS_DIR

    where_clause = "WHERE embedding IS NULL" if only_missing else ""
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT mart_id FROM mart_definitions {where_clause}")
        ).fetchall()
    targets = [r.mart_id for r in rows]

    updated = 0
    failed: list[str] = []
    for mart_id in targets:
        yaml_path = _find_mart_yaml(mart_id, base_dir)
        if yaml_path is None:
            failed.append(f"{mart_id}:yaml_not_found")
            continue
        try:
            mart = load_mart(yaml_path)
            payload = _build_mart_embedding_text(mart)
            embedding_vec = _compute_mart_embedding(payload)
            if embedding_vec is None:
                failed.append(f"{mart_id}:embedding_failed")
                continue
            embedding_literal = (
                "[" + ",".join(repr(float(v)) for v in embedding_vec) + "]"
            )
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE mart_definitions
                        SET embedding = CAST(:emb AS vector),
                            updated_at = NOW()
                        WHERE mart_id = :id
                        """
                    ),
                    {"id": mart_id, "emb": embedding_literal},
                )
            updated += 1
        except Exception as exc:
            logger.exception("backfill_mart_embeddings failed for %s", mart_id)
            failed.append(f"{mart_id}:{type(exc).__name__}")

    logger.info(
        "backfill_mart_embeddings: %d updated, %d failed (targets=%d)",
        updated,
        len(failed),
        len(targets),
    )
    return {
        "status": "ok",
        "targets": len(targets),
        "updated": updated,
        "failed": failed,
    }
