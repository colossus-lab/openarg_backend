"""Catalog enrichment tasks — generate semantic metadata for cached tables.

Uses LLM to generate display names, descriptions, tags, and domains
for each cached dataset table.  Generates vector embeddings for
semantic search over the catalog.
"""

from __future__ import annotations

import json
import json as _json
import logging
import os

import boto3
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.prompts import load_prompt

logger = logging.getLogger(__name__)

_EMBEDDING_MODEL = os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3")
_LLM_MODEL = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0")
_EMBEDDING_DIMS = 1024


def _get_bedrock_client():
    """Return a Bedrock Runtime client for embedding calls."""
    return boto3.client(
        "bedrock-runtime",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def _embed_text(text_to_embed: str) -> list[float]:
    """Generate an embedding via Bedrock Cohere."""
    bedrock = _get_bedrock_client()
    resp = bedrock.invoke_model(
        modelId=_EMBEDDING_MODEL,
        body=_json.dumps(
            {"texts": [text_to_embed], "input_type": "search_document", "truncate": "END"}
        ),
    )
    result = _json.loads(resp["body"].read())
    return result["embeddings"][0]


def _generate_metadata_for_table(
    table_name: str,
    columns: list[str],
    sample_rows: list[dict],
    row_count: int,
) -> dict:
    """Use Bedrock Claude Haiku to generate semantic metadata for a table."""
    bedrock = _get_bedrock_client()

    sample_text = ""
    if sample_rows:
        raw = json.dumps(
            sample_rows[:3],
            ensure_ascii=False,
            default=str,
        )[:2000]
        sample_text = f"Filas de ejemplo:\n{raw}"

    columns_str = ", ".join(columns[:30])
    prompt = load_prompt(
        "catalog_enrichment",
        table_name=table_name,
        columns=columns_str,
        row_count=str(row_count),
        sample_text=sample_text,
    )

    try:
        resp = bedrock.converse(
            modelId=_LLM_MODEL,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 1024, "temperature": 0.1},
        )
        text_resp = resp["output"]["message"]["content"][0]["text"].strip()
        # Strip markdown code blocks if present
        if text_resp.startswith("```"):
            lines = text_resp.split("\n")
            lines = [ln for ln in lines if not ln.strip().startswith("```")]
            text_resp = "\n".join(lines).strip()
        return json.loads(text_resp)
    except Exception:
        logger.warning("LLM metadata generation failed for %s", table_name, exc_info=True)
        # Return minimal metadata from table name
        clean_name = table_name.replace("cache_", "").replace("_", " ").title()
        return {
            "display_name": clean_name,
            "description": f"Datos de {clean_name}",
            "domain": "otro",
            "subdomain": "",
            "key_columns": columns[:3] if columns else [],
            "column_types": {},
            "sample_queries": [],
            "tags": [w for w in table_name.replace("cache_", "").split("_") if len(w) > 2],
        }


def _resolve_resource_identity_for_table(engine, table_name: str) -> str | None:
    """Look up the `resource_identity` that owns a physical relation.

    Resolution strategy, in priority order:
      1. If the input is `mart.<view>`, resolve directly from
         `mart_definitions` to `mart::<mart_id>`.
      2. If the input is another qualified name (`raw.foo`, `staging.foo`),
         prefer the exact match in `raw_table_versions` (live version). This
         handles the 7% of raw tables that have no `cached_datasets` row —
         the bare-name lookup below would mis-resolve those onto a different
         dataset that happens to share the table_name.
      3. Otherwise (or if the qualified lookup misses), fall back to the
         legacy `cached_datasets` → `datasets` join. This still owns the
         public.cache_* legacy path.

    Returning the canonical `{portal}::{source_id}` shape means callers
    don't need to know whether the resolution went through rtv or cd.
    """
    schema, bare = _split_qualified_name(table_name)
    try:
        with engine.connect() as conn:
            if schema == "mart":
                mart_row = conn.execute(
                    text(
                        """
                        SELECT mart_id
                        FROM mart_definitions
                        WHERE mart_schema = :sch
                          AND mart_view_name = :tn
                        LIMIT 1
                        """
                    ),
                    {"sch": schema, "tn": bare},
                ).fetchone()
                conn.rollback()
                if mart_row and mart_row.mart_id:
                    return f"mart::{mart_row.mart_id}"

            # Prefer rtv when the input is explicitly raw/staging.
            # `resource_identity` already encodes the portal/source so we
            # surface it directly without rebuilding from datasets.
            if schema not in {"public", "mart"}:
                rtv_row = conn.execute(
                    text(
                        """
                        SELECT resource_identity
                        FROM raw_table_versions
                        WHERE schema_name = :sch
                          AND table_name = :tn
                          AND superseded_at IS NULL
                        ORDER BY version DESC
                        LIMIT 1
                        """
                    ),
                    {"sch": schema, "tn": bare},
                ).fetchone()
                conn.rollback()
                if rtv_row and rtv_row.resource_identity:
                    return str(rtv_row.resource_identity)

            # Legacy path: bare name -> cached_datasets -> datasets.
            # Restrict by schema_name on rtv via LEFT JOIN so we never
            # match a raw row when the caller meant the public legacy row.
            cd_row = conn.execute(
                text(
                    """
                    SELECT d.portal, d.source_id
                    FROM cached_datasets cd
                    JOIN datasets d ON d.id = cd.dataset_id
                    WHERE cd.table_name = :tn
                    ORDER BY cd.updated_at DESC NULLS LAST
                    LIMIT 1
                    """
                ),
                {"tn": bare},
            ).fetchone()
            conn.rollback()
    except Exception:
        return None
    if cd_row is None or not cd_row.portal or not cd_row.source_id:
        return None
    return f"{cd_row.portal}::{cd_row.source_id}"


def _project_enrichment_to_catalog_resources(
    engine,
    *,
    table_name: str,
    metadata: dict,
    row_count: int,
    embedding_str: str | None,
    quality_score: float,
) -> None:
    """UPSERT the LLM-generated semantic metadata into `catalog_resources`.

    Idempotent on `resource_identity`: re-runs UPDATE the existing row.
    Skips silently if the table can't be resolved to a resource (e.g. a
    leftover orphan `cache_*` table that has no entry in `cached_datasets`).
    """
    resource_identity = _resolve_resource_identity_for_table(engine, table_name)
    if resource_identity is None:
        logger.debug(
            "catalog_resources projection skipped for %s (no resource_identity)",
            table_name,
        )
        return
    if resource_identity.startswith("mart::"):
        # Curated marts live in `mart_definitions`, not in
        # `catalog_resources`. Keep the legacy `table_catalog`
        # enrichment for these rows, but don't pretend we projected them
        # into the canonical resource catalog when no such row exists.
        logger.debug(
            "catalog_resources projection skipped for %s (%s belongs to mart_definitions)",
            table_name,
            resource_identity,
        )
        return

    # Derive a `taxonomy_key` from the LLM tags (first non-empty short tag).
    tags = metadata.get("tags") or []
    taxonomy_key: str | None = None
    for tag in tags:
        if isinstance(tag, str) and 0 < len(tag) <= 64:
            taxonomy_key = tag.lower().replace(" ", "_")
            break

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE catalog_resources
                SET display_name = COALESCE(:dn, display_name),
                    domain = COALESCE(:dom, domain),
                    subdomain = COALESCE(:sub, subdomain),
                    taxonomy_key = COALESCE(:tk, taxonomy_key),
                    embedding = COALESCE(CAST(:emb AS vector), embedding),
                    title_confidence = GREATEST(COALESCE(title_confidence, 0), :qs),
                    updated_at = NOW()
                WHERE resource_identity = :rid
                """
            ),
            {
                "dn": metadata.get("display_name"),
                "dom": metadata.get("domain"),
                "sub": metadata.get("subdomain"),
                "tk": taxonomy_key,
                "emb": embedding_str,
                "qs": float(quality_score),
                "rid": resource_identity,
            },
        )


def _split_qualified_name(table_name: str) -> tuple[str, str]:
    """Split `schema.table` (or just `table`) into (schema, bare). Mirrors
    the helper in `ingestion_findings_sweep`: see that module's comment for
    the three input shapes we accept (legacy public, qualified, quoted)."""
    if not table_name:
        return "public", ""
    if "." not in table_name:
        return "public", table_name
    schema, _, rest = table_name.partition(".")
    return schema.strip('"'), rest.strip('"')


def _enrich_table(engine, table_name: str) -> bool:
    """Enrich a single table with metadata and embedding. Returns True if successful."""
    schema, bare = _split_qualified_name(table_name)
    safe_schema = schema.replace('"', '""')
    safe_bare = bare.replace('"', '""')
    try:
        # Get columns and sample rows
        with engine.connect() as conn:
            # Get column info — handle qualified names so this works for
            # raw.* / staging.* / mart.* and not just legacy public.cache_*.
            col_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :tn AND table_schema = :sch "
                    "ORDER BY ordinal_position"
                ),
                {"tn": bare, "sch": schema},
            )
            columns = [r.column_name for r in col_result.fetchall()]
            conn.rollback()

            if not columns:
                logger.warning("No columns found for table %s, skipping", table_name)
                return False

            # Get sample rows (limit 3)
            try:
                sample_result = conn.execute(
                    text(f'SELECT * FROM "{safe_schema}"."{safe_bare}" LIMIT 3')  # noqa: S608
                )
                sample_rows = [dict(r._mapping) for r in sample_result.fetchall()]
                conn.rollback()
            except Exception:
                sample_rows = []
                logger.debug("Could not fetch sample rows for %s", table_name, exc_info=True)

            # Get row count — use the qualified name so raw.* / mart.*
            # work, not just legacy public.cache_*. The earlier
            # `SELECT COUNT(*) FROM "{table_name}"` shape silently
            # interpreted `raw.foo` as a table named literally `raw.foo`
            # in the search_path's first schema (public), giving 0 rows
            # for every raw landing.
            try:
                count_result = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{safe_schema}"."{safe_bare}"')  # noqa: S608
                )
                row_count = count_result.scalar() or 0
                conn.rollback()
            except Exception:
                row_count = 0

        # Generate metadata via LLM
        metadata = _generate_metadata_for_table(table_name, columns, sample_rows, row_count)

        # Generate embedding from display_name + description + tags
        embed_text = " ".join(
            [
                metadata.get("display_name", ""),
                metadata.get("description", ""),
                " ".join(metadata.get("tags", [])),
            ]
        ).strip()

        embedding = _embed_text(embed_text) if embed_text else None
        embedding_str = "[" + ",".join(str(x) for x in embedding) + "]" if embedding else None

        # Upsert into table_catalog
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO table_catalog (
                        table_name, display_name, description, domain, subdomain,
                        key_columns, column_types, sample_queries, tags,
                        row_count, quality_score, catalog_embedding, updated_at
                    ) VALUES (
                        :tn, :dn, :desc, :dom, :sub,
                        :kc, :ct, :sq, :tags,
                        :rc, :qs, CAST(:emb AS vector), NOW()
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        description = EXCLUDED.description,
                        domain = EXCLUDED.domain,
                        subdomain = EXCLUDED.subdomain,
                        key_columns = EXCLUDED.key_columns,
                        column_types = EXCLUDED.column_types,
                        sample_queries = EXCLUDED.sample_queries,
                        tags = EXCLUDED.tags,
                        row_count = EXCLUDED.row_count,
                        quality_score = EXCLUDED.quality_score,
                        catalog_embedding = EXCLUDED.catalog_embedding,
                        updated_at = NOW()
                """),
                {
                    "tn": table_name,
                    "dn": metadata.get("display_name"),
                    "desc": metadata.get("description"),
                    "dom": metadata.get("domain"),
                    "sub": metadata.get("subdomain"),
                    "kc": json.dumps(metadata.get("key_columns", [])),
                    "ct": json.dumps(metadata.get("column_types", {})),
                    "sq": json.dumps(metadata.get("sample_queries", [])),
                    "tags": json.dumps(metadata.get("tags", [])),
                    "rc": row_count,
                    "qs": 1.0 if sample_rows else 0.5,
                    "emb": embedding_str,
                },
            )

        logger.info("Enriched table_catalog for %s: %s", table_name, metadata.get("display_name"))

        # MASTERPLAN Fase 4.5d — also write to `catalog_resources` so the
        # canonical catalog accumulates the same semantic enrichment.
        # `table_catalog` is preserved as a legacy projection until its
        # readers migrate; the eventual deprecation lives outside this
        # function. Any failure here is non-fatal — we logged success
        # above for the legacy write.
        try:
            _project_enrichment_to_catalog_resources(
                engine,
                table_name=table_name,
                metadata=metadata,
                row_count=row_count,
                embedding_str=embedding_str,
                quality_score=1.0 if sample_rows else 0.5,
            )
        except Exception:
            logger.warning(
                "Could not project enrichment to catalog_resources for %s",
                table_name,
                exc_info=True,
            )

        return True

    except Exception:
        logger.warning("Failed to enrich %s", table_name, exc_info=True)
        return False


@celery_app.task(
    name="openarg.enrich_single_table",
    bind=True,
    max_retries=1,
    soft_time_limit=120,
    time_limit=180,
)
def enrich_single_table(self, table_name: str):
    """Enrich a single cached table with semantic metadata."""
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        logger.warning("AWS credentials not set, skipping catalog enrichment")
        return {"error": "no_aws_credentials"}

    engine = get_sync_engine()
    try:
        success = _enrich_table(engine, table_name)
        return {"table_name": table_name, "enriched": success}
    except Exception as exc:
        logger.warning("enrich_single_table failed for %s", table_name, exc_info=True)
        raise self.retry(exc=exc, countdown=30) from exc
    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.enrich_all_tables",
    bind=True,
    soft_time_limit=3600,
    time_limit=3900,
)
def enrich_all_tables(self, batch_size: int = 50):
    """Enrich all cached tables that don't have a catalog entry yet."""
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        logger.warning("AWS credentials not set, skipping catalog enrichment")
        return {"error": "no_aws_credentials"}

    engine = get_sync_engine()
    try:
        # Find tables with status=ready that have no catalog entry
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT cd.table_name
                    FROM cached_datasets cd
                    LEFT JOIN table_catalog tc ON tc.table_name = cd.table_name
                    WHERE cd.status = 'ready'
                      AND tc.id IS NULL
                    ORDER BY cd.row_count DESC NULLS LAST
                    LIMIT :lim
                """),
                {"lim": batch_size},
            ).fetchall()
            conn.rollback()

        if not rows:
            logger.info("All cached tables already have catalog entries")
            return {"enriched": 0, "total": 0}

        logger.info("Enriching %d tables without catalog entries", len(rows))
        enriched = 0
        for row in rows:
            if _enrich_table(engine, row.table_name):
                enriched += 1

        logger.info("Catalog enrichment complete: %d/%d tables enriched", enriched, len(rows))
        return {"enriched": enriched, "total": len(rows)}
    finally:
        engine.dispose()
