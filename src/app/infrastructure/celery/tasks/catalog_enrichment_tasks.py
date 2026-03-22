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

    prompt = (
        "Sos un experto en datos abiertos de Argentina. "
        "Analizá esta tabla de un portal de datos "
        f"públicos:\n\n"
        f"Nombre: {table_name}\n"
        f"Columnas: {', '.join(columns[:30])}\n"
        f"Cantidad de filas: {row_count}\n"
        f"{sample_text}\n\n"
        "Respondé SOLO con JSON válido (sin markdown):\n"
        '{"display_name": "nombre descriptivo corto",'
        '"description": "1-2 oraciones de qué contiene",'
        '"domain": "economía|gobierno|congreso|social'
        '|infraestructura|geografía|otro",'
        '"subdomain": "subdominio específico",'
        '"key_columns": ["columnas importantes"],'
        '"column_types": {"col": "descripción breve"},'
        '"sample_queries": ["3 preguntas en lenguaje '
        'natural respondibles con esta tabla"],'
        '"tags": ["etiquetas para búsqueda"]}'
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


def _enrich_table(engine, table_name: str) -> bool:
    """Enrich a single table with metadata and embedding. Returns True if successful."""
    try:
        # Get columns and sample rows
        with engine.connect() as conn:
            # Get column info
            col_result = conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :tn AND table_schema = 'public' "
                    "ORDER BY ordinal_position"
                ),
                {"tn": table_name},
            )
            columns = [r.column_name for r in col_result.fetchall()]
            conn.rollback()

            if not columns:
                logger.warning("No columns found for table %s, skipping", table_name)
                return False

            # Get sample rows (limit 3)
            try:
                sample_result = conn.execute(
                    text(f'SELECT * FROM "{table_name}" LIMIT 3')  # noqa: S608
                )
                sample_rows = [dict(r._mapping) for r in sample_result.fetchall()]
                conn.rollback()
            except Exception:
                sample_rows = []
                logger.debug("Could not fetch sample rows for %s", table_name, exc_info=True)

            # Get row count
            try:
                count_result = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{table_name}"')  # noqa: S608
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
