"""
Analyst Worker — Agente analista.

Recibe una query del usuario, busca datasets relevantes via vector search,
recolecta los datos necesarios, y genera un análisis real con código Python.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import boto3
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.adapters.sandbox.table_validation import safe_table_query
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import collect_dataset
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


@celery_app.task(
    name="openarg.analyze_query", bind=True, max_retries=2, soft_time_limit=600, time_limit=720
)  # type: ignore[misc]
def analyze_query(self: Any, query_id: str, question: str) -> dict[str, Any]:
    """
    Pipeline completo de análisis:
    1. Planner decide qué datasets necesita
    2. Vector search encuentra los datasets relevantes
    3. Collector descarga los datos reales
    4. Analyst analiza con los datos concretos
    """
    start = time.time()
    engine = get_sync_engine()
    _llm_model = os.getenv("BEDROCK_MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0")

    try:
        # Update query status
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE user_queries SET status = 'planning' WHERE id = CAST(:id AS uuid)"),
                {"id": query_id},
            )

        bedrock_llm = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

        # --- STEP 1: Planner ---
        logger.info(f"[{query_id}] Planning...")
        planner_system = load_prompt("celery_planner")
        plan_resp = bedrock_llm.converse(
            modelId=_llm_model,
            system=[{"text": planner_system}],
            messages=[{"role": "user", "content": [{"text": question}]}],
            inferenceConfig={"maxTokens": 1024, "temperature": 0.3},
        )
        plan_text = plan_resp["output"]["message"]["content"][0]["text"] or "{}"
        plan_tokens = plan_resp.get("usage", {}).get("inputTokens", 0) + plan_resp.get(
            "usage", {}
        ).get("outputTokens", 0)

        try:
            plan = json.loads(plan_text)
        except json.JSONDecodeError:
            plan = {
                "datasets_needed": [question],
                "analysis_steps": ["analyze"],
                "output_format": "text",
            }

        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE user_queries SET plan_json = :plan, status = 'collecting' WHERE id = CAST(:id AS uuid)"
                ),
                {"plan": json.dumps(plan), "id": query_id},
            )

        # --- STEP 2: Hybrid search (cosine + BM25 RRF) for relevant datasets ---
        logger.info(f"[{query_id}] Searching datasets (hybrid)...")
        import json as _json

        _bedrock = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        _emb_resp = _bedrock.invoke_model(
            modelId=os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3"),
            body=_json.dumps(
                {"texts": [question], "input_type": "search_query", "truncate": "END"}
            ),
        )
        _emb_result = _json.loads(_emb_resp["body"].read())
        query_embedding = _emb_result["embeddings"][0]
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

        # Sanitize query text for websearch_to_tsquery
        query_text = question.strip()
        if query_text.count('"') % 2 != 0:
            query_text = query_text.replace('"', "")

        with engine.begin() as conn:
            results = conn.execute(
                text("""
                    WITH vector_ranked AS (
                        SELECT
                            CAST(d.id AS text) AS dataset_id,
                            d.title,
                            d.description,
                            d.download_url,
                            d.portal,
                            d.columns,
                            d.is_cached,
                            ROW_NUMBER() OVER (
                                ORDER BY dc.embedding <=> CAST(:emb AS vector)
                            ) AS vec_rank
                        FROM dataset_chunks dc
                        JOIN datasets d ON d.id = dc.dataset_id
                        ORDER BY dc.embedding <=> CAST(:emb AS vector)
                        LIMIT 20
                    ),
                    bm25_ranked AS (
                        SELECT
                            CAST(d.id AS text) AS dataset_id,
                            d.title,
                            d.description,
                            d.download_url,
                            d.portal,
                            d.columns,
                            d.is_cached,
                            ROW_NUMBER() OVER (
                                ORDER BY ts_rank_cd(dc.tsv, websearch_to_tsquery('spanish', :query_text)) DESC
                            ) AS bm25_rank
                        FROM dataset_chunks dc
                        JOIN datasets d ON d.id = dc.dataset_id
                        WHERE dc.tsv @@ websearch_to_tsquery('spanish', :query_text)
                        ORDER BY ts_rank_cd(dc.tsv, websearch_to_tsquery('spanish', :query_text)) DESC
                        LIMIT 20
                    ),
                    fused AS (
                        SELECT
                            COALESCE(v.dataset_id, b.dataset_id) AS dataset_id,
                            COALESCE(v.title, b.title) AS title,
                            COALESCE(v.description, b.description) AS description,
                            COALESCE(v.download_url, b.download_url) AS download_url,
                            COALESCE(v.portal, b.portal) AS portal,
                            COALESCE(v.columns, b.columns) AS columns,
                            COALESCE(v.is_cached, b.is_cached) AS is_cached,
                            COALESCE(1.0 / (60 + v.vec_rank), 0)
                              + COALESCE(1.0 / (60 + b.bm25_rank), 0) AS score
                        FROM vector_ranked v
                        FULL OUTER JOIN bm25_ranked b ON v.dataset_id = b.dataset_id
                    )
                    SELECT dataset_id, title, description, download_url,
                           portal, columns, is_cached, score
                    FROM fused
                    ORDER BY score DESC
                    LIMIT 5
                """),
                {"emb": embedding_str, "query_text": query_text},
            ).fetchall()

        if not results:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE user_queries SET status = 'error', "
                        "error_message = 'No se encontraron datasets relevantes' "
                        "WHERE id = CAST(:id AS uuid)"
                    ),
                    {"id": query_id},
                )
            return {"error": "no_datasets_found"}

        datasets_info: list[dict[str, Any]] = []
        for row in results:
            datasets_info.append(
                {
                    "id": row.dataset_id,
                    "title": row.title,
                    "description": row.description or "",
                    "portal": row.portal,
                    "columns": row.columns or "",
                    "score": float(row.score),
                }
            )

            # Ensure dataset is cached
            if not row.is_cached and row.download_url:
                collect_dataset.delay(row.dataset_id)

        # --- STEP 3: Gather real data from cached tables ---
        logger.info(f"[{query_id}] Gathering data...")
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE user_queries SET status = 'analyzing' WHERE id = CAST(:id AS uuid)"),
                {"id": query_id},
            )

        data_context: list[str] = []
        for ds in datasets_info:
            with engine.begin() as conn:
                cached = conn.execute(
                    text(
                        "SELECT table_name FROM cached_datasets "
                        "WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                    ),
                    {"did": ds["id"]},
                ).fetchone()

            if cached:
                try:
                    sample_sql = safe_table_query(cached.table_name, 'SELECT * FROM "{}" LIMIT 20')
                    cols_sql = safe_table_query(cached.table_name, 'SELECT * FROM "{}" LIMIT 0')
                    if not sample_sql or not cols_sql:
                        logger.warning("Invalid table name: %s", cached.table_name)
                        continue
                    with engine.begin() as conn:
                        sample = conn.execute(text(sample_sql)).fetchall()
                        cols = conn.execute(text(cols_sql)).keys()

                    col_names = list(cols)
                    rows_str = "\n".join(["\t".join(str(v) for v in row) for row in sample])
                    data_context.append(
                        f"Dataset: {ds['title']} (portal: {ds['portal']})\n"
                        f"Columnas: {', '.join(col_names)}\n"
                        f"Primeras filas:\n{rows_str}\n"
                    )
                except Exception as e:
                    logger.warning(f"Could not read cached table for {ds['id']}: {e}")
                    data_context.append(
                        f"Dataset: {ds['title']}\n"
                        f"Descripción: {ds['description']}\n"
                        f"Columnas: {ds['columns']}\n"
                        f"(datos no cacheados aún)\n"
                    )
            else:
                data_context.append(
                    f"Dataset: {ds['title']}\n"
                    f"Descripción: {ds['description']}\n"
                    f"Columnas: {ds['columns']}\n"
                    f"(datos pendientes de descarga)\n"
                )

        # --- STEP 4: Analyst generates answer ---
        logger.info(f"[{query_id}] Analyzing...")
        context_str = "\n---\n".join(data_context)
        analyst_system = load_prompt("celery_analyst")
        analyst_resp = bedrock_llm.converse(
            modelId=_llm_model,
            system=[{"text": analyst_system}],
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"text": f"Pregunta: {question}\n\nDatos disponibles:\n{context_str}"}
                    ],
                }
            ],
            inferenceConfig={"maxTokens": 4096, "temperature": 0.1},
        )
        analysis = analyst_resp["output"]["message"]["content"][0]["text"] or ""
        analyst_tokens = analyst_resp.get("usage", {}).get("inputTokens", 0) + analyst_resp.get(
            "usage", {}
        ).get("outputTokens", 0)
        total_tokens = plan_tokens + analyst_tokens

        duration_ms = int((time.time() - start) * 1000)

        # Save results
        sources = [
            {"title": ds["title"], "portal": ds["portal"], "score": ds["score"]}
            for ds in datasets_info
        ]

        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE user_queries SET
                        status = 'completed',
                        analysis_result = :result,
                        datasets_used = :datasets,
                        sources_json = :sources,
                        tokens_used = :tokens,
                        duration_ms = :duration
                    WHERE id = CAST(:id AS uuid)
                """),
                {
                    "result": analysis,
                    "datasets": json.dumps([ds["id"] for ds in datasets_info]),
                    "sources": json.dumps(sources),
                    "tokens": total_tokens,
                    "duration": duration_ms,
                    "id": query_id,
                },
            )

        logger.info(f"[{query_id}] Analysis completed in {duration_ms}ms")
        return {
            "query_id": query_id,
            "datasets_used": len(datasets_info),
            "tokens": total_tokens,
            "duration_ms": duration_ms,
        }

    except SoftTimeLimitExceeded:
        logger.error(f"Analysis timed out for query {query_id}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE user_queries SET status = 'error', error_message = :msg "
                    "WHERE id = CAST(:id AS uuid)"
                ),
                {"msg": "Task timed out", "id": query_id},
            )
        raise
    except Exception as exc:
        logger.exception(f"Analysis failed for query {query_id}")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE user_queries SET status = 'error', error_message = :msg "
                    "WHERE id = CAST(:id AS uuid)"
                ),
                {"msg": str(exc), "id": query_id},
            )
        raise self.retry(exc=exc, countdown=30)
    finally:
        engine.dispose()
