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

import google.generativeai as genai  # type: ignore[import-untyped]
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.adapters.sandbox.table_validation import safe_table_query
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import collect_dataset
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


@celery_app.task(name="openarg.analyze_query", bind=True, max_retries=2, soft_time_limit=600, time_limit=720)  # type: ignore[misc]
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
    gemini_key = os.getenv("GEMINI_API_KEY", "")

    try:
        # Update query status
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE user_queries SET status = 'planning' WHERE id = CAST(:id AS uuid)"),
                {"id": query_id},
            )

        genai.configure(api_key=gemini_key)  # type: ignore[attr-defined]
        planner_model = genai.GenerativeModel(  # type: ignore[attr-defined]
            "gemini-2.5-flash",
            system_instruction=load_prompt("celery_planner"),
            generation_config=genai.types.GenerationConfig(  # type: ignore[attr-defined]
                temperature=0.3,
                max_output_tokens=1024,
                response_mime_type="application/json",
            ),
        )

        # --- STEP 1: Planner ---
        logger.info(f"[{query_id}] Planning...")
        plan_response = planner_model.generate_content(question)
        plan_text = plan_response.text or "{}"
        plan_tokens = 0
        if plan_response.usage_metadata:
            plan_tokens = (
                plan_response.usage_metadata.prompt_token_count
                + plan_response.usage_metadata.candidates_token_count
            )

        try:
            plan = json.loads(plan_text)
        except json.JSONDecodeError:
            plan = {"datasets_needed": [question], "analysis_steps": ["analyze"], "output_format": "text"}

        with engine.begin() as conn:
            conn.execute(
                text("UPDATE user_queries SET plan_json = :plan, status = 'collecting' WHERE id = CAST(:id AS uuid)"),
                {"plan": json.dumps(plan), "id": query_id},
            )

        # --- STEP 2: Hybrid search (cosine + BM25 RRF) for relevant datasets ---
        logger.info(f"[{query_id}] Searching datasets (hybrid)...")
        embed_resp = genai.embed_content(  # type: ignore[attr-defined]
            model="models/gemini-embedding-001",
            content=question,
            output_dimensionality=768,
        )
        query_embedding = embed_resp["embedding"]
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
            datasets_info.append({
                "id": row.dataset_id,
                "title": row.title,
                "description": row.description or "",
                "portal": row.portal,
                "columns": row.columns or "",
                "score": float(row.score),
            })

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
                    rows_str = "\n".join(
                        ["\t".join(str(v) for v in row) for row in sample]
                    )
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
        analyst_model = genai.GenerativeModel(  # type: ignore[attr-defined]
            "gemini-2.5-flash",
            system_instruction=load_prompt("celery_analyst"),
        )
        analyst_response = analyst_model.generate_content(
            f"Pregunta: {question}\n\nDatos disponibles:\n{context_str}",
            generation_config=genai.types.GenerationConfig(  # type: ignore[attr-defined]
                temperature=0.1,
                max_output_tokens=4096,
            ),
        )
        analysis = analyst_response.text or ""
        analyst_tokens = 0
        if analyst_response.usage_metadata:
            analyst_tokens = (
                analyst_response.usage_metadata.prompt_token_count
                + analyst_response.usage_metadata.candidates_token_count
            )
        total_tokens = plan_tokens + analyst_tokens

        duration_ms = int((time.time() - start) * 1000)

        # Save results
        sources = [{"title": ds["title"], "portal": ds["portal"], "score": ds["score"]} for ds in datasets_info]

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
