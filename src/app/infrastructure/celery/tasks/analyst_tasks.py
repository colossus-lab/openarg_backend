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

import google.generativeai as genai
from sqlalchemy import create_engine, text

from app.infrastructure.adapters.sandbox.table_validation import safe_table_query
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks.collector_tasks import collect_dataset

logger = logging.getLogger(__name__)


def _get_sync_engine():
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db")
    return create_engine(url)


PLANNER_SYSTEM_PROMPT = """Sos un planificador de análisis de datos públicos de Argentina.
Dada una pregunta del usuario, generá un plan JSON con:
1. "datasets_needed": lista de descripciones de datasets que necesitás
2. "analysis_steps": lista de pasos de análisis
3. "output_format": tipo de output esperado (tabla, gráfico, número, texto)

Respondé SOLO con JSON válido, sin markdown."""

ANALYST_SYSTEM_PROMPT = """Sos un analista de datos públicos de Argentina.
Te doy datos reales en formato tabular. Analizalos y respondé la pregunta del usuario.

Reglas:
- Usá SOLO los datos proporcionados, no inventes números
- Citá la fuente de cada dato
- Si los datos son insuficientes, decilo explícitamente
- Respondé en español
- Si es relevante, sugierí una visualización"""


@celery_app.task(name="openarg.analyze_query", bind=True, max_retries=2)
def analyze_query(self, query_id: str, question: str):
    """
    Pipeline completo de análisis:
    1. Planner decide qué datasets necesita
    2. Vector search encuentra los datasets relevantes
    3. Collector descarga los datos reales
    4. Analyst analiza con los datos concretos
    """
    start = time.time()
    engine = _get_sync_engine()
    gemini_key = os.getenv("GEMINI_API_KEY", "")

    try:
        # Update query status
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE user_queries SET status = 'planning' WHERE id = CAST(:id AS uuid)"),
                {"id": query_id},
            )

        genai.configure(api_key=gemini_key)
        planner_model = genai.GenerativeModel(
            "gemini-2.5-flash",
            system_instruction=PLANNER_SYSTEM_PROMPT,
            generation_config=genai.types.GenerationConfig(
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

        # --- STEP 2: Vector search for relevant datasets ---
        logger.info(f"[{query_id}] Searching datasets...")
        embed_resp = genai.embed_content(
            model="models/gemini-embedding-001",
            content=question,
            output_dimensionality=768,
        )
        query_embedding = embed_resp["embedding"]
        embedding_str = "[" + ",".join(str(v) for v in query_embedding) + "]"

        with engine.begin() as conn:
            results = conn.execute(
                text("""
                    SELECT
                        CAST(d.id AS text) AS dataset_id,
                        d.title,
                        d.description,
                        d.download_url,
                        d.portal,
                        d.columns,
                        d.is_cached,
                        1 - (dc.embedding <=> CAST(:emb AS vector)) AS score
                    FROM dataset_chunks dc
                    JOIN datasets d ON d.id = dc.dataset_id
                    ORDER BY dc.embedding <=> CAST(:emb AS vector)
                    LIMIT 5
                """),
                {"emb": embedding_str},
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

        datasets_info = []
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

        data_context = []
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
        analyst_model = genai.GenerativeModel(
            "gemini-2.5-flash",
            system_instruction=ANALYST_SYSTEM_PROMPT,
        )
        analyst_response = analyst_model.generate_content(
            f"Pregunta: {question}\n\nDatos disponibles:\n{context_str}",
            generation_config=genai.types.GenerationConfig(
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
