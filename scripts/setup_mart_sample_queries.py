"""Set up `mart_sample_queries` table + populate with 35 hand-curated rows.

Architecture choice (vs the embed-concat shortcut from
`enrich_mart_embeddings.py`): a separate row per (mart_id, query_text)
lets the ranker compute MAX similarity over samples instead of an
average-of-concat. With concat-embed, "elecciones PASO" looks vaguely
similar to every mart's broadened embedding because all 7 marts now
contain enough Spanish-government tokens to score >0.5 against any
gov-domain query. With max-over-rows, the ranker can answer "does ANY
sample for this mart actually match the query?" — which is the
discriminator we lacked.

After this script runs, the ranker uses:

    mart_score = base_similarity(query, mart.embedding) + (
        +0.17 if max_similarity(query, mart_sample_queries[mart_id]) >= 0.45
        else 0.0
    )

That gates the boost: only marts with a sample within cosine 0.45 of
the user's query get the lift. Negative controls (queries with no mart
relevance, e.g. "resultados electorales PASO" — none of the 7 marts has
an electoral sample) get NO boost, so raw wins as expected.

This script is a one-shot. The proper sprint-level packaging is an
alembic migration that creates the table + a yaml field
`sample_queries:` that `build_mart` upserts into the table.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import create_engine, text


# Same set as `enrich_mart_embeddings.py` — keep in sync until both are
# replaced by the proper YAML-driven backfill.
SAMPLE_QUERIES: dict[str, list[str]] = {
    "salud_establecimientos": [
        "cuántos hospitales hay en Argentina",
        "establecimientos de salud por provincia",
        "centros de salud en Buenos Aires",
        "listado de hospitales públicos",
        "clínicas privadas por departamento",
        "consultorios por localidad",
        "salas de internación por región",
    ],
    "presupuesto_consolidado": [
        "presupuesto vigente por jurisdicción",
        "ejecución presupuestaria 2024",
        "crédito devengado del Ministerio de Salud",
        "ejecución del presupuesto de educación",
        "gasto público por programa",
        "cuánto se ejecutó del presupuesto",
        "presupuesto nacional consolidado",
    ],
    "escuelas_argentina": [
        "cantidad de escuelas en CABA",
        "establecimientos educativos por provincia",
        "escuelas primarias y secundarias",
        "matrícula educativa por departamento",
        "padrón de escuelas argentinas",
        "colegios públicos por región",
        "infraestructura educativa",
    ],
    "staff_estado": [
        "cantidad de empleados públicos por organismo",
        "dotación de personal del Estado",
        "planta permanente del Ministerio",
        "personal del Senado",
        "empleados de Diputados",
        "empleo público por jurisdicción",
        "staff del estado argentino",
    ],
    "legislatura_actividad": [
        "sesiones del Senado en 2024",
        "proyectos de ley en Diputados",
        "actividad legislativa del Congreso",
        "votaciones del Congreso",
        "leyes sancionadas",
        "diarios de sesiones",
    ],
    "series_economicas": [
        "inflación mensual del IPC",
        "tipo de cambio BCRA histórico",
        "tasa de política monetaria",
        "reservas internacionales",
        "agregados monetarios M2 M3",
        "tasas de interés históricas",
    ],
}


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.stderr.write("ERROR: DATABASE_URL required.\n")
        return 2
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(db_url)

    # 1) Create the table if missing. Idempotent — safe to re-run.
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS mart_sample_queries (
                id BIGSERIAL PRIMARY KEY,
                mart_id TEXT NOT NULL,
                sample_text TEXT NOT NULL,
                embedding vector(1024),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(mart_id, sample_text)
            )
            """
        ))
        # No HNSW: only ~50 rows expected, sequential scan is faster.
        # Add an index on mart_id for the GROUP BY in the ranker.
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_mart_sample_queries_mart_id "
            "ON mart_sample_queries(mart_id)"
        ))

    # 2) Embed each sample, upsert into the table.
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )
    region = os.environ.get("AWS_REGION", "us-east-1")
    embedder = BedrockEmbeddingAdapter(region=region)

    inserted = 0
    skipped = 0
    for mart_id, samples in SAMPLE_QUERIES.items():
        for sample in samples:
            emb = await embedder.embed(sample)
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            with engine.begin() as conn:
                result = conn.execute(text(
                    """
                    INSERT INTO mart_sample_queries (mart_id, sample_text, embedding)
                    VALUES (:mid, :st, CAST(:emb AS vector))
                    ON CONFLICT (mart_id, sample_text) DO UPDATE
                    SET embedding = EXCLUDED.embedding,
                        created_at = NOW()
                    RETURNING id
                    """
                ), {"mid": mart_id, "st": sample, "emb": emb_str})
                if result.first():
                    inserted += 1
                else:
                    skipped += 1
        print(f"  ✓ {mart_id}: {len(samples)} samples")

    print(f"\nDone. Inserted/updated: {inserted}, skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
