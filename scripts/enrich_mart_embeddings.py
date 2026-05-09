"""Re-embed mart_definitions with sample queries for sharper routing.

Why: after Fase 2 boost +0.17 we hit 19/19 expected mart hits but get a
false positive on `resultados electorales PASO` (no mart of elections,
but boost lifts every mart above the raws). Root cause: each mart's
`embedding` column was generated from `description` only — too abstract.
By re-embedding each mart's `description + sample_queries`, the vector
becomes lexically anchored to the actual user-facing question patterns.
A query with no semantic overlap with any sample_query will score
proportionally lower against every mart's embedding, restoring the
intended discrimination.

This is a HOT-FIX. The proper sprint-level fix is a `sample_queries
TEXT[]` column on `mart_definitions` plus a separate `mart_sample_queries`
table with one row per (mart, query) so we can compute MAX similarity
across samples instead of avg-via-concatenation. For now we ship the
concat-embed shortcut to stop the bleeding.
"""

from __future__ import annotations

import asyncio
import os
import sys

from sqlalchemy import create_engine, text

# Hand-curated sample queries per mart. 5-7 per mart. These are a
# best-effort representation of how a real user would ask for the data
# the mart serves — same lexical/semantic shape as production queries.
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
    # demo_energia_pozos: skipped — mart is broken (0 rows, upstream
    # pattern mismatch). Re-enrich would mask the underlying problem.
}


async def main() -> int:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.stderr.write("ERROR: DATABASE_URL required.\n")
        return 2
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(db_url)

    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )
    region = os.environ.get("AWS_REGION", "us-east-1")
    embedder = BedrockEmbeddingAdapter(region=region)

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT mart_id, description FROM mart_definitions")
        ).fetchall()

    by_id = {r.mart_id: r.description for r in rows}
    print(f"Found {len(by_id)} marts in mart_definitions")

    updated = 0
    skipped = 0
    for mart_id, samples in SAMPLE_QUERIES.items():
        if mart_id not in by_id:
            print(f"  ⚠ {mart_id}: not in DB, skipping")
            skipped += 1
            continue
        original_desc = by_id[mart_id] or mart_id
        # Concat-embed: original description + the literal user-facing
        # phrasings the mart should match. Anchoring the vector to the
        # query language (not just the data engineer's description) is
        # the whole point of the fix.
        sample_block = "\n\nSample queries this mart answers:\n" + "\n".join(
            f"- {q}" for q in samples
        )
        enriched_text = original_desc + sample_block
        new_embedding = await embedder.embed(enriched_text)
        embedding_str = "[" + ",".join(str(x) for x in new_embedding) + "]"

        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE mart_definitions "
                    "SET embedding = CAST(:emb AS vector), "
                    "    updated_at = NOW() "
                    "WHERE mart_id = :mid"
                ),
                {"emb": embedding_str, "mid": mart_id},
            )
        print(f"  ✓ {mart_id}: re-embedded with {len(samples)} sample queries")
        updated += 1

    print(f"\nDone. Updated={updated}, Skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
