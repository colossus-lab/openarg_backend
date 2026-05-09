"""Diagnostic harness for mart vs raw routing — Fase 1 of the marts plan.

Runs a set of test queries through both vector search paths in parallel
(table_catalog for raw, mart_definitions for marts) and reports the
top-5 candidates with similarity scores from each side. The output makes
it concrete why marts aren't being chosen: are they in the candidate
pool at all? do they score lower than the raw alternatives? is the gap
small enough to fix with a boost or large enough to need re-embedding?

Reads embeddings via the same Bedrock Cohere adapter the production
pipeline uses (Provided by Dishka container — runs inside the worker
container so all wiring is real).

USAGE
-----
    docker exec -e DATABASE_URL=... openarg_worker_ingest \\
        python /tmp/diagnose_mart_routing.py [--limit-queries 15]

OUTPUT
------
Per query:
  Q: <query text>
    Raw candidates (top 5 from table_catalog):
      0.78  raw.bcra__series_de_tasas__abc__v1
      0.62  cache_caba_x
      ...
    Mart candidates (top 5 from mart_definitions):
      0.71  mart.salud_establecimientos
      ...
    VERDICT: <which side won, by how much>

Final summary:
  Wins by mart: N/M
  Wins by raw : M-N/M
  Avg gap     : <delta>
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

from sqlalchemy import create_engine, text


# ---------------------------------------------------------------- queries
# Each query is one a curious user could plausibly ask. The expectation
# column is the mart_id we EXPECT the system to choose. If the actual
# top hit is a raw cache_* / raw.* table when a mart exists, that's the
# bug we're after.
TEST_QUERIES: list[tuple[str, str]] = [
    ("cuántos hospitales hay en Argentina por provincia", "salud_establecimientos"),
    ("listado de centros de salud", "salud_establecimientos"),
    ("establecimientos de salud en Buenos Aires", "salud_establecimientos"),
    ("hospitales públicos por departamento", "salud_establecimientos"),

    ("presupuesto vigente 2024 por jurisdicción", "presupuesto_consolidado"),
    ("crédito devengado del Ministerio de Salud", "presupuesto_consolidado"),
    ("ejecución presupuestaria por programa", "presupuesto_consolidado"),
    ("cuánto se ejecutó del presupuesto de educación", "presupuesto_consolidado"),

    ("cantidad de escuelas en CABA", "escuelas_argentina"),
    ("establecimientos educativos provincia de Buenos Aires", "escuelas_argentina"),
    ("escuelas primarias por departamento", "escuelas_argentina"),

    ("cantidad de empleados públicos por organismo", "staff_estado"),
    ("dotación de personal del Senado", "staff_estado"),
    ("planta permanente del Ministerio", "staff_estado"),

    ("sesiones del Senado en 2024", "legislatura_actividad"),
    ("proyectos de ley en Diputados", "legislatura_actividad"),

    ("inflación mensual últimos 12 meses", "series_economicas"),
    ("tipo de cambio BCRA histórico", "series_economicas"),
    ("tasa de política monetaria", "series_economicas"),

    # NEGATIVE controls: queries with NO matching mart — the harness should
    # NOT pick a mart, only raw candidates. If a mart still wins one of
    # these, the boost (Fase 2) will need a confidence floor.
    ("resultados electorales PASO 2023 por provincia", None),
    ("transferencias de autos por mes", None),
    ("flujo vehicular peajes AUSA", None),
]


@dataclass
class Candidate:
    table_name: str
    score: float
    is_mart: bool


def _format_candidate(c: Candidate) -> str:
    icon = "🏛 " if c.is_mart else "   "
    return f"{icon}{c.score:.3f}  {c.table_name}"


async def _embed_query(query: str) -> list[float]:
    """Embed via the same Bedrock Cohere adapter as production.

    Instantiated directly (no Dishka container) so the script can run
    standalone in any worker container. AWS region/model match the env
    the production embedder uses.
    """
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )

    region = os.environ.get("AWS_REGION", "us-east-1")
    embedder = BedrockEmbeddingAdapter(region=region)
    return await embedder.embed(query)


def _search_table_catalog(engine, embedding_str: str, limit: int = 5) -> list[Candidate]:
    sql = text(
        """
        SELECT table_name,
               1 - (catalog_embedding <=> CAST(:emb AS vector)) AS score
        FROM table_catalog
        WHERE catalog_embedding IS NOT NULL
        ORDER BY catalog_embedding <=> CAST(:emb AS vector)
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"emb": embedding_str, "lim": limit}).fetchall()
    return [Candidate(table_name=r.table_name, score=float(r.score), is_mart=False)
            for r in rows]


# Mart-side scoring includes the +0.12 boost so the diagnostic mirrors the
# production ranking after the Fase 2 fix. Without the boost the harness
# would report pre-fix scores even though the deployed pipeline applies
# the boost — making the test misleading. Match this constant with the
# value in `discover_tables_by_catalog_search`.
_MART_BOOST = 0.17


def _search_mart_definitions(engine, embedding_str: str, limit: int = 5) -> list[Candidate]:
    """Mirror the production gated-boost ranker.

    Boost +_MART_BOOST applies only when the user query has cosine
    similarity >= 0.45 to at least ONE of the mart's curated sample
    queries (table `mart_sample_queries`). Without the gate the boost
    lifts every mart above the raw floor for any gov-domain query,
    causing false positives like "elecciones PASO" landing on
    legislatura_actividad. See `discover_tables_by_catalog_search` for
    the production-side implementation.
    """
    sql = text(
        """
        WITH ranked AS (
          SELECT mart_schema, mart_view_name, mart_id,
                 1 - (embedding <=> CAST(:emb AS vector)) AS base_score
          FROM mart_definitions
          WHERE embedding IS NOT NULL AND COALESCE(last_row_count, 0) > 0
          ORDER BY embedding <=> CAST(:emb AS vector)
          LIMIT :lim
        )
        SELECT r.mart_schema, r.mart_view_name, r.base_score,
               COALESCE((
                 SELECT MAX(1 - (msq.embedding <=> CAST(:emb AS vector)))
                 FROM mart_sample_queries msq
                 WHERE msq.mart_id = r.mart_id
               ), 0) AS sample_max_sim
        FROM ranked r
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"emb": embedding_str, "lim": limit}).fetchall()
    out = []
    for r in rows:
        base = float(r.base_score)
        sample_sim = float(r.sample_max_sim or 0)
        boosted = base + _MART_BOOST if sample_sim >= 0.70 else base
        out.append(Candidate(
            table_name=f"{r.mart_schema}.{r.mart_view_name}",
            score=boosted, is_mart=True,
        ))
    return out


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-queries", type=int, default=len(TEST_QUERIES))
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sys.stderr.write("ERROR: DATABASE_URL env var required.\n")
        return 2
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(db_url)

    queries = TEST_QUERIES[:args.limit_queries]

    # Aggregate stats for the final summary.
    mart_wins = 0
    raw_wins = 0
    expected_match = 0
    expected_total = 0
    margin_sum = 0.0

    print(f"Running {len(queries)} test queries...\n")
    for query, expected_mart in queries:
        try:
            emb = await _embed_query(query)
        except Exception as exc:
            print(f"Q: {query}")
            print(f"  ERROR embedding: {exc!r}")
            print()
            continue
        emb_str = "[" + ",".join(str(x) for x in emb) + "]"

        raw_candidates = _search_table_catalog(engine, emb_str, limit=5)
        mart_candidates = _search_mart_definitions(engine, emb_str, limit=5)

        top_raw = raw_candidates[0] if raw_candidates else None
        top_mart = mart_candidates[0] if mart_candidates else None

        # Combined ranking — what the planner sees when both lists merge.
        combined = sorted(
            raw_candidates + mart_candidates, key=lambda c: -c.score
        )[:5]
        winner = combined[0] if combined else None

        print(f"Q: {query}")
        if expected_mart:
            print(f"   expected: mart.{expected_mart}")

        print("   Top combined (raw+mart, sorted by score):")
        for c in combined:
            print(f"     {_format_candidate(c)}")

        if top_mart and top_raw:
            margin = top_mart.score - top_raw.score
            margin_sum += margin
            if winner and winner.is_mart:
                mart_wins += 1
                verdict = f"MART wins (margin +{margin:+.3f})"
            else:
                raw_wins += 1
                verdict = f"RAW wins (mart loses by {margin:+.3f})"
            print(f"   VERDICT: {verdict}")

        if expected_mart:
            expected_total += 1
            if winner and winner.is_mart and expected_mart in winner.table_name:
                expected_match += 1
                print(f"   ✓ Routed correctly to expected mart")
            elif winner and winner.is_mart:
                print(f"   ⚠ Routed to a mart but NOT the expected one")
            else:
                print(f"   ✗ Did NOT route to a mart (expected {expected_mart})")
        else:
            if winner and winner.is_mart:
                print(f"   ⚠ Routed to mart but NO mart was expected (potential false positive)")
            else:
                print(f"   ✓ Stayed on raw (correct, no mart expected)")
        print()

    # Final summary.
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    n = len(queries)
    print(f"Total queries:        {n}")
    print(f"Mart wins:            {mart_wins}/{n}")
    print(f"Raw wins:             {raw_wins}/{n}")
    if expected_total:
        print(f"Expected mart hit:    {expected_match}/{expected_total} ({100*expected_match/expected_total:.0f}%)")
    if (mart_wins + raw_wins) > 0:
        avg_margin = margin_sum / (mart_wins + raw_wins)
        print(f"Avg margin (mart-raw):{avg_margin:+.3f}")
        print(f"  Negative margin = raw scores higher than mart on average.")
        print(f"  A boost of +|margin| would flip the average winner.")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
