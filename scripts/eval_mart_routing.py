"""Evaluate mart routing quality across similarity thresholds.

For each query in `tests/evaluation/mart_routing_cases.json`:
  1. Compute embedding (Bedrock Cohere v3, 1024-dim).
  2. Run the SAME SQL the planner uses to discover marts (HNSW vector
     search over `mart_definitions.embedding`, with optional sample
     boost +0.17 if any `mart_sample_queries` row matches sim >= 0.45).
  3. Apply a sweep of similarity-cutoff thresholds (post-filter) and
     measure precision@1, precision@3, recall@5, false-positive rate
     on negative cases.

Run on staging worker:
  docker cp scripts/eval_mart_routing.py openarg_worker_ingest:/tmp/
  docker cp tests/evaluation/mart_routing_cases.json openarg_worker_ingest:/tmp/
  docker exec -e DATABASE_URL=... openarg_worker_ingest \\
    python /tmp/eval_mart_routing.py /tmp/mart_routing_cases.json

Output is a Markdown table on stdout per threshold + per-query traceability.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import defaultdict
from typing import Any

from sqlalchemy import create_engine, text


def _get_engine():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL required")
    return create_engine(db_url, pool_pre_ping=True)


async def _embed(query: str) -> list[float]:
    """Use the project's IEmbeddingProvider (Bedrock Cohere v3, 1024-dim)."""
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import (
        BedrockEmbeddingAdapter,
    )

    adapter = BedrockEmbeddingAdapter()
    return await adapter.embed(query)


def _discover_marts(engine, query_embedding: list[float], limit: int = 10) -> list[dict]:
    """Mirror of `_discover_marts` from LegacyServingAdapter — without
    the threshold (we apply that post-hoc), with the sample boost +0.17
    gated at sample_max_sim >= 0.45 (today's gating logic).
    """
    vec_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
    sql = text(
        """
        WITH ranked AS (
            SELECT
                md.mart_id,
                1 - (md.embedding <=> CAST(:vec AS vector)) AS base_sim,
                COALESCE((
                    SELECT MAX(1 - (msq.embedding <=> CAST(:vec AS vector)))
                    FROM mart_sample_queries msq
                    WHERE msq.mart_id = md.mart_id
                ), 0) AS sample_max_sim
            FROM mart_definitions md
            WHERE md.embedding IS NOT NULL
              AND md.last_refresh_status IN ('built', 'refreshed')
              AND COALESCE(md.last_row_count, 0) > 0
        )
        SELECT
            mart_id,
            base_sim,
            sample_max_sim,
            CASE WHEN sample_max_sim >= 0.70 THEN base_sim + 0.17 ELSE base_sim END AS boosted_sim
        FROM ranked
        ORDER BY boosted_sim DESC
        LIMIT :lim
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"vec": vec_str, "lim": limit}).fetchall()
    return [
        {
            "mart_id": r.mart_id,
            "base_sim": float(r.base_sim or 0),
            "sample_max_sim": float(r.sample_max_sim or 0),
            "boosted_sim": float(r.boosted_sim or 0),
        }
        for r in rows
    ]


async def run(eval_path: str) -> int:
    with open(eval_path) as f:
        data = json.load(f)
    entries: list[dict] = data["entries"]

    engine = _get_engine()

    # Collect raw results once, then sweep thresholds in pure Python.
    print(f"Embedding {len(entries)} queries via Bedrock Cohere v3 1024d…", flush=True)
    raw_results: list[dict] = []
    for i, e in enumerate(entries):
        emb = await _embed(e["query"])
        topk = _discover_marts(engine, emb, limit=10)
        raw_results.append({"entry": e, "topk": topk})
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(entries)}", flush=True)

    # Sweep thresholds.
    thresholds = [0.0, 0.30, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    print()
    print("| threshold | precision@1 | precision@3 | recall@5 | FP_rate (neg) |")
    print("|-----------|-------------|-------------|----------|---------------|")

    per_threshold: dict[float, dict[str, Any]] = {}
    for thr in thresholds:
        positives = [r for r in raw_results if r["entry"]["expected_mart_id"] is not None]
        negatives = [r for r in raw_results if r["entry"]["expected_mart_id"] is None]

        p_at_1 = 0
        p_at_3 = 0
        r_at_5 = 0
        for r in positives:
            filtered = [c for c in r["topk"] if c["boosted_sim"] >= thr]
            top1 = filtered[:1]
            top3 = filtered[:3]
            top5 = filtered[:5]
            exp = r["entry"]["expected_mart_id"]
            if any(c["mart_id"] == exp for c in top1):
                p_at_1 += 1
            if any(c["mart_id"] == exp for c in top3):
                p_at_3 += 1
            if any(c["mart_id"] == exp for c in top5):
                r_at_5 += 1

        # FP for negatives = at least one mart returned with sim >= threshold
        fp = 0
        for r in negatives:
            filtered = [c for c in r["topk"] if c["boosted_sim"] >= thr]
            if filtered:
                fp += 1

        n_pos = len(positives) or 1
        n_neg = len(negatives) or 1
        per_threshold[thr] = {
            "p_at_1": p_at_1 / n_pos,
            "p_at_3": p_at_3 / n_pos,
            "r_at_5": r_at_5 / n_pos,
            "fp_rate": fp / n_neg,
        }
        print(
            f"| {thr:.2f} | "
            f"{p_at_1}/{n_pos} = {p_at_1/n_pos:.0%} | "
            f"{p_at_3}/{n_pos} = {p_at_3/n_pos:.0%} | "
            f"{r_at_5}/{n_pos} = {r_at_5/n_pos:.0%} | "
            f"{fp}/{n_neg} = {fp/n_neg:.0%} |"
        )

    print()
    print("## Per-query trace (threshold=0.0, no filter — baseline)")
    print()
    for r in raw_results:
        e = r["entry"]
        exp = e["expected_mart_id"] or "(none)"
        top3 = r["topk"][:3]
        rank = next(
            (i + 1 for i, c in enumerate(r["topk"]) if c["mart_id"] == e["expected_mart_id"]),
            None,
        )
        rank_s = f"#{rank}" if rank else ("✓neg" if e["expected_mart_id"] is None and not top3 else "✗")
        top3_s = " > ".join(
            f"{c['mart_id']}({c['boosted_sim']:.2f})" for c in top3
        )
        print(f"- {e['id']} `{e['query']}` → expected={exp} | got: {top3_s} | hit={rank_s}")

    # Cluster failures by category.
    print()
    print("## Failures by category (threshold=0.0)")
    by_cat: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "miss": 0})
    for r in raw_results:
        e = r["entry"]
        cat = e.get("category", "?")
        by_cat[cat]["total"] += 1
        exp = e["expected_mart_id"]
        top3 = [c["mart_id"] for c in r["topk"][:3]]
        if exp is None:
            if r["topk"][:1]:
                by_cat[cat]["miss"] += 1
        elif exp not in top3:
            by_cat[cat]["miss"] += 1
    for cat, v in sorted(by_cat.items()):
        print(f"- {cat}: {v['miss']}/{v['total']} miss")

    return 0


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "tests/evaluation/mart_routing_cases.json"
    sys.exit(asyncio.run(run(path)))
