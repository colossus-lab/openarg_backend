"""CLI to compare vector-only retrieval against the active hybrid strategy.

Usage:
    python -m tests.evaluation.run_retrieval_strategy_eval --dry-run
    python -m tests.evaluation.run_retrieval_strategy_eval --output retrieval_report.json

This runner expects a live environment with:
    - DATABASE_URL pointing to the target database
    - AWS credentials + BEDROCK_EMBEDDING_MODEL for query embeddings
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

import boto3
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter

from .retrieval_strategy_evaluator import (
    RetrievalCandidate,
    build_case_result,
    compare_strategy_results,
    comparison_to_dict,
    load_retrieval_cases,
    validate_retrieval_cases,
)

logger = logging.getLogger(__name__)

DEFAULT_CASES = Path(__file__).parent / "retrieval_strategy_cases.json"


def embed_query(query: str) -> list[float]:
    """Generate a search-query embedding using the configured Bedrock model."""
    client = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION", "us-east-1"))
    response = client.invoke_model(
        modelId=os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3"),
        body=json.dumps({"texts": [query], "input_type": "search_query", "truncate": "END"}),
    )
    payload = json.loads(response["body"].read())
    return payload["embeddings"][0]


async def run_comparison(cases_path: Path, output_path: Path | None = None) -> dict:
    """Run retrieval evaluation against the configured live environment."""
    cases = load_retrieval_cases(cases_path)
    errors = validate_retrieval_cases(cases)
    if errors:
        return {"errors": errors}

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"error": "DATABASE_URL is not set"}

    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    baseline_results = []
    candidate_results = []

    async with session_factory() as session:
        adapter = PgVectorSearchAdapter(session)

        for case in cases:
            embedding = embed_query(case.query)

            start = time.perf_counter()
            baseline = await adapter.search_datasets(embedding, limit=5)
            baseline_latency_ms = (time.perf_counter() - start) * 1000

            start = time.perf_counter()
            candidate = await adapter.search_datasets_hybrid(embedding, case.query, limit=5)
            candidate_latency_ms = (time.perf_counter() - start) * 1000

            baseline_candidates = [
                RetrievalCandidate(title=row.title, portal=row.portal, score=row.score)
                for row in baseline
            ]
            candidate_candidates = [
                RetrievalCandidate(title=row.title, portal=row.portal, score=row.score)
                for row in candidate
            ]

            baseline_results.append(
                build_case_result(
                    case=case,
                    strategy="vector_only",
                    candidates=baseline_candidates,
                    latency_ms=baseline_latency_ms,
                )
            )
            candidate_results.append(
                build_case_result(
                    case=case,
                    strategy="hybrid_strategy",
                    candidates=candidate_candidates,
                    latency_ms=candidate_latency_ms,
                )
            )

    await engine.dispose()

    comparison = compare_strategy_results(baseline_results, candidate_results)
    report = {
        "cases_path": str(cases_path),
        "total_cases": len(cases),
        "comparison": comparison_to_dict(comparison),
        "baseline_results": [result.__dict__ for result in baseline_results],
        "candidate_results": [result.__dict__ for result in candidate_results],
    }

    if output_path:
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info("Retrieval comparison report written to %s", output_path)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare vector-only vs hybrid retrieval")
    parser.add_argument(
        "--cases",
        type=Path,
        default=DEFAULT_CASES,
        help="Path to retrieval strategy cases JSON",
    )
    parser.add_argument("--output", type=Path, help="Optional output report path")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only validate the dataset file; do not run live retrieval",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    cases = load_retrieval_cases(args.cases)
    errors = validate_retrieval_cases(cases)
    if errors:
        for error in errors:
            logger.error(error)
        sys.exit(1)

    logger.info("Loaded %d retrieval cases from %s", len(cases), args.cases)
    if args.dry_run:
        logger.info("Retrieval cases are valid")
        return

    report = asyncio.run(run_comparison(args.cases, args.output))
    if "errors" in report or "error" in report:
        logger.error("%s", report)
        sys.exit(1)

    comparison = report["comparison"]
    logger.info(
        "hit@5 baseline=%.3f candidate=%.3f delta=%.3f",
        comparison["baseline"]["hit_at_5"],
        comparison["candidate"]["hit_at_5"],
        comparison["hit_at_5_delta"],
    )
    logger.info(
        "mrr baseline=%.3f candidate=%.3f delta=%.3f",
        comparison["baseline"]["mean_reciprocal_rank"],
        comparison["candidate"]["mean_reciprocal_rank"],
        comparison["mean_reciprocal_rank_delta"],
    )


if __name__ == "__main__":
    main()
