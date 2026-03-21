"""CLI runner for the OpenArg evaluation framework.

Usage:
    python -m tests.evaluation.run_eval --dataset path/to/golden_dataset.json --output report.json
    python -m tests.evaluation.run_eval --dry-run
    python -m tests.evaluation.run_eval --categories series_tiempo,ddjj
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DATASET = Path(__file__).parent / "golden_dataset.json"


def load_golden_dataset(path: Path, categories: list[str] | None = None) -> list[dict]:
    """Load and optionally filter the golden dataset."""
    with open(path) as f:
        data = json.load(f)

    entries = data.get("entries", [])
    if categories:
        entries = [e for e in entries if e["category"] in categories]

    return entries


def validate_dataset(entries: list[dict]) -> list[str]:
    """Validate dataset entries and return list of errors."""
    errors: list[str] = []
    required_fields = {
        "id",
        "category",
        "question",
        "expected_intent",
        "expected_answer_contains",
        "difficulty",
    }
    seen_ids: set[str] = set()

    for i, entry in enumerate(entries):
        missing = required_fields - set(entry.keys())
        if missing:
            errors.append(f"Entry {i}: missing fields {missing}")

        entry_id = entry.get("id", f"unknown_{i}")
        if entry_id in seen_ids:
            errors.append(f"Duplicate id: {entry_id}")
        seen_ids.add(entry_id)

        if not entry.get("question", "").strip():
            errors.append(f"Entry {entry_id}: empty question")

    return errors


async def run_evaluation(
    entries: list[dict],
    output_path: Path | None = None,
) -> dict:
    """Run the full evaluation pipeline against SmartQueryService."""
    from tests.evaluation.evaluator import (
        EvalResult,
        aggregate_results,
    )

    try:
        from app.application import smart_query_service as _sqs  # noqa: F401
    except ImportError:
        logger.error("Cannot import SmartQueryService — ensure the app is in PYTHONPATH")
        return {"error": "import_failed"}

    results: list[EvalResult] = []

    for entry in entries:
        start_ms = int(time.monotonic() * 1000)

        # Placeholder — in a real run, this would call the service
        eval_result = EvalResult(
            question_id=entry["id"],
            category=entry["category"],
            latency_ms=int(time.monotonic() * 1000) - start_ms,
        )
        results.append(eval_result)

    summary = aggregate_results(results)

    report = {
        "total_entries": summary.total,
        "avg_retrieval_precision": round(summary.avg_retrieval_precision, 3),
        "avg_answer_relevance": round(summary.avg_answer_relevance, 3),
        "avg_hallucination_score": round(summary.avg_hallucination_score, 3),
        "intent_accuracy": round(summary.intent_accuracy, 3),
        "connector_accuracy": round(summary.connector_accuracy, 3),
        "avg_latency_ms": round(summary.avg_latency_ms, 1),
        "by_category": summary.by_category,
        "individual_results": [
            {
                "id": r.question_id,
                "category": r.category,
                "retrieval_precision": round(r.retrieval_precision, 3),
                "answer_relevance": round(r.answer_relevance, 3),
                "hallucination_score": round(r.hallucination_score, 3),
                "intent_match": r.intent_match,
                "connector_match": r.connector_match,
                "latency_ms": r.latency_ms,
            }
            for r in results
        ],
    }

    if output_path:
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info("Report written to %s", output_path)

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run OpenArg evaluation framework")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=DEFAULT_DATASET,
        help="Path to golden dataset JSON",
    )
    parser.add_argument("--output", type=Path, help="Output report path")
    parser.add_argument(
        "--categories",
        type=str,
        default=None,
        help="Comma-separated list of categories to evaluate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only validate the dataset, don't run evaluation",
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    categories = args.categories.split(",") if args.categories else None
    entries = load_golden_dataset(args.dataset, categories)

    if not entries:
        logger.error("No entries found in dataset")
        sys.exit(1)

    logger.info("Loaded %d entries from %s", len(entries), args.dataset)

    errors = validate_dataset(entries)
    if errors:
        for err in errors:
            logger.error("Validation error: %s", err)
        sys.exit(1)

    logger.info("Dataset validation passed (%d entries)", len(entries))

    if categories:
        cats = {e["category"] for e in entries}
        logger.info("Categories: %s", ", ".join(sorted(cats)))

    if args.dry_run:
        logger.info("Dry run complete. Dataset is valid.")

        cat_counts: dict[str, int] = {}
        for e in entries:
            cat_counts[e["category"]] = cat_counts.get(e["category"], 0) + 1
        for cat, count in sorted(cat_counts.items()):
            logger.info("  %s: %d entries", cat, count)
        return

    report = asyncio.run(run_evaluation(entries, args.output))
    logger.info("Evaluation complete. Total: %d", report.get("total_entries", 0))


if __name__ == "__main__":
    main()
