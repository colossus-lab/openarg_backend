"""Optional live retrieval comparison against a real configured environment.

This test is skipped by default. Enable with:
    OPENARG_RUN_LIVE_RETRIEVAL_EVAL=1 pytest -q tests/evaluation/test_retrieval_strategy_live.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from .run_retrieval_strategy_eval import run_comparison

CASES_PATH = Path(__file__).parent / "retrieval_strategy_cases.json"


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("OPENARG_RUN_LIVE_RETRIEVAL_EVAL") != "1",
    reason="Live retrieval evaluation disabled by default",
)
async def test_hybrid_strategy_outperforms_or_matches_vector_only_live() -> None:
    report = await run_comparison(CASES_PATH)

    assert "error" not in report
    assert "errors" not in report

    comparison = report["comparison"]
    baseline = comparison["baseline"]
    candidate = comparison["candidate"]

    assert candidate["hit_at_5"] >= baseline["hit_at_5"]
    assert candidate["mean_reciprocal_rank"] >= baseline["mean_reciprocal_rank"]
