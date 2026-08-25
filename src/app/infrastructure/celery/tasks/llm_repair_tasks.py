"""Ask a model to name the columns the heuristics could not.

This is the tier the measurement asked for, not the one the plan assumed.

Running the deterministic repairs against production on 2026-08-22 with the
verifier in place: of 186 tables carrying `col_N`, **none** reached the
verifier — 156 were declined by `garbage_ratio_below_threshold`, meaning fewer
than 40 % of their columns are broken. Of 200 `title_as_columns` candidates,
190 were declined by `too_few_cols`. The heuristics were written in May for two
specific shapes and they handle those; the 1,118 tables that remain are a
different case, and the common one is small: **two or three placeholder columns
sitting among thirty perfectly good ones**.

A whole-header recovery cannot help there — there is no buried header row to
find, just a few columns whose names were lost. But the values are right there,
and reading three columns of values to say what they are is precisely what a
model is for and a regex is not.

**Every proposal answers to the same verifier as a heuristic's.** A model that
misreads does not produce obvious nonsense; it produces confident, well-formed,
plausible names for the wrong columns, which is worse than a visible failure
because nothing looks wrong afterwards. Coming from a model earns a proposal no
lower bar.

Off unless a portal-shaped switch is thrown, bounded per run, and every call
costs money — so the candidate query is narrow by design rather than by budget.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# The shape the heuristics cannot reach: a table that is mostly fine with a few
# lost column names. `garbage_ratio_below_threshold` is exactly this population
# seen from the other side.
#
# Bounded on both ends. Below two broken columns there is little to gain for the
# cost of a call; above the ratio the deterministic repairs should be handling
# it and a model would be papering over a header that was never read.
_MIN_BROKEN = 1
_MAX_BROKEN_RATIO = 0.40

# The proposer's own ceiling, mirrored here so the query never spends a slot on
# a table it will refuse.
_MAX_COLS = 100

_CANDIDATES_SQL = text(
    r"""
    WITH cols AS (
        SELECT c.table_schema, c.table_name, c.column_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema
         AND t.table_name = c.table_name
         AND t.table_type = 'BASE TABLE'
        WHERE c.table_schema = 'raw'
          AND c.column_name NOT LIKE '\_%'
    ),
    counted AS (
        SELECT table_schema, table_name,
               count(*) AS total,
               count(*) FILTER (
                   WHERE column_name ~ '^col_[0-9]+$'
                      OR column_name ~ '^[Uu]nnamed'
               ) AS broken
        FROM cols
        GROUP BY table_schema, table_name
    )
    SELECT table_schema, table_name, total, broken
    FROM counted
    WHERE broken >= :min_broken
      AND total > broken
      AND broken::float / total <= :max_ratio
      -- `propose_llm_assisted_rename` declines anything past 100 columns: a
      -- table that wide is usually a pivot needing an unpivot, and the prompt
      -- cost grows with the column list. Selecting them anyway is how the first
      -- production run returned three candidates and three `too_many_cols`.
      AND total <= :max_cols
    -- Widest-first was the bug. Ordering by the lowest broken ratio put the
    -- 841-column tables at the front — precisely the ones the proposer refuses.
    -- Fewest broken columns first instead: those are the cheapest calls and the
    -- easiest inferences, which is where a tier being evaluated should start.
    ORDER BY broken ASC, total ASC, table_name
    LIMIT :limit
    """
)

_MAX_PER_RUN = int(os.getenv("OPENARG_LLM_REPAIR_MAX_PER_RUN", "25"))


def _enabled() -> bool:
    """Off unless switched on. Every candidate costs a model call."""
    return os.getenv("OPENARG_LLM_REPAIR", "").strip().lower() in {"1", "true", "yes"}


@celery_app.task(
    name="openarg.repair_columns_with_llm",
    bind=True,
    soft_time_limit=1500,
    time_limit=1800,
)
def repair_columns_with_llm(
    self, *, limit: int | None = None, dry_run: bool = True
) -> dict[str, Any]:
    """Propose names for the few lost columns in otherwise healthy tables.

    Defaults to `dry_run=True` and to disabled. A tier that spends money and
    rewrites schemas should require two deliberate acts to start, not one.
    """
    if not _enabled():
        logger.info("llm repair: OPENARG_LLM_REPAIR is not set, so nothing runs")
        return {"enabled": False, "reason": "not_enabled", "repaired": 0}

    from app.application.repair.parse_repair import (
        propose_llm_assisted_rename,
        repair_with_llm_assist,
    )
    from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter

    engine = get_sync_engine()
    run_id = uuid.uuid4()
    cap = limit or _MAX_PER_RUN

    with engine.connect() as conn:
        rows = conn.execute(
            _CANDIDATES_SQL,
            {
                "min_broken": _MIN_BROKEN,
                "max_ratio": _MAX_BROKEN_RATIO,
                "max_cols": _MAX_COLS,
                "limit": cap,
            },
        ).fetchall()
        conn.rollback()

    llm = BedrockLLMAdapter()

    # Ask the model something we know the answer to before letting it rename
    # anything. `verify_intrinsic` checks that a proposal is identifier-like,
    # distinct and less broken than what it replaces — it checks shape, not
    # meaning, and a column of CUITs named `fecha` passes every structural test
    # there is. A degraded model keeps producing well-formed names for the
    # wrong columns, which is the failure this catches and the verifier cannot.
    from app.application.quality.model_canary import run_canary

    canary = asyncio.run(run_canary(llm, propose_llm_assisted_rename))
    if not canary.ok:
        logger.warning("llm repair: canary failed, writing nothing — %s", canary.detail)
        return {
            "enabled": True,
            "reason": "canary_failed",
            "canary": canary.detail,
            "repaired": 0,
        }

    by_reason: dict[str, int] = {}
    repaired: list[str] = []

    async def _run_one(schema: str, table: str):
        return await repair_with_llm_assist(
            engine,
            llm=llm,
            table_schema=schema,
            table_name=table,
            run_id=run_id,
            dry_run=dry_run,
        )

    for row in rows:
        try:
            outcome = asyncio.run(_run_one(row.table_schema, row.table_name))
        except Exception:
            # A model call can fail for reasons that have nothing to do with the
            # table — throttling, a timeout, a transient credential problem. The
            # table stays as it is and the next run tries again.
            logger.warning("llm repair raised for %s", row.table_name, exc_info=True)
            by_reason["raised"] = by_reason.get("raised", 0) + 1
            continue
        key = outcome.reason.split(":")[0] if outcome.reason else "unknown"
        by_reason[key] = by_reason.get(key, 0) + 1
        if outcome.ok and not dry_run:
            repaired.append(row.table_name)

    result = {
        "enabled": True,
        "canary": canary.detail,
        "candidates": len(rows),
        "dry_run": dry_run,
        "by_reason": by_reason,
        # Reported separately because it is the number that says whether this
        # tier is worth its cost: proposals the model made and the verifier
        # threw out. A high count means we are paying for plausible nonsense.
        "refused_by_verifier": by_reason.get("verification_refused", 0),
        "repaired": len(repaired),
        "samples": repaired[:5],
        "run_id": str(run_id),
    }
    logger.info("llm repair: %s", result)
    return result
