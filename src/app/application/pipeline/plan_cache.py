from __future__ import annotations

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


def invalidate_query_plan_cache(engine, *, reason: str | None = None) -> int:
    """Delete all cached execution plans.

    Coarse invalidation is intentional here: plan-cache correctness depends on
    the planner surface (`catalog_hints`, mart availability, mart metadata)
    staying aligned with the cached strategy. When that surface changes, a
    blanket flush is safer than trying to surgically preserve entries.
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text("DELETE FROM query_plan_cache"))
            deleted = int(getattr(result, "rowcount", 0) or 0)
        logger.info(
            "query_plan_cache invalidated deleted=%s reason=%s",
            deleted,
            reason or "unspecified",
        )
        return deleted
    except Exception:
        logger.debug("query_plan_cache invalidation failed", exc_info=True)
        return 0
