"""Admin Query Analytics Router — read-only views over `query_analytics`.

Protected by ADMIN_API_KEY (Header: `X-Admin-Key`). Same auth pattern as
`admin/tasks_router.py` and `admin/monitoring_router.py` so ops doesn't
need a second credential.

These endpoints are designed for ops/PM consumption. They aggregate the
raw query_analytics rows (one per user query attempt) into summaries
that answer "what is the demand and where is the system failing?".
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin/analytics", tags=["admin-analytics"])


def _rows(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    """Run a read-only SELECT and return rows as dicts."""
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            cols = list(res.keys())
            return [dict(zip(cols, row, strict=False)) for row in res.fetchall()]
    finally:
        engine.dispose()


@router.get("/overview", dependencies=[Depends(verify_admin_key)])
def overview(hours: int = Query(24, ge=1, le=720)) -> dict:
    """Quick-glance KPIs over the last N hours (default 24h)."""
    rows = _rows(
        """
        SELECT
            COUNT(*)                                            AS total_queries,
            COUNT(*) FILTER (WHERE success)                     AS successes,
            COUNT(*) FILTER (WHERE NOT success)                 AS failures,
            COUNT(*) FILTER (WHERE mart_used)                   AS mart_routed,
            COUNT(*) FILTER (WHERE mart_used AND success)       AS mart_successes,
            COUNT(DISTINCT served_table) FILTER (WHERE served_table IS NOT NULL)
                                                                AS distinct_tables,
            ROUND(AVG(NULLIF(duration_ms, 0))::numeric, 0)      AS avg_duration_ms
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
        """,
        {"h": str(hours)},
    )
    return {"hours": hours, **rows[0]}


@router.get("/top-queries", dependencies=[Depends(verify_admin_key)])
def top_queries(
    hours: int = Query(168, ge=1, le=720),
    limit: int = Query(20, ge=1, le=200),
) -> list[dict]:
    """Most-asked questions over the window. Surfaces the demand signal."""
    return _rows(
        """
        SELECT
            question,
            COUNT(*)                                       AS hits,
            ROUND(AVG(success::int)::numeric, 2)           AS success_rate,
            COUNT(*) FILTER (WHERE mart_used)              AS mart_routed,
            MAX(ts)::timestamp(0)                          AS last_seen
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
        GROUP BY question
        ORDER BY hits DESC, success_rate ASC
        LIMIT :lim
        """,
        {"h": str(hours), "lim": limit},
    )


@router.get("/top-tables", dependencies=[Depends(verify_admin_key)])
def top_tables(
    hours: int = Query(168, ge=1, le=720),
    limit: int = Query(30, ge=1, le=200),
) -> list[dict]:
    """Tables most frequently picked by the planner. Tells us which marts
    are pulling weight vs sitting idle, and which raws are doing what
    a mart should be doing."""
    return _rows(
        """
        SELECT
            served_table,
            COUNT(*)                                       AS hits,
            ROUND(AVG(success::int)::numeric, 2)           AS success_rate,
            ROUND(AVG(row_count)::numeric, 0)              AS avg_rows,
            BOOL_OR(mart_used)                             AS is_mart
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
          AND served_table IS NOT NULL
        GROUP BY served_table
        ORDER BY hits DESC
        LIMIT :lim
        """,
        {"h": str(hours), "lim": limit},
    )


@router.get("/mart-coverage", dependencies=[Depends(verify_admin_key)])
def mart_coverage(hours: int = Query(168, ge=1, le=720)) -> dict:
    """Side-by-side mart vs raw breakdown. The headline metric is what
    fraction of queries land on a mart vs raw — and how the success rate
    differs between the two."""
    raw = _rows(
        """
        SELECT
            mart_used,
            COUNT(*)                                AS queries,
            COUNT(*) FILTER (WHERE success)         AS successes,
            ROUND(AVG(success::int)::numeric, 3)    AS success_rate,
            ROUND(AVG(row_count)::numeric, 1)       AS avg_rows
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
        GROUP BY mart_used
        """,
        {"h": str(hours)},
    )
    by_route = {bool(r["mart_used"]): r for r in raw}
    mart = by_route.get(True, {"queries": 0, "successes": 0, "success_rate": 0.0, "avg_rows": 0.0})
    raw_route = by_route.get(
        False, {"queries": 0, "successes": 0, "success_rate": 0.0, "avg_rows": 0.0}
    )
    return {"hours": hours, "mart": mart, "raw": raw_route}


@router.get("/persistent-failures", dependencies=[Depends(verify_admin_key)])
def persistent_failures(
    hours: int = Query(168, ge=1, le=720),
    min_attempts: int = Query(2, ge=1, le=20),
    limit: int = Query(30, ge=1, le=200),
) -> list[dict]:
    """Questions that fail every time they're asked. These are the
    candidates to evaluate as "missing mart" or "broken table" issues."""
    return _rows(
        """
        SELECT
            question,
            COUNT(*)                                       AS attempts,
            SUM(success::int)                              AS successes,
            STRING_AGG(DISTINCT served_table, ', ')        AS tables_tried,
            STRING_AGG(DISTINCT error_message, ' | ')      AS error_samples,
            MAX(ts)::timestamp(0)                          AS last_seen
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
        GROUP BY question
        HAVING COUNT(*) >= :min_attempts AND SUM(success::int) = 0
        ORDER BY attempts DESC
        LIMIT :lim
        """,
        {"h": str(hours), "min_attempts": min_attempts, "lim": limit},
    )


@router.get("/timeline", dependencies=[Depends(verify_admin_key)])
def timeline(hours: int = Query(48, ge=1, le=720)) -> list[dict]:
    """Hourly buckets of total queries and mart_used %. Useful to spot
    trends after a deploy (mart usage should trend up over time)."""
    return _rows(
        """
        SELECT
            DATE_TRUNC('hour', ts)                          AS hour,
            COUNT(*)                                        AS queries,
            COUNT(*) FILTER (WHERE mart_used)               AS mart_routed,
            COUNT(*) FILTER (WHERE success)                 AS successes
        FROM query_analytics
        WHERE ts > NOW() - (:h || ' hours')::interval
        GROUP BY 1
        ORDER BY 1 DESC
        """,
        {"h": str(hours)},
    )


@router.get("/recent", dependencies=[Depends(verify_admin_key)])
def recent(
    limit: int = Query(50, ge=1, le=500),
    only_failures: bool = Query(False),
) -> list[dict]:
    """Raw query log tail. Useful to read individual question text
    when triaging persistent failures."""
    where = "WHERE NOT success" if only_failures else ""
    return _rows(
        f"""
        SELECT
            id,
            ts::timestamp(0)                                AS ts,
            question,
            served_table,
            mart_used,
            row_count,
            success,
            duration_ms,
            error_message
        FROM query_analytics
        {where}
        ORDER BY id DESC
        LIMIT :lim
        """,
        {"lim": limit},
    )
