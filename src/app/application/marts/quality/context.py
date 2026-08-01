"""What a mart quality check gets to look at.

Deliberately not `ResourceContext`: that one describes a downloaded file
(`raw_bytes`, `zip_member_names`, `http_status`) and roughly none of it applies
to a materialized view. Forcing a mart through it would produce a dataclass
whose populated fields are the uninteresting ones.

Everything here is gathered once per mart by the auditor and handed to every
check, so a sweep over 71 marts costs one pass of catalog queries rather than
one per check.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceTable:
    """One `raw.*` table feeding a mart, with what the macro decided about it."""

    schema: str
    name: str
    row_count: int | None = None
    columns: tuple[str, ...] = ()

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class MartColumn:
    name: str
    pg_type: str
    declared_type: str | None = None


@dataclass
class MartAuditContext:
    """Everything known about one mart at audit time."""

    mart_id: str
    view_name: str
    schema: str = "mart"

    # --- as registered in mart_definitions ---
    description: str | None = None
    resolved_sql: str | None = None
    last_row_count: int | None = None
    last_refresh_status: str | None = None
    serving_blocked: bool = False
    yaml_version: str | None = None

    # --- as materialized in Postgres ---
    columns: tuple[MartColumn, ...] = ()
    # Planner estimate for the view itself (`pg_class.reltuples`). An estimate
    # on purpose: distinguishing "empty" from "52 million rows" does not need
    # an exact count, and a nightly sweep cannot afford `count(*)` over every
    # mart. PostgreSQL reports -1 for a relation never analysed, which is
    # normalised to None here — unknown, not zero.
    approx_row_count: int | None = None

    # --- the universe the mart was built from ---
    source_tables: tuple[SourceTable, ...] = ()
    # How many tables the macro considered before its column filter, and how
    # many survived. A mart serving a fraction of its domain looks perfectly
    # healthy from `last_row_count` alone.
    candidate_table_count: int | None = None
    kept_table_count: int | None = None

    # --- how much this mart actually matters ---
    hits_30d: int = 0
    success_rate_30d: float | None = None

    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.view_name}"

    @property
    def kept_ratio(self) -> float | None:
        """Share of candidate source tables that survived the column filter."""
        if not self.candidate_table_count:
            return None
        kept = self.kept_table_count
        if kept is None:
            kept = len(self.source_tables)
        return kept / self.candidate_table_count

    @property
    def source_row_total(self) -> int | None:
        counts = [t.row_count for t in self.source_tables if t.row_count is not None]
        return sum(counts) if counts else None
