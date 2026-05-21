"""Metadata + integrity detectors.

These run mostly post-parse — they need either materialized state or the
metadata that `cached_datasets` already stored.
"""

from __future__ import annotations

import json

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)


class RowCountDetector(Detector):
    """Materialized table is empty (0 rows) or row count diverges >threshold
    from the declared row count for the raw download.

    12 cases in prod with `status='ready'`, `row_count=0`, `size_bytes=0`.
    """

    name = "row_count"
    version = "1"
    severity = Severity.WARN
    DIVERGENCE_THRESHOLD = 0.5

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return ctx.materialized_row_count is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        rows = int(ctx.materialized_row_count or 0)
        size = int(ctx.declared_size_bytes or 0)
        if rows == 0 and size == 0:
            return self._finding(
                mode=mode,
                payload={"row_count": rows, "size_bytes": size},
                message="materialized table is empty and source size is 0",
                severity=Severity.CRITICAL,
                should_redownload=True,
            )
        if rows == 0:
            return self._finding(
                mode=mode,
                payload={"row_count": rows, "size_bytes": size},
                message="materialized table has 0 rows but source has content",
                severity=Severity.CRITICAL,
                should_redownload=True,
            )
        declared = ctx.declared_row_count
        if declared and declared > 0:
            divergence = abs(rows - declared) / max(declared, 1)
            if divergence > self.DIVERGENCE_THRESHOLD:
                return self._finding(
                    mode=mode,
                    payload={
                        "materialized_rows": rows,
                        "declared_rows": declared,
                        "divergence": round(divergence, 3),
                    },
                    message="row count divergence between materialized and declared",
                )
        return None


class MissingKeyColumnDetector(Detector):
    """A required key column is missing from the materialized schema.

    Per-domain: cueanexo for educacion, cuit for fiscal datasets, dataset_id
    for our internal joins, etc. Each detector instance carries its rule set.
    """

    name = "missing_key_column"
    version = "1"
    severity = Severity.WARN

    DEFAULT_RULES: dict[str, tuple[str, ...]] = {
        # portal-prefix → expected key columns (any-of semantics)
        "educacion": ("cueanexo", "cue_anexo", "cue"),
        "afip": ("cuit", "cuil"),
    }

    def __init__(self, rules: dict[str, tuple[str, ...]] | None = None) -> None:
        self._rules = rules or self.DEFAULT_RULES

    def applicable_to(self, ctx: ResourceContext) -> bool:
        if not ctx.materialized_columns:
            return False
        if not ctx.portal:
            return False
        return any(ctx.portal.startswith(prefix) for prefix in self._rules)

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        if not ctx.portal or not ctx.materialized_columns:
            return None
        cols_norm = {str(c).strip().lower() for c in ctx.materialized_columns}
        for prefix, expected in self._rules.items():
            if not ctx.portal.startswith(prefix):
                continue
            if not any(key in cols_norm for key in expected):
                return self._finding(
                    mode=mode,
                    payload={
                        "portal": ctx.portal,
                        "expected_any": list(expected),
                        "columns_sample": [str(c) for c in ctx.materialized_columns[:10]],
                    },
                    message=f"none of the expected key columns found for portal {ctx.portal}",
                )
        return None


class MetadataIntegrityDetector(Detector):
    """`cached_datasets.size_bytes`, `row_count`, `columns_json` are inconsistent
    with the actual materialized table.

    553 ready rows in prod with `size_bytes=0`. 12 of them are real corruption
    (row_count=0); 541 are an ingester bug that doesn't update the metadata.
    This detector separates them.
    """

    name = "metadata_integrity"
    version = "1"
    severity = Severity.WARN

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return ctx.materialized_columns is not None or ctx.materialized_row_count is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        rows_real = ctx.materialized_row_count
        cols_real = ctx.materialized_columns or []
        size_meta = int(ctx.declared_size_bytes or 0)
        cols_meta_raw = ctx.columns_json or "[]"
        try:
            cols_meta = json.loads(cols_meta_raw)
        except (TypeError, ValueError):
            cols_meta = []

        bug_inconsistencies: list[str] = []
        critical_inconsistencies: list[str] = []

        if size_meta == 0 and rows_real and rows_real > 0:
            bug_inconsistencies.append("size_bytes=0 but rows>0 (ingester metadata bug)")

        if cols_meta and cols_real:
            meta_set = {str(c) for c in cols_meta}
            real_set = {str(c) for c in cols_real}
            if meta_set != real_set:
                bug_inconsistencies.append(
                    f"columns_json mismatch ({len(meta_set)} declared vs {len(real_set)} real)"
                )

        if not bug_inconsistencies and not critical_inconsistencies:
            return None
        return self._finding(
            mode=mode,
            payload={
                "size_meta": size_meta,
                "rows_real": rows_real,
                "cols_meta_count": len(cols_meta),
                "cols_real_count": len(cols_real),
                "issues": bug_inconsistencies + critical_inconsistencies,
            },
            message="; ".join(bug_inconsistencies + critical_inconsistencies),
            severity=Severity.CRITICAL if critical_inconsistencies else Severity.WARN,
        )
