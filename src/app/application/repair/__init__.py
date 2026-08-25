"""In-place repair operations for parse-quality bugs.

These functions operate on tables that already landed in Postgres (raw or
public schemas) and apply DDL/DML fixes derived from the same primitives the
collector uses for new ingests (`app.application.pipeline.parsers.*`).

Use case: a bug in the parser produced 506 tables with placeholder column
names. The collector fix prevents the bug for FUTURE ingests; the repair
function fixes the EXISTING tables without re-downloading from upstream.

Specs: `specs/021-parser-hardening` Phase 2.
"""

from app.application.repair.parse_repair import (
    RepairOutcome,
    list_col_n_candidates,
    list_trailing_garbage_candidates,
    repair_col_n_table,
    repair_trailing_garbage_cols,
)
from app.application.repair.revert import RevertOutcome, revert_repair
from app.application.repair.verify import (
    VerificationOutcome,
    verify_against_previous_version,
    verify_rename,
)

__all__ = [
    "VerificationOutcome",
    "verify_against_previous_version",
    "verify_rename",
    "RevertOutcome",
    "revert_repair",
    "RepairOutcome",
    "list_col_n_candidates",
    "list_trailing_garbage_candidates",
    "repair_col_n_table",
    "repair_trailing_garbage_cols",
]
