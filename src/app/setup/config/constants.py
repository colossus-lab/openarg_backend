"""Cross-connector limit constants — single source of truth.

Many task modules used to copy these values inline (`MAX_ROWS = 500_000`
in 9+ files). Changing the cap required editing every file and inevitably
some connectors drifted. This module centralises every numeric limit a
connector touches so:

  1. A single env override can change behaviour across the whole pipeline.
  2. New connectors can `from app.setup.config.constants import ...`
     instead of copy-pasting magic numbers.
  3. Test mocks can monkey-patch one place.

Each constant reads from an `OPENARG_*` env var with a sensible default,
so operators can override without redeploying code.

Ownership: any new connector or task module that needs a limit MUST
import from here. Direct numeric literals in connector modules are
considered a code smell and should be flagged in review.
"""

from __future__ import annotations

import os


def _int_env(name: str, default: int) -> int:
    """Read an int env var with a fallback. Robust against empty strings
    and bad casts — returns `default` rather than raising at module load."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


# ── Row caps ────────────────────────────────────────────────────────────────

# Maximum rows to materialise per raw / cache_* table. CSV/Excel/JSON
# loaders truncate at this point and set `raw_table_versions.is_truncated`
# (Sprint 0.1). 500K covers >99% of the corpus while keeping individual
# tables under ~250 MB on disk.
MAX_TABLE_ROWS: int = _int_env("OPENARG_MAX_TABLE_ROWS", 500_000)


# ── Download caps ───────────────────────────────────────────────────────────

# Hard ceiling on the bytes a connector will download in one go. Anything
# bigger than this is rejected by the multi-file expander (for ZIPs) or
# by the streaming download loop (for plain files). 500 MB is the largest
# legit dataset we've seen (CABA buenos_aires_compras at ~328 MB).
MAX_DOWNLOAD_BYTES: int = _int_env(
    "OPENARG_MAX_DOWNLOAD_BYTES", 500 * 1024 * 1024
)


# ── Retry caps ──────────────────────────────────────────────────────────────

# `cached_datasets.retry_count` is clamped to this value by both the
# state machine (`_apply_cached_outcome`) and by `cleanup_invariants`.
# A row that hits the cap transitions to `permanently_failed`.
MAX_TOTAL_ATTEMPTS: int = _int_env("OPENARG_MAX_TOTAL_ATTEMPTS", 5)


# ── Mart refresh debounce ───────────────────────────────────────────────────

# Window inside which repeated `register_via_b_table` calls collapse to
# a single `refresh_mart` dispatch. Was hard-coded as
# `_BCRA_PROD_DEBOUNCE_SECONDS = 110` in `_db.py` even though it applies
# to every via-B writer (BCRA, presupuesto, senado, staff_*).
MART_REFRESH_DEBOUNCE_SECONDS: int = _int_env(
    "OPENARG_MART_REFRESH_DEBOUNCE_SECONDS", 110
)


# ── DB statement timeout ────────────────────────────────────────────────────

# Default Postgres `statement_timeout` for sync engine connections used
# by Celery tasks. Without this a runaway query holds a worker until the
# soft_time_limit kicks in (10 minutes). 600 seconds gives the longest
# legitimate operation (mart matview refresh on 500K rows) plenty of
# headroom while still bounding worker hang.
DB_STATEMENT_TIMEOUT_SECONDS: int = _int_env(
    "OPENARG_DB_STATEMENT_TIMEOUT_SECONDS", 600
)


# ── Raw retention ───────────────────────────────────────────────────────────

# How many raw versions per `resource_identity` to keep before
# `retain_raw_versions` drops the older ones. Lowering this from 3 to 2
# trims ~13 GB of staging disk; from 3 to 1 trims ~32 GB.
RAW_RETENTION_KEEP_LAST: int = _int_env("OPENARG_RAW_RETENTION_KEEP_LAST", 3)


__all__ = [
    "MAX_TABLE_ROWS",
    "MAX_DOWNLOAD_BYTES",
    "MAX_TOTAL_ATTEMPTS",
    "MART_REFRESH_DEBOUNCE_SECONDS",
    "DB_STATEMENT_TIMEOUT_SECONDS",
    "RAW_RETENTION_KEEP_LAST",
]
