"""MASTERPLAN Fase 5 — Optional dbt orchestration wrappers.

dbt is not required for the medallion to work — `openarg.application.marts`
already provides build/refresh via Celery. These wrappers exist so operators
who installed the `[dbt]` extra can run dbt from the same Celery cluster as
the rest of the pipeline:

    celery -A app call openarg.dbt_run --args='["build"]'
    celery -A app call openarg.dbt_test
    celery -A app call openarg.dbt_docs_generate

The task fails cleanly with a clear error if `dbt` isn't installed; nothing
auto-triggers it. Operator-driven only.

The dbt project lives at `<repo>/dbt/`. Override with `OPENARG_DBT_DIR`.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path

from app.infrastructure.celery.app import celery_app

logger = logging.getLogger(__name__)

_DEFAULT_DBT_DIR = Path(
    os.getenv("OPENARG_DBT_DIR")
    or Path(__file__).resolve().parents[5] / "dbt"
)
_DEFAULT_TARGET = os.getenv("OPENARG_DBT_TARGET", "dev")


def _resolve_dbt_binary() -> str | None:
    """Locate `dbt` on PATH. Returns None if not installed."""
    return shutil.which("dbt")


def _run_dbt(
    command: str,
    *,
    extra_args: list[str] | None = None,
    target: str | None = None,
    dbt_dir: str | None = None,
    timeout: int = 600,
) -> dict:
    """Shell out to `dbt <command>` and capture exit + tail of output.

    `command` is one of `run`, `test`, `build`, `docs generate`, `seed`,
    `compile`, `parse`. `extra_args` is forwarded raw (for `--select`,
    `--full-refresh`, etc.).
    """
    binary = _resolve_dbt_binary()
    if binary is None:
        return {
            "status": "skipped",
            "reason": "dbt_not_installed",
            "hint": "pip install 'openarg[dbt]'",
        }

    project_dir = Path(dbt_dir) if dbt_dir else _DEFAULT_DBT_DIR
    if not project_dir.exists():
        return {
            "status": "skipped",
            "reason": "dbt_project_not_found",
            "project_dir": str(project_dir),
        }

    cmd = [binary, *command.split(), "--project-dir", str(project_dir)]
    if target:
        cmd.extend(["--target", target])
    if extra_args:
        cmd.extend(extra_args)

    logger.info("Running dbt: %s", " ".join(cmd))
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning("dbt %s timed out after %ds", command, timeout)
        return {
            "status": "timeout",
            "command": command,
            "timeout_seconds": timeout,
            "stdout_tail": (exc.stdout or "")[-500:] if exc.stdout else "",
        }

    return {
        "status": "ok" if completed.returncode == 0 else "failed",
        "command": command,
        "returncode": completed.returncode,
        # Tail only — full output is in dbt's own logs/.
        "stdout_tail": (completed.stdout or "")[-1000:],
        "stderr_tail": (completed.stderr or "")[-1000:],
    }


@celery_app.task(
    name="openarg.dbt_run",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def dbt_run(self, select: str | None = None, target: str | None = None) -> dict:
    """`dbt run` — materialize models. `select` is forwarded as `--select`."""
    extra: list[str] = []
    if select:
        extra.extend(["--select", select])
    return _run_dbt("run", extra_args=extra, target=target or _DEFAULT_TARGET)


@celery_app.task(
    name="openarg.dbt_test",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def dbt_test(self, select: str | None = None, target: str | None = None) -> dict:
    """`dbt test` — run schema + data tests."""
    extra: list[str] = []
    if select:
        extra.extend(["--select", select])
    return _run_dbt("test", extra_args=extra, target=target or _DEFAULT_TARGET)


@celery_app.task(
    name="openarg.dbt_build",
    bind=True,
    soft_time_limit=1200,
    time_limit=1500,
)
def dbt_build(self, select: str | None = None, target: str | None = None) -> dict:
    """`dbt build` — run + test in dependency order. The recommended entry
    point for a full pipeline rebuild.
    """
    extra: list[str] = []
    if select:
        extra.extend(["--select", select])
    return _run_dbt("build", extra_args=extra, target=target or _DEFAULT_TARGET)


@celery_app.task(
    name="openarg.dbt_docs_generate",
    bind=True,
    soft_time_limit=300,
    time_limit=420,
)
def dbt_docs_generate(self, target: str | None = None) -> dict:
    """`dbt docs generate` — emit the lineage / docs site to `target/`.
    Operator serves it with `dbt docs serve` from a workstation.
    """
    return _run_dbt("docs generate", target=target or _DEFAULT_TARGET)


@celery_app.task(
    name="openarg.dbt_parse",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def dbt_parse(self, target: str | None = None) -> dict:
    """`dbt parse` — validate the project compiles without running anything.
    Useful as a CI check.
    """
    return _run_dbt("parse", target=target or _DEFAULT_TARGET)
