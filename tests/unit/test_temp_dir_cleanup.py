from __future__ import annotations

import os
import time
from pathlib import Path


def _age(path: Path, *, seconds_old: int) -> None:
    ts = time.time() - seconds_old
    os.utime(path, (ts, ts))


def test_temp_dir_cleanup_removes_non_empty_stale_tmp_dir(tmp_path, monkeypatch):
    from app.infrastructure.celery.tasks.ops_fixes import temp_dir_cleanup

    stale_dir = tmp_path / "tmpabc123"
    stale_dir.mkdir()
    nested = stale_dir / "payload.bin"
    nested.write_bytes(b"x" * 32)
    _age(nested, seconds_old=7200)
    _age(stale_dir, seconds_old=7200)

    monkeypatch.setenv("OPENARG_TEMP_DIR", str(tmp_path))
    monkeypatch.setenv("OPENARG_TEMP_CLEANUP_AGE_SECONDS", "3600")

    summary = temp_dir_cleanup.run()

    assert not stale_dir.exists()
    assert summary["removed"] == 1
    assert summary["bytes_freed"] >= 32


def test_temp_dir_cleanup_keeps_recent_tmp_dir(tmp_path, monkeypatch):
    from app.infrastructure.celery.tasks.ops_fixes import temp_dir_cleanup

    recent_dir = tmp_path / "tmprecent"
    recent_dir.mkdir()
    (recent_dir / "payload.bin").write_bytes(b"x" * 16)

    monkeypatch.setenv("OPENARG_TEMP_DIR", str(tmp_path))
    monkeypatch.setenv("OPENARG_TEMP_CLEANUP_AGE_SECONDS", "3600")

    summary = temp_dir_cleanup.run()

    assert recent_dir.exists()
    assert summary["removed"] == 0
    assert summary["skipped"] >= 1
