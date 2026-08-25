"""Tests for what happens to a finding that has nowhere to live.

`persist_findings` writes the whole batch with one `executemany`, so a row the
table cannot accept does not fail alone — it aborts the transaction and takes
every valid finding beside it. In production that was 78,840 rejected
transactions a day, each one also discarding real findings nobody ever saw.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.validation.findings_repository import persist_findings


class _Ctx:
    def __init__(self, resource_id):
        self.resource_id = resource_id


class _Finding:
    def __init__(self, name="missing_download_url"):
        self.detector_name = name
        self.detector_version = 1
        self.severity = type("S", (), {"value": "warn"})()
        self.mode = type("M", (), {"value": "retrospective"})()
        self.payload = {}
        self.should_redownload = False
        self.message = "x"


def _engine():
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = MagicMock()
    return engine


def test_a_finding_with_no_resource_is_not_written():
    engine = _engine()
    n = persist_findings(engine, _Ctx(None), [_Finding()], input_hash="h")
    assert n == 0
    engine.begin.assert_not_called(), "no vale abrir una transacción para nada"


def test_a_normal_finding_is_written():
    engine = _engine()
    assert persist_findings(engine, _Ctx("r1"), [_Finding()], input_hash="h") == 1


def test_no_findings_writes_nothing():
    assert persist_findings(_engine(), _Ctx("r1"), [], input_hash="h") == 0


def test_a_write_failure_returns_zero_rather_than_raising():
    engine = MagicMock()
    engine.begin.side_effect = RuntimeError("db caída")
    assert persist_findings(engine, _Ctx("r1"), [_Finding()], input_hash="h") == 0
