"""A channel people mute is worse than no channel.

These tests are about restraint. Sending is the easy half; the half that decides
whether anyone still reads the channel in a month is what it declines to send.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.application.quality import alerting
from app.application.quality.alerting import MAX_PER_RUN, Alert, notify


class _Conn:
    def __init__(self, new_for=None):
        self.new_for = new_for  # set of fingerprints that are "new"
        self.sent_claims: list[dict] = []

    def execute(self, stmt, params=None):
        if params and "fp" in params:
            self.sent_claims.append(params)
            is_new = self.new_for is None or params["fp"] in self.new_for
            return SimpleNamespace(fetchone=lambda: SimpleNamespace(is_new=is_new))
        return SimpleNamespace(fetchone=lambda: None)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def begin(self):
        return self._conn

    def connect(self):
        return self._conn


@pytest.fixture
def sent(monkeypatch):
    box: list[str] = []
    monkeypatch.setattr(alerting, "_send", lambda body: (box.append(body), True)[1])
    return box


def _alerts(n, kind="drift"):
    return [Alert(kind=kind, key=f"r{i}", title=f"Problema {i}") for i in range(n)]


def test_nothing_wrong_sends_nothing(sent):
    """No message is the answer. A daily 'all clear' is furniture."""
    out = notify(_Engine(_Conn()), [], heading="h")
    assert out["sent"] == 0
    assert sent == []


def test_a_repeat_finding_stays_quiet(sent):
    """The weekly report re-derives the same pairs every Monday.

    Without this, the first run produces N alerts and every run after produces
    the same N, which is how a channel gets muted.
    """
    conn = _Conn(new_for=set())  # nothing is new
    out = notify(_Engine(conn), _alerts(3), heading="h")
    assert out["considered"] == 3
    assert out["new"] == 0
    assert sent == []


def test_the_fingerprint_identifies_the_problem_not_the_sighting():
    """Two sightings of the same thing must collapse to one fingerprint."""
    a = Alert(kind="drift", key="indec::pobreza", title="visto el lunes")
    b = Alert(kind="drift", key="indec::pobreza", title="visto el lunes siguiente")
    assert a.fingerprint() == b.fingerprint()
    # A different problem is a different alert.
    c = Alert(kind="drift", key="indec::empleo", title="visto el lunes")
    assert c.fingerprint() != a.fingerprint()
    # Same key, different kind, is also different.
    d = Alert(kind="mart_failed", key="indec::pobreza", title="x")
    assert d.fingerprint() != a.fingerprint()


def test_a_flood_reports_the_count_not_the_flood(sent):
    """Something systemic produces hundreds at once, and the number is the finding."""
    out = notify(_Engine(_Conn()), _alerts(300), heading="h")
    assert out["new"] == 300
    assert out["sent"] == MAX_PER_RUN
    body = sent[0]
    assert body.count("• ") == MAX_PER_RUN
    assert "295 más sin listar" in body


def test_it_stays_quiet_when_it_cannot_tell_new_from_old(sent, monkeypatch):
    """Losing the dedup must not mean re-sending everything it suppressed."""

    class _Broken:
        def begin(self):
            raise RuntimeError("pg is down")

    out = notify(_Broken(), _alerts(4), heading="h")
    assert out["sent"] == 0
    assert sent == []


def test_a_dead_channel_never_raises(monkeypatch):
    """Alerting must not break the sweep that raised the alert."""
    monkeypatch.delenv("OPENARG_TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("OPENARG_TELEGRAM_CHAT_ID", raising=False)
    out = notify(_Engine(_Conn()), _alerts(2), heading="h")
    assert out["sent"] == 0  # nothing configured, and that is not an error


def test_missing_configuration_reads_as_not_set_up(monkeypatch):
    monkeypatch.setenv("OPENARG_TELEGRAM_TOKEN", "t")
    monkeypatch.delenv("OPENARG_TELEGRAM_CHAT_ID", raising=False)
    assert alerting._enabled() is None
    monkeypatch.setenv("OPENARG_TELEGRAM_CHAT_ID", "123")
    assert alerting._enabled() == ("t", "123")
