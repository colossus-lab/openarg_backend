"""The reader has to be able to tell how old the answer is.

These tests are mostly about what the module refuses to say. A freshness label
that is confidently wrong is worse than none, because it converts "I don't know"
into a promise.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.application.quality.data_age import (
    STALE_AFTER_DAYS,
    DataAge,
    data_age_for,
    staleness_warning,
)


class _Conn:
    def __init__(self, registry=None, mart=None, raises=False):
        self.registry, self.mart, self.raises = registry, mart, raises
        self.queried: list[str] = []

    def execute(self, stmt, params=None):
        if self.raises:
            raise RuntimeError("pg is down")
        sql = str(stmt)
        self.queried.append(sql)
        if "raw_table_versions" in sql:
            return SimpleNamespace(fetchone=lambda: SimpleNamespace(as_of=self.registry))
        return SimpleNamespace(fetchone=lambda: SimpleNamespace(as_of=self.mart))

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _ago(days):
    return datetime.now(UTC) - timedelta(days=days)


def test_fresh_data_earns_no_line():
    """A notice on every answer becomes furniture the reader stops seeing."""
    e = _Engine(_Conn(registry=_ago(3)))
    assert staleness_warning(e, "raw.cache_x") is None


def test_stale_data_says_when_it_was_read():
    e = _Engine(_Conn(registry=datetime(2026, 5, 6, tzinfo=UTC)))
    line = staleness_warning(e, "raw.cache_indec_pobreza")
    assert line is not None
    assert "mayo" in line and "2026" in line


def test_a_lookup_failure_costs_no_answer():
    """Worst case the line is absent — never an exception into the response."""
    assert staleness_warning(_Engine(_Conn(raises=True)), "t") is None


def test_unknown_table_says_nothing_rather_than_guessing():
    assert data_age_for(_Engine(_Conn()), "never_heard_of_it") is None
    assert data_age_for(_Engine(_Conn()), None) is None
    assert data_age_for(_Engine(_Conn()), "  ") is None


def test_mart_falls_back_to_its_source_dates_not_its_rebuild_time():
    """The distinction the whole module exists for.

    A mart rebuilt this morning over sources last read in May holds May's data.
    The registry has no row for a mart view, so the lookup moves on to
    `mart_definitions.source_data_oldest` — which is recorded from the tables
    the macros resolved to, never from `last_refreshed_at`.
    """
    conn = _Conn(registry=None, mart=datetime(2026, 5, 6, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "mart.pobreza_indec_aglomerados")
    assert age is not None
    assert age.source == "mart"
    assert age.is_stale
    # It asked the registry first and only then the mart.
    assert "raw_table_versions" in conn.queried[0]
    assert "mart_definitions" in conn.queried[1]
    # And it never consulted the rebuild time.
    assert not any("last_refreshed_at" in q for q in conn.queried)


def test_the_stale_threshold_matches_the_collector_backstop():
    """If chat and refresh disagree about 'stale', one of them is lying."""
    from app.application.collection.freshness import backstop_age

    assert STALE_AFTER_DAYS == backstop_age().days


def test_schema_qualified_names_are_accepted():
    e = _Engine(_Conn(registry=_ago(200)))
    assert staleness_warning(e, 'raw."cache_x"') is not None


def test_days_never_goes_negative_on_a_future_timestamp():
    age = DataAge(as_of=datetime.now(UTC) + timedelta(days=2), days=0, source="registry")
    assert age.days == 0
    assert not age.is_stale
