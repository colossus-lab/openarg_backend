"""Retrying our failures, and refusing to retry the source's.

The discrimination is the whole design. Retrying what the source refused is a
loop that costs bandwidth to arrive at the same answer, and the second time
somebody sees that loop they stop trusting the sweep.
"""

from __future__ import annotations

from app.infrastructure.celery.tasks.retry_our_failures import (
    _CANDIDATES_SQL,
    OUR_FAULT,
)


def test_only_our_own_orchestration_is_retried():
    """1,031 resources sat at `orchestration_recovery_loop` since 2026-05-06,
    every one with a URL, on portals that answered a probe the same morning.
    They were never refused: they got stuck and the retry counter ran out."""
    assert "orchestration_recovery_loop" in OUR_FAULT
    assert "orchestration_table_missing" in OUR_FAULT
    assert "materialize_table_collision" in OUR_FAULT


def test_the_source_saying_no_is_not_retried():
    """A 404 is an answer. Asking again is a loop."""
    for cat in ("download_http_error", "download_network", "download_timeout"):
        assert cat not in OUR_FAULT, cat


def test_our_own_correct_decisions_are_not_retried():
    """`policy_non_tabular` and `policy_too_large` are the system deciding
    correctly. Retrying them re-derives the same refusal."""
    for cat in ("policy_non_tabular", "policy_too_large"):
        assert cat not in OUR_FAULT, cat


def test_validation_failed_is_excluded_despite_being_tempting():
    """It looks like our fault and mostly is not: 85 % of this catalogue's
    `html_as_data` cases are a portal serving an auth page, which no amount of
    retrying fixes."""
    assert "validation_failed" not in OUR_FAULT
    assert "parse_format" not in OUR_FAULT


def test_a_resource_that_just_failed_uses_the_ordinary_retry_path():
    """These have been stuck for months. Something that failed an hour ago is
    already being handled and does not need this."""
    assert "interval '24 hours'" in str(_CANDIDATES_SQL)


def test_the_oldest_go_first():
    assert "ORDER BY cd.updated_at ASC" in str(_CANDIDATES_SQL)


def test_it_only_takes_resources_that_have_somewhere_to_download_from():
    sql = str(_CANDIDATES_SQL)
    assert "d.download_url IS NOT NULL" in sql
