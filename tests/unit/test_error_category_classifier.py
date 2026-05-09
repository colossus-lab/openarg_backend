"""Tests for `_classify_error_category` covering Phase 6 additions.

Specs: `specs/021-parser-hardening` Phase 6. The classifier maps free-text
error messages to a closed enum used by ops dashboards. Phase 6 added
three new buckets — `header_degraded`, `orchestration_rerouted`,
`truncation_sampled` — plus broader coverage of pandas-internal parse
errors and SSL failures that previously fell through to `unknown`.
"""

from __future__ import annotations

import pytest

from app.infrastructure.celery.tasks.collector_tasks import (
    _classify_error_category,
)


@pytest.mark.parametrize(
    "msg,expected",
    [
        # Phase 6 — informational notes that aren't really errors
        ("header_quality:degraded;layout_profile:simple_tabular", "header_degraded"),
        ("header_quality:degraded;layout_profile:wide_csv", "header_degraded"),
        ("rerouted_heavy:force_heavy_portal:cordoba_estadistica", "orchestration_rerouted"),
        ("rerouted_heavy:metadata:datos_gob_ar:csv", "orchestration_rerouted"),
        ("rerouted_heavy:size_route:107055219_bytes", "orchestration_rerouted"),
        ("sampled: first 500000 rows kept (limit 500000)", "truncation_sampled"),
        # Network / SSL
        ("[SSL] record layer failure (_ssl.c:2580)", "download_network"),
        ("SSL certificate verification failed", "download_network"),
        ("ssl handshake failed", "download_network"),
        # Pandas-internal parse errors
        ("The 'low_memory' option is not supported with the 'python' engine", "parse_format"),
        ("The truth value of an array with more than one element is ambiguous.", "parse_format"),
        ("Could not determine delimiter", "parse_format"),
        ("Unmatched '\"' when when decoding 'string'", "parse_format"),
        ("Expecting value: line 1 column 1 (char 0)", "parse_format"),
        ("unexpected end of data", "parse_format"),
        ("list index out of range", "parse_format"),
        ("excel_no_worksheets", "parse_format"),
        ("xml_parse_failed", "parse_format"),
        # Schema range errors
        ("(psycopg.errors.NumericValueOutOfRange) integer out of range", "parse_schema_mismatch"),
        # Pre-existing categories shouldn't regress
        ("ingestion_validation_failed:html_as_data", "validation_failed"),
        ("Recovered: stuck in downloading state", "orchestration_recovery_loop"),
        ("403 Forbidden", "download_http_error"),
        ("connection refused", "download_network"),
        ("timed out", "download_timeout"),
        ("no_download_url", "metadata_no_url"),
        # Empty input falls through
        ("", "unknown"),
        (None, "unknown"),
        # Truly unknown
        ("some completely random error nobody has seen", "unknown"),
    ],
)
def test_classify_error_category(msg, expected):
    assert _classify_error_category(msg) == expected
