from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.application.validation.collector_hooks import validate_post_parse, validate_retrospective
from app.application.validation.detector import Finding, Mode, Severity


@patch("app.application.validation.collector_hooks.persist_findings")
@patch("app.application.validation.collector_hooks.get_validator")
def test_validate_retrospective_accepts_declared_row_count(
    mock_get_validator,
    mock_persist_findings,
):
    validator = MagicMock()
    finding = Finding(
        detector_name="metadata_row_count_mismatch",
        detector_version="1",
        severity=Severity.WARN,
        mode=Mode.RETROSPECTIVE,
        payload={"declared": 100, "materialized": 10},
        message="row count mismatch",
    )
    validator.run.return_value = [finding]
    mock_get_validator.return_value = validator

    engine = MagicMock()

    result = validate_retrospective(
        engine,
        dataset_id="rid-1",
        portal="datos_gob_ar",
        source_id="src-1",
        declared_format="csv",
        table_name="cache_test",
        materialized_columns=["a", "b"],
        materialized_row_count=10,
        declared_row_count=100,
        columns_json='["a","b"]',
    )

    assert result == [finding]
    validator.run.assert_called_once()
    ctx = validator.run.call_args.args[0]
    assert ctx.declared_row_count == 100
    assert ctx.materialized_row_count == 10
    mock_persist_findings.assert_called_once()


@patch("app.application.validation.collector_hooks.persist_findings")
@patch("app.application.validation.collector_hooks.get_validator")
def test_validate_post_parse_flags_placeholder_headers(
    mock_get_validator,
    mock_persist_findings,
):
    validator = MagicMock()
    validator.run.return_value = []
    validator.first_critical.side_effect = lambda findings: next(
        (finding for finding in findings if finding.severity == Severity.CRITICAL),
        None,
    )
    mock_get_validator.return_value = validator

    engine = MagicMock()

    finding = validate_post_parse(
        engine,
        dataset_id="rid-2",
        portal="caba",
        source_id="src-2",
        declared_format="xlsx",
        table_name="cache_test",
        materialized_columns=["Unnamed: 0", "Unnamed: 1", "1", "2", "3", "total"],
        materialized_row_count=10,
        columns_json='["Unnamed: 0","Unnamed: 1","1","2","3","total"]',
    )

    assert finding is not None
    assert finding.detector_name == "placeholder_headers"
    mock_persist_findings.assert_called_once()
