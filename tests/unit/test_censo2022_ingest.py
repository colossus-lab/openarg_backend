from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from app.infrastructure.celery.tasks.censo2022_ingest import ingest_censo2022


@patch("app.infrastructure.celery.tasks.censo2022_ingest._config_path")
def test_ingest_censo2022_reports_missing_config_reason(mock_config_path):
    mock_config_path.return_value = Path("/tmp/does-not-exist-censo.json")

    result = ingest_censo2022.run()

    assert result["seeded"] == 0
    assert result["skipped"] == 0
    assert result["reason"].startswith("missing_or_empty_config:")
