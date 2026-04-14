from __future__ import annotations

from app.application.pipeline.citation_guard import collect_numeric_evidence, ground_citations
from app.domain.entities.connectors.data_result import DataResult


def _make_result(records: list[dict], metadata: dict | None = None) -> DataResult:
    return DataResult(
        source="argentina_datos",
        portal_name="datos.gob.ar",
        portal_url="https://datos.gob.ar",
        dataset_title="IPC Nacional",
        format="json",
        records=records,
        metadata=metadata or {"fetched_at": "2026-04-13"},
    )


def test_collect_numeric_evidence_extracts_record_and_metadata_numbers() -> None:
    result = _make_result(
        [{"anio": 2025, "inflacion": 117.8}],
        {"fetched_at": "2026-04-13", "total_records": 1},
    )

    evidence = collect_numeric_evidence([result])

    assert any(item.path == "records[0].inflacion" and item.raw_value == 117.8 for item in evidence)
    assert any(item.path == "metadata.total_records" and item.raw_value == 1 for item in evidence)


def test_ground_citations_marks_supported_numeric_claim_as_verified() -> None:
    result = _make_result([{"anio": 2025, "inflacion": 117.8}])
    citations = [{"claim": "La inflación fue 117,8 en 2025", "source": "IPC Nacional"}]

    grounded, warnings, confidence = ground_citations(
        "La inflación fue 117,8 en 2025.",
        citations,
        [result],
        0.9,
    )

    assert warnings == []
    assert confidence == 0.9
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert grounded[0]["grounding"]


def test_ground_citations_degrades_confidence_when_number_is_unsupported() -> None:
    result = _make_result([{"anio": 2025, "inflacion": 117.8}])
    citations = [{"claim": "La inflación fue 200 en 2025", "source": "IPC Nacional"}]

    grounded, warnings, confidence = ground_citations(
        "La inflación fue 200 en 2025.",
        citations,
        [result],
        0.92,
    )

    assert grounded[0]["verified"] is False
    assert grounded[0]["unsupported_numbers"]
    assert warnings
    assert confidence == 0.45


def test_ground_citations_warns_when_answer_has_numbers_without_citations() -> None:
    result = _make_result([{"anio": 2025, "inflacion": 117.8}])

    grounded, warnings, confidence = ground_citations(
        "La inflación fue 117,8 en 2025.",
        [],
        [result],
        0.88,
    )

    assert grounded == []
    assert warnings
    assert confidence == 0.6
