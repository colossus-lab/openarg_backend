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


def test_ground_citations_accepts_dataset_count_summary_when_results_are_listings() -> None:
    results = [
        DataResult(
            source=f"pgvector:{idx}",
            portal_name="datos.gob.ar",
            portal_url="https://datos.gob.ar",
            dataset_title=f"Dataset {idx}",
            format="json",
            records=[],
            metadata={"fetched_at": "2026-04-14"},
        )
        for idx in range(1, 6)
    ]
    citations = [{"claim": "Hay 5 datasets de educación disponibles", "source": "datos.gob.ar"}]

    grounded, warnings, confidence = ground_citations(
        "Hay 5 datasets de educación disponibles.",
        citations,
        results,
        0.9,
    )

    assert warnings == []
    assert confidence == 0.9
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert any(item["path"] == "summary.results_count" for item in grounded[0]["grounding"])


def test_ground_citations_accepts_record_count_summary_for_time_series_context() -> None:
    result = DataResult(
        source="bcra",
        portal_name="BCRA",
        portal_url="https://www.bcra.gob.ar",
        dataset_title="Reservas Internacionales",
        format="json",
        records=[{"fecha": f"2026-0{month}-01", "reservas": month * 1000} for month in range(1, 4)],
        metadata={"fetched_at": "2026-04-14"},
    )
    citations = [{"claim": "La serie muestra 3 registros recientes de reservas", "source": "Reservas Internacionales"}]

    grounded, warnings, confidence = ground_citations(
        "La serie muestra 3 registros recientes de reservas.",
        citations,
        [result],
        0.88,
    )

    assert warnings == []
    assert confidence == 0.88
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert any(item["path"] == "summary.record_count" for item in grounded[0]["grounding"])


def test_ground_citations_accepts_years_embedded_in_record_dates() -> None:
    result = DataResult(
        source="bcra",
        portal_name="BCRA",
        portal_url="https://www.bcra.gob.ar",
        dataset_title="Reservas Internacionales",
        format="json",
        records=[
            {"fecha": "2026-03-01", "reservas": 43125},
            {"fecha": "2026-02-01", "reservas": 42810},
        ],
        metadata={"fetched_at": "2026-04-14"},
    )
    citations = [{"claim": "En 2026 las reservas llegaron a 43125", "source": "Reservas Internacionales"}]

    grounded, warnings, confidence = ground_citations(
        "En 2026 las reservas llegaron a 43125.",
        citations,
        [result],
        0.91,
    )

    assert warnings == []
    assert confidence == 0.91
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert any(item["path"] == "records[0].fecha.year" for item in grounded[0]["grounding"])


def test_ground_citations_accepts_years_embedded_in_dataset_titles_for_listings() -> None:
    results = [
        DataResult(
            source="pgvector:1",
            portal_name="datos.gob.ar",
            portal_url="https://datos.gob.ar",
            dataset_title="Relevamiento educativo 2024",
            format="json",
            records=[],
            metadata={"fetched_at": "2026-04-14"},
        ),
        DataResult(
            source="pgvector:2",
            portal_name="datos.gob.ar",
            portal_url="https://datos.gob.ar",
            dataset_title="Infraestructura escolar 2023",
            format="json",
            records=[],
            metadata={"fetched_at": "2026-04-14"},
        ),
    ]
    citations = [{"claim": "Hay 2 datasets de educación, incluyendo un relevamiento 2024", "source": "datos.gob.ar"}]

    grounded, warnings, confidence = ground_citations(
        "Hay 2 datasets de educación, incluyendo un relevamiento 2024.",
        citations,
        results,
        0.89,
    )

    assert warnings == []
    assert confidence == 0.89
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert any(item["path"] == "summary.results_count" for item in grounded[0]["grounding"])
    assert any(item["path"] == "dataset_title.year" and item["value"] == 2024 for item in grounded[0]["grounding"])
