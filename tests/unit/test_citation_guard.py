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


def test_collect_numeric_evidence_adds_scaled_variant_when_units_are_millions() -> None:
    result = DataResult(
        source="bcra",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=174.1_RRVAS_IDOS_0_0_36",
        dataset_title="Series históricas de estadísticas monetarias",
        format="time_series",
        records=[{"fecha": "2025-12-01", "reservas": 41167.0}],
        metadata={"fetched_at": "2026-04-14", "units": "Millones de dólares"},
    )

    evidence = collect_numeric_evidence([result])

    assert any(item.path == "records[0].reservas" and item.normalized == 41167.0 for item in evidence)
    assert any(
        item.path == "records[0].reservas.scaled_units" and item.normalized == 41167000000.0
        for item in evidence
    )


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


def test_ground_citations_keeps_low_confidence_when_only_supported_number_is_temporal() -> None:
    result = _make_result([{"anio": 2025, "inflacion": 117.8}])
    citations = [{"claim": "La inflación fue 200 en 2025", "source": "IPC Nacional"}]

    grounded, warnings, confidence = ground_citations(
        "La inflación fue 200 en 2025.",
        citations,
        [result],
        0.92,
    )

    assert grounded[0]["verified"] is False
    assert grounded[0]["unsupported_numbers"] == [200.0]
    assert warnings
    assert confidence == 0.45


def test_ground_citations_keeps_medium_confidence_for_single_partially_grounded_derived_claim() -> None:
    result = DataResult(
        source="bcra",
        portal_name="BCRA",
        portal_url="https://www.bcra.gob.ar",
        dataset_title="Reservas Internacionales",
        format="json",
        records=[
            {"fecha": "2025-03-01", "reservas": 24986},
            {"fecha": "2025-12-01", "reservas": 41167},
        ],
        metadata={"fetched_at": "2026-04-14"},
    )
    citations = [
        {
            "claim": "En 2025 las reservas llegaron a 41167, un salto del 56% desde 24986 en marzo de 2025",
            "source": "Reservas Internacionales",
        }
    ]

    grounded, warnings, confidence = ground_citations(
        "Las reservas llegaron a 41167, un salto del 56% desde 24986 en marzo de 2025.",
        citations,
        [result],
        0.92,
    )

    assert grounded[0]["verified"] is False
    assert grounded[0]["unsupported_numbers"] == [56.0]
    assert warnings
    assert confidence == 0.7


def test_ground_citations_accepts_unit_scaled_claim_and_comparative_derived_delta() -> None:
    result = DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=174.1_RRVAS_IDOS_0_0_36",
        dataset_title="Series históricas de estadísticas monetarias",
        format="time_series",
        records=[
            {"fecha": "2025-03-01", "reservas": 24986.0},
            {"fecha": "2025-04-01", "reservas": 38928.0},
            {"fecha": "2025-12-01", "reservas": 41167.0},
        ],
        metadata={"fetched_at": "2026-04-14", "units": "Millones de dólares"},
    )
    citations = [
        {
            "claim": "Reservas alcanzaron $41.167 millones en diciembre de 2025",
            "source": "Series de Tiempo - Banco Central",
        },
        {
            "claim": "Salto de $14.000 millones en abril de 2025",
            "source": "Series de Tiempo - Banco Central (comparación marzo-abril 2025)",
        },
    ]

    grounded, warnings, confidence = ground_citations(
        "Las reservas alcanzaron $41.167 millones en diciembre de 2025 y hubo un salto de $14.000 millones en abril de 2025.",
        citations,
        [result],
        0.92,
    )

    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert grounded[1]["verified"] is False
    assert grounded[1]["unsupported_numbers"] == [14000000000.0]
    assert warnings
    assert confidence == 0.85


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
    assert confidence == 0.9
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
    assert confidence == 0.9
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
    assert confidence == 0.9
    assert grounded[0]["verified"] is True
    assert grounded[0]["unsupported_numbers"] == []
    assert any(item["path"] == "summary.results_count" for item in grounded[0]["grounding"])
    assert any(item["path"] == "dataset_title.year" and item["value"] == 2024 for item in grounded[0]["grounding"])


def test_ground_citations_promotes_basic_bcra_query_to_high_when_only_secondary_delta_is_derived() -> None:
    result = DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=174.1_RRVAS_IDOS_0_0_36",
        dataset_title="Reservas Internacionales BCRA Saldos",
        format="time_series",
        records=[
            {"fecha": "2025-03-01", "reservas": 24986.0},
            {"fecha": "2025-04-01", "reservas": 38928.0},
            {"fecha": "2025-12-01", "reservas": 41167.0},
        ],
        metadata={"fetched_at": "2026-04-14", "units": "Millones de dólares"},
    )
    citations = [
        {
            "claim": "Las reservas alcanzaron $41.167 millones en diciembre de 2025",
            "source": "Reservas Internacionales BCRA Saldos",
        },
        {
            "claim": "En abril de 2025 hubo un salto de casi $14.000 millones",
            "source": "Reservas Internacionales BCRA Saldos",
        },
    ]

    grounded, warnings, confidence = ground_citations(
        "Las reservas del BCRA alcanzaron $41.167 millones en diciembre de 2025, tras un salto de casi $14.000 millones en abril.",
        citations,
        [result],
        0.42,
    )

    assert warnings
    assert grounded[0]["verified"] is True
    assert grounded[1]["verified"] is False
    assert confidence == 0.85


def test_ground_citations_keeps_high_confidence_for_staging_bcra_recovery_claim() -> None:
    result = DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url="https://datos.gob.ar/series/api/series/?ids=174.1_RRVAS_IDOS_0_0_36",
        dataset_title="Series históricas de estadísticas monetarias",
        format="time_series",
        records=[
            {"fecha": "2023-05-01", "Reservas Internacionales BCRA Saldos": 33001.0},
            {"fecha": "2025-03-01", "Reservas Internacionales BCRA Saldos": 24986.0},
            {"fecha": "2025-12-01", "Reservas Internacionales BCRA Saldos": 41167.0},
        ],
        metadata={"fetched_at": "2026-04-14", "units": "Millones de dólares"},
    )
    citations = [
        {
            "claim": "Reservas alcanzaron $41.167 millones en diciembre de 2025",
            "source": "API de Series de Tiempo (datos.gob.ar) - Series históricas de estadísticas monetarias",
        },
        {
            "claim": "Caída de $33.001 a $24.986 millones entre mayo 2023 y marzo 2025",
            "source": "API de Series de Tiempo (datos.gob.ar)",
        },
        {
            "claim": "Recuperación de $16.181 millones desde abril 2025",
            "source": "API de Series de Tiempo (datos.gob.ar)",
        },
    ]

    grounded, warnings, confidence = ground_citations(
        (
            "Las reservas del BCRA alcanzaron $41.167 millones en diciembre de 2025. "
            "Entre mayo de 2023 y marzo de 2025 cayeron de $33.001 a $24.986 millones. "
            "Pero después hubo una recuperación de $16.181 millones."
        ),
        citations,
        [result],
        0.38,
    )

    assert grounded[0]["verified"] is True
    assert grounded[1]["verified"] is True
    assert grounded[2]["verified"] is False
    assert grounded[2]["unsupported_numbers"] == [16181000000.0]
    assert warnings
    assert confidence == 0.85


def test_ground_citations_promotes_basic_education_listing_to_high_from_grounded_counts() -> None:
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
    citations = [
        {
            "claim": "Hay 2 datasets de educación, incluyendo un relevamiento 2024",
            "source": "datos.gob.ar",
        }
    ]

    grounded, warnings, confidence = ground_citations(
        "Hay 2 datasets de educación, incluyendo un relevamiento 2024.",
        citations,
        results,
        0.33,
    )

    assert warnings == []
    assert grounded[0]["verified"] is True
    assert confidence == 0.9
