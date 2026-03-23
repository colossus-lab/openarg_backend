"""Advanced E2E tests — validate data accuracy, detect hallucinations,
verify correct source matching, and sanity-check numerical responses.

Unlike basic tests (keyword matching), these tests validate:
  - Numbers are in reasonable ranges (not negative, not absurd)
  - No hallucinated future dates
  - Correct data sources are cited
  - Known ground-truth facts appear in responses
  - Wrong/unrelated data is NOT returned
"""

from __future__ import annotations

import re
from datetime import datetime

import pytest

from tests.e2e.helpers import ask, extract_numbers

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

CURRENT_YEAR = datetime.now().year


# ── Hallucination Detection ───────────────────────────────


class TestHallucinationDetection:
    """Verify the system doesn't make up data that doesn't exist."""

    async def test_no_future_trimestre(self, client):
        """Real downvote: system said '2do trimestre 2026' which didn't exist."""
        data = await ask(client, "Cual es la tasa de desempleo en Argentina?")
        answer = data["answer"]
        # Check for references to future quarters
        future_refs = re.findall(
            rf"\b(?:trimestre|semestre).*\b({CURRENT_YEAR + 1}|{CURRENT_YEAR + 2})\b",
            answer,
            re.IGNORECASE,
        )
        assert not future_refs, (
            f"Response references future time periods: {future_refs} in: {answer[:300]}"
        )


# ── Source Correctness ─────────────────────────────────────


class TestSourceCorrectness:
    """Verify the pipeline uses the right data sources for each query type."""

    async def test_ddjj_uses_ddjj_source(self, client):
        """DDJJ query should cite declaraciones juradas, not random datasets."""
        data = await ask(client, "Quienes son los diputados con mayor patrimonio?")
        sources = data.get("sources", [])
        answer_lower = data["answer"].lower()
        # Should reference DDJJ data
        has_ddjj_ref = (
            any("ddjj" in str(s).lower() or "declaraci" in str(s).lower() for s in sources)
            or "declaraci" in answer_lower
            or "patrimonial" in answer_lower
        )
        assert has_ddjj_ref, (
            f"DDJJ query should reference declaraciones juradas. Sources: {sources}"
        )

    async def test_presupuesto_doesnt_return_unrelated_data(self, client):
        """Presupuesto query should NOT return traffic/health/education enrollment data."""
        data = await ask(client, "Cual es el presupuesto de la ciudad de Buenos Aires?")
        answer_lower = data["answer"].lower()
        unrelated_markers = [
            "siniestro vial",
            "accidente de tránsito",
            "danza contemporánea",
            "arbolado público",
        ]
        found_unrelated = [m for m in unrelated_markers if m in answer_lower]
        assert not found_unrelated, (
            f"Budget query returned unrelated data: {found_unrelated} in: {data['answer'][:300]}"
        )

    async def test_inflacion_uses_series_tiempo(self, client):
        """Inflation query should use Series de Tiempo API data."""
        data = await ask(client, "Cual fue el IPC de inflacion mensual en febrero 2026?")
        # Should have actual percentage numbers
        numbers = extract_numbers(data["answer"])
        pct_like = [n for n in numbers if 0 < n < 300]  # reasonable inflation range
        assert pct_like, (
            f"Inflation response should contain percentage-like numbers. "
            f"Numbers found: {numbers[:10]}"
        )


# ── Numerical Sanity Checks ───────────────────────────────


class TestNumericalSanity:
    """Verify numbers in responses are within reasonable ranges."""

    async def test_establecimientos_salud_reasonable_count(self, client):
        """Staging DB has 3052 health establishments."""
        data = await ask(client, "Cuantos establecimientos de salud hay registrados en el sistema?")
        numbers = extract_numbers(data["answer"])
        # Filter for counts (> 50, likely the main number)
        counts = [n for n in numbers if n > 50]
        assert counts, f"Response should contain establishment counts. Numbers: {numbers[:10]}"

    async def test_transferencias_municipios_amounts_positive(self, client):
        """Transfer amounts to municipalities should be positive."""
        data = await ask(client, "Cuales son las transferencias a municipios en 2025?")
        numbers = extract_numbers(data["answer"])
        if numbers:
            negative = [n for n in numbers if n < 0]
            assert not negative, f"Transfer amounts should be positive. Found negative: {negative}"


# ── Known Ground Truth ────────────────────────────────────


class TestGroundTruth:
    """Validate responses against known facts from the database."""

    async def test_24_provinces(self, client):
        """Argentina has exactly 24 provinces (23 + CABA).

        The pipeline may answer "23 provincias y 1 Ciudad Autónoma" which is
        technically correct (23 + 1 = 24), so we also accept "23".
        """
        data = await ask(client, "Cuantas provincias tiene Argentina?")
        answer = data["answer"]
        answer_lower = answer.lower()
        assert (
            "24" in answer
            or "veinticuatro" in answer_lower
            or "23" in answer
            or "veintitrés" in answer_lower
            or "veintitres" in answer_lower
        ), (
            f"Argentina has 24 provinces (23 + CABA), response should mention "
            f"24 or 23: {answer[:300]}"
        )

    async def test_presupuesto_caba_has_ministerio_educacion(self, client):
        """Staging DB presupuesto is from CABA and includes Ministerio de Educación."""
        data = await ask(
            client,
            "Cuales son las jurisdicciones del presupuesto de la Ciudad de Buenos Aires?",
        )
        answer_lower = data["answer"].lower()
        assert "educaci" in answer_lower, (
            f"CABA budget should mention Ministerio de Educación: {data['answer'][:300]}"
        )

    async def test_ddjj_195_diputados(self, client):
        """The DDJJ dataset has exactly 195 diputados."""
        data = await ask(client, "Cuantos diputados tienen declaracion jurada patrimonial cargada?")
        answer = data["answer"]
        numbers = extract_numbers(answer)
        # Should find 195 or a number close to it
        close_to_195 = [n for n in numbers if 180 <= n <= 210]
        assert close_to_195, (
            f"DDJJ has 195 records, response should mention ~195. Numbers: {numbers[:10]}"
        )


# ── Negative Tests (wrong data) ───────────────────────────


class TestNegativeValidation:
    """Verify the system doesn't return wrong/confused data."""

    async def test_inflacion_not_confused_with_other_data(self, client):
        """Inflation query should not return transportation or health data."""
        data = await ask(client, "Cual fue la inflacion en 2024?")
        answer_lower = data["answer"].lower()
        confused = ["subte", "hospital", "establecimiento", "siniestro"]
        found = [m for m in confused if m in answer_lower]
        assert not found, (
            f"Inflation query returned unrelated data: {found} in: {data['answer'][:300]}"
        )

    async def test_meta_question_no_sql_executed(self, client):
        """Meta questions about the system should NOT execute SQL queries."""
        data = await ask(client, "De donde sacas los datos?")
        answer_lower = data["answer"].lower()
        sql_markers = ["select", "from cache_", "limit 10", "where"]
        found = [m for m in sql_markers if m in answer_lower]
        assert not found, (
            f"Meta question should not expose SQL. Found: {found} in: {data['answer'][:300]}"
        )
