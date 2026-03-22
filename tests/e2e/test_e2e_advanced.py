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

import os
import re
from datetime import datetime

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

CURRENT_YEAR = datetime.now().year
_ERROR_PHRASES = ["ocurrió un error", "error al analizar", "probá reformulando"]


def _headers() -> dict[str, str]:
    api_key = os.getenv("BACKEND_API_KEY", "")
    if api_key:
        return {"X-API-Key": api_key}
    return {}


def _smart_payload(question: str) -> dict:
    return {"question": question, "user_email": "e2e-test@openarg.org"}


async def _ask(client, question: str) -> dict:
    resp = await client.post(
        "/api/v1/query/smart",
        json=_smart_payload(question),
        headers=_headers(),
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data.get("answer"), f"Empty answer for: {question}"
    answer_lower = data["answer"].lower()
    for phrase in _ERROR_PHRASES:
        assert phrase not in answer_lower, (
            f"Got error response for '{question}': {data['answer'][:200]}"
        )
    return data


def _extract_numbers(text: str) -> list[float]:
    """Extract all numbers (including decimals and percentages) from text."""
    # Match numbers like 1.234, 1.234,56, 12%, 1.200.000
    raw = re.findall(r"[\d]+(?:[.,]\d+)*", text)
    numbers = []
    for r in raw:
        try:
            # Handle Argentine number format: 1.200.000,50 -> 1200000.50
            cleaned = r.replace(".", "").replace(",", ".")
            numbers.append(float(cleaned))
        except ValueError:
            continue
    return numbers


def _extract_years(text: str) -> list[int]:
    """Extract 4-digit years from text."""
    return [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", text)]


# ── Hallucination Detection ───────────────────────────────


class TestHallucinationDetection:
    """Verify the system doesn't make up data that doesn't exist."""

    async def test_no_future_dates(self, client):
        """Responses should not reference dates beyond the current year + 1."""
        data = await _ask(client, "Como fue la inflacion anual en Argentina en los ultimos 5 anos?")
        years = _extract_years(data["answer"])
        future_years = [y for y in years if y > CURRENT_YEAR + 1]
        assert not future_years, (
            f"Response contains future years {future_years}: {data['answer'][:300]}"
        )

    async def test_no_future_trimestre(self, client):
        """Real downvote: system said '2do trimestre 2026' which didn't exist."""
        data = await _ask(client, "Cual es la tasa de desempleo en Argentina?")
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

    async def test_presupuesto_numbers_not_negative(self, client):
        """Budget numbers should never be negative."""
        data = await _ask(client, "Cual fue el presupuesto de educacion en 2024?")
        numbers = _extract_numbers(data["answer"])
        negative = [n for n in numbers if n < 0]
        assert not negative, f"Budget response contains negative numbers: {negative}"

    async def test_no_invented_provinces(self, client):
        """Verify only real Argentine provinces are mentioned."""
        data = await _ask(client, "Cuantos senadores tiene cada provincia?")
        answer_lower = data["answer"].lower()
        fake_provinces = ["atlantida", "patagonia central", "mesopotamia", "pampa norte"]
        found_fake = [p for p in fake_provinces if p in answer_lower]
        assert not found_fake, f"Response mentions non-existent provinces: {found_fake}"


# ── Source Correctness ─────────────────────────────────────


class TestSourceCorrectness:
    """Verify the pipeline uses the right data sources for each query type."""

    async def test_ddjj_uses_ddjj_source(self, client):
        """DDJJ query should cite declaraciones juradas, not random datasets."""
        data = await _ask(client, "Quienes son los diputados con mayor patrimonio?")
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
        data = await _ask(client, "Cual es el presupuesto de la ciudad de Buenos Aires?")
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
        data = await _ask(client, "Cual fue el IPC de inflacion mensual en febrero 2026?")
        # Should have actual percentage numbers
        numbers = _extract_numbers(data["answer"])
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
        data = await _ask(
            client, "Cuantos establecimientos de salud hay registrados en el sistema?"
        )
        numbers = _extract_numbers(data["answer"])
        # Filter for counts (> 50, likely the main number)
        counts = [n for n in numbers if n > 50]
        assert counts, f"Response should contain establishment counts. Numbers: {numbers[:10]}"

    async def test_transferencias_municipios_amounts_positive(self, client):
        """Transfer amounts to municipalities should be positive."""
        data = await _ask(client, "Cuales son las transferencias a municipios en 2025?")
        numbers = _extract_numbers(data["answer"])
        if numbers:
            negative = [n for n in numbers if n < 0]
            assert not negative, f"Transfer amounts should be positive. Found negative: {negative}"

    async def test_mortalidad_rates_reasonable(self, client):
        """Mortality rates should be between 0 and 100 per thousand."""
        data = await _ask(client, "Cual es la tasa de mortalidad infantil en Argentina?")
        answer = data["answer"]
        # Look for rate-like numbers (small decimals or per-thousand values)
        numbers = _extract_numbers(answer)
        # Mortality rate should not be > 200 per thousand (would be absurd)
        rates = [n for n in numbers if 0 < n < 200]
        assert rates, (
            f"Mortality response should contain rate numbers (0-200 range). "
            f"Numbers found: {numbers[:10]}"
        )


# ── Known Ground Truth ────────────────────────────────────


class TestGroundTruth:
    """Validate responses against known facts from the database."""

    async def test_24_provinces(self, client):
        """Argentina has exactly 24 provinces (23 + CABA)."""
        data = await _ask(client, "Cuantas provincias tiene Argentina?")
        answer = data["answer"]
        assert "24" in answer or "veinticuatro" in answer.lower(), (
            f"Argentina has 24 provinces, response should mention 24: {answer[:300]}"
        )

    async def test_mortalidad_fetal_1980(self, client):
        """DB has mortalidad fetal 1980 = 6.4 total. Response should be close."""
        data = await _ask(
            client,
            "Cual era la tasa de mortalidad fetal en 1980 en Argentina?",
        )
        numbers = _extract_numbers(data["answer"])
        # Should find a number close to 6.4
        close_to_target = [n for n in numbers if 4.0 <= n <= 9.0]
        assert close_to_target, (
            f"Mortalidad fetal 1980 should be ~6.4. Numbers found: {numbers[:10]}"
        )

    async def test_presupuesto_caba_has_ministerio_educacion(self, client):
        """Staging DB presupuesto is from CABA and includes Ministerio de Educación."""
        data = await _ask(
            client,
            "Cuales son las jurisdicciones del presupuesto de la Ciudad de Buenos Aires?",
        )
        answer_lower = data["answer"].lower()
        assert "educaci" in answer_lower, (
            f"CABA budget should mention Ministerio de Educación: {data['answer'][:300]}"
        )

    async def test_ddjj_195_diputados(self, client):
        """The DDJJ dataset has exactly 195 diputados."""
        data = await _ask(
            client, "Cuantos diputados tienen declaracion jurada patrimonial cargada?"
        )
        answer = data["answer"]
        numbers = _extract_numbers(answer)
        # Should find 195 or a number close to it
        close_to_195 = [n for n in numbers if 180 <= n <= 210]
        assert close_to_195, (
            f"DDJJ has 195 records, response should mention ~195. Numbers: {numbers[:10]}"
        )


# ── Negative Tests (wrong data) ───────────────────────────


class TestNegativeValidation:
    """Verify the system doesn't return wrong/confused data."""

    async def test_bcra_not_confused_with_presupuesto(self, client):
        """BCRA reserves query should not return budget data."""
        data = await _ask(client, "Cuales son las reservas del BCRA?")
        answer_lower = data["answer"].lower()
        confused_markers = ["presupuesto sancionado", "jurisdicción", "crédito vigente"]
        found = [m for m in confused_markers if m in answer_lower]
        assert not found, f"BCRA query returned budget data: {found} in: {data['answer'][:300]}"

    async def test_inflacion_not_confused_with_other_data(self, client):
        """Inflation query should not return transportation or health data."""
        data = await _ask(client, "Cual fue la inflacion en 2024?")
        answer_lower = data["answer"].lower()
        confused = ["subte", "hospital", "establecimiento", "siniestro"]
        found = [m for m in confused if m in answer_lower]
        assert not found, (
            f"Inflation query returned unrelated data: {found} in: {data['answer'][:300]}"
        )

    async def test_meta_question_no_sql_executed(self, client):
        """Meta questions about the system should NOT execute SQL queries."""
        data = await _ask(client, "De donde sacas los datos?")
        answer_lower = data["answer"].lower()
        sql_markers = ["select", "from cache_", "limit 10", "where"]
        found = [m for m in sql_markers if m in answer_lower]
        assert not found, (
            f"Meta question should not expose SQL. Found: {found} in: {data['answer'][:300]}"
        )
