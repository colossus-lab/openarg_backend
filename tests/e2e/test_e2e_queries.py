"""E2E query tests — validate full pipeline with real data.

Each test sends a real question through POST /api/v1/query/smart
and validates:
  1. HTTP 200 response
  2. Non-empty answer
  3. Relevant keywords in the answer
  4. Sources returned (when applicable)
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


def _headers() -> dict[str, str]:
    """Include API key header if BACKEND_API_KEY is set."""
    api_key = os.getenv("BACKEND_API_KEY", "")
    if api_key:
        return {"X-API-Key": api_key}
    return {}


def _smart_payload(question: str) -> dict:
    return {
        "question": question,
        "user_email": "e2e-test@openarg.org",
    }


# ── Test Cases ─────────────────────────────────────────────


class TestE2EQueries:
    """Core E2E query tests against the full LangGraph pipeline."""

    async def test_diputados_patrimonio(self, client):
        """DDJJ connector: patrimonio declarado de diputados."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("¿Quienes son los 10 diputados con mayor patrimonio declarado?"),
            headers=_headers(),
        )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        assert data["answer"], "Answer should not be empty"
        answer_lower = data["answer"].lower()
        assert any(kw in answer_lower for kw in ["diputado", "patrimonio", "declarad"]), (
            f"Answer should mention diputados/patrimonio: {data['answer'][:200]}"
        )

    async def test_inflacion(self, client):
        """Series de Tiempo connector: inflacion / IPC."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("¿Como viene la inflacion en los ultimos meses?"),
            headers=_headers(),
        )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        assert data["answer"], "Answer should not be empty"
        answer_lower = data["answer"].lower()
        assert any(kw in answer_lower for kw in ["inflaci", "ipc", "precio", "%", "porcentaje"]), (
            f"Answer should mention inflacion/IPC: {data['answer'][:200]}"
        )

    async def test_reservas_bcra(self, client):
        """BCRA connector: reservas internacionales."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("Mostrame la evolucion de las reservas del BCRA"),
            headers=_headers(),
        )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        assert data["answer"], "Answer should not be empty"
        answer_lower = data["answer"].lower()
        assert any(
            kw in answer_lower for kw in ["reserva", "bcra", "banco central", "usd", "dólar"]
        ), f"Answer should mention BCRA/reservas: {data['answer'][:200]}"

    async def test_datasets_educacion(self, client):
        """CKAN/Vector search: datasets de educacion."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("¿Que datasets de educacion hay en datos.gob.ar?"),
            headers=_headers(),
        )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        assert data["answer"], "Answer should not be empty"
        answer_lower = data["answer"].lower()
        assert any(kw in answer_lower for kw in ["educaci", "dataset", "escuela", "universidad"]), (
            f"Answer should mention educacion/datasets: {data['answer'][:200]}"
        )

    async def test_query_fuera_de_alcance(self, client):
        """Off-topic query: should still return 200 with a response."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("Quien es milei?"),
            headers=_headers(),
        )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()

        # Should return something (even if it says it's out of scope)
        assert data["answer"], "Answer should not be empty even for off-topic queries"


# ── Response Structure Tests ───────────────────────────────


class TestE2EResponseStructure:
    """Validate response schema compliance."""

    async def test_response_has_required_fields(self, client):
        """Every response should have answer + sources at minimum."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=_smart_payload("¿Como viene la inflacion?"),
            headers=_headers(),
        )

        assert resp.status_code == 200
        data = resp.json()

        # Required fields from SmartQueryV2Response
        assert "answer" in data
        assert "sources" in data
        assert isinstance(data["sources"], list)
        assert "confidence" in data
        assert isinstance(data["confidence"], int | float)
        assert 0 <= data["confidence"] <= 1

    async def test_empty_question_rejected(self, client):
        """Empty question should be rejected with 422."""
        resp = await client.post(
            "/api/v1/query/smart",
            json={"question": ""},
            headers=_headers(),
        )

        assert resp.status_code == 422, "Empty question should return 422 validation error"
