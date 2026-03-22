"""E2E query tests — validate full pipeline with real data.

Tests are organized by category:
  - Core: basic queries that must always work (original 5)
  - Presupuesto: budget queries against 640K+ rows of presupuesto data
  - Congreso: diputados, senado, leyes, staff
  - Salud: mortalidad, establecimientos de salud
  - Economia: inflacion, BCRA, deuda, dolar
  - Downvotes: regression tests from real user complaints
  - Edge cases: meta questions, typos, off-topic

Each test validates:
  1. HTTP 200 response
  2. Non-empty answer (not an error message)
  3. Relevant keywords in the answer
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_ERROR_PHRASES = ["ocurrió un error", "error al analizar", "probá reformulando"]


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


async def _ask(client, question: str) -> dict:
    """Send a question and return parsed response. Asserts 200 + no error."""
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


def _answer_contains(data: dict, keywords: list[str], question: str) -> None:
    """Assert at least one keyword is in the answer."""
    answer_lower = data["answer"].lower()
    assert any(kw in answer_lower for kw in keywords), (
        f"Answer for '{question}' should mention {keywords}: {data['answer'][:300]}"
    )


# ── Core Queries (original 5) ─────────────────────────────


class TestCoreQueries:
    """Baseline queries that must always work."""

    async def test_diputados_patrimonio(self, client):
        """DDJJ connector: patrimonio declarado de diputados."""
        data = await _ask(client, "¿Quienes son los 10 diputados con mayor patrimonio declarado?")
        _answer_contains(data, ["diputado", "patrimonio", "declarad"], "patrimonio diputados")

    async def test_inflacion(self, client):
        """Series de Tiempo: inflacion / IPC."""
        data = await _ask(client, "¿Como viene la inflacion en los ultimos meses?")
        _answer_contains(data, ["inflaci", "ipc", "precio", "%", "porcentaje"], "inflacion")

    async def test_reservas_bcra(self, client):
        """BCRA connector: reservas internacionales."""
        data = await _ask(client, "Mostrame la evolucion de las reservas del BCRA")
        _answer_contains(
            data, ["reserva", "bcra", "banco central", "usd", "dólar"], "reservas BCRA"
        )

    async def test_datasets_educacion(self, client):
        """CKAN/Vector search: datasets de educacion."""
        data = await _ask(client, "¿Que datasets de educacion hay en datos.gob.ar?")
        _answer_contains(
            data, ["educaci", "dataset", "escuela", "universidad", "dato"], "datasets educacion"
        )

    async def test_query_fuera_de_alcance(self, client):
        """Off-topic: should return 200 with some response."""
        data = await _ask(client, "Quien es milei?")
        # Just verify it answered something (could be off-scope message)
        assert len(data["answer"]) > 20, "Answer too short for off-topic query"


# ── Presupuesto (127 production failures) ─────────────────


class TestPresupuesto:
    """Budget queries — system has 640K+ rows of presupuesto data.
    These were failing 127 times in production despite having the data."""

    async def test_presupuesto_educacion(self, client):
        """User asked this repeatedly and got no data."""
        data = await _ask(client, "Cual fue el presupuesto de educacion en 2024?")
        _answer_contains(
            data,
            ["presupuesto", "educaci", "crédito", "millones", "$", "2024"],
            "presupuesto educacion",
        )

    async def test_presupuesto_salud(self, client):
        """Gasto en salud — common query that failed in production."""
        data = await _ask(client, "Cuanto se gasto en salud en el presupuesto 2025?")
        _answer_contains(
            data, ["salud", "presupuesto", "crédito", "gasto", "2025"], "presupuesto salud"
        )

    async def test_presupuesto_por_ministerio(self, client):
        """Breakdown by jurisdiction — 640K rows should answer this."""
        data = await _ask(client, "Cuales son los 5 ministerios con mayor presupuesto en 2026?")
        _answer_contains(
            data,
            ["ministerio", "jurisdicci", "presupuesto", "crédito"],
            "presupuesto por ministerio",
        )


# ── Congreso (diputados, senado, leyes, staff) ────────────


class TestCongreso:
    """Congressional data — diputados, senadores, leyes, staff."""

    async def test_cantidad_diputados(self, client):
        """Basic count from cache_diputados (572 rows)."""
        data = await _ask(client, "Cuantos diputados hay en actividad?")
        _answer_contains(data, ["diputado", "actividad", "banca", "cámara"], "cantidad diputados")

    async def test_senadores_por_provincia(self, client):
        """Senadores table has 72 rows with PROVINCIA column."""
        data = await _ask(client, "Cuantos senadores tiene cada provincia?")
        _answer_contains(
            data, ["senador", "provincia", "buenos aires", "córdoba", "santa fe"], "senadores"
        )

    async def test_leyes_sancionadas(self, client):
        """cache_leyes_sancionadas has 8207 rows."""
        data = await _ask(client, "Cuantas leyes se sancionaron en la camara de diputados?")
        _answer_contains(data, ["ley", "sancion", "diputado"], "leyes sancionadas")

    async def test_empleados_diputados(self, client):
        """staff_snapshots has 18K+ rows of HCDN staff."""
        data = await _ask(client, "Cuantos empleados tiene la camara de diputados?")
        _answer_contains(
            data, ["empleado", "personal", "diputado", "cámara", "staff"], "staff HCDN"
        )


# ── Salud / Mortalidad ─────────────────────────────────────


class TestSalud:
    """Health data — mortality, hospitals, health professionals."""

    async def test_mortalidad_infantil(self, client):
        """3 failures in prod. Table has 454K+ rows."""
        data = await _ask(client, "Cual es la tasa de mortalidad infantil en Argentina?")
        _answer_contains(
            data,
            ["mortalidad", "infantil", "tasa", "defunci", "nacido"],
            "mortalidad infantil",
        )

    async def test_establecimientos_salud(self, client):
        """32K+ establishments registered."""
        data = await _ask(client, "Cuantos establecimientos de salud hay registrados?")
        _answer_contains(
            data,
            ["establecimiento", "salud", "hospital", "registrad"],
            "establecimientos salud",
        )


# ── Economia ───────────────────────────────────────────────


class TestEconomia:
    """Economic queries — inflation, BCRA, debt, exchange rates."""

    async def test_dolar(self, client):
        """ArgentinaDatos connector: tipo de cambio."""
        data = await _ask(client, "A cuanto esta el dolar hoy?")
        _answer_contains(data, ["dólar", "dolar", "tipo de cambio", "$", "peso"], "dolar")

    async def test_deuda_externa(self, client):
        """3 failures in prod. Should have data."""
        data = await _ask(client, "Cual es la deuda externa de Argentina?")
        _answer_contains(data, ["deuda", "extern", "millones", "usd", "dólar"], "deuda externa")

    async def test_inflacion_historica(self, client):
        """Series de Tiempo should handle historical queries."""
        data = await _ask(client, "Como fue la inflacion anual en Argentina en los ultimos 5 anos?")
        _answer_contains(
            data, ["inflaci", "ipc", "anual", "%", "2024", "2023"], "inflacion historica"
        )


# ── Downvotes — Regression Tests ──────────────────────────


class TestDownvoteRegression:
    """Tests based on real user downvotes with comments.
    These are the queries where users explicitly said the answer was wrong."""

    async def test_programas_sociales(self, client):
        """User asked for social programs list — system failed."""
        data = await _ask(client, "Listado de programas sociales vigentes en Argentina")
        _answer_contains(
            data, ["programa", "social", "potenciar", "beneficiar"], "programas sociales"
        )

    async def test_desempleo(self, client):
        """User complained: 'no hizo comparacion con otras provincias'."""
        data = await _ask(client, "Cual es la tasa de desempleo en Argentina?")
        _answer_contains(data, ["desempleo", "empleo", "tasa", "ocupaci", "%"], "desempleo")

    async def test_estaciones_subte(self, client):
        """Popular query — infrastructure data from CABA."""
        data = await _ask(client, "Cuantas estaciones de subte hay en Buenos Aires?")
        _answer_contains(data, ["estacion", "subte", "línea", "buenos aires"], "estaciones subte")


# ── Edge Cases ─────────────────────────────────────────────


class TestEdgeCases:
    """Edge cases found in production — meta questions, typos, validation."""

    async def test_meta_question_no_crash(self, client):
        """Meta questions about the system should NOT crash or hit SQL."""
        data = await _ask(client, "De donde sacas los datos que me mostras?")
        assert len(data["answer"]) > 30, "Meta question should get a substantial response"

    async def test_typo_inflacion(self, client):
        """Users often write with typos — system should still work."""
        data = await _ask(client, "imflacion ultimos meses")
        _answer_contains(data, ["inflaci", "ipc", "precio", "%"], "typo inflacion")

    async def test_very_short_query(self, client):
        """Single word queries should not crash."""
        data = await _ask(client, "presupuesto")
        assert len(data["answer"]) > 20, "Single-word query should get some response"

    async def test_empty_question_rejected(self, client):
        """Empty question should be rejected with 422."""
        resp = await client.post(
            "/api/v1/query/smart",
            json={"question": ""},
            headers=_headers(),
        )
        assert resp.status_code == 422, "Empty question should return 422 validation error"


# ── Response Structure ─────────────────────────────────────


class TestResponseStructure:
    """Validate response schema compliance."""

    async def test_response_has_required_fields(self, client):
        """Every response should have answer + sources at minimum."""
        data = await _ask(client, "¿Como viene la inflacion?")

        assert "answer" in data
        assert "sources" in data
        assert isinstance(data["sources"], list)
        assert "confidence" in data
        assert isinstance(data["confidence"], int | float)
        assert 0 <= data["confidence"] <= 1
