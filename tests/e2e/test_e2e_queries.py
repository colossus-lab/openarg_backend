"""E2E query tests — validate full pipeline with real data.

Tests are organized by category:
  - Core: basic queries that must always work (original 5)
  - Presupuesto: budget queries against 640K+ rows of presupuesto data
  - Congreso: diputados, senado, leyes, staff
  - Salud: mortalidad, establecimientos de salud
  - Economia: inflacion, BCRA, deuda, dolar
  - Downvotes: regression tests from real user complaints
  - Edge cases: meta questions, typos, off-topic
"""

from __future__ import annotations

import pytest

from tests.e2e.helpers import (
    answer_contains,
    ask,
    assert_substantive,
    extract_numbers,
    headers,
    validate_numbers_in_range,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


# ── Core Queries (original 5) ─────────────────────────────


class TestCoreQueries:
    """Baseline queries that must always work."""

    async def test_diputados_patrimonio(self, client):
        """DDJJ connector: patrimonio declarado de diputados."""
        data = await ask(client, "¿Quienes son los 10 diputados con mayor patrimonio declarado?")
        answer_contains(
            data,
            ["diputado", "patrimonio", "declarad"],
            "patrimonio diputados",
            require_numbers=True,
        )

    async def test_inflacion(self, client):
        """Series de Tiempo: inflacion / IPC + response structure validation."""
        data = await ask(client, "¿Como viene la inflacion en los ultimos meses?")
        answer_contains(
            data, ["inflaci", "ipc", "precio", "%", "porcentaje"], "inflacion", require_numbers=True
        )
        validate_numbers_in_range(data, "inflacion_mensual", "inflacion mensual")
        assert "sources" in data
        assert isinstance(data["sources"], list)
        assert "confidence" in data
        assert isinstance(data["confidence"], int | float)
        assert 0 <= data["confidence"] <= 1

    async def test_reservas_bcra(self, client):
        """BCRA connector: reservas internacionales."""
        data = await ask(client, "Mostrame la evolucion de las reservas del BCRA")
        answer_contains(
            data,
            ["reserva", "bcra", "banco central", "usd", "dólar"],
            "reservas BCRA",
            require_numbers=True,
        )

    async def test_datasets_educacion(self, client):
        """CKAN/Vector search: datasets de educacion."""
        data = await ask(client, "¿Que datasets de educacion hay en datos.gob.ar?")
        answer_contains(
            data, ["educaci", "dataset", "escuela", "universidad", "dato"], "datasets educacion"
        )

    async def test_query_fuera_de_alcance(self, client):
        """Off-topic: should return 200 with some response."""
        data = await ask(client, "Quien es milei?")
        assert_substantive(data, "Quien es milei?")


# ── Presupuesto (127 production failures) ─────────────────


class TestPresupuesto:
    """Budget queries — system has 640K+ rows of presupuesto data."""

    async def test_presupuesto_educacion(self, client):
        """User asked this repeatedly. Also validates no negative numbers."""
        data = await ask(client, "Cual fue el presupuesto de educacion en 2024?")
        answer_contains(
            data,
            ["presupuesto", "educaci", "crédito", "millones", "$", "2024"],
            "presupuesto educacion",
            require_numbers=True,
        )
        numbers = extract_numbers(data["answer"])
        negative = [n for n in numbers if n < 0]
        assert not negative, f"Budget numbers should not be negative: {negative}"

    async def test_presupuesto_salud(self, client):
        """Gasto en salud — common query that failed in production."""
        data = await ask(client, "Cuanto se gasto en salud en el presupuesto 2025?")
        answer_contains(
            data,
            ["salud", "presupuesto", "crédito", "gasto", "2025"],
            "presupuesto salud",
            require_numbers=True,
        )

    async def test_presupuesto_por_ministerio(self, client):
        """Breakdown by jurisdiction — 640K rows should answer this."""
        data = await ask(client, "Cuales son los 5 ministerios con mayor presupuesto en 2026?")
        answer_contains(
            data,
            ["ministerio", "jurisdicci", "presupuesto", "crédito"],
            "presupuesto por ministerio",
            require_numbers=True,
        )


# ── Congreso (diputados, senado, leyes, staff) ────────────


class TestCongreso:
    """Congressional data — diputados, senadores, leyes, staff."""

    async def test_cantidad_diputados(self, client):
        """Basic count from cache_diputados (572 rows)."""
        data = await ask(client, "Cuantos diputados hay en actividad?")
        answer_contains(
            data,
            ["diputado", "actividad", "banca", "cámara"],
            "cantidad diputados",
            require_numbers=True,
        )

    async def test_senadores_por_provincia(self, client):
        """Senadores table has 72 rows with PROVINCIA column."""
        data = await ask(client, "Cuantos senadores tiene cada provincia?")
        answer_contains(
            data,
            ["senador", "provincia", "buenos aires", "córdoba", "santa fe"],
            "senadores",
            require_numbers=True,
        )

    async def test_leyes_sancionadas(self, client):
        """cache_leyes_sancionadas has 8207 rows."""
        data = await ask(client, "Cuantas leyes se sancionaron en la camara de diputados?")
        answer_contains(
            data, ["ley", "sancion", "diputado"], "leyes sancionadas", require_numbers=True
        )

    async def test_empleados_diputados(self, client):
        """staff_snapshots has 18K+ rows of HCDN staff."""
        data = await ask(client, "Cuantos empleados tiene la camara de diputados?")
        answer_contains(
            data,
            ["empleado", "personal", "diputado", "cámara", "staff"],
            "staff HCDN",
            require_numbers=True,
        )


# ── Salud / Mortalidad ─────────────────────────────────────


class TestSalud:
    """Health data — mortality, hospitals, health professionals."""

    async def test_mortalidad_infantil(self, client):
        """3 failures in prod. Also validates rates are in reasonable range."""
        data = await ask(client, "Cual es la tasa de mortalidad infantil en Argentina?")
        answer_contains(
            data,
            ["mortalidad", "infantil", "tasa", "defunci", "nacido"],
            "mortalidad infantil",
            require_numbers=True,
        )
        validate_numbers_in_range(data, "tasa_mortalidad", "mortalidad infantil")

    async def test_establecimientos_salud(self, client):
        """32K+ establishments registered."""
        data = await ask(client, "Cuantos establecimientos de salud hay registrados?")
        answer_contains(
            data,
            ["establecimiento", "salud", "hospital", "registrad"],
            "establecimientos salud",
            require_numbers=True,
        )


# ── Economia ───────────────────────────────────────────────


class TestEconomia:
    """Economic queries — inflation, BCRA, debt, exchange rates."""

    async def test_dolar(self, client):
        """ArgentinaDatos connector: tipo de cambio."""
        data = await ask(client, "A cuanto esta el dolar hoy?")
        answer_contains(
            data, ["dólar", "dolar", "tipo de cambio", "$", "peso"], "dolar", require_numbers=True
        )

    async def test_deuda_externa(self, client):
        """3 failures in prod. Should have data."""
        data = await ask(client, "Cual es la deuda externa de Argentina?")
        answer_contains(
            data,
            ["deuda", "extern", "millones", "usd", "dólar"],
            "deuda externa",
            require_numbers=True,
        )

    async def test_inflacion_historica(self, client):
        """Series de Tiempo should handle historical queries."""
        data = await ask(client, "Como fue la inflacion anual en Argentina en los ultimos 5 anos?")
        answer_contains(
            data,
            ["inflaci", "ipc", "anual", "%", "2024", "2023"],
            "inflacion historica",
            require_numbers=True,
        )
        validate_numbers_in_range(data, "inflacion_anual", "inflacion anual")


# ── Downvotes — Regression Tests ──────────────────────────


class TestDownvoteRegression:
    """Tests based on real user downvotes."""

    async def test_programas_sociales(self, client):
        """User asked for social programs list — system failed."""
        data = await ask(client, "Listado de programas sociales vigentes en Argentina")
        answer_contains(
            data, ["programa", "social", "potenciar", "beneficiar"], "programas sociales"
        )

    async def test_desempleo(self, client):
        """User complained: 'no hizo comparacion con otras provincias'."""
        data = await ask(client, "Cual es la tasa de desempleo en Argentina?")
        answer_contains(
            data, ["desempleo", "empleo", "tasa", "ocupaci", "%"], "desempleo", require_numbers=True
        )


# ── Edge Cases ─────────────────────────────────────────────


class TestEdgeCases:
    """Edge cases found in production — meta questions, typos, validation."""

    async def test_meta_question_no_crash(self, client):
        """Meta questions about the system should NOT crash or hit SQL."""
        data = await ask(client, "De donde sacas los datos que me mostras?")
        assert_substantive(data, "meta question")

    async def test_typo_inflacion(self, client):
        """Users often write with typos — system should still work."""
        data = await ask(client, "imflacion ultimos meses")
        answer_contains(
            data, ["inflaci", "ipc", "precio", "%"], "typo inflacion", require_numbers=True
        )

    async def test_very_short_query(self, client):
        """Single word queries should not crash."""
        data = await ask(client, "presupuesto")
        assert_substantive(data, "presupuesto")

    async def test_empty_question_rejected(self, client):
        """Empty question should be rejected with 422."""
        resp = await client.post(
            "/api/v1/query/smart",
            json={"question": ""},
            headers=headers(),
        )
        assert resp.status_code == 422, "Empty question should return 422 validation error"
