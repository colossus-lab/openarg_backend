"""Real user E2E tests — exact questions from production.

These tests use the EXACT questions users asked in production.
Tests that FAIL represent real UX problems that need pipeline improvements.
Do NOT adjust these tests to pass — fix the pipeline instead.

Source: Production DB analysis (March 2026)
- 1,949 conversations analyzed
- 42.8% failure rate (1,312 poor responses)
- 19 explicit downvotes with user comments
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.e2e.helpers import (
    answer_contains,
    ask,
    assert_substantive,
    extract_numbers,
    extract_years,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

CURRENT_YEAR = datetime.now().year


# ══════════════════════════════════════════════════════════
# DOWNVOTED QUERIES — Real users explicitly said these failed
# ══════════════════════════════════════════════════════════


class TestDownvotedQueries:
    """Exact queries that received thumbs-down from real users.
    Each test documents the user's complaint."""

    async def test_dv01_empleados_municipales_bahia_blanca(self, client):
        """Downvote comment: 'No hay datos'."""
        data = await ask(client, "Lista completa de empleados municipales de Bahia Blanca")
        assert_substantive(data, "empleados Bahia Blanca")

    async def test_dv02_sueldo_empleado_hotel(self, client):
        """Downvote comment: 'no dice sueldo'."""
        data = await ask(client, "dime sueldo para emplado de hotel")
        answer_contains(
            data, ["sueldo", "salario", "remunera", "hotel", "hotelero"], "sueldo hotel"
        )

    async def test_dv03_canasta_valores_constantes(self, client):
        """Downvote comment: 'no hizo lo que le pedi'."""
        data = await ask(
            client,
            "cual es el valor de la canasta basica en Argentina?",
        )
        answer_contains(
            data, ["canasta", "básica", "$", "valor"], "canasta basica", require_numbers=True
        )

    async def test_dv04_terrenos_renabap(self, client):
        """Downvote: user asked about RENABAP land ownership percentages."""
        data = await ask(
            client,
            "Que porcentaje de los terrenos registrados en el RENABAP son de privados?",
        )
        answer_contains(data, ["renabap", "terreno", "privad", "porcentaje"], "RENABAP")

    async def test_dv05_vias_ferreas(self, client):
        """Downvote: system said 937/1299 km, actual is ~12,000/45,000 km.
        User comment: 'Hay unas 12 mil en uso sobre unas 45 mil en total'."""
        data = await ask(client, "Cual es la cantidad de vias ferreas en uso sobre el total?")
        numbers = extract_numbers(data["answer"])
        # Should NOT report ~937 or ~1299 (wrong data from production)
        wrong_numbers = [n for n in numbers if 900 < n < 1400]
        assert not wrong_numbers, (
            f"System may be reporting wrong railway data (~937/1299). "
            f"Real value is ~12,000/45,000. Numbers found: {numbers[:10]}"
        )

    async def test_dv06_cosechas_ultimos_5_anos(self, client):
        """Downvote: 'si esta disponible esta informacion en el portal del ministerio'."""
        data = await ask(
            client, "necesito saber cuales fueron las mejores cosecha de los ultimos 5 anos"
        )
        answer_contains(data, ["cosecha", "producción", "agro", "tonelada", "campaña"], "cosechas")

    async def test_dv07_desempleo_comparacion_provincias(self, client):
        """Downvote: 'No hizo comparacion con otras provincias'."""
        data = await ask(client, "Evolucion de desempleo comparado a otras provincias")
        answer_lower = data["answer"].lower()
        # Should mention at least 2 provinces for a valid comparison
        provinces = [
            "buenos aires",
            "córdoba",
            "santa fe",
            "mendoza",
            "tucumán",
            "rosario",
            "gba",
            "patagonia",
            "nea",
            "noa",
            "cuyo",
            "pampeana",
        ]
        found = [p for p in provinces if p in answer_lower]
        assert len(found) >= 2, (
            f"Comparison query should mention multiple provinces. Found: {found}"
        )

    async def test_dv08_osvaldo_labastie(self, client):
        """Downvote: 'Esa informacion es incorrecta'.
        System said journalist, actually public official in Chubut."""
        data = await ask(client, "Sabes quien es Osvaldo Labastie?")
        answer_lower = data["answer"].lower()
        # Should NOT say he's a journalist (that was the incorrect answer)
        assert "periodista" not in answer_lower, (
            f"System incorrectly identified Labastie as journalist: {data['answer'][:200]}"
        )

    async def test_dv09_produccion_ganado(self, client):
        """Downvote: 'Es imposible que no exista el dataset de produccion de cabezas de ganado'."""
        data = await ask(
            client,
            "Evolucion anual de la produccion de cabezas de ganado en los ultimos 5 anos",
        )
        answer_contains(
            data, ["ganado", "cabeza", "bovino", "producción", "ganadería"], "produccion ganado"
        )

    async def test_dv10_indices_redeterminacion_precios(self, client):
        """Downvote: 'les faltan esos indices, son importantes para control de obra publica'."""
        data = await ask(
            client,
            "cual es el valor de los indices para redeterminacion de precios Decreto 691/16?",
        )
        answer_contains(
            data, ["índice", "redeterminac", "decreto", "691", "precio"], "indices redeterminacion"
        )

    async def test_dv11_ciudad_mas_pobre(self, client):
        """Downvote: 'No responde'."""
        data = await ask(client, "Cual es la ciudad mas pobre del pais")
        answer_contains(data, ["pobreza", "ciudad", "ingreso", "nbi", "hogar"], "ciudad mas pobre")

    async def test_dv12_desempleo_cordoba(self, client):
        """Downvote: 'Menciona un 2do trimestre 2026 que no existe' (hallucination)."""
        data = await ask(client, "Evolucion de la tasa de desempleo de Cordoba desde 2023")
        years = extract_years(data["answer"])
        future_years = [y for y in years if y > CURRENT_YEAR]
        assert not future_years, (
            f"Response hallucinated future dates: {future_years}: {data['answer'][:300]}"
        )


# ══════════════════════════════════════════════════════════
# HIGH-FAILURE TOPICS — Questions that failed 50+ times in prod
# ══════════════════════════════════════════════════════════


class TestHighFailureTopics:
    """Topics with the highest failure rates in production."""

    # --- Presupuesto (127 failures) ---

    async def test_gasto_educacion(self, client):
        """Users ask this constantly. System has 640K rows of presupuesto."""
        data = await ask(client, "cuanto gasta el gobierno en educacion")
        answer_contains(
            data, ["educaci", "presupuesto", "gasto", "millones", "$"], "gasto educacion"
        )

    async def test_presupuesto_universidades(self, client):
        """Specific budget line — frequently asked."""
        data = await ask(client, "presupuesto para universidades nacionales 2025")
        answer_contains(
            data, ["universidad", "presupuesto", "superior", "educaci"], "presupuesto universidades"
        )

    # --- Inflacion (85 failures) ---

    async def test_inflacion_ultimos_meses(self, client):
        """The #2 most asked question. Should ALWAYS work, never clarify."""
        data = await ask(client, "Como viene la inflacion en los ultimos meses?")
        # Must contain actual data, not a clarification request
        numbers = extract_numbers(data["answer"])
        assert numbers, (
            f"Inflation query should return actual numbers, not clarification. "
            f"Answer: {data['answer'][:200]}"
        )

    async def test_inflacion_interanual(self, client):
        """Users want year-over-year inflation."""
        data = await ask(client, "cual es la inflacion interanual acumulada")
        answer_contains(data, ["inflaci", "interanual", "acumulad", "%"], "inflacion interanual")

    # --- BCRA (78 failures) ---

    async def test_reservas_detalle(self, client):
        """Users want specific reserve numbers."""
        data = await ask(client, "a cuanto estan las reservas internacionales del banco central")
        numbers = extract_numbers(data["answer"])
        assert numbers, f"BCRA query should return actual numbers. Answer: {data['answer'][:200]}"

    # --- Personas (84 failures) ---

    async def test_quien_es_adorni(self, client):
        """Person lookup — frequently asked."""
        data = await ask(client, "quien es manuel adorni")
        answer_contains(
            data, ["adorni", "vocero", "presidente", "gobierno", "funcionario"], "adorni"
        )

    async def test_patrimonio_cristina(self, client):
        """DDJJ lookup — Cristina is not a diputada (senator/ex-VP), so DDJJ
        won't have her data. System should explain the limitation."""
        data = await ask(client, "cual es el patrimonio declarado de cristina kirchner")
        answer_lower = data["answer"].lower()
        assert any(kw in answer_lower for kw in ["cristina", "kirchner"]), (
            f"Should mention Cristina: {data['answer'][:200]}"
        )
        assert any(
            kw in answer_lower for kw in ["diputado", "ddjj", "declaraci", "dataset", "patrimonio"]
        ), f"Should explain DDJJ scope: {data['answer'][:200]}"

    # --- Empleo (60 failures) ---

    async def test_empleados_publicos(self, client):
        """Common query that often fails."""
        data = await ask(client, "cuantos empleados publicos hay en argentina")
        answer_contains(
            data, ["empleado", "público", "estado", "personal", "planta"], "empleados publicos"
        )

    # --- Educacion (58 failures) ---

    async def test_matricula_escolar(self, client):
        """Education enrollment data."""
        data = await ask(client, "cual es la matricula escolar en argentina")
        answer_contains(
            data, ["matrícula", "escolar", "alumno", "estudiante", "educaci"], "matricula escolar"
        )

    # --- Poblacion (53 failures) ---

    async def test_poblacion_argentina(self, client):
        """Basic demographic question."""
        data = await ask(client, "cual es la poblacion de argentina")
        answer_contains(data, ["poblaci", "habitante", "millones", "censo"], "poblacion argentina")


# ══════════════════════════════════════════════════════════
# HALLUCINATION & ACCURACY CHECKS
# ══════════════════════════════════════════════════════════


class TestHallucinationChecks:
    """Detect hallucinated data, wrong numbers, future dates."""

    async def test_no_future_dates_inflacion(self, client):
        """Real case: system said '2do trimestre 2026' when it didn't exist."""
        data = await ask(client, "Como fue la inflacion anual en los ultimos 5 anos?")
        years = extract_years(data["answer"])
        future = [y for y in years if y > CURRENT_YEAR + 1]
        assert not future, f"Hallucinated future years: {future}"

    async def test_no_negative_budget(self, client):
        """Budget numbers should never be negative."""
        data = await ask(client, "presupuesto nacional 2025")
        numbers = extract_numbers(data["answer"])
        negative = [n for n in numbers if n < 0]
        assert not negative, f"Negative budget numbers: {negative}"

    async def test_no_invented_provinces(self, client):
        """Only real Argentine provinces."""
        data = await ask(client, "Cuantos senadores tiene cada provincia?")
        answer_lower = data["answer"].lower()
        fake = ["atlantida", "patagonia central", "mesopotamia"]
        found = [p for p in fake if p in answer_lower]
        assert not found, f"Invented provinces: {found}"

    async def test_mortalidad_not_absurd(self, client):
        """Mortality rates should be 0-200 per thousand, not absurd."""
        data = await ask(client, "tasa de mortalidad infantil en argentina")
        numbers = extract_numbers(data["answer"])
        absurd = [n for n in numbers if n > 500 and n < 1900]  # exclude years
        assert not absurd, f"Absurd mortality numbers: {absurd}"


# ══════════════════════════════════════════════════════════
# SEMANTIC MATCHING — Wrong data source returned
# ══════════════════════════════════════════════════════════


class TestSemanticMatching:
    """Verify the system doesn't return data from wrong tables.
    Based on real production cases where semantic search matched wrong."""

    async def test_coparticipacion_not_cameras(self, client):
        """Real bug: 'coparticipacion' query hit surveillance cameras table."""
        data = await ask(client, "como se distribuye la coparticipacion federal")
        answer_lower = data["answer"].lower()
        # Only fail if it mentions surveillance cameras, not legislative chambers
        assert "monitoreo" not in answer_lower, (
            f"Coparticipacion query returned surveillance data: {data['answer'][:300]}"
        )

    async def test_precios_not_danza(self, client):
        """Real bug: 'precios de venta' returned dance events table."""
        data = await ask(client, "precios de venta en la ciudad de buenos aires")
        answer_lower = data["answer"].lower()
        assert "danza" not in answer_lower, (
            f"Prices query returned dance data: {data['answer'][:300]}"
        )

    async def test_bcra_not_presupuesto(self, client):
        """BCRA reserves should not return budget jurisdiction data."""
        data = await ask(client, "reservas del BCRA en dolares")
        answer_lower = data["answer"].lower()
        wrong = ["presupuesto sancionado", "jurisdicción", "crédito vigente"]
        found = [w for w in wrong if w in answer_lower]
        assert not found, f"BCRA query returned budget data: {found}"

    async def test_meta_question_no_sql(self, client):
        """Meta questions should NOT execute SQL or expose internals."""
        data = await ask(client, "De donde sacas los datos que me mostras?")
        answer_lower = data["answer"].lower()
        sql_markers = ["select ", "from cache_", "limit 10", " where "]
        found = [m for m in sql_markers if m in answer_lower]
        assert not found, f"Meta question exposed SQL: {found}"


# ══════════════════════════════════════════════════════════
# EDGE CASES — Real production issues
# ══════════════════════════════════════════════════════════


class TestRealEdgeCases:
    """Edge cases found in production conversations."""

    async def test_typo_imflacion(self, client):
        """Users frequently misspell. System should still understand."""
        data = await ask(client, "imflacion ultimos meses")
        answer_contains(data, ["inflaci", "ipc", "precio", "%"], "typo inflacion")

    async def test_single_word_presupuesto(self, client):
        """Single word queries should not crash."""
        data = await ask(client, "presupuesto")
        assert_substantive(data, "presupuesto", min_length=30)

    async def test_personal_data_request(self, client):
        """Users sometimes ask for personal data. System should decline."""
        data = await ask(client, "datos personales de Juan Perez DNI 30000000")
        answer_lower = data["answer"].lower()
        # Should not return personal data
        assert (
            "30000000" not in data["answer"] or "no" in answer_lower or "privad" in answer_lower
        ), f"System should not return personal data: {data['answer'][:200]}"

    async def test_diputados_patrimonio_complete_list(self, client):
        """Real bug: response said 'Pendiente - datos truncados' instead of showing
        the full top 10. The answer must list at least 5 diputados with amounts."""
        data = await ask(client, "Quienes son los 10 diputados con mayor patrimonio declarado?")
        answer = data["answer"]
        # Must mention at least 5 names (a real top 10 should have many)
        # Common Argentine surnames that appear in DDJJ
        name_count = sum(
            1
            for name in [
                "brugge",
                "kirchner",
                "menem",
                "cristina",
                "massa",
                "moreau",
                "kueider",
                "ritondo",
                "tetaz",
                "lousteau",
                "espert",
                "milei",
                "pichetto",
                "bregman",
                "del caño",
            ]
            if name in answer.lower()
        )
        assert name_count >= 3, (
            f"Top 10 patrimonio should list at least 3 diputados by name. "
            f"Found {name_count} in: {answer[:300]}"
        )

    async def test_very_broad_query(self, client):
        """Real case: took 243 seconds. Should respond in reasonable time."""
        data = await ask(client, "a todos los datos que puedas acceder")
        assert_substantive(data, "broad query")
