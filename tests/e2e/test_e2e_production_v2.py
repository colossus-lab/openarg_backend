"""E2E tests v2 — based on production database analysis (March 2026).

Source: 1,965 conversations, 7,573 messages, 30 explicit feedback ratings.
Tests use EXACT user queries from production. Do NOT adjust tests to pass —
fix the pipeline instead.
"""

from __future__ import annotations

import pytest

from tests.e2e.helpers import (
    answer_contains,
    ask,
    assert_substantive,
    extract_numbers,
    headers,
    smart_payload,
    validate_scope,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


# ══════════════════════════════════════════════════════════
# COMPARISONS — Users called this "killer feature"
# ══════════════════════════════════════════════════════════


class TestComparisons:
    """Real comparison queries from production."""

    async def test_patrimonio_cristina_vs_macri(self, client):
        """Neither Cristina (senator/ex-VP) nor Macri (ex-president) are in the
        DDJJ dataset which only covers 195 Diputados Nacionales. The system
        should acknowledge the query and explain the data-coverage limitation
        rather than hallucinate numbers. It's OK to say 'no encontré' here
        because the data genuinely doesn't exist."""
        data = await ask(client, "Patrimonio de Cristina VS Macri")
        answer_lower = data["answer"].lower()
        # Should mention both names and explain the limitation
        assert any(kw in answer_lower for kw in ["cristina", "kirchner"]), (
            f"Should mention Cristina: {data['answer'][:200]}"
        )
        assert any(kw in answer_lower for kw in ["macri", "mauricio"]), (
            f"Should mention Macri: {data['answer'][:200]}"
        )
        # Should mention diputados limitation or DDJJ scope
        assert any(kw in answer_lower for kw in ["diputado", "ddjj", "declaraci", "dataset"]), (
            f"Should explain DDJJ scope: {data['answer'][:200]}"
        )

    async def test_inflacion_nacional_vs_pba_vs_caba(self, client):
        """System only has national IPC — should return national data and explain
        that provincial IPC comparison is not available."""
        data = await ask(
            client, "Comparar inflación del gobierno nacional con la de PBA y la de CABA"
        )
        answer_contains(
            data,
            ["inflaci", "ipc", "nacional", "%", "precio"],
            "inflacion comparacion",
            require_numbers=True,
        )

    async def test_desocupacion_salta_vs_provincias(self, client):
        data = await ask(
            client,
            "ME GUSTARIA VER LA TASA DE DESOCUPACIÓN EN SALTA, COMPARADA CON OTRAS PROVINCIAS",
        )
        answer_contains(
            data,
            ["salta", "desocupaci", "desempleo", "tasa"],
            "desocupacion Salta",
            require_numbers=True,
        )

    async def test_inflacion_milei_vs_fernandez(self, client):
        data = await ask(
            client,
            "Dame comparativas de los índices de desocupación durante la gestión de Alberto Fernández y la de milei",
        )
        answer_contains(
            data,
            ["fernández", "milei", "desocupación", "desempleo", "gestión"],
            "Milei vs Fernandez",
        )

    async def test_inflacion_real_vs_sueldo(self, client):
        data = await ask(
            client,
            "Podés comparar la inflación real vs sueldo bruto de un cpa principal de conicet",
        )
        answer_contains(data, ["inflaci", "sueldo", "salario", "conicet"], "inflacion vs sueldo")

    async def test_reservas_vs_tipo_cambio(self, client):
        data = await ask(
            client,
            "Mostrarme la evolución de las reservas en gráfico de serie de tiempo y compáralo con el tipo de cambio oficial",
        )
        answer_contains(
            data,
            ["reserva", "tipo de cambio", "evolución"],
            "reservas vs tipo cambio",
            require_numbers=True,
        )


# ══════════════════════════════════════════════════════════
# SPECIFIC DATA REQUESTS
# ══════════════════════════════════════════════════════════


class TestSpecificDataRequests:
    """Queries where users want specific, concrete data."""

    async def test_presupuesto_universidades(self, client):
        data = await ask(client, "Presupuesto para universidades publicas 2023, 2024, 2025, 2026")
        answer_contains(
            data,
            ["universidad", "presupuesto", "$", "millones"],
            "presupuesto universidades",
            require_numbers=True,
        )

    async def test_gasto_publico_por_ano(self, client):
        data = await ask(client, "gasto público total por año")
        answer_contains(
            data,
            ["gasto", "público", "año", "$", "presupuesto"],
            "gasto publico",
            require_numbers=True,
        )

    async def test_ipc_nacional(self, client):
        data = await ask(client, "ipc nacional")
        answer_contains(
            data, ["ipc", "inflaci", "índice", "precio"], "IPC nacional", require_numbers=True
        )
        validate_scope(data, "nacional", ["gba", "gran buenos aires"], "IPC scope")

    async def test_pbi_argentina(self, client):
        data = await ask(client, "Cual es el PBI de Argentina")
        answer_contains(
            data, ["pbi", "producto", "bruto", "$", "millones"], "PBI", require_numbers=True
        )

    async def test_cuantos_barrios_caba(self, client):
        data = await ask(client, "cuantos barrios tiene la ciudad de buenos aires?")
        answer_contains(
            data, ["barrio", "buenos aires", "comuna"], "barrios CABA", require_numbers=True
        )

    async def test_estaciones_subte(self, client):
        data = await ask(client, "cuantas estaciones de subte hay en la ciudad de buenos aires?")
        answer_contains(
            data, ["estacion", "subte", "línea"], "estaciones subte", require_numbers=True
        )

    async def test_vacunas_covid_2025(self, client):
        data = await ask(
            client, "Cuantas dosis de vacunas contra el covid se dieron en argentina en 2025?"
        )
        assert_substantive(data, "vacunas covid")

    async def test_reservas_netas(self, client):
        data = await ask(
            client,
            "me interesa que busques datos sobre las reservas netas o de libre disponibilidad",
        )
        answer_contains(
            data,
            ["reserva", "neta", "libre disponibilidad", "bcra"],
            "reservas netas",
            require_numbers=True,
        )

    async def test_serie_desestacionalizada_empleo(self, client):
        """Downvote: 'Quería una serie, me dio un dato'."""
        data = await ask(client, "Serie desestionalizada de ocupación y empleo, últimos 10 años")
        numbers = extract_numbers(data["answer"])
        assert len(numbers) >= 3, (
            f"User wants a SERIES (multiple data points), not a single number. "
            f"Found {len(numbers)} numbers: {data['answer'][:200]}"
        )


# ══════════════════════════════════════════════════════════
# PERSON LOOKUPS
# ══════════════════════════════════════════════════════════


class TestPersonLookups:
    """Queries about specific people."""

    async def test_adorni_patrimonio(self, client):
        data = await ask(client, "cuanta plata tiene adorni")
        answer_contains(data, ["adorni", "patrimonio", "declaraci", "vocero"], "adorni patrimonio")

    async def test_adorni_empresas_estado(self, client):
        data = await ask(
            client, "que empresas relacionadas a manuel adorni tienen contrato con el estado"
        )
        answer_contains(data, ["adorni", "empresa", "contrato", "estado"], "adorni empresas")

    async def test_bullrich_typo(self, client):
        data = await ask(client, "patricia bullrich")
        answer_contains(data, ["bullrich", "patricia", "ministr", "seguridad"], "bullrich")

    async def test_diputado_yeza(self, client):
        data = await ask(client, "quien es el diputado martin yeza?")
        answer_contains(data, ["yeza", "diputado"], "diputado yeza")

    async def test_diputados_partido(self, client):
        data = await ask(
            client,
            "Quienes son los 10 diputados con mayor patrimonio declarado? De qué partido es cada uno?",
        )
        answer_contains(
            data,
            ["partido", "bloque", "patrimonio", "diputado"],
            "diputados con partido",
            require_numbers=True,
        )


# ══════════════════════════════════════════════════════════
# GEOGRAPHIC / LOCAL
# ══════════════════════════════════════════════════════════


class TestGeographic:
    """Users want local/provincial data."""

    async def test_datos_lujan(self, client):
        data = await ask(
            client,
            "Vivo en Luján y quiero tener datos de mi ciudad. Que tienes de Luján, provincia de Buenos Aires?",
        )
        assert_substantive(data, "datos Lujan")

    async def test_datasets_santiago_del_estero(self, client):
        data = await ask(client, "Que datasets dispones de Santiago del Estero?")
        answer_contains(data, ["santiago", "estero", "dataset", "dato"], "datasets Santiago")

    async def test_siniestros_entre_rios(self, client):
        data = await ask(
            client, "Dame todos los datos que tengas sobre siniestros viales en Entre Ríos"
        )
        answer_contains(
            data,
            ["siniestro", "vial", "entre ríos", "accidente", "tránsito"],
            "siniestros Entre Rios",
        )

    async def test_inflacion_alimentos_por_ciudad(self, client):
        data = await ask(client, "necesito ver la inflación en alimentos dividido por ciudades")
        answer_contains(
            data, ["inflaci", "alimento", "ciudad", "región", "ipc"], "inflacion alimentos ciudades"
        )


# ══════════════════════════════════════════════════════════
# FRUSTRATION QUERIES
# ══════════════════════════════════════════════════════════


class TestFrustrationQueries:
    """Queries that show user frustration."""

    async def test_inflacion_enero_frustrated(self, client):
        """Asked 14 times by frustrated users."""
        data = await ask(client, "Inflacion de enero 2026")
        answer_contains(
            data, ["inflaci", "enero", "2026", "%", "ipc"], "inflacion enero", require_numbers=True
        )

    async def test_gba_not_wanted(self, client):
        """User complained system keeps returning GBA instead of national."""
        data = await ask(client, "pero por que me envias el de gba si no es lo que te pedi")
        assert_substantive(data, "GBA not wanted")

    async def test_user_says_no(self, client):
        """Downvote: 'No pregunté nada y respondió cualquier cosa'."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=smart_payload("No"),
            headers=headers(),
        )
        assert resp.status_code == 200
        data = resp.json()
        if data.get("answer"):
            numbers = extract_numbers(data["answer"])
            assert len(numbers) <= 5, f"System dumped data for 'No' input: {data['answer'][:200]}"

    async def test_follow_up_los_tres(self, client):
        """Downvote: 'No me respondió lo que pregunté'."""
        data = await ask(client, "Los tres si podes")
        assert_substantive(data, "Los tres", min_length=30)


# ══════════════════════════════════════════════════════════
# ENGLISH QUERIES
# ══════════════════════════════════════════════════════════


class TestEnglishQueries:
    async def test_english_dataset_source(self, client):
        data = await ask(
            client, "Where does the dataset come from? For example from a government source?"
        )
        assert_substantive(data, "English dataset source")

    async def test_english_factories_milei(self, client):
        data = await ask(client, "Are argentine factories closing due to milei's policies?")
        assert_substantive(data, "English factories")


# ══════════════════════════════════════════════════════════
# META / SYSTEM
# ══════════════════════════════════════════════════════════


class TestMetaQueries:
    async def test_que_modelo_llm(self, client):
        data = await ask(client, "que modelo de llm es el que estás corriendo?")
        assert_substantive(data, "modelo LLM", min_length=30)
        answer_lower = data["answer"].lower()
        assert "api_key" not in answer_lower
        assert "secret" not in answer_lower

    async def test_resumen_tematico(self, client):
        data = await ask(client, "puedes hacer un resumen tematico de los datos que tienes?")
        answer_contains(
            data,
            ["presupuesto", "educaci", "salud", "economía", "portal", "dato"],
            "resumen tematico",
        )

    async def test_datos_desactualizados(self, client):
        data = await ask(
            client,
            "qué datasets se encuentran desactualizados en el portal de datos abiertos de honorable de la cámara de diputados de la nación",
        )
        assert_substantive(data, "datos desactualizados")


# ══════════════════════════════════════════════════════════
# COMPLEX / AMBITIOUS
# ══════════════════════════════════════════════════════════


class TestComplexQueries:
    async def test_obras_sociales_analisis(self, client):
        data = await ask(
            client,
            "Quiero hacer un analisis de las 30 obras sociales y prepagas mas grandes del pais, saber su cantidad de afiliados y su aporte",
        )
        answer_contains(data, ["obra social", "prepaga", "afiliado", "salud"], "obras sociales")

    async def test_presupuesto_inamu(self, client):
        data = await ask(client, "Cual es el presupuesto del inamu")
        assert_substantive(data, "presupuesto INAMU")

    async def test_actividad_metalurgica(self, client):
        data = await ask(client, "Evolución de la actividad del sector metalúrgico el último año")
        answer_contains(
            data,
            ["metalúrg", "industrial", "actividad", "sector", "manufactur"],
            "actividad metalurgica",
        )

    async def test_ley_modernizacion_laboral(self, client):
        data = await ask(client, "Quisiera que me expliques la ley de modernización laboral")
        assert_substantive(data, "ley modernizacion")

    async def test_criptomoneda_argentina(self, client):
        data = await ask(client, "Alguna criptomoneda nativa de Argentina?")
        assert_substantive(data, "criptomoneda", min_length=30)
