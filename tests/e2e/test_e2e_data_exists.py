"""E2E tests: queries where the data EXISTS but the pipeline failed to find it.

Source: cross-reference of 444 production "no tengo datos" responses against
cached_datasets that ARE available. These are pipeline bugs, not data gaps.

Each test uses the EXACT query a real user asked. The system MUST return
actual data (not "no tengo", not clarification, not error).
"""

from __future__ import annotations

import pytest

from tests.e2e.helpers import answer_contains, ask, assert_substantive

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


# ── Presupuesto (89 fallos con dato disponible) ───────────


class TestPresupuestoDataExists:
    """System has 640K+ rows of presupuesto but pipeline said 'no tengo'."""

    async def test_presupuesto_por_ministerio(self, client):
        data = await ask(
            client, "Cómo se viene administrando el presupuesto al día de hoy por ministerio?"
        )
        answer_contains(
            data,
            ["presupuesto", "ministerio", "jurisdicci", "$"],
            "presupuesto ministerio",
            require_numbers=True,
        )

    async def test_presupuesto_congreso(self, client):
        data = await ask(client, "caul es el presupuesto del congreso de la nacion?")
        answer_contains(
            data,
            ["presupuesto", "congreso", "legislat", "$"],
            "presupuesto congreso",
            require_numbers=True,
        )

    async def test_gasto_educacion_evolucion(self, client):
        data = await ask(
            client, "Cómo se compara esta evolución con el presupuesto total destinado a educación?"
        )
        answer_contains(
            data,
            ["presupuesto", "educaci", "$"],
            "presupuesto educacion evolucion",
            require_numbers=True,
        )

    async def test_presupuesto_seguridad(self, client):
        data = await ask(
            client,
            "Cuál es el presupuesto ejecutado para el área de seguridad en 2022, 2023, 2024 y 2025?",
        )
        answer_contains(
            data, ["presupuesto", "seguridad", "$"], "presupuesto seguridad", require_numbers=True
        )


# ── Inflación (73 fallos con dato disponible) ─────────────


class TestInflacionDataExists:
    """System has Series de Tiempo IPC but pipeline said 'no tengo'."""

    async def test_ipc_enero_2026(self, client):
        data = await ask(client, "ipc de enero 2026?")
        answer_contains(data, ["ipc", "inflaci", "enero", "%"], "IPC enero", require_numbers=True)

    async def test_inflacion_26_anos(self, client):
        data = await ask(client, "como evoluciono la inflacion en los ultimos 26 años?")
        answer_contains(data, ["inflaci", "ipc", "%"], "inflacion 26 anos", require_numbers=True)

    async def test_inflacion_acumulada_12m(self, client):
        data = await ask(client, "cual es la inflacion acumulada IPC de los ultimos 12 meses")
        answer_contains(
            data, ["inflaci", "ipc", "acumulad", "%"], "inflacion acumulada", require_numbers=True
        )


# ── Empleo (47 fallos con dato disponible) ────────────────


class TestEmpleoDataExists:
    """System has employment series but pipeline said 'no tengo'."""

    async def test_tasa_desempleo_actual(self, client):
        data = await ask(client, "y la taza de desempleo actual?")
        answer_contains(
            data, ["desempleo", "desocupaci", "tasa", "%"], "desempleo actual", require_numbers=True
        )

    async def test_empleo_por_provincia(self, client):
        data = await ask(client, "creación de empleo por provincia")
        answer_contains(data, ["empleo", "provincia", "región"], "empleo provincia")

    async def test_salario_docente_caba(self, client):
        data = await ask(
            client, "Cómo evoluciona el salario docente en CABA en los últimos 20 años?"
        )
        answer_contains(data, ["salario", "docente", "caba", "$"], "salario docente")


# ── Población (44 fallos con dato disponible) ─────────────


class TestPoblacionDataExists:
    """System has census/population data but pipeline said 'no tengo'."""

    async def test_ultimo_censo(self, client):
        data = await ask(client, "Datos del último censo")
        assert_substantive(data, "censo")

    async def test_poblacion_bahia_blanca(self, client):
        data = await ask(client, "Población de Bahía Blanca")
        answer_contains(data, ["bahía blanca", "poblaci", "habitant"], "poblacion bahia blanca")

    async def test_cambio_poblacional_censos(self, client):
        data = await ask(
            client, "Cambios en la estructura poblacional entre el censo 2010 y el de 2022"
        )
        answer_contains(data, ["censo", "2010", "2022", "poblaci"], "censos 2010 vs 2022")


# ── Educación (34 fallos con dato disponible) ─────────────


class TestEducacionDataExists:
    """System has education datasets but pipeline said 'no tengo'."""

    async def test_gasto_educacion(self, client):
        data = await ask(client, "Gasto en educación y su evolución")
        answer_contains(
            data, ["educaci", "gasto", "presupuesto", "$"], "gasto educacion", require_numbers=True
        )

    async def test_estudiantes_por_universidad(self, client):
        data = await ask(client, "Cantidad de estudiantes por universidad")
        answer_contains(data, ["universidad", "estudiante", "alumno"], "estudiantes universidad")

    async def test_docentes_universidades(self, client):
        data = await ask(
            client,
            "cual es la cantidad de DOCENTES en las universidades publicas y cual es su distribucion por universidad?",
        )
        answer_contains(data, ["docente", "universidad"], "docentes universidades")


# ── BCRA / Finanzas (28 fallos con dato disponible) ───────


class TestBCRADataExists:
    """System has BCRA connector + series but pipeline said 'no tengo'."""

    async def test_deficit_fiscal(self, client):
        data = await ask(client, "Como viene el deficit fiscal de argentina los ultimos 2 años?")
        answer_contains(
            data, ["deficit", "fiscal", "resultado", "$"], "deficit fiscal", require_numbers=True
        )


# ── Salud (19 fallos con dato disponible) ─────────────────


class TestSaludDataExists:
    """System has health datasets but pipeline said 'no tengo'."""

    async def test_gasto_salud_nacional(self, client):
        data = await ask(client, "Gasto en salud nacional 2022")
        answer_contains(
            data, ["salud", "gasto", "presupuesto", "$"], "gasto salud", require_numbers=True
        )

    async def test_efectores_salud_cordoba(self, client):
        data = await ask(
            client, "quiero conocer todos los efectores de salud de la provincia de cordoba"
        )
        answer_contains(
            data, ["salud", "córdoba", "establecimiento", "efector"], "efectores cordoba"
        )


# ── Comercio Exterior (18 fallos con dato disponible) ─────


class TestComercioDataExists:
    """System has trade datasets but pipeline said 'no tengo'."""

    async def test_exportaciones_por_provincia(self, client):
        data = await ask(client, "Los productos exportados por Provincia")
        answer_contains(data, ["exporta", "provincia", "producto"], "exportaciones provincia")

    async def test_importacion_cosmeticos(self, client):
        data = await ask(client, "Aranceles de importación de cosméticos")
        answer_contains(data, ["importaci", "arancel", "cosmétic"], "importacion cosmeticos")


# ── Seguridad (16 fallos con dato disponible) ─────────────


class TestSeguridadDataExists:
    """System has security/crime datasets but pipeline said 'no tengo'."""

    async def test_datasets_seguridad(self, client):
        data = await ask(
            client, "Quiero saber sobre Seguridad en Argentina: que dataset tenes sobre esto"
        )
        assert_substantive(data, "datasets seguridad")

    async def test_robos_caba_por_comuna(self, client):
        data = await ask(
            client, "Como han evolucionado los robos en CABA desde 2015 a la actualidad por comuna?"
        )
        answer_contains(data, ["robo", "delito", "caba", "comuna", "seguridad"], "robos CABA")


# ── Transporte (5 fallos con dato disponible) ─────────────


class TestTransporteDataExists:
    """System has transport datasets but pipeline said 'no tengo'."""

    async def test_datos_ferrocarril(self, client):
        data = await ask(client, "Qué datos sobre el transporte por ferrocarril?")
        answer_contains(data, ["ferrocarril", "tren", "transporte", "estacion"], "ferrocarril")
