"""Unit tests for the SkillRegistry and automatic skill detection."""

from __future__ import annotations

import pytest

from app.application.skills.registry import SkillRegistry


@pytest.fixture
def registry() -> SkillRegistry:
    return SkillRegistry()


# ── Verificar skill ────────────────────────────────────────


class TestSkillVerificar:
    def test_es_verdad_que(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("Es verdad que la inflación bajó 5%?")
        assert skill is not None
        assert skill.name == "verificar"

    def test_es_cierto_que(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("es cierto que aumentó el gasto en educación?")
        assert skill is not None
        assert skill.name == "verificar"

    def test_dijo_que(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("El ministro dijo que el desempleo bajó")
        assert skill is not None
        assert skill.name == "verificar"

    def test_afirmo_que_sin_tilde(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("Milei afirmo que el gasto publico bajo 30%")
        assert skill is not None
        assert skill.name == "verificar"

    def test_aumento_porcentaje(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("el gasto en salud aumentó 40%")
        assert skill is not None
        assert skill.name == "verificar"

    def test_bajo_porcentaje(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("la pobreza bajó 15%")
        assert skill is not None
        assert skill.name == "verificar"

    def test_chequear(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("quiero chequear si el presupuesto subió")
        assert skill is not None
        assert skill.name == "verificar"

    def test_fact_check(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("fact-check: la inflación fue del 2%")
        assert skill is not None
        assert skill.name == "verificar"


# ── Comparar skill ─────────────────────────────────────────


class TestSkillComparar:
    def test_vs(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("Patrimonio de Kirchner vs Macri")
        assert skill is not None
        assert skill.name == "comparar"

    def test_versus(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("inflación Argentina versus Brasil")
        assert skill is not None
        assert skill.name == "comparar"

    def test_comparar(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("Comparar presupuesto de educación y salud")
        assert skill is not None
        assert skill.name == "comparar"

    def test_comparacion(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("comparación entre CABA y Provincia de Buenos Aires")
        assert skill is not None
        assert skill.name == "comparar"

    def test_diferencia_entre(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("diferencia entre presupuesto 2023 y 2024")
        assert skill is not None
        assert skill.name == "comparar"


# ── Presupuesto skill ──────────────────────────────────────


class TestSkillPresupuesto:
    def test_presupuesto_de(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("presupuesto de educación 2024")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_presupuesto_del(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("presupuesto del Ministerio de Defensa")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_presupuesto_para(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("presupuesto para universidades públicas")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_gasto_publico(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("gasto público en salud")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_cuanto_se_gasto(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("cuánto se gastó en defensa en 2024?")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_ejecucion_presupuestaria(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("ejecución presupuestaria del congreso")
        assert skill is not None
        assert skill.name == "presupuesto"

    def test_cuanto_se_destino(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("cuanto se destinó a ciencia y tecnología?")
        assert skill is not None
        assert skill.name == "presupuesto"


# ── Perfil skill ────────────────────────────────────────────


class TestSkillPerfil:
    def test_quien_es_diputado(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("quién es el diputado Martín Yeza?")
        assert skill is not None
        assert skill.name == "perfil"

    def test_quien_es_senador(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("quien es la senadora Cristina Kirchner")
        assert skill is not None
        assert skill.name == "perfil"

    def test_patrimonio_de(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("patrimonio de Máximo Kirchner")
        assert skill is not None
        assert skill.name == "perfil"

    def test_bienes_de(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("bienes de Manuel Adorni")
        assert skill is not None
        assert skill.name == "perfil"

    def test_ddjj_de(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("DDJJ del diputado López")
        assert skill is not None
        assert skill.name == "perfil"

    def test_asesores_de_legislador(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("asesores del diputado Yeza")
        assert skill is not None
        assert skill.name == "perfil"

    def test_personal_de_senador(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("personal del senador Parrilli")
        assert skill is not None
        assert skill.name == "perfil"


# ── Precio skill ────────────────────────────────────────────


class TestSkillPrecio:
    def test_a_cuanto_esta_dolar(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("a cuánto está el dólar?")
        assert skill is not None
        assert skill.name == "precio"

    def test_cuanto_sale_dolar(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("cuánto sale el dólar hoy?")
        assert skill is not None
        assert skill.name == "precio"

    def test_dolar_blue(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("dólar blue hoy")
        assert skill is not None
        assert skill.name == "precio"

    def test_dolar_sin_tilde(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("dolar oficial")
        assert skill is not None
        assert skill.name == "precio"

    def test_cotizacion_dolar(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("cotización del dólar")
        assert skill is not None
        assert skill.name == "precio"

    def test_riesgo_pais(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("riesgo país hoy")
        assert skill is not None
        assert skill.name == "precio"

    def test_canasta_basica(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("canasta básica familiar")
        assert skill is not None
        assert skill.name == "precio"

    def test_linea_de_pobreza(self, registry: SkillRegistry) -> None:
        skill = registry.match_auto("línea de pobreza actual")
        assert skill is not None
        assert skill.name == "precio"


# ── No-match cases ──────────────────────────────────────────


class TestNoSkillMatch:
    def test_greeting(self, registry: SkillRegistry) -> None:
        assert registry.match_auto("hola qué tal") is None

    def test_generic_question(self, registry: SkillRegistry) -> None:
        assert registry.match_auto("cuántos datasets hay disponibles?") is None

    def test_inflacion_simple(self, registry: SkillRegistry) -> None:
        """Simple inflation query should NOT trigger verificar (no claim to verify)."""
        assert registry.match_auto("inflación últimos meses") is None

    def test_meta_question(self, registry: SkillRegistry) -> None:
        assert registry.match_auto("qué podés hacer?") is None

    def test_empty(self, registry: SkillRegistry) -> None:
        assert registry.match_auto("") is None

    def test_off_topic(self, registry: SkillRegistry) -> None:
        assert registry.match_auto("escribime un poema sobre la luna") is None


# ── Registry API ────────────────────────────────────────────


class TestRegistryAPI:
    def test_list_all_returns_five_skills(self, registry: SkillRegistry) -> None:
        skills = registry.list_all()
        assert len(skills) == 5
        names = {s.name for s in skills}
        assert names == {"verificar", "comparar", "presupuesto", "perfil", "precio"}

    def test_skill_has_planner_injection(self, registry: SkillRegistry) -> None:
        for skill in registry.list_all():
            assert skill.planner_injection, f"Skill {skill.name} missing planner_injection"
            assert len(skill.planner_injection) > 50

    def test_skill_has_analyst_injection(self, registry: SkillRegistry) -> None:
        for skill in registry.list_all():
            assert skill.analyst_injection, f"Skill {skill.name} missing analyst_injection"
            assert len(skill.analyst_injection) > 50

    def test_skill_has_patterns(self, registry: SkillRegistry) -> None:
        for skill in registry.list_all():
            assert len(skill.patterns) > 0, f"Skill {skill.name} has no patterns"
