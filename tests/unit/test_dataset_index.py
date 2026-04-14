"""Tests for the deterministic keyword-to-action routing index."""

from __future__ import annotations

from app.infrastructure.adapters.connectors.dataset_index import (
    _KEYWORD_PATTERNS,
    KEYWORD_ROUTES,
    TAXONOMY,
    format_hints_for_prompt,
    normalize_query,
    resolve_hints,
    resolve_taxonomy_context,
)


class TestNormalizeQuery:
    def test_strips_accents(self):
        assert normalize_query("inflación") == "inflacion"

    def test_lowercases(self):
        assert normalize_query("DOLAR BLUE") == "dolar blue"

    def test_removes_punctuation(self):
        assert normalize_query("¿cuánto vale?") == "cuanto vale"

    def test_collapses_whitespace(self):
        assert normalize_query("  dolar   blue  ") == "dolar blue"

    def test_empty_string(self):
        assert normalize_query("") == ""


class TestResolveHints:
    """Test that queries route to the correct action with expected confidence."""

    def test_vacunacion_covid(self):
        hints = resolve_hints("vacunacion covid en buenos aires")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert top.confidence >= 0.85
        assert any("covid" in t for t in top.params.get("tables", []))

    def test_homicidios(self):
        hints = resolve_hints("cuantos homicidios hubo en 2023")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("homicid" in t for t in top.params.get("tables", []))

    def test_escuelas_primarias(self):
        hints = resolve_hints("datos de escuelas primarias")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("escuel" in t for t in top.params.get("tables", []))

    def test_soja(self):
        hints = resolve_hints("produccion de soja por provincia")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("soja" in t for t in top.params.get("tables", []))

    def test_contaminacion(self):
        hints = resolve_hints("contaminacion del riachuelo")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("contamin" in t for t in top.params.get("tables", []))

    def test_celulares(self):
        hints = resolve_hints("cuantos celulares hay en argentina")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("celular" in t or "telefon" in t for t in top.params.get("tables", []))

    def test_brecha_de_genero(self):
        hints = resolve_hints("brecha de genero salarial")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("genero" in t or "brecha" in t for t in top.params.get("tables", []))

    def test_censo(self):
        hints = resolve_hints("censo 2022 poblacion")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("censo" in t or "poblaci" in t for t in top.params.get("tables", []))

    def test_ecobici(self):
        hints = resolve_hints("ecobici viajes por mes")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("ecobici" in t or "biciclet" in t for t in top.params.get("tables", []))

    def test_derechos_humanos(self):
        hints = resolve_hints("derechos humanos desaparecidos")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("derecho" in t or "memoria" in t for t in top.params.get("tables", []))

    def test_empleo_informal(self):
        hints = resolve_hints("empleo informal en tucuman")
        assert hints
        top = hints[0]
        assert top.action in ("query_sandbox", "query_series")

    def test_gasto_educacion(self):
        hints = resolve_hints("gasto en educacion por ano")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"

    def test_femicidios(self):
        hints = resolve_hints("femicidios por provincia")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("femicid" in t for t in top.params.get("tables", []))

    def test_bosques(self):
        hints = resolve_hints("bosques nativos deforestacion")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("bosqu" in t or "forestal" in t for t in top.params.get("tables", []))

    def test_accidentes_transito(self):
        hints = resolve_hints("accidentes de transito caba")
        assert hints
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("siniestro" in t or "transit" in t for t in top.params.get("tables", []))

    def test_precompiled_keyword_patterns_cover_sorted_keywords(self):
        assert [keyword for keyword, _ in _KEYWORD_PATTERNS] == sorted(
            KEYWORD_ROUTES.keys(), key=len, reverse=True
        )


class TestEconomicRoutes:
    """Test that core economic queries still route to query_series."""

    def test_inflacion(self):
        hints = resolve_hints("inflacion ultimos 12 meses")
        top = hints[0]
        assert top.action == "query_series"
        assert "148.3_INIVELNAL_DICI_M_26" in top.params.get("series_ids", [])

    def test_dolar_blue(self):
        hints = resolve_hints("cotizacion dolar blue hoy")
        top = hints[0]
        assert top.action == "query_argentina_datos"
        assert top.params.get("type") == "dolar"
        assert top.params.get("casa") == "blue"
        assert top.params.get("ultimo") is True

    def test_dolar_historico_keeps_series_mode(self):
        hints = resolve_hints("cotizacion historica del dolar blue")
        top = hints[0]
        assert top.action == "query_argentina_datos"
        assert top.params.get("type") == "dolar"
        assert top.params.get("casa") == "blue"
        assert top.params.get("historico") is True

    def test_dolar_unspecified_returns_dual_mode(self):
        hints = resolve_hints("cotizacion dolar blue")
        top = hints[0]
        assert top.action == "query_argentina_datos"
        assert top.params.get("type") == "dolar"
        assert top.params.get("casa") == "blue"
        assert "ultimo" not in top.params
        assert "historico" not in top.params

    def test_riesgo_pais(self):
        hints = resolve_hints("riesgo pais argentina")
        top = hints[0]
        assert top.action == "query_argentina_datos"
        assert top.params.get("type") == "riesgo_pais"

    def test_desempleo(self):
        hints = resolve_hints("tasa de desempleo")
        top = hints[0]
        assert top.action == "query_series"
        assert "45.2_ECTDT_0_T_33" in top.params.get("series_ids", [])

    def test_reservas_bcra(self):
        hints = resolve_hints("reservas internacionales del bcra")
        top = hints[0]
        assert top.action == "query_series"
        assert "174.1_RRVAS_IDOS_0_0_36" in top.params.get("series_ids", [])


class TestCachedDataRoutes:
    """Test that known cached data categories route to query_sandbox."""

    def test_presupuesto(self):
        hints = resolve_hints("presupuesto nacional 2024")
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("presupuesto" in t for t in top.params.get("tables", []))

    def test_elecciones(self):
        hints = resolve_hints("resultados elecciones 2023")
        top = hints[0]
        assert top.action == "query_sandbox"
        assert any("elecciones" in t for t in top.params.get("tables", []))

    def test_ddjj(self):
        hints = resolve_hints("declaraciones juradas de diputados")
        top = hints[0]
        assert top.action == "query_ddjj"

    def test_sesiones(self):
        hints = resolve_hints("diario de sesiones congreso")
        top = hints[0]
        assert top.action == "query_sesiones"

    def test_coparticipacion_uses_generic_budget_tables(self):
        hints = resolve_hints("como se distribuye la coparticipacion federal")
        top = hints[0]
        assert top.action == "query_sandbox"
        assert top.params.get("tables") == ["cache_presupuesto_*"]
        assert "coparticipacion" in top.params.get("table_notes", "").lower()


class TestFormatHints:
    def test_empty_hints(self):
        assert format_hints_for_prompt([]) == ""

    def test_limits_to_3_hints(self):
        hints = resolve_hints("inflacion dolar blue presupuesto elecciones reservas")
        formatted = format_hints_for_prompt(hints)
        # Should have header + max 3 hint lines + optional taxonomy context
        lines = [line for line in formatted.strip().split("\n") if line.strip()]
        # Count only the hint lines (numbered 1., 2., 3.)
        hint_lines = [line for line in lines if line.strip().startswith(("1.", "2.", "3.", "4."))]
        assert len(hint_lines) <= 3

    def test_includes_action_and_confidence(self):
        hints = resolve_hints("inflacion")
        formatted = format_hints_for_prompt(hints)
        assert "query_series" in formatted
        assert "confianza" in formatted


class TestNoMatch:
    def test_gibberish(self):
        hints = resolve_hints("asdfghjkl zxcvbnm")
        assert hints == []

    def test_empty_query(self):
        hints = resolve_hints("")
        assert hints == []


class TestTaxonomy:
    """Test the hierarchical taxonomy structure and context resolution."""

    def test_taxonomy_has_6_domains(self):
        assert len(TAXONOMY) == 6
        expected = {
            "economia",
            "gobierno",
            "social",
            "infraestructura",
            "recursos_naturales",
            "ciencia",
        }
        assert set(TAXONOMY.keys()) == expected

    def test_each_domain_has_children(self):
        for domain_key, domain in TAXONOMY.items():
            assert "label" in domain
            assert "children" in domain
            assert len(domain["children"]) > 0, f"{domain_key} has no children"

    def test_children_have_required_fields(self):
        for domain_key, domain in TAXONOMY.items():
            for cat_key, cat in domain["children"].items():
                assert "label" in cat, f"{domain_key}.{cat_key} missing label"
                assert "actions" in cat, f"{domain_key}.{cat_key} missing actions"
                assert "cache_pattern" in cat, f"{domain_key}.{cat_key} missing cache_pattern"
                assert len(cat["actions"]) > 0, f"{domain_key}.{cat_key} has no actions"

    def test_inflacion_context(self):
        ctx = resolve_taxonomy_context("inflacion mensual")
        assert "CONTEXTO TAXONOMICO" in ctx
        assert "Economía" in ctx

    def test_educacion_context(self):
        ctx = resolve_taxonomy_context("escuelas primarias")
        assert "CONTEXTO TAXONOMICO" in ctx
        assert "Educación" in ctx

    def test_presupuesto_context(self):
        ctx = resolve_taxonomy_context("presupuesto nacional")
        assert "CONTEXTO TAXONOMICO" in ctx
        assert "Presupuesto" in ctx

    def test_transporte_context(self):
        ctx = resolve_taxonomy_context("subte buenos aires")
        assert "CONTEXTO TAXONOMICO" in ctx

    def test_no_context_for_gibberish(self):
        ctx = resolve_taxonomy_context("xyzabc123")
        assert ctx == ""

    def test_format_includes_taxonomy(self):
        hints = resolve_hints("inflacion")
        formatted = format_hints_for_prompt(hints)
        assert "CONTEXTO TAXONOMICO" in formatted
        assert "Dominio:" in formatted


class TestKeywordCount:
    def test_minimum_routes(self):
        """Ensure we have at least 190 keyword routes after expansion."""
        assert len(KEYWORD_ROUTES) >= 190
