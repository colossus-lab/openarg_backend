"""Tests for staff regex safeguard patterns and _detect_staff_intent."""
from __future__ import annotations

import pytest

from app.application.smart_query_service import (
    SmartQueryService,
    _STAFF_CHANGES_PATTERN,
    _STAFF_COUNT_PATTERN,
    _STAFF_COUNT_TIENE_PATTERN,
    _STAFF_NAME_PATTERN,
    _STAFF_NAME_TIENE_PATTERN,
    _STAFF_PATTERN,
)


# ── _STAFF_PATTERN gate ─────────────────────────────────────


class TestStaffPatternMatches:
    """Verify _STAFF_PATTERN catches staff-related queries."""

    @pytest.mark.parametrize(
        "query",
        [
            "asesores de Yeza",
            "personal de Menem",
            "empleados de Milei",
            "cuántos asesores tiene Kicillof",
            "equipo de Cristina",
            "trabajan con Massa",
            "trabajan para Larreta",
            "nómina de personal",
            "nómina del personal",
            "quiénes trabajan con Máximo Kirchner",
            "quién trabaja para Bullrich",
            "Asesores de Tolosa Paz",
            "PERSONAL DE YEZA",
            "lista de asesores de Yeza",
            "listado de personal de Menem",
            "altas y bajas de personal",
            "bajas y altas de personal",
            "cambios de personal",
            "cambio personal",
            "rotación de personal",
            "quiénes se fueron del equipo de Yeza",
            "cuáles se fueron",
            "quién llegó al equipo de Menem",
            "quiénes entraron",
            "quiénes salieron",
            "cuantos asesores de Milei",
            "personal del diputado Menem",
        ],
    )
    def test_pattern_matches(self, query):
        assert _STAFF_PATTERN.search(query), f"Should match: {query}"

    @pytest.mark.parametrize(
        "query",
        [
            "inflación últimos meses",
            "patrimonio de Yeza",
            "qué dijo Milei en el congreso",
            "presupuesto 2025",
            "dólar hoy",
            "declaraciones juradas de Yeza",
            "votaciones del senado",
            "proyectos de ley",
        ],
    )
    def test_pattern_no_match(self, query):
        assert not _STAFF_PATTERN.search(query), f"Should NOT match: {query}"


# ── Name extraction ──────────────────────────────────────────


class TestStaffNameExtraction:
    @pytest.mark.parametrize(
        "query,expected",
        [
            ("asesores de Yeza", "Yeza"),
            ("personal del diputado Menem", "diputado Menem"),
            ("empleados de Tolosa Paz", "Tolosa Paz"),
            ("equipo de Cristina Kirchner", "Cristina Kirchner"),
        ],
    )
    def test_name_extraction_de_pattern(self, query, expected):
        m = _STAFF_NAME_PATTERN.search(query)
        assert m is not None
        assert m.group(1).strip() == expected

    @pytest.mark.parametrize(
        "query,expected",
        [
            ("asesores tiene Yeza", "Yeza"),
            ("personal tiene Massa", "Massa"),
        ],
    )
    def test_name_tiene_pattern(self, query, expected):
        m = _STAFF_NAME_TIENE_PATTERN.search(query)
        assert m is not None
        assert m.group(1).strip() == expected

    @pytest.mark.parametrize(
        "query,expected",
        [
            ("cuántos asesores tiene Yeza", "Yeza"),
            ("cuantos empleados tiene Massa", "Massa"),
        ],
    )
    def test_count_tiene_pattern(self, query, expected):
        m = _STAFF_COUNT_TIENE_PATTERN.search(query)
        assert m is not None
        assert m.group(1).strip() == expected


# ── _clean_staff_name ────────────────────────────────────────


class TestCleanStaffName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Ritondo hay", "Ritondo"),
            ("Menem tiene", "Menem"),
            ("Yeza son", "Yeza"),
            ("Cristina están", "Cristina"),
            ("Massa en el congreso", "Massa"),
            ("Yeza de la cámara", "Yeza"),
            ("Kirchner", "Kirchner"),  # no stop words → unchanged
        ],
    )
    def test_strips_stop_words(self, raw, expected):
        assert SmartQueryService._clean_staff_name(raw) == expected


# ── _extract_staff_name ──────────────────────────────────────


class TestExtractStaffName:
    @pytest.mark.parametrize(
        "query,expected",
        [
            ("asesores de Yeza", "Yeza"),
            ("cuántos asesores tiene Kicillof", "Kicillof"),
            ("personal tiene Massa", "Massa"),
            ("empleados de Ritondo hay", "Ritondo"),
            ("equipo de Tolosa Paz", "Tolosa Paz"),
        ],
    )
    def test_extracts_name(self, query, expected):
        assert SmartQueryService._extract_staff_name(query) == expected

    def test_returns_empty_when_no_name(self):
        assert SmartQueryService._extract_staff_name("nómina de personal") == ""

    def test_returns_empty_for_unrelated(self):
        assert SmartQueryService._extract_staff_name("inflación 2025") == ""


# ── Count pattern ────────────────────────────────────────────


class TestStaffCountPattern:
    @pytest.mark.parametrize(
        "query",
        [
            "cuántos asesores tiene Yeza",
            "cuantos empleados tiene el bloque",
            "cuántos asesores",
            "cuantos asesores de Milei",
            "cuánto personal tiene",
        ],
    )
    def test_count_matches(self, query):
        assert _STAFF_COUNT_PATTERN.search(query)

    def test_count_no_match(self):
        assert not _STAFF_COUNT_PATTERN.search("cuántos votos tuvo")


# ── Changes pattern ──────────────────────────────────────────


class TestStaffChangesPattern:
    @pytest.mark.parametrize(
        "query",
        [
            "quiénes se fueron del equipo de Yeza",
            "quién llegó al equipo de Menem",
            "altas y bajas de personal",
            "bajas y altas",
            "cambios de personal",
            "cambio personal",
            "rotación de personal",
            "quiénes entraron",
            "cuáles salieron",
        ],
    )
    def test_changes_matches(self, query):
        assert _STAFF_CHANGES_PATTERN.search(query)

    def test_changes_no_match(self):
        assert not _STAFF_CHANGES_PATTERN.search("asesores de Yeza")


# ── _detect_staff_intent ─────────────────────────────────────


class TestDetectStaffIntent:
    """Test SmartQueryService._detect_staff_intent static method."""

    def test_get_by_legislator(self):
        plan = SmartQueryService._detect_staff_intent("asesores de Yeza")
        assert plan is not None
        assert plan.intent == "staff_legislator"
        assert plan.steps[0].action == "query_staff"
        assert plan.steps[0].params["name"] == "Yeza"
        assert plan.steps[0].params["action"] == "get_by_legislator"

    def test_count_query_de(self):
        plan = SmartQueryService._detect_staff_intent("cuántos asesores de Menem")
        assert plan is not None
        assert plan.intent == "staff_count"
        assert plan.steps[0].params["action"] == "count"
        assert plan.steps[0].params["name"] == "Menem"

    def test_count_query_tiene(self):
        plan = SmartQueryService._detect_staff_intent("cuántos asesores tiene Kicillof")
        assert plan is not None
        assert plan.intent == "staff_count"
        assert plan.steps[0].params["action"] == "count"
        assert plan.steps[0].params["name"] == "Kicillof"

    def test_changes_query_with_name(self):
        plan = SmartQueryService._detect_staff_intent(
            "quiénes se fueron del equipo de Yeza"
        )
        assert plan is not None
        assert plan.intent == "staff_changes"
        assert plan.steps[0].params["action"] == "changes"
        assert plan.steps[0].params.get("name") == "Yeza"

    def test_changes_query_without_name(self):
        plan = SmartQueryService._detect_staff_intent("cambios de personal")
        assert plan is not None
        assert plan.intent == "staff_changes"
        assert plan.steps[0].params["action"] == "changes"
        assert "name" not in plan.steps[0].params

    def test_non_staff_returns_none(self):
        plan = SmartQueryService._detect_staff_intent("inflación últimos meses")
        assert plan is None

    def test_generic_staff(self):
        plan = SmartQueryService._detect_staff_intent("nómina de personal")
        assert plan is not None
        assert plan.intent == "staff_busqueda"
        assert plan.steps[0].params["action"] == "search"

    def test_trailing_words_stripped(self):
        plan = SmartQueryService._detect_staff_intent("empleados de Ritondo hay")
        assert plan is not None
        assert plan.steps[0].params["name"] == "Ritondo"

    def test_count_without_name_falls_through(self):
        """cuantos asesores (no name) → should NOT create a count plan, falls to generic."""
        plan = SmartQueryService._detect_staff_intent("cuántos asesores")
        assert plan is not None
        # Count pattern matches but no name → falls through to generic search
        assert plan.intent == "staff_busqueda"

    def test_lista_de_asesores(self):
        plan = SmartQueryService._detect_staff_intent("lista de asesores de Yeza")
        assert plan is not None
        assert plan.steps[0].params["name"] == "Yeza"

    def test_whitespace_handling(self):
        plan = SmartQueryService._detect_staff_intent("  asesores de Yeza  ")
        assert plan is not None
        assert plan.steps[0].params["name"] == "Yeza"


# ── Ambiguous DDJJ/Staff queries ─────────────────────────────


class TestAmbiguousQueries:
    """Ensure DDJJ-related queries are NOT captured by staff patterns."""

    @pytest.mark.parametrize(
        "query",
        [
            "declaración jurada de Yeza",
            "patrimonio de Menem",
            "bienes de Cristina",
        ],
    )
    def test_ddjj_not_staff(self, query):
        assert not _STAFF_PATTERN.search(query), f"Staff should NOT match DDJJ query: {query}"
