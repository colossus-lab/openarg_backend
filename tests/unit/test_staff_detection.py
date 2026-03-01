"""Tests for staff regex safeguard patterns and _detect_staff_intent."""
from __future__ import annotations

import pytest

from app.application.smart_query_service import (
    SmartQueryService,
    _STAFF_CHANGES_PATTERN,
    _STAFF_COUNT_PATTERN,
    _STAFF_NAME_PATTERN,
    _STAFF_PATTERN,
)


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
            "quiénes trabajan con Máximo Kirchner",
            "quién trabaja para Bullrich",
            "Asesores de Tolosa Paz",
            "PERSONAL DE YEZA",
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
        ],
    )
    def test_pattern_no_match(self, query):
        assert not _STAFF_PATTERN.search(query), f"Should NOT match: {query}"


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
    def test_name_extraction(self, query, expected):
        m = _STAFF_NAME_PATTERN.search(query)
        assert m is not None
        assert m.group(1).strip() == expected


class TestStaffCountPattern:
    @pytest.mark.parametrize(
        "query",
        [
            "cuántos asesores tiene Yeza",
            "cuantos empleados tiene el bloque",
            "cuántos asesores",
        ],
    )
    def test_count_matches(self, query):
        assert _STAFF_COUNT_PATTERN.search(query)


class TestStaffChangesPattern:
    @pytest.mark.parametrize(
        "query",
        [
            "quiénes se fueron del equipo de Yeza",
            "quién llegó al equipo de Menem",
            "altas y bajas de personal",
            "cambios de personal",
            "rotación de personal",
        ],
    )
    def test_changes_matches(self, query):
        assert _STAFF_CHANGES_PATTERN.search(query)


class TestDetectStaffIntent:
    """Test SmartQueryService._detect_staff_intent static method."""

    def test_get_by_legislator(self):
        plan = SmartQueryService._detect_staff_intent("asesores de Yeza")
        assert plan is not None
        assert plan.intent == "staff_legislator"
        assert plan.steps[0].action == "query_staff"
        assert plan.steps[0].params["name"] == "Yeza"

    def test_count_query(self):
        plan = SmartQueryService._detect_staff_intent("cuántos asesores de Menem")
        assert plan is not None
        assert plan.intent == "staff_count"
        assert plan.steps[0].params["action"] == "count"

    def test_changes_query(self):
        plan = SmartQueryService._detect_staff_intent(
            "quiénes se fueron del equipo de Yeza"
        )
        assert plan is not None
        assert plan.intent == "staff_changes"
        assert plan.steps[0].params["action"] == "changes"

    def test_non_staff_returns_none(self):
        plan = SmartQueryService._detect_staff_intent("inflación últimos meses")
        assert plan is None

    def test_generic_staff(self):
        plan = SmartQueryService._detect_staff_intent("nómina de personal")
        assert plan is not None
        assert plan.intent == "staff_busqueda"
        assert plan.steps[0].params["action"] == "search"
