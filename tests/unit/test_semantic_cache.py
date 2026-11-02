"""Tests for semantic cache hash utility, TTL constants, intent-based TTL and near questions."""

from __future__ import annotations

from itertools import combinations

import pytest

from app.infrastructure.adapters.cache.semantic_cache import (
    INTENT_TTL_MAP,
    TTL_DAILY,
    TTL_REALTIME,
    TTL_STATIC,
    SemanticCache,
    asks_the_same,
    ttl_for_intent,
)


class TestSemanticCacheHash:
    def test_hash_deterministic(self):
        h1 = SemanticCache._hash("¿cuánto vale el dólar?")
        h2 = SemanticCache._hash("¿cuánto vale el dólar?")
        assert h1 == h2

    def test_hash_normalized(self):
        h1 = SemanticCache._hash("   Test Question   ")
        h2 = SemanticCache._hash("test question")
        assert h1 == h2

    def test_hash_different_questions(self):
        h1 = SemanticCache._hash("inflación")
        h2 = SemanticCache._hash("desempleo")
        assert h1 != h2

    def test_hash_length(self):
        h = SemanticCache._hash("test")
        assert len(h) == 64  # SHA-256 hex


class TestTTLConstants:
    def test_realtime_is_shortest(self):
        assert TTL_REALTIME < TTL_DAILY < TTL_STATIC

    def test_values(self):
        assert TTL_REALTIME == 300
        assert TTL_DAILY == 1800
        assert TTL_STATIC == 7200


class TestDefaultThreshold:
    def test_default_threshold_is_092(self):
        from unittest.mock import AsyncMock

        cache = SemanticCache(session_factory=AsyncMock())
        assert cache._similarity_threshold == 0.92


class TestTTLForIntent:
    def test_dolar_is_realtime(self):
        assert ttl_for_intent("dolar") == TTL_REALTIME

    def test_riesgo_pais_is_realtime(self):
        assert ttl_for_intent("riesgo_pais") == TTL_REALTIME

    def test_inflacion_is_daily(self):
        assert ttl_for_intent("inflacion") == TTL_DAILY

    def test_emae_is_daily(self):
        assert ttl_for_intent("emae") == TTL_DAILY

    def test_series_is_daily(self):
        assert ttl_for_intent("series") == TTL_DAILY

    def test_ddjj_is_static(self):
        assert ttl_for_intent("ddjj") == TTL_STATIC

    def test_sesiones_is_static(self):
        assert ttl_for_intent("sesiones") == TTL_STATIC

    def test_ckan_is_static(self):
        assert ttl_for_intent("ckan") == TTL_STATIC

    def test_unknown_defaults_to_daily(self):
        assert ttl_for_intent("unknown_intent") == TTL_DAILY

    def test_case_insensitive(self):
        assert ttl_for_intent("DOLAR") == TTL_REALTIME

    def test_partial_match_in_compound_intent(self):
        assert ttl_for_intent("consulta_dolar_blue") == TTL_REALTIME

    def test_intent_ttl_map_not_empty(self):
        assert len(INTENT_TTL_MAP) >= 10


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeSession:
    """The exact lookup misses; the vector lookup returns *near*, closest first."""

    def __init__(self, near):
        self._near = near

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, statement, params):
        return _FakeResult(self._near if "embedding <=>" in str(statement) else [])


def _cache_with(near) -> SemanticCache:
    return SemanticCache(session_factory=lambda: _FakeSession(near))


# The pair that showed the bug (staging, 2026-11-02, over WebSocket): the second
# got the first one's 33.19 % in 0 s. Measured similarity: 0.9269.
_INTERANUAL_GBA = (
    "Según las aperturas del IPC del INDEC, ¿cuánto dio la variación interanual "
    "del nivel general en la región GBA en febrero de 2026?"
)
_INDICE_GBA = (
    "¿Cuánto vale el índice de precios del IPC aperturas del INDEC, nivel general "
    "de la región GBA, en febrero de 2026?"
)

_MEDIDAS = (
    "el índice",
    "la variación mensual",
    "la variación interanual",
    "la variación acumulada",
)
# 47 of these 48 pairs score 0.92 or more with the real embedding.
_PLANTILLAS = (
    "¿Cuánto dio {m} del IPC nivel general en la región GBA en febrero de 2026?",
    "¿Cuál fue {m} del IPC en Argentina en marzo de 2025?",
    "¿Cuánto fue {m} de alimentos y bebidas del IPC en enero de 2026?",
    "Según las aperturas del IPC del INDEC, ¿cuánto dio {m} del nivel general "
    "en la región Pampeana en junio de 2025?",
    "{m} IPC febrero 2026",
    "¿Cuánto dio {m} del EMAE en diciembre de 2025?",
    "¿Cuál fue {m} del IPI manufacturero en mayo de 2025?",
    "¿Cuánto dio {m} del índice de salarios en agosto de 2025?",
)


class TestAsksTheSame:
    def test_measured_pair_asks_for_different_figures(self):
        assert not asks_the_same(_INDICE_GBA, _INTERANUAL_GBA)

    @pytest.mark.parametrize("plantilla", _PLANTILLAS)
    @pytest.mark.parametrize("a,b", list(combinations(_MEDIDAS, 2)))
    def test_questions_that_differ_only_in_the_measure(self, plantilla, a, b):
        assert not asks_the_same(plantilla.format(m=a), plantilla.format(m=b))

    @pytest.mark.parametrize(
        "a,b",
        [
            # Over 0.92 with the real embedding: one was served for the other.
            (
                "¿Cuál fue la tasa de desempleo en el cuarto trimestre de 2024?",
                "¿Cuál fue la tasa de empleo en el cuarto trimestre de 2024?",
            ),
            (
                "¿Cuál es el total de empleados públicos en 2024?",
                "¿Cuál es el promedio de empleados públicos en 2024?",
            ),
            ("¿Cuál es la provincia con más pobreza?", "¿Cuál es la provincia con menos pobreza?"),
            (
                "¿Cuánto creció el salario real en 2024?",
                "¿Cuánto creció el salario nominal en 2024?",
            ),
            (
                "¿Cuál fue la tasa de pobreza en el primer semestre de 2025?",
                "¿Cuál fue la tasa de indigencia en el primer semestre de 2025?",
            ),
            # From staging's own question history, all over 0.92.
            ("Qué es el riesgo país?", "Cuánto está el riesgo país?"),
            (
                "Cual fue el IPC de inflacion mensual en febrero 2026?",
                "¿Cuál fue la inflación interanual de febrero de 2026 según el INDEC?",
            ),
            (
                "Cuál es la inflación mensual actual?",
                "¿Cuál fue la inflación mensual del último año?",
            ),
            (
                "Quién es el diputado con mayor patrimonio?",
                "Quienes son los diputados con mayor patrimonio?",
            ),
            # What the filler list must not swallow.
            ("¿Cuánto fue la inflación de 2023?", "¿Cuánto fue la inflación de 2024?"),
            ("Exportaciones de Argentina a Brasil", "Exportaciones de Brasil a Argentina"),
            ("Exportaciones a Brasil", "Exportaciones de Brasil"),
            ("¿Cuántos diputados hay?", "¿Cuáles diputados hay?"),
        ],
    )
    def test_different_questions(self, a, b):
        assert not asks_the_same(a, b)

    @pytest.mark.parametrize(
        "a,b",
        [
            ("¿Cuál es el PBI de Argentina?", "Cual es el PBI de Argentina"),
            ("Cuál es el PBI de Argentina?", "¿Cuál es el PBI de Argentina?"),
            (
                "¿Cómo viene la inflación en los últimos meses?",
                "¿Como viene la inflacion en los ultimos meses?",
            ),
            ("Tasa de desempleo en Argentina", "¿Cuál es la tasa de desempleo en Argentina?"),
            ("¿Cuánto fue la inflación de 2023?", "¿Cuál fue la inflación en 2023?"),
            ("¿Cuánto fue la inflación de 2023?", "inflacion 2023"),
            ("¿Cuánto vale el dólar blue?", "¿Cuánto está el dólar blue?"),
            ("cuánto cuesta el dólar hoy", "cuánto está el dólar"),
        ],
    )
    def test_same_question_asked_differently(self, a, b):
        assert asks_the_same(a, b)

    def test_a_question_with_nothing_left_matches_nothing(self):
        assert not asks_the_same("¿Cuál es?", "¿Cuánto es?")


class TestSemanticLookup:
    async def test_measured_pair_is_a_miss(self):
        cache = _cache_with([({"answer": "33,19 %"}, _INTERANUAL_GBA, 0.9269)])
        assert await cache.get(_INDICE_GBA, embedding=[0.1, 0.2]) is None

    async def test_same_question_asked_differently_is_a_hit(self):
        cache = _cache_with([({"answer": "ok"}, "¿Cuál es el PBI de Argentina?", 0.99)])
        assert await cache.get("Cual es el PBI de Argentina", embedding=[0.1]) == {"answer": "ok"}

    async def test_the_closest_can_ask_for_another_figure(self):
        cache = _cache_with(
            [
                ({"answer": "interanual"}, "¿Cuál fue la variación interanual del IPC?", 0.98),
                ({"answer": "mensual"}, "¿Cuánto dio la variación mensual del IPC?", 0.95),
            ]
        )
        pregunta = "¿Cuál fue la variación mensual del IPC?"
        assert await cache.get(pregunta, embedding=[0.1]) == {"answer": "mensual"}

    async def test_the_threshold_still_applies(self):
        cache = _cache_with([({"answer": "ok"}, "¿Cuál es el PBI de Argentina?", 0.80)])
        assert await cache.get("Cual es el PBI de Argentina", embedding=[0.1]) is None

    async def test_response_stored_as_text(self):
        cache = _cache_with([('{"answer": "ok"}', "¿Cuál es el PBI de Argentina?", 0.99)])
        assert await cache.get("Cual es el PBI de Argentina", embedding=[0.1]) == {"answer": "ok"}
