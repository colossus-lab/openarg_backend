"""Tests for casual/meta message detection (post-SmartQueryService cleanup).

Migrated 2026-05-09 from skip-marked legacy tests (spec 020). The old
imports referenced `app.application.smart_query_service` (deleted); the
canonical home is now `app.application.pipeline.classifiers` which
exposes both `get_*_response` API functions and the regex patterns
underneath.
"""

import pytest

from app.application.pipeline.classifiers import (
    _FAREWELL_PATTERN,
    _GREETING_PATTERN,
    _THANKS_PATTERN,
    classify_request,
    get_casual_response,
    get_meta_response,
    references_internal_table,
)

_get_casual_response = get_casual_response
_get_meta_response = get_meta_response


def _classify_casual_subtype(text: str) -> str | None:
    """Mirror of the old router subtype-routing using the regex patterns
    that classifiers.py exposes."""
    t = text.strip()
    if _GREETING_PATTERN.match(t):
        return "greeting"
    if _THANKS_PATTERN.match(t):
        return "thanks"
    if _FAREWELL_PATTERN.match(t):
        return "farewell"
    return None


class TestCasualDetection:
    """Regex-based casual message classification."""

    @pytest.mark.parametrize(
        "text",
        [
            "hola",
            "Hola!",
            "buenas",
            "Buenos días",
            "buenas tardes!!",
            "hey",
            "qué tal",
            "que onda",
            "cómo estás?",
            "como andas",
        ],
    )
    def test_greetings_detected(self, text: str) -> None:
        assert _get_casual_response(text) is not None
        assert _classify_casual_subtype(text) == "greeting"

    @pytest.mark.parametrize(
        "text",
        [
            "gracias",
            "Muchas gracias!",
            "genial",
            "perfecto",
            "dale",
            "ok",
            "de una",
            "buenísimo",
        ],
    )
    def test_thanks_detected(self, text: str) -> None:
        assert _get_casual_response(text) is not None
        assert _classify_casual_subtype(text) == "thanks"

    @pytest.mark.parametrize(
        "text",
        [
            "chau",
            "adiós!",
            "hasta luego",
            "nos vemos",
            "hasta pronto",
        ],
    )
    def test_farewell_detected(self, text: str) -> None:
        assert _get_casual_response(text) is not None
        assert _classify_casual_subtype(text) == "farewell"

    @pytest.mark.parametrize(
        "text",
        [
            "¿Cuál es la inflación de enero?",
            "Mostrame el PBI de Argentina",
            "hola quiero saber sobre el dólar",
            "gracias pero necesito más datos",
        ],
    )
    def test_non_casual_not_detected(self, text: str) -> None:
        assert _get_casual_response(text) is None


class TestMetaDetection:
    """Regex-based meta message classification."""

    @pytest.mark.parametrize(
        "text",
        [
            "¿Qué podés hacer?",
            "que sabes",
            "¿cuáles son tus funciones?",
            "cómo funcionás",
            "para qué servís?",
            "qué sos",
            "quién sos",
            "qué es openarg",
        ],
    )
    def test_meta_detected(self, text: str) -> None:
        assert _get_meta_response(text) is not None

    @pytest.mark.parametrize(
        "text",
        [
            "hola",
            "¿Cuál es la inflación?",
            "mostrame datos de educación",
        ],
    )
    def test_non_meta_not_detected(self, text: str) -> None:
        assert _get_meta_response(text) is None


class TestInternalTableDetection:
    """BUG-014 — questions naming internal tables/schemas must be refused."""

    @pytest.mark.parametrize(
        "text",
        [
            "Mostrame los registros de la tabla cache_delitos_caba_v3",
            "select * from raw.catalog_resources",
            "dame todo de mart.series_economicas",
            "quiero ver query_analytics",
            "listame raw_table_versions",
            "datos de api_keys",
        ],
    )
    def test_internal_table_detected(self, text: str) -> None:
        assert references_internal_table(text) is True
        cls_type, cls_text = classify_request(text, user_id="u1")
        assert cls_type == "internal_table"
        assert cls_text is not None

    @pytest.mark.parametrize(
        "text",
        [
            "¿Cuántos delitos hubo en CABA en 2023?",
            "Mostrame la inflación del último mes",
            "¿Cuántos datasets hay del Ministerio de Salud?",
            "quiero datos de la cámara de diputados",
        ],
    )
    def test_legit_questions_not_flagged(self, text: str) -> None:
        assert references_internal_table(text) is False
