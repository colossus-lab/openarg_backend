"""Tests for casual/meta message detection in SmartQueryService."""

import pytest

from app.application.smart_query_service import SmartQueryService

# Bind static methods for convenience
_get_casual_response = SmartQueryService._get_casual_response
_get_meta_response = SmartQueryService._get_meta_response


def _classify_casual_subtype(text: str) -> str | None:
    """Helper that mirrors the old router function using the service's static method."""
    from app.application.smart_query_service import (
        _CASUAL_PATTERNS,
        _FAREWELL_PATTERN,
        _GREETING_PATTERN,
        _THANKS_PATTERN,
    )
    t = text.strip()
    if not _CASUAL_PATTERNS.match(t):
        return None
    if _GREETING_PATTERN.match(t):
        return "greeting"
    if _THANKS_PATTERN.match(t):
        return "thanks"
    if _FAREWELL_PATTERN.match(t):
        return "farewell"
    return "generic"


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
