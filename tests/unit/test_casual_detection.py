"""Tests for casual/meta message detection in smart_query_router."""

import pytest

from app.presentation.http.controllers.query.smart_query_router import (
    _classify_casual_subtype,
    _get_casual_response,
    _get_meta_response,
)


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
