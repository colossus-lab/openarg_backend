"""Lógica pura del MCP público (`mcp_publico/core.py`). Sin `mcp` instalado."""

from __future__ import annotations

import pytest
from mcp_publico import core

KEY = "oarg_sk_" + "Ab3_-" * 8


class TestExtractKey:
    def test_bearer_key(self) -> None:
        assert core.extract_key({"Authorization": f"Bearer {KEY}"}) == KEY

    def test_header_name_and_scheme_are_case_insensitive(self) -> None:
        assert core.extract_key({"authorization": f"bearer {KEY}"}) == KEY

    @pytest.mark.parametrize(
        "headers", [None, {}, {"Authorization": ""}, {"Authorization": "Bearer "}]
    )
    def test_missing_key_points_to_where_to_get_one(self, headers: dict[str, str] | None) -> None:
        with pytest.raises(core.UserFacingError, match="openarg.org/desarrolladores"):
            core.extract_key(headers)

    def test_foreign_token_is_rejected_before_reaching_the_backend(self) -> None:
        with pytest.raises(core.UserFacingError, match="oarg_sk_"):
            core.extract_key({"Authorization": "Bearer sk-ant-xxxx"})


class TestClientIp:
    def test_first_forwarded_ip(self) -> None:
        assert core.client_ip({"X-Forwarded-For": "181.1.2.3, 172.18.0.5"}) == "181.1.2.3"

    def test_ipv6(self) -> None:
        assert core.client_ip({"x-forwarded-for": "2800:810::1"}) == "2800:810::1"

    @pytest.mark.parametrize("value", ["", "no-es-ip", "1.2.3.4; rm -rf /"])
    def test_garbage_is_not_forwarded(self, value: str) -> None:
        assert core.client_ip({"X-Forwarded-For": value}) is None

    def test_backend_headers_only_carry_a_valid_ip(self) -> None:
        assert "X-Forwarded-For" not in core.backend_headers(KEY, None)
        assert core.backend_headers(KEY, "1.2.3.4")["X-Forwarded-For"] == "1.2.3.4"


class TestRedact:
    def test_key_never_reaches_a_log(self) -> None:
        line = f"ConnectError while calling with Bearer {KEY} at /ask"
        assert KEY not in core.redact(line)
        assert "oarg_sk_[REDACTED]" in core.redact(line)


class TestQuestion:
    def test_strips(self) -> None:
        assert core.validate_question("  desempleo  ") == "desempleo"

    def test_empty(self) -> None:
        with pytest.raises(core.UserFacingError):
            core.validate_question("   ")

    def test_too_long(self) -> None:
        with pytest.raises(core.UserFacingError, match="demasiado larga"):
            core.validate_question("x" * (core.MAX_QUESTION_CHARS + 1))


class TestErrorMessages:
    def test_day_quota(self) -> None:
        msg = core.error_message(429, "Rate limit exceeded: 10 requests per day")
        assert "10 consultas de hoy" in msg and "21:00" in msg

    def test_minute_quota(self) -> None:
        assert "minuto" in core.error_message(429, "Rate limit exceeded: 2 requests per minute")

    def test_ip_quota(self) -> None:
        assert "conexión" in core.error_message(
            429, "Too many requests from this IP. Try again tomorrow."
        )

    def test_global_cap(self) -> None:
        assert "cupo público" in core.error_message(503, "Free tier daily capacity reached.")

    def test_invalid_key(self) -> None:
        assert "revocada" in core.error_message(401)

    def test_unknown_status_does_not_leak_detail(self) -> None:
        msg = core.error_message(500, "Traceback: secret stuff")
        assert "Traceback" not in msg


class TestFormatAnswer:
    def test_answer_sources_warnings_and_quota(self) -> None:
        text = core.format_answer(
            {
                "answer": "La tasa fue 7,6 %.",
                "sources": [
                    {"name": "EPH", "url": "https://datos.gob.ar/eph", "portal": "datos_gob_ar"},
                    {"name": "EPH", "url": "https://datos.gob.ar/eph", "portal": "datos_gob_ar"},
                ],
                "warnings": ["Dato provisorio"],
                "usage": {"requests_remaining_today": 7},
            }
        )
        assert text.startswith("La tasa fue 7,6 %.")
        assert text.count("[EPH (datos_gob_ar)](https://datos.gob.ar/eph)") == 1
        assert "- Dato provisorio" in text
        assert "restantes hoy: 7" in text

    def test_non_http_url_is_not_linked(self) -> None:
        text = core.format_answer(
            {"answer": "x", "sources": [{"name": "A", "url": "javascript:alert(1)"}]}
        )
        assert "javascript:" not in text

    def test_empty_payload(self) -> None:
        assert "no devolvió" in core.format_answer({})


class TestFormatSources:
    def test_lists_portals(self) -> None:
        text = core.format_sources(
            {
                "total_datasets": 30,
                "fuentes": [{"portal": "caba", "datasets": 20}, {"portal": "pba", "datasets": 10}],
            }
        )
        assert "30 datasets de 2 portales" in text
        assert "- caba: 20 datasets" in text


class TestLinks:
    def test_docs_links_in_messages_point_to_pages_that_exist(self) -> None:
        import re
        from pathlib import Path

        site = Path(core.__file__).parent / "site"
        source = Path(core.__file__).read_text(encoding="utf-8")
        pages = re.findall(r"\{DOCS_URL\}/([\w.-]+)", source)
        assert pages, "core.py no enlaza ninguna página de la web"
        for page in pages:
            assert (site / page).is_file(), f"{page} no existe en mcp_publico/site"
