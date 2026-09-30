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

    def test_backend_headers_mark_the_mcp_and_the_client(self) -> None:
        headers = core.backend_headers(KEY, None, "claude-code/2.1.0")
        assert headers["X-OpenArg-Via"] == "mcp"
        assert headers["X-OpenArg-Client"] == "claude-code/2.1.0"
        assert "X-OpenArg-Client" not in core.backend_headers(KEY, None)

    def test_the_key_only_travels_in_authorization(self) -> None:
        headers = core.backend_headers(KEY, "1.2.3.4", "cursor/1.0")
        assert [k for k, v in headers.items() if KEY in v] == ["Authorization"]

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
    def test_monthly_quota_is_402(self) -> None:
        """Desde el 30-sep-2026 el cupo es mensual y se agota con 402, como en Tomi."""
        msg = core.error_message(402, "Monthly quota exceeded: 10 questions per month")
        assert "10 preguntas de este mes" in msg and "modo datos" in msg
        assert "21:00" in msg and core.SUPPORT_URL in msg and core.CONTACT_EMAIL in msg

    def test_founder_quota_number_comes_from_the_backend(self) -> None:
        msg = core.error_message(402, "Monthly quota exceeded: 100 questions per month")
        assert "100 preguntas de este mes" in msg

    def test_renewal_is_the_first_of_next_month(self) -> None:
        from datetime import UTC, datetime

        assert core.renewal_label(datetime(2026, 10, 15, tzinfo=UTC)).startswith(
            "1/11 (el 31/10 a las 21:00"
        )
        assert core.renewal_label(datetime(2026, 12, 31, 23, 0, tzinfo=UTC)).startswith(
            "1/1 (el 31/12"
        )
        assert core.renewal_label(datetime(2027, 2, 3, tzinfo=UTC)).startswith("1/3 (el 28/2")

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
                "usage": {"requests_remaining_today": 7, "requests_remaining_month": 7},
            }
        )
        assert text.startswith("La tasa fue 7,6 %.")
        assert text.count("[EPH (datos_gob_ar)](https://datos.gob.ar/eph)") == 1
        assert "- Dato provisorio" in text
        assert "te quedan este mes: 7" in text

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


class TestContacto:
    def test_la_web_usa_el_mail_que_existe(self) -> None:
        from pathlib import Path

        site = Path(core.__file__).parent / "site"
        for page in site.glob("*.html"):
            html = page.read_text(encoding="utf-8")
            assert "hola@colossuslab.org" not in html, f"{page.name}: ese mail no existe"
            assert "devops@colossuslab.org" in html, f"{page.name}: falta el mail de contacto"


class TestModoDatosCore:
    def test_catalog_monthly_quota_message(self) -> None:
        msg = core.error_message(402, "Monthly quota exceeded: 200 catalog requests per month")
        assert "200 consultas del modo datos de este mes" in msg

    def test_catalog_minute_quota_message(self) -> None:
        msg = core.error_message(429, "Rate limit exceeded: 30 catalog requests per minute")
        assert "minuto" in msg

    def test_data_mode_passes_our_validation_message(self) -> None:
        msg = core.error_message(404, "No existe esa tabla en el catálogo.", data_mode=True)
        assert msg == "No existe esa tabla en el catálogo."

    def test_answer_mode_does_not_pass_details(self) -> None:
        assert "Traceback" not in core.error_message(400, "Traceback x")

    def test_csv_quotes_commas_and_flattens_newlines(self) -> None:
        out = core.rows_to_csv(["a", "b"], [{"a": "x,y", "b": "l1\nl2"}, {"a": None, "b": 3}])
        assert out == 'a,b\n"x,y",l1 l2\n,3'

    def test_search_without_results(self) -> None:
        assert "No encontré" in core.format_search({"resultados": []})

    def test_rows_without_results(self) -> None:
        assert "no devolvió filas" in core.format_rows({"filas": [], "fuente": "X"})


class TestClientLabel:
    def test_client_info_wins_over_user_agent(self) -> None:
        assert core.client_label({"User-Agent": "node"}, "claude-code/2.1.0") == "claude-code/2.1.0"

    def test_falls_back_to_user_agent(self) -> None:
        assert core.client_label({"user-agent": "Cursor/1.7.2"}) == "Cursor/1.7.2"

    def test_nothing_known(self) -> None:
        assert core.client_label({}) is None
        assert core.client_label(None, "  ") is None

    def test_cannot_smuggle_another_header(self) -> None:
        label = core.client_label({"User-Agent": "evil\r\nX-Admin-Key: x"})
        assert label is not None
        assert "\r" not in label and "\n" not in label

    def test_is_truncated(self) -> None:
        assert len(core.client_label({"User-Agent": "a" * 500}) or "") == 160


class TestSupportLine:
    """El pedido de apoyo aparece sólo cuando se agota un cupo."""

    @pytest.mark.parametrize(
        ("status", "detail"),
        [
            (402, "Monthly quota exceeded: 10 questions per month"),
            (402, "Monthly quota exceeded: 200 catalog requests per month"),
            (503, "Free tier daily capacity reached."),
        ],
    )
    def test_daily_quota_messages_invite_support(self, status: int, detail: str) -> None:
        assert core.SUPPORT_URL in core.error_message(status, detail)

    @pytest.mark.parametrize(
        ("status", "detail", "data_mode"),
        [
            (429, "Rate limit exceeded: 2 requests per minute", False),
            (429, "Rate limit exceeded: 30 catalog requests per minute", True),
            (401, "", False),
            (408, "", False),
            (500, "", False),
            (404, "No existe esa tabla", True),
        ],
    )
    def test_other_messages_do_not(self, status: int, detail: str, data_mode: bool) -> None:
        assert core.SUPPORT_URL not in core.error_message(status, detail, data_mode=data_mode)
