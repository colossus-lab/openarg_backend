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

    def test_minute_quota_says_how_many_seconds_to_wait(self) -> None:
        """QW10: el backend manda los segundos reales en `Retry-After`; antes el
        MCP los descartaba y decía siempre "esperá un minuto"."""
        msg = core.error_message(
            429, "Rate limit exceeded: 30 catalog requests per minute", retry_after="17"
        )
        assert "esperá 17 segundos" in msg and "30 por minuto" in msg
        assert "esperá 1 segundo " in core.error_message(429, "2 per minute", retry_after=1)

    @pytest.mark.parametrize("retry_after", [None, "", "mañana", "0", "-5", "99999"])
    def test_a_useless_retry_after_falls_back_to_a_minute(self, retry_after: str | None) -> None:
        msg = core.error_message(429, "2 requests per minute", retry_after=retry_after)
        assert "esperá un minuto" in msg

    def test_the_limits_are_spelled_out_for_the_client(self) -> None:
        for fragment in (
            "30 pedidos por minuto",
            "200 por mes",
            "2.000",
            "2 preguntas por minuto",
            "10 por mes",
            "100)",
            "500 filas",
            "segundos esperar",
        ):
            assert fragment in core.LIMITES, fragment

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

    def test_search_names_the_file_so_siblings_can_be_told_apart(self) -> None:
        """Cabecera y detalle de "Votaciones Nominales" tienen el mismo título."""
        out = core.format_search(
            {
                "resultados": [
                    {
                        "titulo": "Votaciones Nominales",
                        "portal": "diputados",
                        "archivo": "actas-cabecera-137-2.0.csv",
                        "formato": "CSV",
                        "tablas": [{"tabla": "raw.a", "filas": 100}],
                    },
                    {
                        "titulo": "Votaciones Nominales",
                        "portal": "diputados",
                        "archivo": "actas-detalle-137-2.0.csv",
                        "formato": "CSV",
                        "tablas": [{"tabla": "raw.b", "filas": 25700}],
                    },
                ]
            }
        )
        assert "`actas-cabecera-137-2.0.csv` (CSV)" in out
        assert "`actas-detalle-137-2.0.csv` (CSV)" in out

    def test_search_without_file_keeps_the_old_shape(self) -> None:
        out = core.format_search({"resultados": [{"titulo": "X", "portal": "p", "tablas": []}]})
        assert "Archivo:" not in out

    def test_rows_without_results(self) -> None:
        assert "no devolvió filas" in core.format_rows({"filas": [], "fuente": "X"})

    def test_rows_without_results_shows_why_and_what_exists(self) -> None:
        """Auditoría 3.2: el modelo cliente no recibía ninguna pista."""
        out = core.format_rows(
            {
                "filas": [],
                "fuente": "X",
                "aviso": "Ninguna fila cumple los filtros pedidos. Valores parecidos: «Educación y Cultura».",
            }
        )
        assert "no devolvió filas" in out and "«Educación y Cultura»" in out

    def test_rows_say_which_value_was_used(self) -> None:
        out = core.format_rows(
            {
                "filas": [{"a": 1}],
                "columnas": ["a"],
                "fuente": "X",
                "filtros_aplicados": ["funcion_desc: filtré por «Educación y Cultura»"],
            }
        )
        assert "filtré por «Educación y Cultura»" in out

    def test_table_shows_the_date_warning(self) -> None:
        out = core.format_table({"tabla": "t", "columnas": [], "aviso_fecha": "formato raro"})
        assert "Aviso: formato raro" in out

    def test_table_shows_its_freshness(self) -> None:
        """Auditoría 3.4: no había ni fecha de lectura ni fecha de corte."""
        out = core.format_table(
            {
                "tabla": "t",
                "columnas": [],
                "frescura": {
                    "actualizada": "2026-05-09",
                    "ultimo_dato": "2023-04-01",
                    "serie": True,
                    "nota": "OpenArg la leyó de su fuente por última vez hace 149 días.",
                },
            }
        )
        assert "Último dato: 2023-04-01" in out
        assert "Leída de la fuente por OpenArg: 2026-05-09" in out
        assert "Nota: OpenArg la leyó" in out
        foto = core.format_table(
            {
                "tabla": "t",
                "columnas": [],
                "frescura": {"actualizada": "2026-10-05", "fecha_corte": "2026-10-05"},
            }
        )
        assert "Fecha de corte: 2026-10-05" in foto and "Último dato" not in foto

    def test_table_without_freshness_keeps_the_old_shape(self) -> None:
        assert "Frescura" not in core.format_table({"tabla": "t", "columnas": []})

    def test_search_marks_datasets_without_a_table(self) -> None:
        """QW11: el modelo no puede usarlos con las otras herramientas."""
        out = core.format_search({"resultados": [{"titulo": "X", "portal": "p", "tablas": []}]})
        assert "Sin tabla consultable" in out and "sólo el link de descarga" in out

    def test_truncated_rows_give_the_next_offset(self) -> None:
        out = core.format_rows(
            {
                "filas": [{"a": 1}],
                "columnas": ["a"],
                "fuente": "X",
                "truncado": True,
                "siguiente_offset": 100,
            }
        )
        assert "`offset=100`" in out and "agregar_datos" in out

    def test_aggregate_says_over_how_many_rows(self) -> None:
        out = core.format_aggregate(
            {
                "tabla": "raw.cache_presupuesto_credito_2026",
                "calculo": "suma de credito_devengado",
                "columnas": ["jurisdiccion_desc", "valor", "filas_usadas"],
                "filas": [
                    {
                        "jurisdiccion_desc": "Ministerio de Capital Humano",
                        "valor": 60622921.86,
                        "filas_usadas": 412,
                    },
                ],
                "filas_usadas": 4905,
                "fuente": "Crédito 2026",
                "url": "https://presupuestoabierto.gob.ar/x",
                "notas": ["Hay más de 1 grupos: se muestran los primeros 1 según el orden pedido."],
            }
        )
        assert "sobre 4.905 filas" in out and "calculado por OpenArg" in out
        assert "[Crédito 2026](https://presupuestoabierto.gob.ar/x)" in out
        assert "Ministerio de Capital Humano,60622921.86,412" in out
        assert "Hay más de 1 grupos" in out

    def test_aggregate_without_rows_is_not_a_zero(self) -> None:
        out = core.format_aggregate(
            {
                "tabla": "t",
                "calculo": "suma de x",
                "filas": [],
                "filas_usadas": 0,
                "aviso": "Ninguna fila cumple los filtros pedidos. Valores parecidos: «Salud».",
            }
        )
        assert out.startswith("Sin resultado para suma de x") and "«Salud»" in out
        assert "```csv" not in out

    def test_a_big_total_is_not_written_in_scientific_notation(self) -> None:
        """`valor` ahora llega como número: str(1.2e16) era "1.2e+16"."""
        out = core.format_aggregate(
            {
                "tabla": "t",
                "calculo": "suma de monto",
                "columnas": ["valor", "filas_usadas"],
                "filas": [{"valor": 12345678901234568.0, "filas_usadas": 3}],
                "filas_usadas": 3,
            }
        )
        assert "12345678901234568,3" in out and "e+" not in out
        assert core.rows_to_csv(["v"], [{"v": 1e-7}]).endswith("0.0000001")
        assert core.rows_to_csv(["v"], [{"v": 33.14}]).endswith("33.14")

    def test_an_approximate_last_datum_says_so(self) -> None:
        out = core.format_table(
            {
                "tabla": "t",
                "columnas": [],
                "frescura": {"ultimo_dato": "2025-11-01", "serie": True, "aproximado": True},
            }
        )
        assert "Último dato (aproximado): 2025-11-01" in out

    def test_unknown_period_shows_no_last_datum_nor_cutoff(self) -> None:
        """Con columna de fecha pero sin rango, el backend manda serie=None."""
        out = core.format_table(
            {
                "tabla": "t",
                "columnas": [],
                "frescura": {
                    "actualizada": "2026-10-02",
                    "serie": None,
                    "nota": "No se pudo determinar qué período cubre.",
                },
            }
        )
        assert "Último dato" not in out and "Fecha de corte" not in out
        assert "Leída de la fuente por OpenArg: 2026-10-02" in out

    def test_a_422_in_data_mode_says_which_parameter(self) -> None:
        """Antes «La consulta no tiene un formato válido.» y nada más: el modelo no
        sabía que el offset pasaba el tope o que agrupar_por admite 3 columnas."""
        detail = core.error_detail(
            [
                {
                    "type": "less_than_equal",
                    "loc": ["body", "offset"],
                    "msg": "Input should be less than or equal to 10000",
                    "input": 10400,
                    "ctx": {"le": 10000},
                }
            ]
        )
        assert detail == "`offset`: Input should be less than or equal to 10000"
        msg = core.error_message(422, detail, data_mode=True)
        assert "formato válido" in msg and "`offset`" in msg and "10000" in msg
        # El modo respuestas no cambia.
        assert core.error_message(422, detail) == "La consulta no tiene un formato válido."

    def test_error_detail_keeps_our_text_and_survives_junk(self) -> None:
        assert core.error_detail("Columnas que no existen: x.") == "Columnas que no existen: x."
        assert core.error_detail(None) == ""
        assert core.error_detail([{"loc": ["body", "agrupar_por"], "msg": "too long"}, 3]) == (
            "`agrupar_por`: too long"
        )


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
