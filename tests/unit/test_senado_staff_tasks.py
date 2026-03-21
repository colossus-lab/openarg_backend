"""Tests for senado_staff_tasks parser and helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks.senado_staff_tasks import (
    _fetch_senators,
    _parse_staff_from_html,
    _persist_all,
)

# ── _parse_staff_from_html ──────────────────────────────────


class TestParseStaffFromHtml:
    def test_normal_html(self):
        html = """
        <div id="Personal">
          <table>
            <tr><th>Nombre</th><th>Categoría</th></tr>
            <tr><td>DIEGO GASTON SALVO</td><td>A-4</td></tr>
            <tr><td>MARIA JOSE LOPEZ</td><td>B-2</td></tr>
          </table>
        </div>
        """
        result = _parse_staff_from_html(html)
        assert len(result) == 2
        assert result[0] == ("DIEGO GASTON SALVO", "A-4")
        assert result[1] == ("MARIA JOSE LOPEZ", "B-2")

    def test_malformed_html_no_tr(self):
        """HTML with missing <tr> tags (common server bug) — regex still matches."""
        html = """
        <div id="Personal">
          <table>
            <td>JUAN PEREZ</td><td>A-1</td>
            <td>ANA GOMEZ</td><td>C-3</td>
          </table>
        </div>
        """
        result = _parse_staff_from_html(html)
        assert len(result) == 2
        assert result[0] == ("JUAN PEREZ", "A-1")
        assert result[1] == ("ANA GOMEZ", "C-3")

    def test_no_personal_section(self):
        html = '<div id="Actividad"><table><td>FOO</td><td>A-1</td></table></div>'
        result = _parse_staff_from_html(html)
        assert result == []

    def test_empty_table(self):
        html = '<div id="Personal"><table></table></div>'
        result = _parse_staff_from_html(html)
        assert result == []

    def test_names_with_accents(self):
        html = """
        <div id="Personal">
          <table>
            <tr><td>JOSÉ MARÍA GONZÁLEZ</td><td>A-2</td></tr>
          </table>
        </div>
        """
        result = _parse_staff_from_html(html)
        assert len(result) == 1
        assert result[0] == ("JOSÉ MARÍA GONZÁLEZ", "A-2")

    def test_category_without_hyphen(self):
        """Categories like 'A4' (no hyphen) should also match."""
        html = """
        <div id="Personal">
          <table>
            <tr><td>PEDRO GARCIA</td><td>A4</td></tr>
          </table>
        </div>
        """
        result = _parse_staff_from_html(html)
        assert len(result) == 1
        assert result[0] == ("PEDRO GARCIA", "A4")

    def test_whitespace_trimmed(self):
        html = """
        <div id="Personal">
          <table>
            <tr><td>  SPACES NAME  </td><td>  B-1  </td></tr>
          </table>
        </div>
        """
        result = _parse_staff_from_html(html)
        assert len(result) == 1
        assert result[0] == ("SPACES NAME", "B-1")


# ── _fetch_senators ─────────────────────────────────────────


class TestFetchSenators:
    def test_successful_fetch(self):
        mock_client = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {
            "table": {
                "rows": [
                    {
                        "ID": "546",
                        "APELLIDO": "ABAD",
                        "NOMBRE": "MAXIMILIANO",
                        "BLOQUE": "UCR",
                        "PROVINCIA": "BUENOS AIRES",
                    },
                    {
                        "ID": "789",
                        "APELLIDO": "LOPEZ",
                        "NOMBRE": "ANA",
                        "BLOQUE": "FdT",
                        "PROVINCIA": "CABA",
                    },
                ]
            }
        }
        mock_client.get.return_value = resp

        result = _fetch_senators(mock_client)
        assert len(result) == 2
        assert result[0]["ID"] == "546"
        assert result[1]["APELLIDO"] == "LOPEZ"

    def test_http_error_returns_empty(self):
        import httpx

        mock_client = MagicMock()
        mock_client.get.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=MagicMock()
        )

        result = _fetch_senators(mock_client)
        assert result == []

    def test_malformed_json_returns_empty(self):
        mock_client = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"unexpected": "format"}
        mock_client.get.return_value = resp

        result = _fetch_senators(mock_client)
        assert result == []


# ── _persist_all ────────────────────────────────────────────


class TestPersistAll:
    def test_persist_records(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        records = [
            {
                "senator_id": "546",
                "senator_name": "ABAD, MAXIMILIANO",
                "bloque": "UCR",
                "provincia": "BUENOS AIRES",
                "employee_name": "DIEGO GASTON SALVO",
                "categoria": "A-4",
            }
        ]

        result = _persist_all(mock_engine, records)

        assert result == 1
        assert mock_conn.execute.call_count == 2  # DELETE + INSERT

    def test_persist_empty_still_deletes(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        result = _persist_all(mock_engine, [])

        assert result == 0
        assert mock_conn.execute.call_count == 1  # only DELETE


# ── scrape_senado_staff integration ─────────────────────────


class TestScrapeIntegration:
    @patch("app.infrastructure.celery.tasks.senado_staff_tasks.get_sync_engine")
    @patch("app.infrastructure.celery.tasks.senado_staff_tasks.httpx.Client")
    @patch("app.infrastructure.celery.tasks.senado_staff_tasks.time.sleep")
    def test_full_scrape_flow(self, mock_sleep, mock_client_cls, mock_get_engine):
        # Setup mock engine
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_get_engine.return_value = mock_engine

        # Setup mock HTTP client
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # Senators JSON response
        senators_resp = MagicMock()
        senators_resp.raise_for_status = MagicMock()
        senators_resp.json.return_value = {
            "table": {
                "rows": [
                    {
                        "ID": "546",
                        "APELLIDO": "ABAD",
                        "NOMBRE": "MAXIMILIANO",
                        "BLOQUE": "UCR",
                        "PROVINCIA": "BUENOS AIRES",
                    },
                ]
            }
        }

        # Profile HTML response
        profile_resp = MagicMock()
        profile_resp.raise_for_status = MagicMock()
        profile_resp.text = """
        <div id="Personal">
          <table>
            <tr><td>DIEGO GASTON SALVO</td><td>A-4</td></tr>
            <tr><td>MARIA LOPEZ</td><td>B-2</td></tr>
          </table>
        </div>
        """

        mock_client.get.side_effect = [senators_resp, profile_resp]

        # Import and call the task directly (bypassing Celery)
        from app.infrastructure.celery.tasks.senado_staff_tasks import scrape_senado_staff

        result = scrape_senado_staff()

        assert result["status"] == "ok"
        assert result["senators_scraped"] == 1
        assert result["staff_found"] == 2
        # Verify DELETE + INSERT were called
        assert mock_conn.execute.call_count == 2
