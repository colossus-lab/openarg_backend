"""Tests for boletin_tasks parser and helpers."""
from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.celery.tasks.boletin_tasks import (
    _parse_assignments,
    _search_bo,
)

# ── _parse_assignments ──────────────────────────────────────


class TestParseAssignmentsSingle:
    def test_single_diputado_single_asesor(self):
        text = (
            "RESOLUCION Nº 123/2025. "
            "Designar al agente GOMEZ, Juan Carlos (Legajo Nº 4567) "
            "en el cargo de asesor del diputado nacional Yeza, Martín "
            "a partir del 1 de marzo de 2025."
        )
        result = _parse_assignments(text, "2025-03-01", "RES-123", "BO-456")

        assert len(result) == 1
        r = result[0]
        assert r["employee_apellido"] == "GOMEZ"
        assert r["employee_nombre"] == "Juan Carlos"
        assert r["legislator_name"] == "YEZA, MARTÍN"
        assert r["legajo"] == "4567"
        assert r["cargo"] == "asesor"
        assert r["boletin_date"] == "2025-03-01"
        assert r["boletin_numero"] == "BO-456"
        assert r["resolution_id"] == "RES-123"


class TestParseAssignmentsMultiple:
    def test_multiple_empleados_for_one_diputado(self):
        text = (
            "RESOLUCION Nº 200/2025. "
            "Designar al agente PEREZ, María en el cargo de asesor "
            "del diputado Fernández. "
            "Designar a la señora LOPEZ, Ana en el cargo de secretario "
            "del diputado Fernández."
        )
        result = _parse_assignments(text, "2025-06-15", "RES-200")

        assert len(result) == 2
        apellidos = {r["employee_apellido"] for r in result}
        assert "PEREZ" in apellidos
        assert "LOPEZ" in apellidos
        for r in result:
            assert r["legislator_name"] == "FERNÁNDEZ"


class TestParseLegajoExtraction:
    def test_legajo_extracted_from_text(self):
        text = (
            "Desígnase al agente RODRIGUEZ, Pedro (Legajo N° 9876) "
            "en el cargo de asesor del diputado nacional García, Juan."
        )
        result = _parse_assignments(text, "2025-01-10")

        assert len(result) == 1
        assert result[0]["legajo"] == "9876"

    def test_legajo_lowercase_format(self):
        text = (
            "Designar al agente MARTINEZ, Luis, legajo 1234, "
            "en el cargo de asesor del diputado López."
        )
        result = _parse_assignments(text, "2025-01-10")

        assert len(result) == 1
        assert result[0]["legajo"] == "1234"


class TestParseNoMatch:
    def test_unrelated_text_returns_empty(self):
        text = (
            "La Cámara de Diputados de la Nación resuelve aprobar "
            "el presupuesto general para el ejercicio 2025."
        )
        result = _parse_assignments(text, "2025-01-01")
        assert result == []

    def test_empty_text_returns_empty(self):
        result = _parse_assignments("", "2025-01-01")
        assert result == []

    def test_none_like_text_returns_empty(self):
        result = _parse_assignments("", "2025-01-01", None, None)
        assert result == []

    def test_no_diputado_returns_empty(self):
        """Empleado found but no diputado → no assignment."""
        text = "Designar al agente GARCIA, Carlos en el cargo de asesor."
        result = _parse_assignments(text, "2025-01-01")
        assert result == []

    def test_no_legislator_falls_through(self):
        """Neither diputado nor senador → no assignment."""
        text = (
            "Designar al agente SOSA, Roberto en el cargo de asesor "
            "del Ministerio de Economía a partir del 1 de enero de 2025."
        )
        result = _parse_assignments(text, "2025-01-01")
        assert result == []


# ── _search_bo ──────────────────────────────────────────────


class TestSearchBoPagination:
    def test_pagination_collects_all_pages(self):
        """Mock HTTP client to verify pagination stops when items < page_size."""
        mock_client = MagicMock()

        page1_resp = MagicMock()
        page1_resp.status_code = 200
        page1_resp.raise_for_status = MagicMock()
        page1_resp.json.return_value = {
            "dataList": [{"id": str(i)} for i in range(20)]
        }

        page2_resp = MagicMock()
        page2_resp.status_code = 200
        page2_resp.raise_for_status = MagicMock()
        page2_resp.json.return_value = {
            "dataList": [{"id": str(i)} for i in range(20, 25)]
        }

        mock_client.post.side_effect = [page1_resp, page2_resp]

        results = _search_bo(mock_client, "test query", "01/01/2025", "01/03/2025")

        assert len(results) == 25
        assert mock_client.post.call_count == 2

    def test_pagination_stops_on_empty(self):
        mock_client = MagicMock()

        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"dataList": []}
        mock_client.post.return_value = resp

        results = _search_bo(mock_client, "test", "01/01/2025", "01/03/2025")

        assert results == []
        assert mock_client.post.call_count == 1

    def test_http_error_returns_partial(self):
        """If HTTP fails mid-pagination, return what we got so far."""
        import httpx

        mock_client = MagicMock()

        page1_resp = MagicMock()
        page1_resp.status_code = 200
        page1_resp.raise_for_status = MagicMock()
        page1_resp.json.return_value = {
            "dataList": [{"id": "1"}, {"id": "2"}]
        }

        mock_client.post.side_effect = [
            page1_resp,
            httpx.HTTPStatusError("500", request=MagicMock(), response=MagicMock()),
        ]

        # First page has < _PAGE_SIZE items, so pagination stops after page 1
        results = _search_bo(mock_client, "test", "01/01/2025", "01/03/2025")
        assert len(results) == 2


# ── Senado parsing ─────────────────────────────────────────


class TestParseSenador:
    def test_parse_senador_single(self):
        """Resolution with one senador + one employee → camara='senado'."""
        text = (
            "RESOLUCION Nº 50/2025. "
            "Designar al agente ROMERO, Luis (Legajo Nº 7890) "
            "en el cargo de asesor del senador nacional Parrilli, Oscar "
            "a partir del 1 de junio de 2025."
        )
        result = _parse_assignments(text, "2025-06-01", "RES-50", "BO-789", "senado")

        assert len(result) == 1
        r = result[0]
        assert r["employee_apellido"] == "ROMERO"
        assert r["employee_nombre"] == "Luis"
        assert r["legislator_name"] == "PARRILLI, OSCAR"
        assert r["legajo"] == "7890"
        assert r["camara"] == "senado"

    def test_parse_senador_sets_camara(self):
        """When text has senador (not diputado), camara is set to 'senado'."""
        text = (
            "Designar al agente TORRES, Ana "
            "en el cargo de secretario del senador Lousteau, Martín "
            "a partir del 1 de marzo de 2025."
        )
        result = _parse_assignments(text, "2025-03-01")

        assert len(result) == 1
        assert result[0]["camara"] == "senado"
        assert result[0]["legislator_name"] == "LOUSTEAU, MARTÍN"

    def test_parse_diputado_keeps_camara(self):
        """Diputado resolution keeps camara='diputados'."""
        text = (
            "Designar al agente SILVA, Pedro "
            "en el cargo de asesor del diputado nacional Yeza, Martín "
            "a partir del 1 de marzo de 2025."
        )
        result = _parse_assignments(text, "2025-03-01")

        assert len(result) == 1
        assert result[0]["camara"] == "diputados"

    def test_senadora_feminine_form(self):
        """Feminine form 'senadora' is also matched."""
        text = (
            "Designar al agente VEGA, María "
            "en el cargo de asesor de la senadora Fernández, Cristina "
            "a partir del 1 de abril de 2025."
        )
        result = _parse_assignments(text, "2025-04-01", camara="senado")

        assert len(result) == 1
        assert result[0]["camara"] == "senado"
        assert result[0]["legislator_name"] == "FERNÁNDEZ, CRISTINA"
