"""Tests for DDJJAdapter in-memory dataset operations."""

from __future__ import annotations

import pytest

from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter


@pytest.fixture
def adapter():
    """Create adapter with test data instead of loading real file."""
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [
        {
            "cuit": "20-12345678-9",
            "nombre": "PEREZ, JUAN CARLOS",
            "sexo": "M",
            "fechaNacimiento": "1970-01-01",
            "estadoCivil": "CASADO/A",
            "cargo": "DIPUTADO NACIONAL",
            "organismo": "H.C.D.N.",
            "anioDeclaracion": 2024,
            "tipoDeclaracion": "ANUAL",
            "bienesInicio": 50000000,
            "deudasInicio": 10000000,
            "bienesCierre": 60000000,
            "deudasCierre": 12000000,
            "patrimonioCierre": 48000000,
            "ingresosTrabajoNeto": 15000000,
            "gastosPersonales": 8000000,
            "bienes": [
                {"tipo": "INMUEBLES EN EL PAIS", "importe": 40000000},
                {"tipo": "AUTOMOTORES EN EL PAIS", "importe": 20000000},
            ],
        },
        {
            "cuit": "27-98765432-1",
            "nombre": "GARCIA, MARIA ELENA",
            "sexo": "F",
            "fechaNacimiento": "1985-06-15",
            "estadoCivil": "SOLTERA",
            "cargo": "DIPUTADO NACIONAL",
            "organismo": "H.C.D.N.",
            "anioDeclaracion": 2024,
            "tipoDeclaracion": "ANUAL",
            "bienesInicio": 100000000,
            "deudasInicio": 5000000,
            "bienesCierre": 120000000,
            "deudasCierre": 4000000,
            "patrimonioCierre": 116000000,
            "ingresosTrabajoNeto": 25000000,
            "gastosPersonales": 12000000,
            "bienes": [
                {"tipo": "INMUEBLES EN EL PAIS", "importe": 80000000},
                {"tipo": "TITULOS Y ACCIONES EN EL PAIS", "importe": 40000000},
            ],
        },
    ]
    return a


class TestDDJJAdapter:
    def test_search_by_name(self, adapter):
        result = adapter.search("perez")
        assert len(result.records) == 1
        assert result.records[0]["nombre"] == "PEREZ, JUAN CARLOS"

    def test_search_by_cuit(self, adapter):
        result = adapter.search("20-12345678-9")
        assert len(result.records) == 1

    def test_search_no_match(self, adapter):
        result = adapter.search("ZZZZZ_NOEXISTE")
        assert len(result.records) == 0

    def test_search_accent_insensitive(self, adapter):
        result = adapter.search("garcia")
        assert len(result.records) == 1
        assert "GARCIA" in result.records[0]["nombre"]

    def test_ranking_desc(self, adapter):
        result = adapter.ranking(sort_by="patrimonio", top=2, order="desc")
        assert len(result.records) == 2
        assert result.records[0]["patrimonio_cierre"] >= result.records[1]["patrimonio_cierre"]

    def test_ranking_asc(self, adapter):
        result = adapter.ranking(sort_by="patrimonio", top=1, order="asc")
        assert len(result.records) == 1
        assert result.records[0]["nombre"] == "PEREZ, JUAN CARLOS"

    def test_get_by_name(self, adapter):
        result = adapter.get_by_name("GARCIA")
        assert len(result.records) == 1

    def test_stats(self, adapter):
        result = adapter.stats()
        assert len(result.records) == 1
        stats = result.records[0]
        assert stats["total"] == 2
        assert stats["patrimonio_promedio"] > 0
        assert stats["patrimonio_maximo_nombre"] == "GARCIA, MARIA ELENA"

    def test_data_result_source(self, adapter):
        result = adapter.search("perez")
        assert result.source == "ddjj:oficina_anticorrupcion"

    def test_asset_summary(self, adapter):
        result = adapter.search("perez")
        bienes = result.records[0]["resumen_bienes"]
        assert "INMUEBLES" in str(bienes)
