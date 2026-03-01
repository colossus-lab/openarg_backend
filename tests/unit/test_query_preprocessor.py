"""Tests for advanced query preprocessor — acronyms, temporal, provinces, LLM reformulation."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock

from app.infrastructure.adapters.search.query_preprocessor import (
    ACRONYM_MAP,
    QueryPreprocessor,
    expand_acronyms,
    normalize_provinces,
    normalize_temporal,
)


@dataclass
class FakeLLMResponse:
    content: str
    tokens_used: int = 50
    model: str = "test"


class TestExpandAcronyms:
    def test_pbi(self):
        result = expand_acronyms("evolución del PBI")
        assert "Producto Bruto Interno" in result

    def test_ipc(self):
        result = expand_acronyms("IPC último mes")
        assert "Índice de Precios al Consumidor" in result

    def test_bcra(self):
        result = expand_acronyms("reservas del BCRA")
        assert "Banco Central" in result

    def test_indec(self):
        result = expand_acronyms("datos del INDEC")
        assert "Instituto Nacional de Estadística" in result

    def test_emae(self):
        result = expand_acronyms("EMAE mensual")
        assert "Estimador Mensual de Actividad Económica" in result

    def test_ddjj(self):
        result = expand_acronyms("las DDJJ de los diputados")
        assert "Declaraciones Juradas" in result

    def test_ccl(self):
        result = expand_acronyms("dólar CCL hoy")
        assert "Contado Con Liquidación" in result

    def test_multiple_acronyms(self):
        result = expand_acronyms("PBI e IPC de Argentina")
        assert "Producto Bruto Interno" in result
        assert "Índice de Precios al Consumidor" in result

    def test_no_acronym(self):
        query = "inflación del último mes"
        assert expand_acronyms(query) == query

    def test_case_insensitive(self):
        result = expand_acronyms("el pbi de argentina")
        assert "Producto Bruto Interno" in result

    def test_all_acronyms_defined(self):
        expected = {"PBI", "IPC", "BCRA", "INDEC", "EMAE", "CBT", "CBA", "DDJJ",
                    "HCDN", "ANSES", "AFIP", "ARCA", "CCL", "MEP", "LELIQ",
                    "EMI", "FMI", "EMBI", "EPH", "UVA", "INTA", "CONICET"}
        assert expected.issubset(set(ACRONYM_MAP.keys()))


class TestNormalizeTemporal:
    def test_ultimo_mes(self):
        result, metadata = normalize_temporal("inflación último mes")
        assert "start_date" in metadata
        assert "end_date" in metadata
        assert "desde" in result

    def test_ultimos_n_meses(self):
        result, metadata = normalize_temporal("inflación últimos 6 meses")
        assert "start_date" in metadata

    def test_ultimo_ano(self):
        result, metadata = normalize_temporal("PBI último año")
        assert "start_date" in metadata

    def test_este_ano(self):
        result, metadata = normalize_temporal("exportaciones este año")
        assert "start_date" in metadata
        assert metadata["start_date"].endswith("-01-01")

    def test_hoy(self):
        result, metadata = normalize_temporal("dólar hoy")
        assert "date" in metadata

    def test_ayer(self):
        result, metadata = normalize_temporal("cotización ayer")
        assert "date" in metadata

    def test_no_temporal(self):
        result, metadata = normalize_temporal("ranking de diputados")
        assert metadata == {}
        assert result == "ranking de diputados"


class TestNormalizeProvinces:
    def test_bsas(self):
        assert "Buenos Aires" in normalize_provinces("datos de bsas")

    def test_caba(self):
        assert "Ciudad Autónoma" in normalize_provinces("transporte en caba")

    def test_cba(self):
        assert "Córdoba" in normalize_provinces("educación en cba")

    def test_no_alias(self):
        query = "datos de Argentina"
        assert normalize_provinces(query) == query


class TestQueryPreprocessor:
    async def test_reformulate_calls_llm(self):
        llm = AsyncMock()
        llm.chat.return_value = FakeLLMResponse(content="consulta reformulada")
        preprocessor = QueryPreprocessor(llm)
        result = await preprocessor.reformulate("IPC último mes")
        assert llm.chat.called
        assert result == "consulta reformulada"

    async def test_reformulate_short_query_still_calls_llm(self):
        """Short queries should NOT be skipped anymore."""
        llm = AsyncMock()
        llm.chat.return_value = FakeLLMResponse(content="expanded")
        preprocessor = QueryPreprocessor(llm)
        await preprocessor.reformulate("IPC")
        assert llm.chat.called

    async def test_reformulate_expands_acronyms_before_llm(self):
        llm = AsyncMock()
        llm.chat.return_value = FakeLLMResponse(content="reformulated")
        preprocessor = QueryPreprocessor(llm)
        await preprocessor.reformulate("PBI de Argentina")
        # The LLM should receive the expanded acronym
        call_args = llm.chat.call_args
        user_msg = (
            call_args[1]["messages"][1].content
            if "messages" in call_args[1]
            else call_args[0][0][1].content
        )
        assert "Producto Bruto Interno" in user_msg

    async def test_reformulate_normalizes_temporal_before_llm(self):
        """normalize_temporal should be called in the pipeline."""
        llm = AsyncMock()
        llm.chat.return_value = FakeLLMResponse(content="reformulated")
        preprocessor = QueryPreprocessor(llm)
        await preprocessor.reformulate("inflación último mes")
        call_args = llm.chat.call_args
        user_msg = (
            call_args[1]["messages"][1].content
            if "messages" in call_args[1]
            else call_args[0][0][1].content
        )
        assert "desde" in user_msg

    async def test_reformulate_fallback_on_error(self):
        llm = AsyncMock()
        llm.chat.side_effect = Exception("LLM error")
        preprocessor = QueryPreprocessor(llm)
        result = await preprocessor.reformulate("PBI de Argentina")
        assert "Producto Bruto Interno" in result

    def test_preprocess_sync(self):
        llm = AsyncMock()
        preprocessor = QueryPreprocessor(llm)
        result = preprocessor.preprocess_sync("IPC en caba")
        assert "Índice de Precios al Consumidor" in result
        assert "Ciudad Autónoma" in result

    def test_preprocess_sync_normalizes_temporal(self):
        llm = AsyncMock()
        preprocessor = QueryPreprocessor(llm)
        result = preprocessor.preprocess_sync("inflación último mes")
        assert "desde" in result
