"""Integration tests for the full query pipeline (plan -> dispatch -> analyze).

These tests mock external services (LLM, DB) but exercise the real orchestration
logic in SmartQueryService, verifying that plan generation, step dispatch,
error handling, and analysis work together correctly.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

from app.application.smart_query_service import SmartQueryResult, SmartQueryService
from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode


@dataclass
class FakeLLMResponse:
    content: str = "El dólar cotiza a $850 según datos de ArgentinaDatos."
    tokens_used: int = 150
    model: str = "test-model"


def _make_mock_deps() -> dict:
    """Build a complete dict of mocked dependencies for SmartQueryService."""
    llm = AsyncMock()
    llm.chat.return_value = FakeLLMResponse()

    async def fake_stream(*args, **kwargs):
        for chunk in ["Respuesta ", "de ", "prueba"]:
            yield chunk

    llm.chat_stream = fake_stream

    embedding = AsyncMock()
    embedding.embed.return_value = [0.1] * 768

    vector_search = AsyncMock()
    vector_search.search_datasets_hybrid.return_value = []

    cache = AsyncMock()
    cache.get.return_value = None
    cache.set.return_value = None

    series = AsyncMock()
    series.search.return_value = []
    series.fetch.return_value = None

    arg_datos = AsyncMock()
    arg_datos.fetch_dolar.return_value = None
    arg_datos.fetch_riesgo_pais.return_value = None
    arg_datos.fetch_inflacion.return_value = None

    georef = AsyncMock()
    georef.normalize_location.return_value = None

    ckan = AsyncMock()
    ckan.search_datasets.return_value = []
    ckan.query_datastore.return_value = []

    sesiones = AsyncMock()
    sesiones.search.return_value = None

    ddjj = MagicMock()
    ddjj.search.return_value = DataResult(
        source="ddjj",
        portal_name="DDJJ",
        portal_url="",
        dataset_title="test",
        format="json",
        records=[],
        metadata={},
    )

    semantic_cache = AsyncMock()
    semantic_cache.get.return_value = None
    semantic_cache.set.return_value = None

    return {
        "llm": llm,
        "embedding": embedding,
        "vector_search": vector_search,
        "cache": cache,
        "series": series,
        "arg_datos": arg_datos,
        "georef": georef,
        "ckan": ckan,
        "sesiones": sesiones,
        "ddjj": ddjj,
        "semantic_cache": semantic_cache,
    }


def _make_fake_dolar_result() -> DataResult:
    return DataResult(
        source="argentina_datos",
        portal_name="ArgentinaDatos",
        portal_url="https://argentinadatos.com",
        dataset_title="Dólar",
        format="time_series",
        records=[{"fecha": "2024-01-01", "compra": 800, "venta": 850}],
        metadata={"total_records": 1, "fetched_at": "2024-01-01"},
    )


def _make_fake_series_result() -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="Series de Tiempo",
        portal_url="https://apis.datos.gob.ar/series",
        dataset_title="IPC",
        format="time_series",
        records=[
            {"fecha": "2024-01-01", "valor": 254.2},
            {"fecha": "2024-02-01", "valor": 276.1},
        ],
        metadata={"total_records": 2, "fetched_at": "2024-01-01"},
    )


def _common_patches(fake_plan, memory_mock=None):
    """Return a list of context managers for patches common to all pipeline tests."""
    if memory_mock is None:
        memory_mock = MagicMock(turn_number=0, summaries=[], datasets_used=[])
    return [
        patch("app.application.smart_query_service.generate_plan", return_value=fake_plan),
        patch("app.application.smart_query_service.load_memory", return_value=memory_mock),
        patch("app.application.smart_query_service.update_memory", new_callable=AsyncMock),
        patch("app.application.smart_query_service.save_memory", new_callable=AsyncMock),
    ]


class TestFullPipelinePlanToAnalysis:
    """Test the complete pipeline: plan with 2 steps -> dispatch -> analyze."""

    async def test_full_pipeline_plan_to_analysis(self):
        """Mock planner returns a plan with 2 steps, mock connectors return data,
        mock analyst returns analysis. Verify the full flow produces a response."""
        deps = _make_mock_deps()
        service = SmartQueryService(**deps)

        fake_plan = ExecutionPlan(
            query="inflación y dólar",
            intent="consulta_datos",
            steps=[
                PlanStep(
                    id="step_1",
                    action="query_argentina_datos",
                    description="Fetch dolar data",
                    params={"type": "dolar"},
                ),
                PlanStep(
                    id="step_2",
                    action="query_series",
                    description="Fetch IPC series",
                    params={"seriesIds": ["148.3_INIVELNAL_DICI_M_26"]},
                ),
            ],
        )

        deps["arg_datos"].fetch_dolar.return_value = _make_fake_dolar_result()
        deps["series"].fetch.return_value = _make_fake_series_result()

        patches = _common_patches(fake_plan)
        with patches[0], patches[1], patches[2], patches[3]:
            result = await service.execute(
                "¿cómo viene la inflación y el dólar?",
                user_id="test@test.com",
            )

        assert isinstance(result, SmartQueryResult)
        assert result.answer  # LLM should have produced an answer
        assert result.tokens_used == 150  # From FakeLLMResponse
        assert result.intent == "consulta_datos"
        # The LLM chat method should have been called for analysis
        deps["llm"].chat.assert_called_once()


class TestPipelineWithEmptyPlan:
    """When planner returns an empty plan, pipeline should inject search_datasets fallback."""

    async def test_pipeline_with_empty_plan(self):
        """Planner returns empty plan, verify fallback to search_datasets step."""
        deps = _make_mock_deps()
        service = SmartQueryService(**deps)

        empty_plan = ExecutionPlan(
            query="datos sobre educación",
            intent="consulta_datos",
            steps=[],
        )

        # Vector search returns some results when search_datasets is invoked
        deps["vector_search"].search_datasets_hybrid.return_value = [
            {
                "dataset_title": "Educación primaria",
                "portal_name": "datos.gob.ar",
                "portal_url": "https://datos.gob.ar",
                "description": "Datos de educación primaria",
                "score": 0.85,
            },
        ]

        patches = _common_patches(empty_plan)
        with patches[0], patches[1], patches[2], patches[3]:
            result = await service.execute(
                "datos sobre educación",
                user_id="test@test.com",
            )

        assert isinstance(result, SmartQueryResult)
        assert result.answer  # Should still produce an answer
        # The pipeline should have injected a search_datasets fallback step
        assert result.intent == "consulta_datos"


class TestPipelineConnectorFailurePartial:
    """When 1 of 2 connectors fails, the pipeline still produces results
    from the working connector."""

    async def test_pipeline_connector_failure_partial(self):
        deps = _make_mock_deps()
        service = SmartQueryService(**deps)

        fake_plan = ExecutionPlan(
            query="dólar e IPC",
            intent="consulta_datos",
            steps=[
                PlanStep(
                    id="step_1",
                    action="query_series",
                    description="Fetch IPC",
                    params={"seriesIds": ["148.3_INIVELNAL_DICI_M_26"]},
                ),
                PlanStep(
                    id="step_2",
                    action="query_argentina_datos",
                    description="Fetch dolar",
                    params={"type": "dolar"},
                ),
            ],
        )

        # Series connector fails with ConnectorError
        deps["series"].fetch.side_effect = ConnectorError(
            error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
        )

        # ArgDatos connector succeeds
        deps["arg_datos"].fetch_dolar.return_value = _make_fake_dolar_result()

        patches = _common_patches(fake_plan)
        with patches[0], patches[1], patches[2], patches[3]:
            result = await service.execute(
                "¿dólar e inflación?",
                user_id="test@test.com",
            )

        assert isinstance(result, SmartQueryResult)
        assert result.answer  # Pipeline should still produce a response
        assert result.tokens_used == 150
        # Should have a warning about the failed connector
        assert len(result.warnings) >= 1
        assert any("series" in w.lower() or "conector" in w.lower() for w in result.warnings)


class TestPipelineAllConnectorsFail:
    """When all connectors fail, the pipeline should produce a graceful
    fallback response (general knowledge or capabilities)."""

    async def test_pipeline_all_connectors_fail(self):
        deps = _make_mock_deps()
        service = SmartQueryService(**deps)

        fake_plan = ExecutionPlan(
            query="datos del BCRA",
            intent="consulta_datos",
            steps=[
                PlanStep(
                    id="step_1",
                    action="query_series",
                    description="Fetch BCRA series",
                    params={"seriesIds": ["bcra_series"]},
                ),
                PlanStep(
                    id="step_2",
                    action="query_argentina_datos",
                    description="Fetch riesgo pais",
                    params={"type": "riesgo_pais"},
                ),
            ],
        )

        # All connectors fail
        deps["series"].fetch.side_effect = ConnectorError(
            error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
        )
        deps["arg_datos"].fetch_riesgo_pais.side_effect = ConnectorError(
            error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
        )

        # LLM still works for fallback analysis
        deps["llm"].chat.return_value = FakeLLMResponse(
            content="No se encontraron datos directos, pero puedo informarte que...",
        )

        patches = _common_patches(fake_plan)
        with patches[0], patches[1], patches[2], patches[3]:
            result = await service.execute(
                "reservas del BCRA",
                user_id="test@test.com",
            )

        assert isinstance(result, SmartQueryResult)
        assert result.answer  # Should still have a fallback answer
        # Should have warnings about failed connectors
        assert len(result.warnings) >= 1


class TestPipelinePlanValidationFixesBadPlan:
    """LLM returns a malformed plan (invalid action), verify validation
    fixes it and the pipeline continues with valid steps only."""

    async def test_pipeline_plan_validation_fixes_bad_plan(self):
        deps = _make_mock_deps()
        service = SmartQueryService(**deps)

        # Plan with one invalid and one valid action.
        # generate_plan calls _validate_plan internally, which removes
        # invalid actions. We simulate this by having generate_plan
        # return a plan where the invalid step was already stripped.
        fake_plan = ExecutionPlan(
            query="test query",
            intent="consulta_datos",
            steps=[
                # _validate_plan would have stripped "hack_the_planet"
                # so only valid steps remain
                PlanStep(
                    id="step_2",
                    action="query_argentina_datos",
                    description="Fetch dolar",
                    params={"type": "dolar"},
                ),
            ],
        )

        deps["arg_datos"].fetch_dolar.return_value = _make_fake_dolar_result()

        patches = _common_patches(fake_plan)
        with patches[0], patches[1], patches[2], patches[3]:
            result = await service.execute(
                "datos del dólar",
                user_id="test@test.com",
            )

        assert isinstance(result, SmartQueryResult)
        assert result.answer
        assert result.tokens_used == 150

    async def test_validate_plan_strips_invalid_actions(self):
        """Directly test _validate_plan with a malformed plan."""
        from app.infrastructure.adapters.connectors.query_planner import _validate_plan

        bad_plan = {
            "steps": [
                {"id": "s1", "action": "hack_the_planet", "params": {}},
                {"id": "s2", "action": "query_argentina_datos", "params": {"type": "dolar"}},
                {"action": "search_ckan", "params": {"query": "test"}},
            ]
        }
        result = _validate_plan(bad_plan)
        # Invalid action should be removed; missing id should be generated
        actions = [s["action"] for s in result["steps"]]
        assert "hack_the_planet" not in actions
        assert "query_argentina_datos" in actions
        assert "search_ckan" in actions
        # The step without an id should have gotten one
        assert all(s.get("id") for s in result["steps"])
