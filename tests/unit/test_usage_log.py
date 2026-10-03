"""Registro de uso de la API pública (`public_api/usage_log.py`).

Lo que no puede pasar: que el modo datos guarde qué buscó alguien, que un 429
en loop llene la tabla, o que una falla de la base le rompa la respuesta al
usuario.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.domain.entities.api_key.api_key import ApiKey
from app.presentation.http.controllers.public_api import usage_log


def _request(headers: dict[str, str] | None = None) -> Any:
    # Starlette's Headers is case-insensitive; a lower-cased dict + .get is enough here.
    lowered = {k.lower(): v for k, v in (headers or {}).items()}
    return SimpleNamespace(headers=SimpleNamespace(get=lambda k, d=None: lowered.get(k.lower(), d)))


def _key() -> ApiKey:
    return ApiKey(id=uuid4(), user_id=uuid4(), key_prefix="oarg_sk_test")


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


def _recorded(repo: AsyncMock) -> Any:
    assert repo.record_usage.await_count == 1
    return repo.record_usage.await_args.args[0]


class TestRequestOrigin:
    def test_mcp_uses_the_forwarded_client_not_httpx(self) -> None:
        via, client, ua = usage_log.request_origin(
            _request(
                {
                    "X-OpenArg-Via": "mcp",
                    "X-OpenArg-Client": "claude-code/2.1.0",
                    "User-Agent": "python-httpx/0.28.1",
                }
            )
        )
        assert (via, client, ua) == ("mcp", "claude-code", "claude-code/2.1.0")

    def test_direct_api_uses_the_user_agent(self) -> None:
        assert usage_log.request_origin(_request({"User-Agent": "curl/8.7.1"})) == (
            "api",
            "curl",
            "curl/8.7.1",
        )

    def test_nothing_known(self) -> None:
        assert usage_log.request_origin(_request()) == ("api", None, None)


class TestLogUsage:
    @pytest.mark.asyncio
    async def test_data_mode_never_stores_the_query(self) -> None:
        repo = AsyncMock()
        await usage_log.log_usage(
            repo,
            _key(),
            _request(),
            endpoint="/api/v1/catalogo/buscar",
            mode="datos",
            tool="buscar_datasets",
            status_code=200,
            question="lo que buscó alguien",
        )
        row = _recorded(repo)
        assert row.question is None
        assert (row.mode, row.tool, row.via) == ("datos", "buscar_datasets", "api")

    @pytest.mark.asyncio
    async def test_answers_mode_keeps_200_chars(self) -> None:
        repo = AsyncMock()
        await usage_log.log_usage(
            repo,
            _key(),
            _request(),
            endpoint="/api/v1/ask",
            mode="respuestas",
            tool="consultar_datos_publicos",
            status_code=200,
            question="x" * 500,
        )
        assert _recorded(repo).question == "x" * 200

    @pytest.mark.asyncio
    async def test_an_answer_keeps_its_model_and_cost(self) -> None:
        repo = AsyncMock()
        await usage_log.log_usage(
            repo,
            _key(),
            _request(),
            endpoint="/api/v1/ask",
            mode="respuestas",
            tool="consultar_datos_publicos",
            status_code=200,
            question="¿Inflación de agosto?",
            model="us.anthropic.claude-sonnet-4-6",
            cost_usd=0.0412,
        )
        row = _recorded(repo)
        assert (row.model, row.cost_usd) == ("us.anthropic.claude-sonnet-4-6", 0.0412)

    @pytest.mark.asyncio
    async def test_without_a_model_both_stay_empty(self) -> None:
        repo = AsyncMock()
        await usage_log.log_usage(
            repo,
            _key(),
            _request(),
            endpoint="/api/v1/ask",
            mode="respuestas",
            tool="consultar_datos_publicos",
            status_code=200,
            model="",
        )
        row = _recorded(repo)
        assert (row.model, row.cost_usd) == (None, None)

    @pytest.mark.asyncio
    async def test_a_database_failure_does_not_break_the_response(self) -> None:
        repo = AsyncMock()
        repo.record_usage.side_effect = RuntimeError("db down")
        await usage_log.log_usage(
            repo,
            _key(),
            _request(),
            endpoint="/api/v1/ask",
            mode="respuestas",
            tool="consultar_datos_publicos",
            status_code=200,
        )


class TestTrackUsage:
    @pytest.mark.asyncio
    async def test_success_is_logged_and_updates_last_used(self) -> None:
        repo, key = AsyncMock(), _key()
        async with usage_log.track_usage(
            repo, key, _request(), endpoint="/api/v1/fuentes", tool="listar_fuentes"
        ):
            pass
        row = _recorded(repo)
        assert (row.status_code, row.mode, row.question) == (200, "datos", None)
        repo.update_last_used.assert_awaited_once_with(key.id)

    @pytest.mark.asyncio
    async def test_the_http_error_status_is_logged_and_reraised(self) -> None:
        repo = AsyncMock()
        with pytest.raises(HTTPException):
            async with usage_log.track_usage(
                repo, _key(), _request(), endpoint="/api/v1/catalogo/tabla", tool="describir_tabla"
            ):
                raise HTTPException(status_code=404, detail="no existe")
        assert _recorded(repo).status_code == 404
        repo.update_last_used.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unexpected_error_is_logged_as_500(self) -> None:
        repo = AsyncMock()
        with pytest.raises(ValueError):
            async with usage_log.track_usage(
                repo, _key(), _request(), endpoint="/api/v1/catalogo/datos", tool="obtener_datos"
            ):
                raise ValueError("boom")
        assert _recorded(repo).status_code == 500


class TestLogRejection:
    @pytest.mark.asyncio
    async def test_at_most_one_per_key_mode_and_minute(self) -> None:
        repo, cache, key = AsyncMock(), FakeCache(), _key()
        for _ in range(50):
            await usage_log.log_rejection(
                repo,
                cache,  # type: ignore[arg-type]
                key,
                _request(),
                endpoint="/api/v1/catalogo/buscar",
                mode="datos",
                tool="buscar_datasets",
                status_code=429,
            )
        assert _recorded(repo).status_code == 429

    @pytest.mark.asyncio
    async def test_modes_are_counted_apart(self) -> None:
        repo, cache, key = AsyncMock(), FakeCache(), _key()
        for mode in ("datos", "respuestas"):
            await usage_log.log_rejection(
                repo,
                cache,  # type: ignore[arg-type]
                key,
                _request(),
                endpoint="/x",
                mode=mode,
                tool="t",
                status_code=429,
            )
        assert repo.record_usage.await_count == 2

    @pytest.mark.asyncio
    async def test_without_redis_nothing_is_logged(self) -> None:
        repo, cache = AsyncMock(), AsyncMock()
        cache.increment_with_ttl.side_effect = ConnectionError("redis down")
        await usage_log.log_rejection(
            repo,
            cache,
            _key(),
            _request(),
            endpoint="/api/v1/ask",
            mode="respuestas",
            tool="consultar_datos_publicos",
            status_code=503,
        )
        repo.record_usage.assert_not_awaited()
