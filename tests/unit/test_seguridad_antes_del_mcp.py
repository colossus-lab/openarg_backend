"""Cierres de seguridad previos a abrir la API pública (MCP de datos públicos).

- `/users/sync` no puede crear ni pisar el usuario de otro email.
- Caddy bloquea `/api/v1/data/*` desde internet (hallazgo del pentest).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from app.domain.entities.user.user import User
from app.domain.ports.user.user_repository import IUserRepository
from app.presentation.http.controllers.users.users_router import router as users_router

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def repo() -> AsyncMock:
    repo = AsyncMock(spec=IUserRepository)

    async def _upsert(user: User) -> User:
        user.created_at = datetime.now(UTC)
        return user

    repo.upsert_by_email.side_effect = _upsert
    return repo


def _app(repo: AsyncMock, authed_email: str) -> FastAPI:
    class TestProvider(Provider):
        scope = Scope.REQUEST

        @provide
        def users(self) -> IUserRepository:
            return repo

    app = FastAPI()

    @app.middleware("http")
    async def _fake_google_jwt(request: Request, call_next):  # type: ignore[no-untyped-def]
        # Lo que hace GoogleJwtAuthMiddleware con el claim `email` validado.
        request.state.user_email = authed_email
        return await call_next(request)

    app.include_router(users_router)
    setup_dishka(container=make_async_container(TestProvider()), app=app)
    return app


async def _sync(app: FastAPI, email: str) -> int:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/users/sync", json={"email": email, "name": "x"})
    return r.status_code


class TestUsersSync:
    async def test_own_email_is_synced(self, repo: AsyncMock) -> None:
        assert await _sync(_app(repo, "ana@gmail.com"), "ana@gmail.com") == 200
        repo.upsert_by_email.assert_awaited_once()

    async def test_email_comparison_ignores_case(self, repo: AsyncMock) -> None:
        assert await _sync(_app(repo, "Ana@Gmail.com"), "ana@gmail.com") == 200

    async def test_someone_elses_email_is_403_and_nothing_is_written(self, repo: AsyncMock) -> None:
        assert await _sync(_app(repo, "ana@gmail.com"), "victima@gmail.com") == 403
        repo.upsert_by_email.assert_not_awaited()


class TestCaddy:
    def test_data_endpoints_are_blocked_from_the_internet(self) -> None:
        caddyfile = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
        blocked = next(line for line in caddyfile.splitlines() if "@blocked path" in line)
        assert "/api/v1/data/*" in blocked
        assert "/api/v1/admin/*" in blocked
