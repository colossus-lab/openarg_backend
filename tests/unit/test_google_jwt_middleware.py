from __future__ import annotations

from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from app.infrastructure.auth import GoogleJwtValidator, InvalidGoogleToken
from app.presentation.http.middleware.google_jwt_middleware import (
    GoogleJwtAuthMiddleware,
    get_request_user_email,
)


class _StubValidator:
    """Stand-in for GoogleJwtValidator that skips async JWKS work."""

    def __init__(self, mapping: dict[str, str], *, raise_for: set[str] | None = None) -> None:
        self._mapping = mapping
        self._raise_for = raise_for or set()

    async def validate(self, token: str) -> str:
        if token in self._raise_for:
            raise InvalidGoogleToken(f"stub-rejected: {token}")
        if token not in self._mapping:
            raise InvalidGoogleToken(f"unknown stub token: {token}")
        return self._mapping[token]


async def _whoami(request: Request) -> JSONResponse:
    return JSONResponse({"email": get_request_user_email(request)})


def _build_app(validator: Any) -> Starlette:
    app = Starlette(routes=[Route("/me", _whoami), Route("/health", _whoami)])
    app.add_middleware(GoogleJwtAuthMiddleware, validator=validator)
    return app


def test_valid_bearer_populates_request_state() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    client = TestClient(_build_app(validator))

    response = client.get("/me", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert response.json() == {"email": "jwt@example.com"}


def test_missing_authorization_header_returns_401() -> None:
    validator = _StubValidator({})
    client = TestClient(_build_app(validator))

    response = client.get("/me")

    assert response.status_code == 401
    assert response.json() == {"detail": "Authorization: Bearer <jwt> required"}


def test_legacy_x_user_email_header_is_ignored() -> None:
    validator = _StubValidator({})
    client = TestClient(_build_app(validator))

    response = client.get("/me", headers={"X-User-Email": "legacy@example.com"})

    assert response.status_code == 401


def test_malformed_authorization_header_returns_401() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    client = TestClient(_build_app(validator))

    # Missing "Bearer " prefix
    response = client.get("/me", headers={"Authorization": "good-token"})

    assert response.status_code == 401


def test_invalid_token_returns_401() -> None:
    validator = _StubValidator(mapping={}, raise_for={"bad-token"})
    client = TestClient(_build_app(validator))

    response = client.get("/me", headers={"Authorization": "Bearer bad-token"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or expired token"}


def test_public_path_skips_validation() -> None:
    validator = _StubValidator({})
    client = TestClient(_build_app(validator))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"email": ""}


def test_helper_returns_empty_when_state_not_set() -> None:
    async def _bare_whoami(request: Request) -> JSONResponse:
        return JSONResponse({"email": get_request_user_email(request)})

    app = Starlette(routes=[Route("/bare", _bare_whoami)])
    client = TestClient(app)

    response = client.get("/bare")

    assert response.status_code == 200
    assert response.json() == {"email": ""}


@pytest.mark.asyncio
async def test_real_validator_path_rejects_bad_token() -> None:
    class _BrokenValidator(GoogleJwtValidator):
        def __init__(self) -> None:
            pass  # bypass parent init; exercising validate() only

        async def validate(self, token: str) -> str:  # type: ignore[override]
            raise InvalidGoogleToken("simulated rejection")

    client = TestClient(_build_app(_BrokenValidator()))

    response = client.get("/me", headers={"Authorization": "Bearer anything"})

    assert response.status_code == 401
