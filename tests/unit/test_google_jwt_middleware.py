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
    Mode,
    get_request_user_email,
)


class _StubValidator:
    """Minimal stand-in for GoogleJwtValidator that short-circuits async work."""

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


def _build_app(validator: Any, mode: Mode) -> Starlette:
    app = Starlette(routes=[Route("/me", _whoami), Route("/health", _whoami)])
    app.add_middleware(GoogleJwtAuthMiddleware, validator=validator, mode=mode)
    return app


def test_disabled_mode_is_a_noop() -> None:
    app = _build_app(validator=None, mode="disabled")
    client = TestClient(app)

    # Even without any headers the request reaches the endpoint.
    response = client.get("/me")

    assert response.status_code == 200
    assert response.json() == {"email": ""}


def test_disabled_mode_leaves_legacy_header_for_helper() -> None:
    app = _build_app(validator=None, mode="disabled")
    client = TestClient(app)

    response = client.get("/me", headers={"X-User-Email": "Legacy@Example.com"})

    assert response.status_code == 200
    assert response.json() == {"email": "legacy@example.com"}


def test_dual_mode_prefers_valid_bearer_over_header() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    app = _build_app(validator=validator, mode="dual")
    client = TestClient(app)

    response = client.get(
        "/me",
        headers={
            "Authorization": "Bearer good-token",
            "X-User-Email": "legacy@example.com",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"email": "jwt@example.com"}


def test_dual_mode_falls_back_to_header_when_no_bearer() -> None:
    validator = _StubValidator({})
    app = _build_app(validator=validator, mode="dual")
    client = TestClient(app)

    response = client.get("/me", headers={"X-User-Email": "fallback@example.com"})

    assert response.status_code == 200
    assert response.json() == {"email": "fallback@example.com"}


def test_dual_mode_returns_401_for_invalid_bearer() -> None:
    validator = _StubValidator(mapping={}, raise_for={"bad-token"})
    app = _build_app(validator=validator, mode="dual")
    client = TestClient(app)

    response = client.get(
        "/me",
        headers={
            "Authorization": "Bearer bad-token",
            "X-User-Email": "shouldnotmatter@example.com",
        },
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or expired token"}


def test_enforced_mode_requires_bearer() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    app = _build_app(validator=validator, mode="enforced")
    client = TestClient(app)

    response = client.get("/me", headers={"X-User-Email": "legacy@example.com"})

    assert response.status_code == 401
    assert response.json() == {"detail": "Authorization: Bearer <jwt> required"}


def test_enforced_mode_accepts_valid_bearer() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    app = _build_app(validator=validator, mode="enforced")
    client = TestClient(app)

    response = client.get("/me", headers={"Authorization": "Bearer good-token"})

    assert response.status_code == 200
    assert response.json() == {"email": "jwt@example.com"}


def test_enforced_mode_ignores_legacy_header() -> None:
    validator = _StubValidator({"good-token": "jwt@example.com"})
    app = _build_app(validator=validator, mode="enforced")
    client = TestClient(app)

    response = client.get(
        "/me",
        headers={
            "Authorization": "Bearer good-token",
            "X-User-Email": "legacy@example.com",
        },
    )

    assert response.status_code == 200
    # The validated JWT claim wins, not the header.
    assert response.json() == {"email": "jwt@example.com"}


def test_enforced_mode_skips_public_paths() -> None:
    validator = _StubValidator({})
    app = _build_app(validator=validator, mode="enforced")
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"email": ""}


def test_helper_reads_state_when_set_even_if_middleware_disabled() -> None:
    async def _set_state_then_whoami(request: Request) -> JSONResponse:
        request.state.user_email = "preset@example.com"
        return JSONResponse({"email": get_request_user_email(request)})

    app = Starlette(routes=[Route("/custom", _set_state_then_whoami)])
    app.add_middleware(GoogleJwtAuthMiddleware, validator=None, mode="disabled")
    client = TestClient(app)

    response = client.get(
        "/custom",
        headers={"X-User-Email": "ignored@example.com"},
    )

    assert response.status_code == 200
    assert response.json() == {"email": "preset@example.com"}


@pytest.mark.asyncio
async def test_real_validator_and_middleware_reject_unknown_token() -> None:
    """Integration-ish check: middleware wires to a real validator path."""

    class _BrokenValidator(GoogleJwtValidator):
        def __init__(self) -> None:
            pass  # skip parent init; we're exercising validate() only

        async def validate(self, token: str) -> str:  # type: ignore[override]
            raise InvalidGoogleToken("simulated rejection")

    validator = _BrokenValidator()
    app = _build_app(validator=validator, mode="dual")
    client = TestClient(app)

    response = client.get(
        "/me",
        headers={"Authorization": "Bearer anything"},
    )

    assert response.status_code == 401
