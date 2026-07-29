"""Whatever a router asks the container for has to be registered.

`/ws/smart` validates the Google JWT itself, because WebSockets bypass
Starlette middleware and the HTTP path's `GoogleJwtAuthMiddleware` never runs
for them. It resolved the validator from the DI container — which never had a
provider for it. `app_factory` builds one directly for the middleware, so
nothing in the HTTP path noticed.

Effect: every WebSocket handshake carrying an `id_token` — that is, every
logged-in user — died on `NoFactoryError`, and the UI showed "Internal error /
Respuesta parcial". Staging was in that state from 2026-06-10 until
2026-07-29, when someone tried the chat by hand. No test failed, because no
test connected the two halves: the router asks for one key, the registry
declares another, and nothing compared them.

Asserted against source text rather than by importing the modules, so the
wiring is checked even where PyJWT and dishka are absent — which is exactly
the environment that let this through.

`X` and `X | None` are separate container keys. Verified against the installed
dishka on staging, 2026-07-29: registering `Thing | None` and requesting
`Thing` raises NoFactoryError. That mismatch is the outage.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[2] / "src" / "app"
_REGISTRY = _SRC / "setup" / "ioc" / "provider_registry.py"
_APP_FACTORY = _SRC / "setup" / "app_factory.py"
_WS_ROUTER = _SRC / "presentation" / "http" / "controllers" / "query" / "smart_query_v2_router.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestValidatorIsRegistered:
    def test_a_provider_declares_the_validator(self) -> None:
        source = _read(_REGISTRY)
        assert "class AuthProvider" in source, (
            "no provider supplies GoogleJwtValidator — /ws/smart cannot resolve it"
        )
        assert "GoogleJwtValidator | None" in source

    def test_the_provider_is_wired_into_the_container(self) -> None:
        """Declaring it is not enough; it has to be in `get_providers()`."""
        source = _read(_REGISTRY)
        providers_block = source.split("def get_providers()")[1]
        assert "AuthProvider()" in providers_block

    def test_an_unset_client_id_yields_none_rather_than_raising(self) -> None:
        """The unset case is legitimate outside prod.

        `GoogleJwtValidator.__init__` rejects an empty client id, so a provider
        that built one unconditionally would break container startup on every
        local and CI run — where `app_factory` deliberately skips the HTTP
        middleware for the same reason.
        """
        source = _read(_REGISTRY)
        provider = source.split("class AuthProvider")[1].split("def get_providers")[0]
        assert "if not client_id:" in provider
        assert "return None" in provider


class TestRouterAsksForTheRegisteredKey:
    def test_the_socket_requests_the_optional_key(self) -> None:
        assert re.search(r"get\(\s*GoogleJwtValidator\s*\|\s*None\s*\)", _read(_WS_ROUTER)), (
            "the socket must request `GoogleJwtValidator | None`, the key the registry declares"
        )

    def test_the_socket_never_requests_the_bare_class(self) -> None:
        assert not re.search(r"get\(\s*GoogleJwtValidator\s*\)", _read(_WS_ROUTER)), (
            "requesting the bare class raises NoFactoryError: it is a different key"
        )

    def test_a_missing_validator_does_not_verify_anything(self) -> None:
        """None means "cannot verify", never "verified".

        The token grants conversation_id access, so treating an unverifiable
        one as valid would hand that access to anyone holding the service key.
        """
        source = _read(_WS_ROUTER)
        block = source.split("raw_id_token = ")[1].split("body_email = ")[0]
        assert "if validator is None:" in block
        assert "if validator is not None:" in block


class TestOneValidatorPerProcess:
    """The validator caches JWKS keys and asks to be shared per process.

    The middleware and the DI provider cannot reach each other — middleware is
    registered while the app is configured, before the container exists — so
    both go through a cached factory instead.
    """

    def test_app_factory_uses_the_shared_factory(self) -> None:
        source = _read(_APP_FACTORY)
        assert "build_google_jwt_validator(client_id)" in source
        assert "GoogleJwtValidator(client_id=client_id)" not in source, (
            "constructing directly gives the middleware its own JWKS cache"
        )

    def test_the_provider_uses_the_shared_factory(self) -> None:
        assert "build_google_jwt_validator(client_id)" in _read(_REGISTRY)

    def test_the_factory_is_cached(self) -> None:
        source = _read(_SRC / "infrastructure" / "auth" / "google_jwt_validator.py")
        block = source.split("def build_google_jwt_validator")[0]
        assert "lru_cache" in block or "functools.cache" in block

    def test_the_same_client_id_yields_the_same_instance(self) -> None:
        """The behavioural half — needs PyJWT, absent in some environments."""
        pytest.importorskip("jwt")
        from app.infrastructure.auth import build_google_jwt_validator

        assert build_google_jwt_validator("abc") is build_google_jwt_validator("abc")
        assert build_google_jwt_validator("abc") is not build_google_jwt_validator("xyz")
