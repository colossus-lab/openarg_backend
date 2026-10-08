"""La API pública (/ask, /fuentes, /catalogo/*) llega a su router con la clave del usuario.

Esas rutas hacen su propia auth con `Authorization: Bearer oarg_sk_…`, así que
los dos middlewares de auth las tienen que dejar pasar sin `X-API-Key` y sin
JWT de Google. Una ruta que falta en `middleware/public_paths.py` anda en los
tests del router (lo montan sin middlewares) y en local, pero en staging y prod, con
`BACKEND_API_KEY` y `GOOGLE_OAUTH_CLIENT_ID` puestos, el middleware la corta con
401 antes de llegar al router. Le pasó a `/catalogo/agregar`, la ruta de la
herramienta `agregar_datos` del MCP.

Las rutas salen del router real, no de una lista escrita acá: una ruta nueva en
`public_api/` entra sola en estos tests.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from dishka import Provider, Scope, make_async_container
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from fastapi.routing import APIRoute
from httpx import ASGITransport, AsyncClient
from mcp_publico import core
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from app.application.pipeline.nodes import PipelineDeps
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.domain.ports.search.vector_search import IVectorSearch
from app.infrastructure.auth import InvalidGoogleToken
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.presentation.http.controllers.root_router import create_root_router
from app.presentation.http.middleware.auth_middleware import APIKeyMiddleware
from app.presentation.http.middleware.google_jwt_middleware import GoogleJwtAuthMiddleware
from app.presentation.http.middleware.public_paths import PUBLIC_API_PATHS
from app.setup.app_factory import configure_app, create_app
from app.setup.config.settings import AppSettings, SecuritySettings

_SERVICE_KEY = "clave-de-servicio-del-frontend"
# Lo mismo que manda el MCP público, con una clave de usuario que no existe.
_MCP_HEADERS = core.backend_headers("oarg_sk_" + "a" * 40, "203.0.113.7", "pytest")
# Lo que contestan los middlewares cuando cortan un pedido.
_MIDDLEWARE_401 = {
    "Invalid or missing API key",
    "Authorization: Bearer <jwt> required",
    "Invalid or expired token",
}
# El 401 de `verify_api_key`: el pedido llegó al router y la clave no está en la base.
_ROUTER_401 = "Invalid or unauthorized API key"


def _public_api_routes() -> list[tuple[str, str]]:
    package = "app.presentation.http.controllers.public_api."
    return sorted(
        (method, route.path)
        for route in create_root_router().routes
        if isinstance(route, APIRoute) and route.endpoint.__module__.startswith(package)
        for method in route.methods
    )


PUBLIC_API_ROUTES = _public_api_routes()

# Un pedido válido por ruta, para que pase la validación y llegue al handler.
_VALID_REQUEST: dict[tuple[str, str], dict[str, Any]] = {
    ("GET", "/api/v1/catalogo/buscar"): {"params": {"q": "inflación"}},
    ("GET", "/api/v1/catalogo/tabla"): {"params": {"nombre": "cache_ipc"}},
    ("GET", "/api/v1/fuentes"): {},
    ("POST", "/api/v1/ask"): {"json": {"question": "¿Cuánto fue la inflación?"}},
    ("POST", "/api/v1/catalogo/agregar"): {
        "json": {"tabla": "cache_ipc", "operacion": "suma", "columna": "valor"}
    },
    ("POST", "/api/v1/catalogo/datos"): {"json": {"tabla": "cache_ipc"}},
}


class _NotAGoogleJwt:
    """Lo que hace el validador real con una `oarg_sk_`: no es un JWT."""

    async def validate(self, token: str) -> str:
        raise InvalidGoogleToken("not a JWT")


async def _reached(request: Request) -> PlainTextResponse:
    return PlainTextResponse("llegó al router")


_MIDDLEWARES: dict[str, tuple[type, dict[str, Any]]] = {
    "APIKeyMiddleware": (APIKeyMiddleware, {"api_key": _SERVICE_KEY}),
    "GoogleJwtAuthMiddleware": (GoogleJwtAuthMiddleware, {"validator": _NotAGoogleJwt()}),
}


async def _send(app: Any, method: str, path: str, **kwargs: Any) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        return await client.request(method, path, **kwargs)


def test_routes_come_from_the_real_routers() -> None:
    # Si falla porque hay una ruta nueva, sumá su pedido válido a _VALID_REQUEST.
    assert PUBLIC_API_ROUTES == sorted(_VALID_REQUEST)


def test_the_shared_list_is_exactly_the_public_api() -> None:
    # Ni una ruta de menos (401 en staging y prod) ni una de más (una ruta
    # borrada que queda pública para lo que se monte después en ese path).
    assert {path for _, path in PUBLIC_API_ROUTES} == PUBLIC_API_PATHS


@pytest.mark.parametrize("middleware", _MIDDLEWARES)
@pytest.mark.parametrize(("method", "path"), PUBLIC_API_ROUTES)
async def test_each_middleware_lets_the_public_api_through(
    middleware: str, method: str, path: str
) -> None:
    cls, kwargs = _MIDDLEWARES[middleware]
    app = Starlette(routes=[Route(path, _reached, methods=[method])])
    app.add_middleware(cls, **kwargs)

    response = await _send(app, method, path, headers=_MCP_HEADERS)

    assert response.status_code == 200, f"{middleware} corta {method} {path}: {response.text}"


@pytest.mark.parametrize("middleware", _MIDDLEWARES)
async def test_each_middleware_still_cuts_a_private_route(middleware: str) -> None:
    # Control: sin esto, el test de arriba pasaría con un middleware que no corta nada.
    cls, kwargs = _MIDDLEWARES[middleware]
    app = Starlette(routes=[Route("/api/v1/datasets/", _reached)])
    app.add_middleware(cls, **kwargs)

    response = await _send(app, "GET", "/api/v1/datasets/", headers=_MCP_HEADERS)

    assert response.status_code == 401


def _mocked_dependencies() -> Provider:
    provider = Provider(scope=Scope.REQUEST)
    api_keys = AsyncMock(spec=IApiKeyRepository)
    api_keys.get_by_key_hash.return_value = None
    provider.provide(lambda: api_keys, provides=IApiKeyRepository)
    for port in (
        ICacheService,
        ICreditRepository,
        IEmbeddingProvider,
        ISQLSandbox,
        IVectorSearch,
        MainAsyncSession,
        PipelineDeps,
    ):
        provider.provide(lambda: AsyncMock(), provides=port)
    return provider


@pytest.fixture
def staging_app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    """La app entera, armada como en staging y prod (los dos middlewares de auth)."""
    monkeypatch.delenv("BACKEND_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_OAUTH_CLIENT_ID", raising=False)
    settings = AppSettings(
        security=SecuritySettings(
            BACKEND_API_KEY=_SERVICE_KEY,
            GOOGLE_OAUTH_CLIENT_ID="pytest.apps.googleusercontent.com",
        )
    )
    app = create_app()
    configure_app(app, create_root_router(), environment="staging", settings=settings)
    setup_dishka(container=make_async_container(_mocked_dependencies()), app=app)
    return app


async def test_staging_app_has_both_middlewares_on(staging_app: FastAPI) -> None:
    # Control: una ruta privada con la clave del usuario la corta APIKeyMiddleware,
    # y con la clave de servicio la corta GoogleJwtAuthMiddleware (validador real:
    # una `oarg_sk_` no es un JWT y se rechaza sin ir a buscar las claves de Google).
    sin_servicio = await _send(staging_app, "GET", "/api/v1/datasets/", headers=_MCP_HEADERS)
    con_servicio = await _send(
        staging_app,
        "GET",
        "/api/v1/datasets/",
        headers={**_MCP_HEADERS, "X-API-Key": _SERVICE_KEY},
    )

    assert (sin_servicio.status_code, sin_servicio.json()["detail"]) == (
        401,
        "Invalid or missing API key",
    )
    assert (con_servicio.status_code, con_servicio.json()["detail"]) == (
        401,
        "Invalid or expired token",
    )


@pytest.mark.parametrize(("method", "path"), sorted(_VALID_REQUEST))
async def test_staging_app_takes_the_mcp_request_to_the_router(
    staging_app: FastAPI, method: str, path: str
) -> None:
    response = await _send(
        staging_app, method, path, headers=_MCP_HEADERS, **_VALID_REQUEST[(method, path)]
    )

    detail = response.json().get("detail")
    assert detail not in _MIDDLEWARE_401, f"un middleware cortó {method} {path}: {detail}"
    # Llegó al handler, que rechaza la clave porque no está en la base.
    assert (response.status_code, detail) == (401, _ROUTER_401)
