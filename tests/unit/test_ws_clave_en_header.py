"""La clave de servicio del WebSocket viaja en un header, no en la URL.

El servidor de Next.js abría `/api/v1/query/ws/smart?api_key=<clave>` y
uvicorn imprime la ruta con su query string en la línea
`"WebSocket /ruta?query" [accepted]`. En prod esa línea dejaba la clave de
servicio frontend→backend (`BACKEND_API_KEY`) en texto plano en los logs.

El arreglo: el handshake acepta la clave en `X-API-Key`, como el POST
`/smart`. El query param sigue andando mientras el frontend viejo esté
desplegado (el backend se despliega primero), pero avisa una vez por proceso
que está deprecado. Un PR posterior lo saca.

Los tests conectan un WebSocket de verdad contra el router con un contenedor
falso. La autorización se observa por lo que contesta el socket a un mensaje
sin pregunta: autorizado llega a "question is required"; no autorizado corta
con 4401 antes.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.domain.ports.cache.cache_port import ICacheService
from app.presentation.http.controllers.query import smart_query_v2_router as mod

_CLAVE = "clave-de-servicio-de-prueba"
_RUTA = "/query/ws/smart"
_LOGGER = mod.logger.name


class _ScopeFalso:
    """Imita lo justo de dishka: `container()` y `scope()` son context managers."""

    def __init__(self, objetos: dict[Any, Any]) -> None:
        self._objetos = objetos

    def __call__(self) -> _ScopeFalso:
        return self

    async def __aenter__(self) -> _ScopeFalso:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def get(self, clave: Any) -> Any:
        return self._objetos.get(clave)


@pytest.fixture
def cliente(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("BACKEND_API_KEY", _CLAVE)
    monkeypatch.setattr(mod, "_ws_query_key_warned", False, raising=False)

    async def _motor_falso(_deps: Any) -> object:
        return object()

    monkeypatch.setattr(mod, "_answer_engine", _motor_falso)

    cache = AsyncMock()
    cache.increment_with_ttl = AsyncMock(return_value=1)
    app = FastAPI()
    app.include_router(mod.router)
    app.state.dishka_container = _ScopeFalso({ICacheService: cache})
    return TestClient(app)


def _primer_mensaje(
    cliente: TestClient,
    *,
    url: str = _RUTA,
    headers: dict[str, Any] | None = None,
    cuerpo: str = "{}",
) -> dict[str, Any]:
    with cliente.websocket_connect(url, headers=headers or {}) as ws:
        ws.send_text(cuerpo)
        mensaje: dict[str, Any] = ws.receive_json()
        return mensaje


_AUTORIZADO = "question is required"
_RECHAZADO = "Invalid or missing API key"


class TestClaveEnElHeader:
    def test_la_clave_en_x_api_key_autoriza_sin_query_string(self, cliente: TestClient) -> None:
        mensaje = _primer_mensaje(cliente, headers={"X-API-Key": _CLAVE})
        assert mensaje["message"] == _AUTORIZADO

    def test_una_clave_equivocada_en_el_header_no_autoriza(self, cliente: TestClient) -> None:
        mensaje = _primer_mensaje(cliente, headers={"X-API-Key": "otra-clave"})
        assert mensaje["message"] == _RECHAZADO

    def test_sin_clave_no_autoriza(self, cliente: TestClient) -> None:
        assert _primer_mensaje(cliente)["message"] == _RECHAZADO

    def test_la_clave_por_header_no_dispara_el_aviso_de_deprecado(
        self, cliente: TestClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            _primer_mensaje(cliente, headers={"X-API-Key": _CLAVE})
        assert not [r for r in caplog.records if "deprecad" in r.getMessage()]

    def test_una_clave_no_ascii_se_rechaza_en_vez_de_reventar(self, cliente: TestClient) -> None:
        """`compare_digest` con un str no ASCII levanta TypeError.

        Antes eso caía en el `except Exception` del handler: "Internal error"
        con traceback en el log, en vez de un 4401 limpio.
        """
        mensaje = _primer_mensaje(cliente, headers={"X-API-Key": "clavé".encode("latin-1")})
        assert mensaje["message"] == _RECHAZADO


class TestQueryParamDeprecado:
    """Compatibilidad: el frontend viejo sigue mandando `?api_key=`."""

    def test_el_query_param_sigue_autorizando(self, cliente: TestClient) -> None:
        mensaje = _primer_mensaje(cliente, url=f"{_RUTA}?api_key={_CLAVE}")
        assert mensaje["message"] == _AUTORIZADO

    def test_avisa_una_sola_vez_por_proceso_y_sin_la_clave(
        self, cliente: TestClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            for _ in range(3):
                _primer_mensaje(cliente, url=f"{_RUTA}?api_key={_CLAVE}")
        avisos = [r for r in caplog.records if "deprecad" in r.getMessage()]
        assert len(avisos) == 1
        assert avisos[0].levelno == logging.WARNING
        assert _CLAVE not in avisos[0].getMessage()
        assert "X-API-Key" in avisos[0].getMessage()

    def test_un_query_param_equivocado_no_autoriza(self, cliente: TestClient) -> None:
        mensaje = _primer_mensaje(cliente, url=f"{_RUTA}?api_key=otra")
        assert mensaje["message"] == _RECHAZADO

    def test_un_query_param_no_ascii_se_rechaza_en_vez_de_reventar(
        self, cliente: TestClient
    ) -> None:
        mensaje = _primer_mensaje(cliente, url=f"{_RUTA}?api_key=clav%C3%A9")
        assert mensaje["message"] == _RECHAZADO


class TestLoQueYaAndaba:
    def test_la_clave_en_el_primer_mensaje_sigue_autorizando(self, cliente: TestClient) -> None:
        mensaje = _primer_mensaje(cliente, cuerpo=f'{{"api_key": "{_CLAVE}"}}')
        assert mensaje["message"] == _AUTORIZADO

    def test_sin_backend_api_key_no_se_exige_clave(
        self, cliente: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("BACKEND_API_KEY")
        assert _primer_mensaje(cliente)["message"] == _AUTORIZADO
