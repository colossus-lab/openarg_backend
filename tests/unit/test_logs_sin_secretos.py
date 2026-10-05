"""Los logs no imprimen credenciales que viajan en el query string de una URL.

En prod, la línea `INFO: <ip> - "WebSocket /api/v1/query/ws/smart?api_key=..."
[accepted]` dejaba la clave de servicio frontend→backend en texto plano.

Esa línea NO sale por `uvicorn.access`: los tres protocolos de WebSocket de
uvicorn la emiten por `uvicorn.error`, que propaga al handler propio del
logger `uvicorn` (formato `INFO:     ...`, por stderr). `uvicorn.access` ni
siquiera imprime en prod: `setup_logging` lo baja a WARNING. Un filtro puesto
sólo en `uvicorn.access` no habría tapado nada.

El test de punta a punta levanta uvicorn de verdad, por su CLI, con la misma
secuencia que prod: uvicorn aplica su dictConfig, después llama a la factory,
y la factory llama a `setup_logging` (como `app.run.make_app`). Mide lo que el
proceso imprime, no un mock del logger.
"""

from __future__ import annotations

import logging
import logging.config
import os
import signal
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest

from app.setup.logging_config import (
    UrlSecretRedactionFilter,
    install_url_secret_redaction,
    redact_url_secrets,
)

_SRC = Path(__file__).resolve().parents[2] / "src"


class TestRedactUrlSecrets:
    @pytest.mark.parametrize(
        ("texto", "esperado"),
        [
            (
                '"WebSocket /api/v1/query/ws/smart?api_key=s3cr3t" [accepted]',
                '"WebSocket /api/v1/query/ws/smart?api_key=[REDACTED]" [accepted]',
            ),
            ("/x?portal=caba&token=abc&n=3", "/x?portal=caba&token=[REDACTED]&n=3"),
            ("/x?key=abc", "/x?key=[REDACTED]"),
            (
                "/x?apikey=a&api-key=b&access_token=c",
                "/x?apikey=[REDACTED]&api-key=[REDACTED]&access_token=[REDACTED]",
            ),
            ("/x?API_KEY=abc", "/x?API_KEY=[REDACTED]"),
            ("/x?client_secret=a&password=b", "/x?client_secret=[REDACTED]&password=[REDACTED]"),
        ],
    )
    def test_tapa_el_valor_y_deja_el_nombre(self, texto: str, esperado: str) -> None:
        assert redact_url_secrets(texto) == esperado

    @pytest.mark.parametrize(
        "texto",
        [
            "/api/v1/query/ws/smart",
            "/x?portal=caba&limit=10",
            "la api_key es obligatoria",  # no es una URL: no hay ? ni & antes
            "monkey=banana",
        ],
    )
    def test_no_toca_lo_que_no_es_una_credencial_en_una_url(self, texto: str) -> None:
        assert redact_url_secrets(texto) == texto


def _record(msg: str, args: object) -> logging.LogRecord:
    return logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, msg, args, None)  # type: ignore[arg-type]


class TestElFiltro:
    def test_respeta_la_tupla_de_args_del_access_log(self) -> None:
        """`AccessFormatter` desarma `record.args` en exactamente 5 valores.

        Colapsar el mensaje en `msg` y vaciar `args` rompería el formatter.
        """
        rec = _record(
            '%s - "%s %s HTTP/%s" %d',
            ("1.2.3.4:5", "GET", "/ping?api_key=s3cr3t&portal=caba", "1.1", 200),
        )
        assert UrlSecretRedactionFilter().filter(rec)
        assert rec.args == ("1.2.3.4:5", "GET", "/ping?api_key=[REDACTED]&portal=caba", "1.1", 200)

    def test_tapa_tambien_el_mensaje_ya_armado(self) -> None:
        rec = _record("fallo GET /x?token=abc", ())
        UrlSecretRedactionFilter().filter(rec)
        assert rec.getMessage() == "fallo GET /x?token=[REDACTED]"

    def test_un_placeholder_en_el_lugar_de_la_credencial_no_rompe_el_formateo(self) -> None:
        """Tapar `token=%s` en el format string se comería el `%s`."""
        rec = _record("GET /x?token=%s&n=%d", ("abc", 3))
        UrlSecretRedactionFilter().filter(rec)
        assert rec.getMessage() == "GET /x?token=[REDACTED]&n=3"

    def test_tapa_una_excepcion_pasada_como_argumento(self) -> None:
        """httpx arma el mensaje de sus errores con la URL completa."""
        rec = _record("falló: %s", (RuntimeError("401 for url 'https://x/y?key=abc'"),))
        UrlSecretRedactionFilter().filter(rec)
        assert "abc" not in rec.getMessage()
        assert "key=[REDACTED]" in rec.getMessage()

    def test_args_como_diccionario(self) -> None:
        rec = _record("%(url)s", ({"url": "/x?api_key=abc"},))
        UrlSecretRedactionFilter().filter(rec)
        assert rec.getMessage() == "/x?api_key=[REDACTED]"


@pytest.fixture
def logging_aislado() -> Iterator[None]:
    """Deja el logging global como estaba: otros tests dependen de él."""
    nombres = ["uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi", "httpx"]
    root = logging.getLogger()
    antes_root = (root.handlers[:], root.filters[:], root.level)
    antes = {
        n: (lg.handlers[:], lg.filters[:], lg.level, lg.propagate)
        for n in nombres
        for lg in [logging.getLogger(n)]
    }
    try:
        yield
    finally:
        root.handlers[:], root.filters[:] = antes_root[0], antes_root[1]
        root.setLevel(antes_root[2])
        for n, (handlers, filters, level, propagate) in antes.items():
            lg = logging.getLogger(n)
            lg.handlers[:], lg.filters[:] = handlers, filters
            lg.setLevel(level)
            lg.propagate = propagate


class TestInstalacion:
    def test_queda_en_los_loggers_que_imprimen_urls_y_en_sus_handlers(
        self, logging_aislado: None
    ) -> None:
        from uvicorn.config import LOGGING_CONFIG

        logging.config.dictConfig(LOGGING_CONFIG)
        install_url_secret_redaction()

        for nombre in ("uvicorn.error", "uvicorn.access"):
            lg = logging.getLogger(nombre)
            assert any(isinstance(f, UrlSecretRedactionFilter) for f in lg.filters), nombre
        for nombre in ("uvicorn", "uvicorn.access"):
            for h in logging.getLogger(nombre).handlers:
                assert any(isinstance(f, UrlSecretRedactionFilter) for f in h.filters), nombre

    def test_instalar_dos_veces_no_duplica(self, logging_aislado: None) -> None:
        install_url_secret_redaction()
        install_url_secret_redaction()
        lg = logging.getLogger("uvicorn.error")
        assert sum(isinstance(f, UrlSecretRedactionFilter) for f in lg.filters) == 1

    def test_setup_logging_lo_instala(self, logging_aislado: None) -> None:
        from app.setup.logging_config import setup_logging

        setup_logging("INFO")
        lg = logging.getLogger("uvicorn.error")
        assert any(isinstance(f, UrlSecretRedactionFilter) for f in lg.filters)
        for h in logging.getLogger().handlers:
            assert any(isinstance(f, UrlSecretRedactionFilter) for f in h.filters)

    def test_make_app_configura_el_logging(self) -> None:
        """El de punta a punta usa una factory mínima; la real hace lo mismo."""
        fuente = (_SRC / "app" / "run.py").read_text(encoding="utf-8")
        assert "setup_logging(" in fuente.split("def make_app")[1]


# ── De punta a punta: uvicorn de verdad ──────────────────────────────────

_APP = textwrap.dedent(
    """
    import logging
    import os

    from fastapi import FastAPI, WebSocket

    from app.setup.logging_config import setup_logging


    def make_app():
        # Mismo orden que app.run.make_app: uvicorn ya aplicó su dictConfig.
        setup_logging("INFO")
        if os.environ.get("REENABLE_ACCESS_LOG"):
            # En prod está en WARNING; acá se prende para probar que el filtro
            # también cubre el access log si alguien lo vuelve a encender.
            logging.getLogger("uvicorn.access").setLevel(logging.INFO)
        app = FastAPI()

        @app.websocket("/api/v1/query/ws/smart")
        async def ws_smart(ws: WebSocket) -> None:
            await ws.accept()
            await ws.send_text("ok")
            await ws.close()

        @app.get("/ping")
        async def ping() -> dict[str, bool]:
            return {"ok": True}

        return app
    """
)

_SECRETO_WS = "clave-ws-que-no-tiene-que-aparecer"
_SECRETO_HTTP = "clave-http-que-no-tiene-que-aparecer"


def _puerto_libre() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def _esperar_puerto(port: int, proc: subprocess.Popen[bytes], timeout: float = 30) -> None:
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        if proc.poll() is not None:
            raise AssertionError("uvicorn terminó antes de escuchar")
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise AssertionError("uvicorn no empezó a escuchar a tiempo")


def _correr_uvicorn(tmp_path: Path, extra_args: list[str]) -> str:
    """Levanta uvicorn, le pega por WS y por HTTP con claves en la URL, y
    devuelve todo lo que imprimió (stdout + stderr)."""
    pytest.importorskip("websockets")
    from websockets.sync.client import connect

    (tmp_path / "wsclave_app.py").write_text(_APP, encoding="utf-8")
    port = _puerto_libre()
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(tmp_path), str(_SRC), os.environ.get("PYTHONPATH", "")]),
        "PYTHONUNBUFFERED": "1",
        "APP_ENV": "prod",
        "REENABLE_ACCESS_LOG": "1",
    }
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "uvicorn",
            "wsclave_app:make_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            *extra_args,
        ],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    error: Exception | None = None
    try:
        _esperar_puerto(port, proc)
        url_ws = f"ws://127.0.0.1:{port}/api/v1/query/ws/smart?api_key={_SECRETO_WS}"
        # Con --workers el puerto abre antes de que carguen los workers.
        limite = time.monotonic() + 30
        while True:
            try:
                with connect(url_ws, open_timeout=5) as ws:
                    assert ws.recv(timeout=5) == "ok"
                break
            except (OSError, TimeoutError):
                if time.monotonic() > limite:
                    raise
                time.sleep(0.2)
        url_http = (
            f"http://127.0.0.1:{port}/ping?api_key={_SECRETO_HTTP}&token={_SECRETO_HTTP}"
            f"&key={_SECRETO_HTTP}&portal=caba"
        )
        with urllib.request.urlopen(url_http, timeout=5) as resp:  # noqa: S310
            assert resp.status == 200
        time.sleep(0.5)
    except Exception as exc:  # se re-lanza abajo, con la salida de uvicorn
        error = exc
    finally:
        # Linux: SIGINT apaga a uvicorn y a sus workers. Windows: nada de
        # CTRL_BREAK (con esta consola se escapa del grupo y mata al proceso
        # que corre los tests); terminate() alcanza porque la salida no tiene
        # buffer y el caso con workers no corre ahí.
        if sys.platform == "win32":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGINT)
        try:
            crudo, _ = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            crudo, _ = proc.communicate()
    salida = crudo.decode("utf-8", errors="replace")
    if error is not None:
        raise AssertionError(f"falló el cliente contra uvicorn; salida:\n{salida}") from error
    return salida


@pytest.mark.parametrize(
    "extra_args",
    [
        pytest.param(["--ws", "websockets"], id="ws-websockets"),
        pytest.param(["--ws", "websockets-sansio"], id="ws-websockets-sansio"),
        # Como corre prod: `--workers 2`. Cada worker vuelve a aplicar el
        # dictConfig de uvicorn y recién después carga la app.
        pytest.param(
            ["--workers", "2"],
            id="workers-2",
            # En Windows, terminate() mata al supervisor pero no a los workers,
            # que se quedan con el pipe abierto. CI corre en Linux.
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="workers huérfanos en Windows"
            ),
        ),
    ],
)
def test_uvicorn_no_imprime_la_clave_de_la_url(tmp_path: Path, extra_args: list[str]) -> None:
    salida = _correr_uvicorn(tmp_path, extra_args)

    assert _SECRETO_WS not in salida
    assert _SECRETO_HTTP not in salida
    # Las líneas se imprimieron (si no, "no aparece" no prueba nada) y sólo
    # perdieron el valor de la credencial.
    assert '"WebSocket /api/v1/query/ws/smart?api_key=[REDACTED]" [accepted]' in salida, salida
    assert (
        "GET /ping?api_key=[REDACTED]&token=[REDACTED]&key=[REDACTED]&portal=caba HTTP/1.1"
        in salida
    ), salida
