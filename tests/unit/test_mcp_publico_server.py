"""El MCP público de punta a punta por HTTP, con el backend falso.

Necesita `mcp` (no es dependencia del proyecto; va en la imagen del MCP),
así que sin él se saltea. Para correrlo:
`uv pip install -r mcp_publico/requirements.txt` y pytest.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("mcp")

import httpx  # noqa: E402
import httpx2  # noqa: E402
from mcp.client import Client  # noqa: E402
from mcp.client.streamable_http import streamable_http_client  # noqa: E402
from mcp_publico import server as mcp_server  # noqa: E402

KEY = "oarg_sk_" + "a" * 43


class FakeBackend:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.status = 200
        self.body: dict[str, Any] = {
            "answer": "La tasa de desempleo fue 7,6 %.",
            "sources": [
                {"name": "EPH", "url": "https://datos.gob.ar/eph", "portal": "datos_gob_ar"}
            ],
            "warnings": [],
            "usage": {"requests_remaining_today": 9, "requests_remaining_month": 9},
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/api/v1/fuentes":
            return httpx.Response(
                200, json={"total_datasets": 10, "fuentes": [{"portal": "caba", "datasets": 10}]}
            )
        if request.url.path.startswith("/api/v1/catalogo/"):
            return self.catalog(request)
        return httpx.Response(self.status, json=self.body)

    catalog_status = 200
    catalog_detail = ""
    catalog_headers: dict[str, str] = {}

    def catalog(self, request: httpx.Request) -> httpx.Response:
        if self.catalog_status != 200:
            return httpx.Response(
                self.catalog_status,
                json={"detail": self.catalog_detail},
                headers=self.catalog_headers,
            )
        path = request.url.path
        if path.endswith("/agregar"):
            return httpx.Response(
                200,
                json={
                    "tabla": "raw.cache_presupuesto_credito_2026",
                    "calculo": "suma de credito_devengado",
                    "agrupado_por": ["jurisdiccion_desc"],
                    "columnas": ["jurisdiccion_desc", "valor", "filas_usadas"],
                    "filas": [
                        {
                            "jurisdiccion_desc": "Ministerio de Capital Humano",
                            "valor": 60622921.86,
                            "filas_usadas": 412,
                        }
                    ],
                    "cantidad": 1,
                    "filas_usadas": 4905,
                    "filas_con_valor": 4905,
                    "truncado": True,
                    "fuente": "Crédito presupuestario 2026",
                    "url": "https://presupuestoabierto.gob.ar/x",
                    "notas": ["Hay más de 1 grupos: se muestran los primeros 1."],
                },
            )
        if path.endswith("/buscar"):
            return httpx.Response(
                200,
                json={
                    "resultados": [
                        {
                            "dataset_id": "d1",
                            "titulo": "Principales tasas de interés",
                            "descripcion": "Tasas diarias",
                            "portal": "datos_gob_ar",
                            "url": "https://infra.datos.gob.ar/x.csv",
                            "tablas": [{"tabla": "raw.tasas", "filas": 8569}],
                        }
                    ]
                },
            )
        if path.endswith("/tabla"):
            return httpx.Response(
                200,
                json={
                    "tabla": "raw.tasas",
                    "titulo": "Principales tasas de interés",
                    "portal": "datos_gob_ar",
                    "url": "https://infra.datos.gob.ar/x.csv",
                    "filas": 8569,
                    "columnas": [
                        {"nombre": "indice_tiempo", "tipo": "text"},
                        {"nombre": "call", "tipo": "double precision"},
                    ],
                    "columna_fecha": "indice_tiempo",
                    "desde": "2003-01-02",
                    "hasta": "2026-06-18",
                    "muestra": [{"indice_tiempo": "2003-01-02", "call": 6.02}],
                    "frescura": {
                        "actualizada": "2026-10-04",
                        "ultimo_dato": "2026-06-18",
                        "serie": True,
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "tabla": "raw.tasas",
                "columnas": ["indice_tiempo", "call"],
                "filas": [
                    {"indice_tiempo": "2026-06-18", "call": 33.14},
                    {"indice_tiempo": "2026-06-17", "call": 33.0},
                ],
                "cantidad": 2,
                "truncado": True,
                "fuente": "Principales tasas de interés",
                "url": "https://infra.datos.gob.ar/x.csv",
            },
        )


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> FakeBackend:
    fake = FakeBackend()
    monkeypatch.setattr(
        mcp_server,
        "_client_factory",
        lambda: httpx.AsyncClient(
            base_url="http://backend", transport=httpx.MockTransport(fake.handler)
        ),
    )
    return fake


async def _session(headers: dict[str, str]):  # type: ignore[no-untyped-def]
    app = mcp_server.create_app()
    http = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app),
        base_url="http://localhost:8000",
        headers={"host": "localhost:8000", **headers},
    )
    return app, http


async def _call(tool: str, args: dict[str, Any], headers: dict[str, str]) -> Any:
    app, http = await _session(headers)
    async with mcp_server.server.session_manager.run(), http:
        transport = streamable_http_client(
            "http://localhost:8000/mcp", http_client=http, terminate_on_close=False
        )
        async with Client(transport) as client:
            if tool == "__list__":
                return await client.list_tools()
            return await client.call_tool(tool, args)


def _text(result: Any) -> str:
    return "\n".join(c.text for c in result.content if getattr(c, "text", None))


async def test_tools_are_listed_without_a_key(backend: FakeBackend) -> None:
    result = await _call("__list__", {}, {})
    names = {t.name for t in result.tools}
    assert names == {
        "consultar_datos_publicos",
        "listar_fuentes",
        "buscar_datasets",
        "describir_tabla",
        "obtener_datos",
        "agregar_datos",
    }
    for tool in result.tools:
        assert "\n    " not in (tool.description or ""), f"{tool.name}: descripción con sangría"
    assert backend.requests == []


async def test_question_without_key_explains_how_to_get_one(backend: FakeBackend) -> None:
    result = await _call("consultar_datos_publicos", {"pregunta": "desempleo"}, {})
    assert result.is_error
    assert "openarg.org/desarrolladores" in _text(result)
    assert backend.requests == []


async def test_question_forwards_key_and_client_ip(backend: FakeBackend) -> None:
    result = await _call(
        "consultar_datos_publicos",
        {"pregunta": "tasa de desempleo"},
        {"Authorization": f"Bearer {KEY}", "X-Forwarded-For": "181.1.2.3"},
    )
    assert not result.is_error, _text(result)
    text = _text(result)
    assert "7,6 %" in text and "datos.gob.ar/eph" in text and "te quedan este mes: 9" in text
    sent = backend.requests[0]
    assert sent.url.path == "/api/v1/ask"
    assert sent.headers["authorization"] == f"Bearer {KEY}"
    assert sent.headers["x-forwarded-for"] == "181.1.2.3"
    assert json.loads(sent.content) == {"question": "tasa de desempleo"}


async def test_backend_learns_it_came_through_the_mcp_and_from_which_client(
    backend: FakeBackend,
) -> None:
    """Para el tablero de uso: vía y programa cliente, nunca la clave en otro header.

    El cliente del SDK manda su `clientInfo` en cada pedido (protocolo nuevo), y
    eso le gana al User-Agent: es el nombre que el propio cliente declara.
    """
    result = await _call(
        "listar_fuentes",
        {},
        {"Authorization": f"Bearer {KEY}", "User-Agent": "claude-code/2.1.0"},
    )
    assert not result.is_error, _text(result)
    sent = backend.requests[0]
    assert sent.headers["x-openarg-via"] == "mcp"
    assert sent.headers["x-openarg-client"] == "mcp/0.1.0"
    assert [k for k, v in sent.headers.items() if KEY in v] == ["authorization"]


async def test_quota_exhausted_is_a_spanish_tool_error(backend: FakeBackend) -> None:
    backend.status = 402
    backend.body = {"detail": "Monthly quota exceeded: 10 questions per month"}
    result = await _call(
        "consultar_datos_publicos", {"pregunta": "x"}, {"Authorization": f"Bearer {KEY}"}
    )
    assert result.is_error
    assert "10 preguntas de este mes" in _text(result)
    assert "modo datos" in _text(result)


async def test_quota_service_down_is_not_shown_as_quota_exhausted(backend: FakeBackend) -> None:
    """Un 503 por Redis caído decía "el cupo público de hoy está agotado"."""
    backend.status = 503
    backend.body = {
        "detail": "Public API temporarily unavailable: the quota service is not "
        "responding. Try again in a few minutes."
    }
    result = await _call(
        "consultar_datos_publicos", {"pregunta": "x"}, {"Authorization": f"Bearer {KEY}"}
    )
    assert result.is_error
    assert "no responde" in _text(result)
    assert "agotado" not in _text(result)

    backend.body = {"detail": "Free tier daily capacity reached. Try again tomorrow."}
    result = await _call(
        "consultar_datos_publicos", {"pregunta": "x"}, {"Authorization": f"Bearer {KEY}"}
    )
    assert "cupo público de OpenArg para hoy está agotado" in _text(result)


async def test_an_answer_that_was_not_charged_says_so(backend: FakeBackend) -> None:
    backend.body = {**backend.body, "usage": {"requests_remaining_month": 9, "charged": False}}
    result = await _call(
        "consultar_datos_publicos", {"pregunta": "x"}, {"Authorization": f"Bearer {KEY}"}
    )
    assert "te quedan este mes: 9 (esta no se descontó)" in _text(result)


async def test_listar_fuentes(backend: FakeBackend) -> None:
    result = await _call("listar_fuentes", {}, {"Authorization": f"Bearer {KEY}"})
    assert "caba: 10 datasets" in _text(result)
    assert backend.requests[0].url.path == "/api/v1/fuentes"


async def test_unknown_host_is_rejected() -> None:
    app = mcp_server.create_app()
    async with mcp_server.server.session_manager.run():
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://evil.example"
        ) as http:
            r = await http.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                headers={"accept": "application/json, text/event-stream"},
            )
    assert r.status_code in (400, 421)


async def test_health_and_site() -> None:
    app = mcp_server.create_app()
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://localhost:8000"
    ) as http:
        assert (await http.get("/health")).json() == {"status": "ok"}
        home = await http.get("/")
    assert home.status_code == 200
    assert "OpenArg" in home.text


async def test_robots_and_sitemap() -> None:
    """Sólo mcp.openarg.org se indexa; staging (misma imagen) responde "no indexar"."""
    app = mcp_server.create_app()
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://localhost:8000"
    ) as http:
        local = await http.get("/robots.txt")
        prod = await http.get("/robots.txt", headers={"host": "mcp.openarg.org"})
        staging = await http.get("/robots.txt", headers={"host": "mcp.staging.openarg.org"})
        sitemap = await http.get("/sitemap.xml")
    assert local.text == staging.text == "User-agent: *\nDisallow: /\n"
    assert "Allow: /\n" in prod.text and "Disallow: /mcp\n" in prod.text
    assert "Sitemap: https://mcp.openarg.org/sitemap.xml" in prod.text
    assert sitemap.status_code == 200
    for page in ("empezar.html", "herramientas.html", "limites.html"):
        assert f"https://mcp.openarg.org/{page}" in sitemap.text
        assert (mcp_server._SITE_DIR / page).is_file()


def test_structured_faq_matches_the_visible_faq() -> None:
    """El FAQ de schema.org tiene que decir lo mismo que la sección visible."""
    import html
    import re

    page = (mcp_server._SITE_DIR / "index.html").read_text(encoding="utf-8")
    ld = json.loads(re.search(r'<script type="application/ld\+json">(.*?)</script>', page, re.S)[1])
    faq = next(n for n in ld["@graph"] if n["@type"] == "FAQPage")["mainEntity"]

    def text(fragment: str) -> str:
        return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", fragment))).strip()

    visible = re.findall(
        r"<details>\s*<summary>(.*?)</summary>\s*<div>(.*?)</div>\s*</details>", page, re.S
    )
    assert [(q["name"], q["acceptedAnswer"]["text"]) for q in faq] == [
        (text(q), text(a)) for q, a in visible
    ]
    app = next(n for n in ld["@graph"] if n["@type"] == "SoftwareApplication")
    assert app["offers"]["price"] == "0"


class TestModoDatos:
    async def test_buscar_sends_query_and_lists_tables(self, backend: FakeBackend) -> None:
        result = await _call(
            "buscar_datasets",
            {"texto": "tasas BCRA", "limite": 99},
            {"Authorization": f"Bearer {KEY}"},
        )
        assert not result.is_error, _text(result)
        req = backend.requests[0]
        assert req.url.path == "/api/v1/catalogo/buscar"
        assert req.url.params["q"] == "tasas BCRA"
        assert req.url.params["limite"] == "25"  # acotado
        text = _text(result)
        assert "`raw.tasas` (8569 filas)" in text and "describir_tabla" in text

    async def test_describir_shows_period_columns_and_sample(self, backend: FakeBackend) -> None:
        result = await _call(
            "describir_tabla", {"tabla": "raw.tasas"}, {"Authorization": f"Bearer {KEY}"}
        )
        text = _text(result)
        assert backend.requests[0].url.params["nombre"] == "raw.tasas"
        assert "2003-01-02 a 2026-06-18" in text
        assert "- call (double precision)" in text
        assert "```csv" in text
        assert "Último dato: 2026-06-18" in text and "OpenArg: 2026-10-04" in text

    async def test_obtener_datos_sends_structured_request_and_returns_csv(
        self, backend: FakeBackend
    ) -> None:
        result = await _call(
            "obtener_datos",
            {
                "tabla": "raw.tasas",
                "columnas": ["indice_tiempo", "call"],
                "desde": "2026-06",
                "orden": "desc",
                "limite": 2,
            },
            {"Authorization": f"Bearer {KEY}"},
        )
        req = backend.requests[0]
        assert req.method == "POST" and req.url.path == "/api/v1/catalogo/datos"
        assert json.loads(req.content) == {
            "tabla": "raw.tasas",
            "columnas": ["indice_tiempo", "call"],
            "desde": "2026-06",
            "orden": "desc",
            "limite": 2,
        }
        text = _text(result)
        assert "indice_tiempo,call\n2026-06-18,33.14" in text
        assert "Hay más filas" in text
        assert "https://infra.datos.gob.ar/x.csv" in text

    async def test_obtener_datos_forwards_operator_filters_and_date_column(
        self, backend: FakeBackend
    ) -> None:
        """Auditoría 3.3: los filtros del modo datos eran sólo de igualdad."""
        filtros = [
            {"columna": "call", "operador": "mayor_que", "valor": "30"},
            {"columna": "serie", "operador": "en", "valores": ["a", "b"]},
        ]
        await _call(
            "obtener_datos",
            {"tabla": "raw.tasas", "filtros": filtros, "columna_fecha": "indice_tiempo"},
            {"Authorization": f"Bearer {KEY}"},
        )
        body = json.loads(backend.requests[0].content)
        assert body["filtros"] == filtros
        assert body["columna_fecha"] == "indice_tiempo"

    async def test_backend_validation_message_reaches_the_model(self, backend: FakeBackend) -> None:
        backend.catalog_status = 400
        backend.catalog_detail = "Columnas que no existen en la tabla: password."
        result = await _call(
            "obtener_datos",
            {"tabla": "raw.tasas", "columnas": ["password"]},
            {"Authorization": f"Bearer {KEY}"},
        )
        assert result.is_error
        assert "Columnas que no existen en la tabla: password." in _text(result)

    async def test_data_mode_without_key_explains_how_to_get_one(
        self, backend: FakeBackend
    ) -> None:
        result = await _call("buscar_datasets", {"texto": "x"}, {})
        assert result.is_error and "openarg.org/desarrolladores" in _text(result)
        assert backend.requests == []


class TestAgregarDatosYLimites:
    async def test_agregar_datos_sends_the_aggregate_and_formats_it(
        self, backend: FakeBackend
    ) -> None:
        """Auditoría 3.1: el modo datos no agregaba; el cliente sumaba 493 filas a mano."""
        result = await _call(
            "agregar_datos",
            {
                "tabla": "raw.cache_presupuesto_credito_2026",
                "operacion": "suma",
                "columna": "credito_devengado",
                "agrupar_por": ["jurisdiccion_desc"],
                "filtros": {"ejercicio_presupuestario": 2026},
                "limite": 999,
            },
            {"Authorization": f"Bearer {KEY}"},
        )
        assert not result.is_error, _text(result)
        req = backend.requests[0]
        assert req.method == "POST" and req.url.path == "/api/v1/catalogo/agregar"
        assert json.loads(req.content) == {
            "tabla": "raw.cache_presupuesto_credito_2026",
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "filtros": {"ejercicio_presupuestario": 2026},
            "orden": "desc",
            "limite": 200,  # acotado
        }
        text = _text(result)
        assert "sobre 4.905 filas" in text
        assert "Ministerio de Capital Humano,60622921.86,412" in text
        assert "presupuestoabierto.gob.ar/x" in text

    async def test_agregar_datos_description_says_to_always_use_it_for_totals(
        self, backend: FakeBackend
    ) -> None:
        result = await _call("__list__", {}, {})
        tool = next(t for t in result.tools if t.name == "agregar_datos")
        assert "SIEMPRE" in (tool.description or "")
        assert "obtener_datos" in (tool.description or "")
        obtener = next(t for t in result.tools if t.name == "obtener_datos")
        assert "agregar_datos" in (obtener.description or "")

    async def test_the_instructions_carry_the_limits(self) -> None:
        """QW10: los límites en el único lugar que leen todos los clientes."""
        from mcp_publico import core

        assert core.LIMITES in (mcp_server.server.instructions or "")
        assert "agregar_datos" in (mcp_server.server.instructions or "")

    async def test_minute_limit_says_how_long_to_wait(self, backend: FakeBackend) -> None:
        backend.catalog_status = 429
        backend.catalog_detail = "Rate limit exceeded: 30 catalog requests per minute"
        backend.catalog_headers = {"Retry-After": "17"}
        result = await _call(
            "describir_tabla", {"tabla": "raw.tasas"}, {"Authorization": f"Bearer {KEY}"}
        )
        assert result.is_error
        assert "esperá 17 segundos" in _text(result)

    async def test_a_422_tells_the_model_which_parameter_to_fix(self, backend: FakeBackend) -> None:
        """Revisión del PR #139: el 422 de FastAPI llegaba como «formato no válido»."""
        backend.catalog_status = 422
        backend.catalog_detail = [  # type: ignore[assignment]
            {
                "type": "too_long",
                "loc": ["body", "agrupar_por"],
                "msg": "List should have at most 3 items after validation, not 4",
            }
        ]
        result = await _call(
            "agregar_datos",
            {"tabla": "t", "operacion": "suma", "agrupar_por": ["a", "b", "c", "d"]},
            {"Authorization": f"Bearer {KEY}"},
        )
        assert result.is_error
        assert "`agrupar_por`: List should have at most 3 items" in _text(result)

    async def test_obtener_datos_sends_offset_and_leaves_the_order_to_the_backend(
        self, backend: FakeBackend
    ) -> None:
        """Sin `orden`, el backend sabe que trajo lo más viejo por defecto y sugiere "desc"."""
        await _call(
            "obtener_datos",
            {"tabla": "raw.tasas", "offset": 100},
            {"Authorization": f"Bearer {KEY}"},
        )
        body = json.loads(backend.requests[0].content)
        assert body == {"tabla": "raw.tasas", "limite": 100, "offset": 100}
