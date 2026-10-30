"""Las planillas del INDEC con el período en el nombre.

`cuadros_tasas_indicadores_eph_09_25.xls` quedó fija en el código en marzo de
2026; para octubre el INDEC ya había publicado `_12_25`, `_03_26`, `_06_26` y
`_09_26`, y OpenArg seguía sirviendo la de septiembre de 2025.

El servidor del INDEC responde **200 con text/html** para un archivo que no
existe (su página de "no encontrado"), así que sólo cuenta una respuesta con
Content-Type de Excel. Las respuestas acá están simuladas: ningún test toca la
red.
"""

from __future__ import annotations

from datetime import date

import httpx
import pytest

from app.infrastructure.celery.tasks import indec_tasks
from app.infrastructure.celery.tasks.indec_tasks import (
    INDEC_DATASETS,
    _dated_url_candidates,
    current_url,
    resolve_dated_url,
)

HOY = date(2026, 10, 30)
XLS = {"content-type": "application/vnd.ms-excel"}
HTML = {"content-type": "text/html; charset=utf-8"}
B = "https://www.indec.gob.ar/ftp/cuadros/sociedad/"


def _ds(ds_id: str) -> dict:
    return next(d for d in INDEC_DATASETS if d["id"] == ds_id)


def _cliente(publicados: set[str], pedidos: list[str]) -> httpx.Client:
    """Un INDEC simulado: Excel para lo publicado, 200-HTML para el resto."""

    def handler(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if request.method != "HEAD":
            return httpx.Response(405)
        headers = XLS if str(request.url) in publicados else HTML
        return httpx.Response(200, headers=headers)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_los_cuatro_archivos_con_fecha_tienen_patron() -> None:
    con_patron = {d["id"] for d in INDEC_DATASETS if d.get("url_pattern")}

    assert con_patron == {"pobreza_informe", "eph_tasas", "eph_hogares", "distribucion_ingreso"}


@pytest.mark.parametrize(
    "ds_id", ["pobreza_informe", "eph_tasas", "eph_hogares", "distribucion_ingreso"]
)
def test_el_patron_reproduce_la_url_fija(ds_id: str) -> None:
    """La URL fija es una de las candidatas: el patrón describe ese archivo."""
    ds = _ds(ds_id)
    candidatas = _dated_url_candidates(ds["url_pattern"], today=HOY, months_back=24)

    assert ds["url"] in candidatas


def test_candidatas_trimestrales_una_por_trimestre_de_la_mas_nueva_a_la_vieja() -> None:
    patron = B + "ingresos_{q}t_{yyyy}.xls"

    assert _dated_url_candidates(patron, today=HOY, months_back=5) == [
        B + "ingresos_4t_2026.xls",
        B + "ingresos_3t_2026.xls",
        B + "ingresos_2t_2026.xls",
    ]


def test_elige_la_edicion_mas_reciente_publicada() -> None:
    pedidos: list[str] = []
    publicados = {
        B + "cuadros_tasas_indicadores_eph_09_26.xls",
        B + "cuadros_tasas_indicadores_eph_06_26.xls",
    }

    url = resolve_dated_url(_ds("eph_tasas"), client=_cliente(publicados, pedidos), today=HOY)

    assert url == B + "cuadros_tasas_indicadores_eph_09_26.xls"
    assert pedidos == [
        B + "cuadros_tasas_indicadores_eph_10_26.xls",
        B + "cuadros_tasas_indicadores_eph_09_26.xls",
    ]


def test_un_200_con_html_no_cuenta_como_publicado() -> None:
    """El caso real: `_09_26` todavía no salió y el INDEC contesta 200 + HTML."""
    pedidos: list[str] = []
    publicados = {B + "cuadros_tasas_indicadores_eph_06_26.xls"}

    url = resolve_dated_url(_ds("eph_tasas"), client=_cliente(publicados, pedidos), today=HOY)

    assert url == B + "cuadros_tasas_indicadores_eph_06_26.xls"
    assert B + "cuadros_tasas_indicadores_eph_09_26.xls" in pedidos


def test_semestral() -> None:
    publicados = {B + "eph_indicadores_hogares_2_sem_2025.xls"}

    url = resolve_dated_url(_ds("eph_hogares"), client=_cliente(publicados, []), today=HOY)

    assert url == B + "eph_indicadores_hogares_2_sem_2025.xls"


def test_sin_nada_mas_nuevo_cae_a_la_url_fija_sin_seguir_buscando() -> None:
    ds = _ds("distribucion_ingreso")
    pedidos: list[str] = []

    url = resolve_dated_url(ds, client=_cliente(set(), pedidos), today=HOY)

    assert url == ds["url"]
    # 4T-2026 … 4T-2025: se detiene al llegar a la edición conocida (3T-2025).
    assert pedidos[-1] == B + "indicadores_eph_total_urbano_ingresos_4t_2025.xls"
    assert ds["url"] not in pedidos


def test_si_la_red_falla_cae_a_la_url_fija() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sin red", request=request)

    ds = _ds("pobreza_informe")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    assert resolve_dated_url(ds, client=client, today=HOY) == ds["url"]


def test_un_404_tampoco_cuenta() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, headers=XLS)

    ds = _ds("pobreza_informe")
    client = httpx.Client(transport=httpx.MockTransport(handler))

    assert resolve_dated_url(ds, client=client, today=HOY) == ds["url"]


def test_los_archivos_sin_fecha_no_tocan_la_red(monkeypatch: pytest.MonkeyPatch) -> None:
    def explota(*a, **k):
        raise AssertionError("no debería consultar nada")

    monkeypatch.setattr(httpx, "Client", explota)
    ipc = _ds("ipc")

    assert resolve_dated_url(ipc) == ipc["url"]
    assert current_url(ipc) == ipc["url"]


def test_current_url_recuerda_la_resolucion(monkeypatch: pytest.MonkeyPatch) -> None:
    llamadas: list[str] = []

    def falso(ds_info: dict, **_: object) -> str:
        llamadas.append(ds_info["id"])
        return "https://ejemplo/nuevo.xls"

    monkeypatch.setattr(indec_tasks, "resolve_dated_url", falso)
    monkeypatch.setattr(indec_tasks, "_resolved_cache", {})
    ds = _ds("eph_tasas")

    assert current_url(ds) == "https://ejemplo/nuevo.xls"
    assert current_url(ds) == "https://ejemplo/nuevo.xls"
    assert llamadas == ["eph_tasas"]
