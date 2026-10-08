"""Lectura de las DDJJ de la Ciudad de Buenos Aires.

Los encabezados y las filas son copias de los CSV reales del 08-oct-2026
(docstring de `application/ddjj/caba.py`).
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from app.application.ddjj import caba

_ARCHIVO = caba.ArchivoCaba(
    ruta="/no/importa.csv",
    url=(
        "https://cdn.buenosaires.gob.ar/datosabiertos/datasets/secretaria-legal-y-tecnica/"
        "declaraciones-juradas/declaraciones-juradas-2026.csv"
    ),
    corte=date(2026, 10, 2),
)


@pytest.mark.parametrize(
    ("crudo", "normalizado"),
    [
        ("AÑO_PRESENTACIÓN", "anio_presentacion"),
        ("anio_presentacion", "anio_presentacion"),
        (" ID_DDJJ ", "id_ddjj"),
        ("TOTAL_DINERO_ELECTRONICO", "total_dinero_electronico"),
    ],
)
def test_normalizar_columna(crudo, normalizado):
    assert caba.normalizar_columna(crudo) == normalizado


def test_anio_de_url():
    assert caba.anio_de_url(_ARCHIVO.url) == 2026
    assert caba.anio_de_url("https://x/anexo-i-valor-venta-2016.pdf") is None


def _escribir(tmp_path, nombre: str, contenido: str) -> str:
    ruta = tmp_path / nombre
    ruta.write_bytes(contenido.encode("utf-8"))
    return str(ruta)


def test_leer_formato_ancho_de_2023(tmp_path):
    ruta = _escribir(
        tmp_path,
        "2023.csv",
        '﻿"ID_DDJJ","AÑO_PRESENTACIÓN","NOMBRE","APELLIDO","ID_CARGO","CARGO",'
        '"TOTAL_BIENES_MUEBLES","TOTAL_BIENES_INMUEBLES","TOTAL_ACCIONES","TOTAL_FONDOS",'
        '"TOTAL_BONOS","TOTAL_TITULOS","TOTAL_DINERO_EFECTIVO","TOTAL_DINERO_ELECTRONICO",'
        '"TOTAL_DEUDAS","FECHA_PRESENTACION"\r\n'
        "21417,2023,Matias Alfredo,KLEIN,2,ASESOR DE PLANTA,414001,802500,0,1106000,0,0,15000,0,,"
        "2023-07-14 00:00:00.000\r\n",
    )
    columnas, filas = caba.leer(ruta)
    assert caba.es_formato_ancho(columnas)
    (fila,) = list(filas)
    t = dict(zip(caba.COLUMNAS_DECLARACION, caba.fila_declaracion(fila, _ARCHIVO), strict=True))
    assert t["fuente"] == "caba" and t["jurisdiccion"] == "caba"
    assert t["dj_id"] == 21417 and t["anio"] == 2023
    assert t["nombre"] == "KLEIN MATIAS ALFREDO"
    assert t["poder"] == "ejecutivo"
    assert t["bienes"] == t["bienes_cierre"] == Decimal(414001 + 802500 + 1106000 + 15000)
    assert json.loads(t["bienes_por_tipo"]) == {
        "inmuebles": 802500,
        "muebles": 414001,
        "acciones": 0,
        "fondos": 1106000,
        "bonos": 0,
        "titulos": 0,
        "dinero_efectivo": 15000,
        "dinero_electronico": 0,
    }
    assert t["fecha_presentacion"] == date(2023, 7, 14)
    assert t["archivo_fuente"] == "declaraciones-juradas-2026.csv"


def test_formato_largo_no_es_ancho(tmp_path):
    ruta = _escribir(
        tmp_path,
        "2021.csv",
        "informacion,tipo_de_dato,valor,presentacion,periodo\r\n"
        "Datos Personales,Tipo de Presentación, Actualizacion,30/06/2021,2021\r\n",
    )
    columnas, _ = caba.leer(ruta)
    assert not caba.es_formato_ancho(columnas)


def test_fila_2026_con_espacios_y_sin_deudas():
    fila = {
        "id_ddjj": "40635",
        "anio_presentacion": "2025",
        "nombre": "Julian Andres                       ",
        "apellido": "Sanchez",
        "cargo": "Director/A  General O Equivalente",
        "total_bienes_inmuebles": "100",
        "fecha_presentacion": "2025-01-02",
    }
    t = dict(zip(caba.COLUMNAS_DECLARACION, caba.fila_declaracion(fila, _ARCHIVO), strict=True))
    assert t["nombre"] == "SANCHEZ JULIAN ANDRES"
    assert t["cargo"] == "Director/A General O Equivalente"
    assert t["bienes"] == Decimal(100)
    assert json.loads(t["bienes_por_tipo"]) == {"inmuebles": 100}


@pytest.mark.parametrize(
    "fila",
    [
        {"id_ddjj": "", "anio_presentacion": "2025"},
        {"id_ddjj": "12", "anio_presentacion": ""},
        {"id_ddjj": "abc", "anio_presentacion": "2025"},
    ],
)
def test_fila_invalida(fila):
    assert caba.fila_declaracion(fila, _ARCHIVO) is None


def test_sin_montos_no_inventa_cero():
    t = dict(
        zip(
            caba.COLUMNAS_DECLARACION,
            caba.fila_declaracion({"id_ddjj": "1", "anio_presentacion": "2025"}, _ARCHIVO),
            strict=True,
        )
    )
    assert t["bienes"] is None and t["bienes_por_tipo"] is None


def test_un_juez_en_el_cargo_no_es_ejecutivo():
    t = caba.fila_declaracion(
        {"id_ddjj": "1", "anio_presentacion": "2025", "cargo": "Juez de Cámara"}, _ARCHIVO
    )
    assert t[3] == "judicial"


# ── la descarga desde el CDN ───────────────────────────────


def test_anios_a_probar_llega_al_anio_que_viene():
    assert list(caba.anios_a_probar(2026)) == list(range(2015, 2028))


def test_la_tarea_baja_del_cdn_saltea_los_404_y_simula(monkeypatch):
    import httpx

    from app.infrastructure.celery.tasks import ddjj_tasks as dt

    ancho = (
        "id_ddjj,anio_presentacion,nombre,apellido,id_cargo,cargo,total_bienes_muebles,"
        "total_bienes_inmuebles,total_acciones,total_fondos,total_bonos,total_titulos,"
        "total_dinero_efectivo,total_dinero_electronico,fecha_presentacion\n"
        "1,2026,Ana,Perez,1,Director,0,100,0,0,0,0,0,0,2026-01-02\n"
    )
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        assert request.headers["user-agent"].startswith("OpenArg/")
        if request.url.path.endswith(("-2023.csv", "-2026.csv")):
            return httpx.Response(
                200,
                text=ancho,
                headers={
                    "content-type": "text/csv",
                    "last-modified": "Wed, 30 Sep 2026 21:19:10 GMT",
                },
            )
        return httpx.Response(404, text="no")

    real = httpx.Client

    def cliente(*a, **kw):
        return real(*a, transport=httpx.MockTransport(responder), **kw)

    monkeypatch.setattr(dt.httpx, "Client", cliente)
    resultado = dt.ingest_ddjj_caba.run(dry_run=True)
    assert resultado["estado"] == "simulada"
    assert resultado["archivos"] == {
        "declaraciones-juradas-2023.csv": "ancho",
        "declaraciones-juradas-2026.csv": "ancho",
    }
    assert len(pedidos) == len(caba.anios_a_probar(dt.datetime.now(dt.UTC).year))
    assert all("cdn.buenosaires.gob.ar" in u for u in pedidos)


def test_un_waf_que_contesta_html_es_una_falla_clara(monkeypatch):
    import httpx
    import pytest as _pytest

    from app.infrastructure.celery.tasks import ddjj_tasks as dt

    def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html><title>Request Rejected</title></html>")

    with httpx.Client(transport=httpx.MockTransport(responder)) as client:
        with _pytest.raises(dt._Falla, match="no devolvió JSON"):
            dt.consultar_paquete(client)
