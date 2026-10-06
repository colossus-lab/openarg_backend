"""El ETL de las 12 tablas `raw.cache_series_*` (modo datos del MCP y NL2SQL).

Lo que este archivo fija, cada cosa por un bug que estuvo en producción:

- Una serie ya cacheada (`ready`) se reescribe cuando la API tiene datos más
  nuevos. Hasta oct-2026 se salteaba siempre: las 12 tablas quedaron paradas
  desde el 06-may mientras la tarea "corría bien" todos los meses.
- La descarga pagina con `start` hasta cubrir `count`. Un solo GET con
  `limit=1000` dejaba tipo de cambio en 2005-09-27 y reservas en 2023-04.
- La identidad del registro es la que ya existe (`series_tiempo::series-tiempo-
  <clave>`); la otra chocaba con el UNIQUE de tabla y el error se tragaba.
- WS0 decide antes de tocar la tabla, y un guardián no deja reemplazarla por
  menos filas ni por otra serie sin aprobación (H026).
- El reemplazo no borra la tabla vieja: la deja en `<tabla>__previa`, así que
  volver atrás es un RENAME (H025/H057).
- Desempleo se guarda en porcentaje, igual que lo da el conector en vivo, y
  no como fracción bajo una columna "En porcentaje" (H023/H045).
- Si una corrida se corta después del reemplazo, la siguiente repara el
  catálogo, el registro y el dataset en vez de decir "al día" (H024).
- Una corrida que no pudo consultar la API no termina en éxito.
- La alarma separa "la tabla está atrás de la API" (bug nuestro) de "la API
  está atrás" (la fuente).

La API va en memoria con `httpx.MockTransport`; la base, con dobles por
función. El camino contra Postgres de verdad está en
`tests/integration/test_series_tiempo_etl_db.py`.
"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pandas as pd
import pytest

from app.infrastructure.celery.tasks import series_tiempo_tasks as st
from tests.unit.series_tiempo_fake import DESEMPLEO_ID, TIPO_CAMBIO_ID, FakeSeriesApi, desempleo

# ── la API, en memoria ─────────────────────────────────────────────────────


def _filas_diarias(n: int, desde: date = date(2003, 1, 2)) -> list[list[Any]]:
    return [[(desde + timedelta(days=i)).isoformat(), float(i)] for i in range(n)]


class _ApiFalsa:
    """count, `metadata=full` y páginas con `start`/`limit`, como la de verdad."""

    def __init__(
        self,
        filas: list[list[Any]],
        *,
        is_updated: str = "True",
        frequency: str = "R/P1D",
        descripcion: str = "Serie de prueba",
        count: int | None = None,
        count_desde_pagina: tuple[int, int] | None = None,
        status: int | None = None,
    ) -> None:
        self.filas = filas
        self.is_updated = is_updated
        self.frequency = frequency
        self.descripcion = descripcion
        self.count = len(filas) if count is None else count
        self.count_desde_pagina = count_desde_pagina
        self.status = status
        self.pedidos: list[dict[str, str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        self.pedidos.append(params)
        if self.status:
            return httpx.Response(self.status, text="caída")
        limit = int(params.get("limit", 100))
        if limit > 5000:
            return httpx.Response(400, json={"errors": [{"error": "limit"}]})
        start = int(params.get("start", 0))
        count = self.count
        if self.count_desde_pagina and start >= self.count_desde_pagina[0]:
            count = self.count_desde_pagina[1]
        field: dict[str, Any] = {"description": self.descripcion, "id": params.get("ids")}
        if params.get("metadata") == "full":
            field.update(
                {
                    "time_index_end": self.filas[-1][0] if self.filas else None,
                    "is_updated": self.is_updated,
                    "frequency": self.frequency,
                    "units": "Pesos",
                }
            )
        meta = [
            {"frequency": "day"},
            {"field": field, "dataset": {"title": "Dataset de prueba", "source": "BCRA"}},
        ]
        return httpx.Response(
            200, json={"data": self.filas[start : start + limit], "count": count, "meta": meta}
        )

    def cliente(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self))

    @property
    def paginas(self) -> list[dict[str, str]]:
        return [p for p in self.pedidos if p.get("metadata") != "full"]


_SERIE = st.SerieETL(
    clave="tipo_cambio",
    clave_catalogo="tipo_cambio",
    serie_id="92.2_TIPO_CAMBIION_0_0_21_24",
    descripcion_catalogo="Tipo de cambio peso/dólar",
)
_COLUMNAS = ("fecha", "Serie de prueba")


def _estado(**kw: Any) -> st.EstadoTabla:
    base: dict[str, Any] = {
        "existe": True,
        "filas": 1000,
        "max_fecha": date(2005, 9, 27),
        "columnas": _COLUMNAS,
        "dataset_id": "00000000-0000-0000-0000-000000000001",
        "titulo": "Series de Tiempo — Tipo de Cambio Peso/Dolar",
        "descripcion": "vieja",
        "columnas_dataset": _COLUMNAS,
        "serie_id_previa": _SERIE.serie_id,
        "dueno_registro": _SERIE.identidad,
    }
    base.update(kw)
    return st.EstadoTabla(**base)


def _meta(**kw: Any) -> st.MetadatosAPI:
    base: dict[str, Any] = {
        "serie_id": _SERIE.serie_id,
        "total": 8643,
        "fin": date(2026, 8, 31),
        "actualizada": True,
        "frecuencia": "diaria",
        "descripcion": "Serie de prueba",
    }
    base.update(kw)
    return st.MetadatosAPI(**base)


def _meta_de_la_api_falsa() -> st.MetadatosAPI:
    """Lo que `consultar_metadatos` lee de `_ApiFalsa` con 8.643 filas diarias."""
    return _meta(
        total=8643,
        fin=date(2003, 1, 2) + timedelta(days=8642),
        unidades="Pesos",
        titulo_dataset="Dataset de prueba",
        fuente="BCRA",
    )


def _estado_al_dia(**kw: Any) -> st.EstadoTabla:
    """La tabla igual a la API Y todo lo que la acompaña en orden."""
    meta = _meta_de_la_api_falsa()
    base: dict[str, Any] = {
        "filas": 8643,
        "max_fecha": meta.fin,
        "titulo": st._titulo(_SERIE, meta),
        "descripcion": st._descripcion(_SERIE, meta),
        "dataset_filas": 8643,
        "dataset_cacheado": True,
        "catalogo_estado": "ready",
        "catalogo_filas": 8643,
        "registro_filas": 8643,
    }
    base.update(kw)
    return _estado(**base)


# ── catálogo: los ids salen del adaptador ─────────────────────────────────


@pytest.mark.parametrize(
    "clave",
    [
        "inflacion_ipc",
        "tipo_cambio",
        "emae",
        "desempleo",
        "salarios",
        "canasta_basica",
        "exportaciones",
        "importaciones",
    ],
)
def test_las_tablas_toman_el_id_del_catalogo_del_adaptador(clave):
    from app.infrastructure.adapters.connectors.series_tiempo_adapter import SERIES_CATALOG

    series, problemas = st.series_del_catalogo(claves=[clave])
    assert clave not in problemas, problemas
    (serie,) = series
    assert serie.serie_id == SERIES_CATALOG[st.SERIES_TABLAS[clave]]["ids"][0]


def test_el_etl_no_mantiene_un_catalogo_de_ids_propio():
    # Había dos `SERIES_CATALOG` y divergieron: "actividad_industrial" era EMAE
    # Comercio en los dos. Ahora el ETL sólo dice qué clave alimenta qué tabla.
    assert not hasattr(st, "SERIES_CATALOG")
    assert all(isinstance(v, str) for v in st.SERIES_TABLAS.values())
    assert len(st.SERIES_TABLAS) == 12


def test_una_clave_que_falta_en_el_catalogo_se_reporta_sin_romper():
    catalogo = {"tipo_cambio": {"ids": ["X.1"], "description": "TC"}}
    series, problemas = st.series_del_catalogo(catalogo, claves=["tipo_cambio", "emae"])
    assert [s.clave for s in series] == ["tipo_cambio"]
    assert "emae" in problemas and "no está en el catálogo" in problemas["emae"]


def test_una_entrada_con_varias_series_no_alimenta_una_tabla():
    catalogo = {"tipo_cambio": {"ids": ["A", "B"]}}
    series, problemas = st.series_del_catalogo(catalogo, claves=["tipo_cambio"])
    assert series == [] and "2 series" in problemas["tipo_cambio"]


def test_la_identidad_es_la_que_ya_tiene_el_registro():
    # El registro de staging y prod tiene `series_tiempo::series-tiempo-<clave>`
    # (backfill del 09-may). Con `series_tiempo::<clave>` el INSERT choca con
    # `uq_raw_table_versions_table_name` (schema_name, table_name).
    assert _SERIE.identidad == "series_tiempo::series-tiempo-tipo_cambio"
    assert _SERIE.tabla == "cache_series_tipo_cambio"
    assert _SERIE.source_id == "series-tiempo-tipo_cambio"


# ── API: metadata y paginación ─────────────────────────────────────────────


def test_la_metadata_trae_fin_frecuencia_e_is_updated():
    api = _ApiFalsa(_filas_diarias(10), is_updated="False", frequency="R/P1D")
    with api.cliente() as c:
        meta = st.consultar_metadatos(c, _SERIE.serie_id)
    assert meta.total == 10
    assert meta.fin == date(2003, 1, 11)
    assert meta.actualizada is False  # la API manda el string "False"
    assert meta.frecuencia == "diaria"
    assert api.pedidos[0]["limit"] == "1" and api.pedidos[0]["metadata"] == "full"


def test_pagina_con_start_hasta_cubrir_count():
    # 12.345 > 2 páginas de 5.000. Antes: un GET con limit=1000, sin start.
    api = _ApiFalsa(_filas_diarias(12_345))
    with api.cliente() as c:
        filas = st.descargar_serie(c, _SERIE.serie_id)
    assert len(filas) == 12_345
    assert filas[-1][0] == (date(2003, 1, 2) + timedelta(days=12_344)).isoformat()
    assert [p["start"] for p in api.paginas] == ["0", "5000", "10000"]
    assert all(p["limit"] == "5000" for p in api.paginas)
    assert all("sort" not in p for p in api.paginas), "ascendente: sort=desc corre la ventana"


def test_si_count_cambia_entre_paginas_no_se_mezclan_versiones():
    api = _ApiFalsa(_filas_diarias(6000), count_desde_pagina=(5000, 6001))
    with api.cliente() as c, pytest.raises(st._SerieFallida, match="count pasó"):
        st.descargar_serie(c, _SERIE.serie_id)


def test_si_baja_menos_de_lo_que_anuncia_falla():
    api = _ApiFalsa(_filas_diarias(100), count=150)
    with api.cliente() as c, pytest.raises(st._SerieFallida, match="anuncia 150"):
        st.descargar_serie(c, _SERIE.serie_id)


def test_reintenta_un_503_pero_no_un_400(monkeypatch):
    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    api = _ApiFalsa(_filas_diarias(3), status=503)
    with api.cliente() as c, pytest.raises(st._SerieFallida):
        st.consultar_metadatos(c, _SERIE.serie_id)
    assert len(api.pedidos) == 3

    api = _ApiFalsa(_filas_diarias(3), status=400)
    with api.cliente() as c, pytest.raises(st._SerieFallida, match="HTTP 400"):
        st.consultar_metadatos(c, _SERIE.serie_id)
    assert len(api.pedidos) == 1


# ── decidir si escribir ────────────────────────────────────────────────────


def test_una_serie_ready_con_datos_nuevos_en_la_api_se_reescribe():
    """El bug central: `ready` no es "al día"."""
    motivo = st.motivo_para_escribir(_meta(), _estado(), list(_COLUMNAS))
    assert motivo == "fuente_mas_nueva"


def test_una_serie_al_dia_no_se_reescribe():
    estado = _estado(filas=8643, max_fecha=date(2026, 8, 31))
    assert st.motivo_para_escribir(_meta(), estado, list(_COLUMNAS)) is None


@pytest.mark.parametrize(
    ("estado_kw", "esperado"),
    [
        ({"existe": False, "filas": 0, "max_fecha": None, "columnas": ()}, "tabla_inexistente"),
        (
            {"serie_id_previa": "OTRA.1", "filas": 8643, "max_fecha": date(2026, 8, 31)},
            "cambio_de_serie",
        ),
        ({"filas": 8000, "max_fecha": date(2026, 8, 31)}, "cantidad_distinta"),
        (
            {"filas": 8643, "max_fecha": date(2026, 8, 31), "columnas": ("fecha", "x")},
            "columnas_distintas",
        ),
    ],
)
def test_otros_motivos_para_reescribir(estado_kw, esperado):
    assert st.motivo_para_escribir(_meta(), _estado(**estado_kw), list(_COLUMNAS)) == esperado


def test_forzar_reescribe_aunque_este_al_dia():
    estado = _estado(filas=8643, max_fecha=date(2026, 8, 31))
    assert st.motivo_para_escribir(_meta(), estado, list(_COLUMNAS), forzar=True) == "forzada"


# ── el guardián ────────────────────────────────────────────────────────────


def test_el_guardian_no_reemplaza_por_menos_filas():
    motivo = st.motivo_para_rechazar(
        filas_nuevas=900, fin_nuevo=date(2026, 8, 31), estado=_estado(), misma_serie=True
    )
    assert motivo and "menos filas" in motivo


def test_el_guardian_no_deja_retroceder_la_fecha():
    motivo = st.motivo_para_rechazar(
        filas_nuevas=5000, fin_nuevo=date(2004, 1, 1), estado=_estado(), misma_serie=True
    )
    assert motivo and "retrocede" in motivo


def test_con_permiso_explicito_se_puede_achicar():
    assert (
        st.motivo_para_rechazar(
            filas_nuevas=900,
            fin_nuevo=date(2004, 1, 1),
            estado=_estado(),
            misma_serie=True,
            permitir_menos_filas=True,
        )
        is None
    )


def test_un_cambio_de_serie_sin_aprobar_se_rechaza():
    # H026. Antes este caso devolvía None: el id sale del catálogo del
    # adaptador, que editan otros PRs, y cualquier edición reemplazaba la
    # tabla sin chequear filas ni fechas (el único chequeo que quedaba era
    # sin_filas). Este test decía "achicarse es esperable" y consagraba eso.
    motivo = st.motivo_para_rechazar(
        filas_nuevas=120,
        fin_nuevo=date(2026, 7, 1),
        estado=_estado(serie_id_previa="OTRA.1"),
        misma_serie=False,
    )
    assert motivo and "cambio de serie" in motivo and "OTRA.1" in motivo


def test_un_cambio_de_serie_aprobado_puede_achicarse():
    # Con la aprobación, la comparación contra la serie anterior no dice nada.
    assert (
        st.motivo_para_rechazar(
            filas_nuevas=120,
            fin_nuevo=date(2026, 7, 1),
            estado=_estado(serie_id_previa="OTRA.1"),
            misma_serie=False,
            cambio_aprobado=True,
        )
        is None
    )


def test_permitir_menos_filas_no_aprueba_un_cambio_de_serie():
    # Son dos permisos distintos: achicar la misma serie no es cambiarla.
    motivo = st.motivo_para_rechazar(
        filas_nuevas=120,
        fin_nuevo=date(2026, 7, 1),
        estado=_estado(serie_id_previa="OTRA.1"),
        misma_serie=False,
        permitir_menos_filas=True,
    )
    assert motivo and "cambio de serie" in motivo


def test_el_cambio_de_actividad_industrial_esta_aprobado_y_ningun_otro():
    # EMAE Comercio (rotulado "actividad industrial" desde 2026-02) → IPI
    # manufacturero del INDEC: el cambio es intencional y se aprueba por
    # escrito. Cualquier otro id nuevo para esa tabla sigue rechazado.
    (serie,), _ = st.series_del_catalogo(claves=["actividad_industrial"])
    assert serie.serie_id == "453.1_SERIE_ORIGNAL_0_0_14_46"
    assert st.cambio_de_serie_aprobado(serie, "11.3_AGCS_2004_M_41")
    assert not st.cambio_de_serie_aprobado(serie, "OTRA.1")
    (tipo_cambio,), _ = st.series_del_catalogo(claves=["tipo_cambio"])
    assert not st.cambio_de_serie_aprobado(tipo_cambio, "11.3_AGCS_2004_M_41")


def test_nunca_se_escribe_una_tabla_vacia():
    assert (
        st.motivo_para_rechazar(
            filas_nuevas=0,
            fin_nuevo=None,
            estado=_estado(existe=False),
            misma_serie=True,
            permitir_menos_filas=True,
        )
        == "sin_filas"
    )


# ── una serie de punta a punta, con la base doblada ────────────────────────


class _Base:
    """Registra las llamadas a las funciones que tocan la base."""

    def __init__(self, monkeypatch, estado: st.EstadoTabla, *, ws0: str | None = None):
        self.escrituras: list[tuple[str, int]] = []
        self.dataframes: list[pd.DataFrame] = []
        self.registros: list[dict[str, Any]] = []
        self.latidos: list[str] = []
        self.embeddings: list[str] = []
        self.finalizadas: list[dict[str, Any]] = []
        self.registro_ok = True
        monkeypatch.setattr(st, "estado_tabla", lambda engine, serie: estado)

        def _escribir(engine, tabla, df):
            self.escrituras.append((tabla, len(df)))
            self.dataframes.append(df)

        monkeypatch.setattr(st, "escribir_atomico", _escribir)
        monkeypatch.setattr(st, "_asegurar_dataset", lambda *a, **k: "nuevo-id")
        monkeypatch.setattr(st, "veredicto_ws0", lambda *a, **k: ws0)
        monkeypatch.setattr(st, "_actualizar_dataset", lambda *a, **k: None)

        def _finalizar(engine, **kw):
            self.finalizadas.append(kw)
            return {"ok": True}

        monkeypatch.setattr(st, "_finalize_cached_dataset", _finalizar)

        def _registrar(engine, **kw):
            self.registros.append(kw)
            return self.registro_ok

        monkeypatch.setattr(st, "register_via_b_table", _registrar)
        monkeypatch.setattr(
            "app.application.quality.heartbeat.record_ingest",
            lambda engine, rid: self.latidos.append(rid),
        )
        from app.infrastructure.celery.tasks import scraper_tasks

        tarea = MagicMock()
        tarea.delay.side_effect = lambda did: self.embeddings.append(did)
        monkeypatch.setattr(scraper_tasks, "index_dataset_embedding", tarea)


def _procesar(api: _ApiFalsa, serie: st.SerieETL = _SERIE, **kw: Any) -> st.ResultadoSerie:
    with api.cliente() as c:
        return st.procesar_serie(MagicMock(), c, serie, **kw)


def test_una_tabla_ready_y_cortada_se_reescribe_entera_y_se_registra(monkeypatch):
    """Lo que pasaba en staging y prod con tipo de cambio: 1.000 filas hasta 2005."""
    base = _Base(monkeypatch, _estado())
    api = _ApiFalsa(_filas_diarias(8643), descripcion="Serie de prueba")

    res = _procesar(api)

    assert res.estado == "escrita", res
    assert res.motivo == "fuente_mas_nueva"
    assert base.escrituras == [("cache_series_tipo_cambio", 8643)]
    (registro,) = base.registros
    assert registro["resource_identity"] == "series_tiempo::series-tiempo-tipo_cambio"
    assert registro["schema_name"] == "raw" and registro["row_count"] == 8643
    assert base.finalizadas[0]["row_count"] == 8643
    assert [p["start"] for p in api.paginas] == ["0", "5000"]


def test_ws0_rechaza_antes_de_tocar_la_tabla(monkeypatch):
    # Antes: to_sql(replace) y DESPUÉS WS0, así que un rechazo dejaba la tabla
    # reescrita y la serie fuera del catálogo (que sólo sirve `ready`).
    base = _Base(monkeypatch, _estado(), ws0="parser_invalid:header_quality_invalid")
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "rechazada" and "WS0" in res.detalle
    assert base.escrituras == [] and base.finalizadas == [] and base.registros == []


def test_el_guardian_frena_antes_de_escribir(monkeypatch):
    base = _Base(monkeypatch, _estado(filas=9000, max_fecha=date(2026, 8, 1)))
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "rechazada" and "menos filas" in res.detalle
    assert base.escrituras == []


def test_una_tabla_registrada_con_otra_identidad_no_se_escribe(monkeypatch):
    base = _Base(monkeypatch, _estado(dueno_registro="series_tiempo::tipo_cambio"))
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "rechazada" and res.motivo == "registro_ajeno"
    assert base.escrituras == []


def test_un_registro_fallido_es_una_falla_y_no_un_exito(monkeypatch):
    base = _Base(monkeypatch, _estado())
    base.registro_ok = False
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "fallida" and res.motivo == "registro"
    assert res.consulto_api is True


def test_una_serie_al_dia_late_sin_bajar_ni_escribir(monkeypatch):
    # El estado era "al día" con la metadata desalineada (descripción
    # 'vieja', sin catálogo ni registro). Desde H024 eso se repara, así que
    # el test arma una serie al día de verdad: tabla, dataset, catálogo y
    # registro en orden.
    base = _Base(monkeypatch, _estado_al_dia())
    api = _ApiFalsa(_filas_diarias(8643))
    res = _procesar(api)
    assert res.estado == "al_dia"
    assert base.latidos == ["series_tiempo::series-tiempo-tipo_cambio"]
    assert base.escrituras == [] and api.paginas == [], "sólo el pedido liviano de metadata"
    assert base.finalizadas == [] and base.registros == [], "nada que reparar"


def test_dry_run_no_escribe_ni_late(monkeypatch):
    base = _Base(monkeypatch, _estado())
    res = _procesar(_ApiFalsa(_filas_diarias(8643)), dry_run=True)
    assert res.estado == "simulada"
    assert base.escrituras == [] and base.latidos == [] and base.registros == []


def test_no_reembebe_si_no_cambia_lo_que_se_busca(monkeypatch):
    api = _ApiFalsa(_filas_diarias(8643))
    meta = _meta(total=8643, descripcion="Serie de prueba")
    meta = st.MetadatosAPI(
        **{
            **meta.__dict__,
            "titulo_dataset": "Dataset de prueba",
            "fuente": "BCRA",
            "unidades": "Pesos",
        }
    )
    estado = _estado(
        titulo=st._titulo(_SERIE, meta),
        descripcion=st._descripcion(_SERIE, meta),
    )
    base = _Base(monkeypatch, estado)
    res = _procesar(api)
    assert res.estado == "escrita"
    assert base.embeddings == []

    base = _Base(monkeypatch, _estado(titulo="Series de Tiempo — Tipo de Cambio Peso/Dolar"))
    _procesar(api)
    assert base.embeddings == ["00000000-0000-0000-0000-000000000001"]


# ── el guardián, de punta a punta (H026) ───────────────────────────────────


def test_otra_serie_sin_aprobar_no_reemplaza_la_tabla(monkeypatch):
    """El catálogo del adaptador cambió el id: antes se escribía sin chequear nada."""
    base = _Base(monkeypatch, _estado(serie_id_previa="OTRA.1"))
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "rechazada", res
    assert "cambio de serie" in res.detalle and "OTRA.1" in res.detalle
    assert base.escrituras == [] and base.finalizadas == [] and base.registros == []


def test_forzar_tampoco_cambia_de_serie_sin_aprobacion(monkeypatch):
    base = _Base(monkeypatch, _estado(serie_id_previa="OTRA.1"))
    res = _procesar(_ApiFalsa(_filas_diarias(8643)), forzar=True, permitir_menos_filas=True)
    assert res.estado == "rechazada" and "cambio de serie" in res.detalle
    assert base.escrituras == []


def test_el_cambio_aprobado_de_actividad_industrial_se_escribe_con_menos_filas(monkeypatch):
    # Staging y prod: 265 filas de EMAE Comercio desde 2004; el IPI arranca
    # en 2016 (127 filas). Sin la aprobación sería "menos filas".
    (serie,), _ = st.series_del_catalogo(claves=["actividad_industrial"])
    estado = _estado(
        filas=265,
        max_fecha=date(2026, 1, 1),
        serie_id_previa="11.3_AGCS_2004_M_41",
        dueno_registro=serie.identidad,
    )
    base = _Base(monkeypatch, estado)
    filas = [[f"{2016 + i // 12}-{i % 12 + 1:02d}-01", float(i)] for i in range(127)]
    res = _procesar(_ApiFalsa(filas, frequency="R/P1M"), serie)
    assert res.estado == "escrita", res
    assert res.motivo == "cambio_de_serie"
    assert base.escrituras == [("cache_series_actividad_industrial", 127)]


# ── desempleo en porcentaje, como el conector (H023/H045) ─────────────────


async def test_el_desempleo_se_guarda_en_porcentaje_como_lo_da_el_conector():
    """La API da 0,079 con unidades «Porcentaje». El conector en vivo (#127)
    escala ×100 y la tabla guardaba 0,079 bajo 'Tasa de desempleo total. En
    porcentaje.': dos cifras distintas para lo mismo, y '0,079 %' leído literal."""
    api = FakeSeriesApi(desempleo())
    en_vivo = await api.adapter().fetch([DESEMPLEO_ID])
    assert en_vivo is not None

    with httpx.Client(transport=httpx.MockTransport(api.handler)) as c:
        meta = st.consultar_metadatos(c, DESEMPLEO_ID)
        columna = st.nombre_columna(meta.descripcion, "desempleo")
        df = st.armar_dataframe(st.descargar_serie(c, DESEMPLEO_ID), columna, serie_id=DESEMPLEO_ID)

    assert columna == "Tasa de desempleo total. En porcentaje."
    assert df[columna].tolist() == [r[columna] for r in en_vivo.records]
    assert df[columna].iloc[-1] == 7.9


def test_una_serie_que_no_es_fraccion_no_se_escala():
    df = st.armar_dataframe(
        [["2026-08-31", 1350.25], ["2026-09-01", 0.5]], "TC", serie_id=TIPO_CAMBIO_ID
    )
    assert df["TC"].tolist() == [1350.25, 0.5]


def test_la_tarea_escribe_el_desempleo_en_porcentaje(monkeypatch):
    (serie,), _ = st.series_del_catalogo(claves=["desempleo"])
    assert serie.serie_id == DESEMPLEO_ID
    estado = _estado(
        existe=False,
        filas=0,
        max_fecha=None,
        columnas=(),
        serie_id_previa=serie.serie_id,
        dueno_registro=serie.identidad,
    )
    base = _Base(monkeypatch, estado)
    filas = [["2025-10-01", 0.075], ["2026-01-01", 0.078], ["2026-04-01", 0.079]]
    res = _procesar(_ApiFalsa(filas, frequency="R/P3M"), serie)
    assert res.estado == "escrita", res
    (df,) = base.dataframes
    assert df.iloc[:, 1].tolist() == [7.5, 7.8, 7.9]


# ── una corrida cortada después del reemplazo (H024) ──────────────────────


class _BaseViva:
    """Una base doblada CON estado: lo que deja una corrida lo ve la siguiente.

    Arranca con la tabla, el dataset, el catálogo y el registro en orden para
    `filas`. `fallas_finalize` es lo que devuelve (o tira) `_finalize_cached_
    dataset` en las próximas llamadas; vacía, finaliza bien.
    """

    def __init__(self, monkeypatch, *, filas: int = 8643, max_fecha: date | None = None):
        meta = _meta_de_la_api_falsa()
        self.tabla: dict[str, Any] = {
            "filas": filas,
            "max_fecha": max_fecha or meta.fin,
            "columnas": _COLUMNAS,
        }
        self.dataset: dict[str, Any] | None = {
            "id": "00000000-0000-0000-0000-000000000001",
            "titulo": st._titulo(_SERIE, meta),
            "descripcion": st._descripcion(_SERIE, meta),
            "columnas": _COLUMNAS,
            "filas": filas,
            "cacheado": True,
        }
        self.catalogo: dict[str, Any] = {"estado": "ready", "filas": filas}
        self.registro: dict[str, Any] = {"dueno": _SERIE.identidad, "filas": filas}
        self.fallas_finalize: list[Any] = []
        self.escrituras: list[int] = []
        self.finalizadas = 0
        self.latidos: list[str] = []
        self.embeddings: list[str] = []

        monkeypatch.setattr(st, "estado_tabla", self._estado)
        monkeypatch.setattr(st, "escribir_atomico", self._escribir)
        monkeypatch.setattr(st, "veredicto_ws0", lambda *a, **k: None)
        monkeypatch.setattr(st, "_asegurar_dataset", self._asegurar)
        monkeypatch.setattr(st, "_actualizar_dataset", self._actualizar)
        monkeypatch.setattr(st, "_finalize_cached_dataset", self._finalizar)
        monkeypatch.setattr(st, "register_via_b_table", self._registrar)
        monkeypatch.setattr(
            "app.application.quality.heartbeat.record_ingest",
            lambda engine, rid: self.latidos.append(rid),
        )
        from app.infrastructure.celery.tasks import scraper_tasks

        tarea = MagicMock()
        tarea.delay.side_effect = lambda did: self.embeddings.append(did)
        monkeypatch.setattr(scraper_tasks, "index_dataset_embedding", tarea)

    def _estado(self, engine, serie):
        ds = self.dataset
        return st.EstadoTabla(
            existe=True,
            filas=self.tabla["filas"],
            max_fecha=self.tabla["max_fecha"],
            columnas=self.tabla["columnas"],
            dataset_id=ds["id"] if ds else None,
            titulo=ds["titulo"] if ds else None,
            descripcion=ds["descripcion"] if ds else None,
            columnas_dataset=ds["columnas"] if ds else (),
            serie_id_previa=serie.serie_id if ds else None,
            dueno_registro=self.registro["dueno"],
            dataset_filas=ds["filas"] if ds else None,
            dataset_cacheado=ds["cacheado"] if ds else None,
            catalogo_estado=self.catalogo["estado"],
            catalogo_filas=self.catalogo["filas"],
            registro_filas=self.registro["filas"],
        )

    def _escribir(self, engine, tabla, df):
        self.escrituras.append(len(df))
        self.tabla = {
            "filas": len(df),
            "max_fecha": df["fecha"].max().date(),
            "columnas": tuple(df.columns),
        }

    def _asegurar(self, engine, serie, meta, columnas, filas):
        self.dataset = {
            "id": "nuevo-id",
            "titulo": st._titulo(serie, meta),
            "descripcion": st._descripcion(serie, meta),
            "columnas": tuple(columnas),
            "filas": filas,
            "cacheado": False,
        }
        return "nuevo-id"

    def _actualizar(self, engine, *, dataset_id, serie, meta, columnas, filas):
        assert self.dataset is not None
        self.dataset.update(
            titulo=st._titulo(serie, meta),
            descripcion=st._descripcion(serie, meta),
            columnas=tuple(columnas),
            filas=filas,
        )

    def _finalizar(self, engine, **kw):
        self.finalizadas += 1
        assert self.dataset is not None
        if self.fallas_finalize:
            falla = self.fallas_finalize.pop(0)
            if isinstance(falla, Exception):
                raise falla
            self.catalogo["estado"] = falla["status"]
            self.dataset["cacheado"] = False
            return falla
        self.catalogo = {"estado": "ready", "filas": kw["row_count"]}
        self.dataset.update(cacheado=True, filas=kw["row_count"])
        return {"ok": True}

    def _registrar(self, engine, **kw):
        # Como el de verdad: registrar es lo que late.
        self.registro = {"dueno": kw["resource_identity"], "filas": kw["row_count"]}
        self.latidos.append(kw["resource_identity"])
        return True


@pytest.mark.parametrize(
    "falla",
    [
        {"ok": False, "status": "permanently_failed", "error": "ws0 post"},
        RuntimeError("se cortó la conexión"),
    ],
    ids=["finalize_rechaza", "finalize_explota"],
)
def test_una_corrida_cortada_despues_del_reemplazo_se_repara_en_la_siguiente(monkeypatch, falla):
    """Lo que reprodujo la revisión: la corrida 2 decía 'al día' y latía sana
    con el catálogo en permanently_failed (o con 1.000 filas contra 8.643)."""
    base = _BaseViva(monkeypatch, filas=1000, max_fecha=date(2005, 9, 27))
    base.fallas_finalize.append(falla)
    api = _ApiFalsa(_filas_diarias(8643))

    r1 = _procesar(api)
    assert r1.estado == "fallida", r1
    assert base.tabla["filas"] == 8643, "el reemplazo ya estaba hecho"
    assert base.catalogo["filas"] == 1000 or base.catalogo["estado"] != "ready"

    paginas = len(api.paginas)
    r2 = _procesar(api)
    assert r2.estado == "reconciliada", r2
    assert len(api.paginas) == paginas, "repara sin volver a bajar la serie"
    assert base.escrituras == [8643], "ni a reescribir la tabla"
    assert base.catalogo == {"estado": "ready", "filas": 8643}
    assert base.registro == {"dueno": _SERIE.identidad, "filas": 8643}
    assert base.dataset is not None and base.dataset["filas"] == 8643
    assert base.embeddings, "no se sabe si la corrida cortada llegó a encolar el embedding"

    latidos = len(base.latidos)
    r3 = _procesar(api)
    assert r3.estado == "al_dia", r3
    assert base.latidos[latidos:] == [_SERIE.identidad]


@pytest.mark.parametrize(
    "desalineo",
    [
        {"catalogo": {"estado": "permanently_failed", "filas": 8643}},
        {"catalogo": {"estado": "ready", "filas": 1000}},
        {"catalogo": {"estado": None, "filas": None}},
        {"registro": {"dueno": None, "filas": None}},
        {"registro": {"dueno": _SERIE.identidad, "filas": 1000}},
        {"dataset": {"titulo": "Series de Tiempo — vieja"}},
        {"dataset": {"filas": 1000}},
        {"dataset": {"cacheado": False}},
        {"dataset": None},
    ],
    ids=[
        "catalogo_no_ready",
        "catalogo_filas_viejas",
        "catalogo_sin_fila",
        "registro_ausente",
        "registro_filas_viejas",
        "dataset_titulo_viejo",
        "dataset_filas_viejas",
        "dataset_no_cacheado",
        "sin_dataset",
    ],
)
def test_al_dia_con_la_metadata_desalineada_se_repara_sin_reescribir(monkeypatch, desalineo):
    base = _BaseViva(monkeypatch)
    for parte, valor in desalineo.items():
        if valor is None:
            setattr(base, parte, None)
        else:
            getattr(base, parte).update(valor)
    api = _ApiFalsa(_filas_diarias(8643))

    res = _procesar(api)

    assert res.estado == "reconciliada", res
    assert res.motivo == "reconciliar" and res.detalle
    assert base.escrituras == [] and api.paginas == []
    assert base.finalizadas == 1
    assert base.catalogo == {"estado": "ready", "filas": 8643}
    assert base.registro == {"dueno": _SERIE.identidad, "filas": 8643}
    assert _procesar(api).estado == "al_dia"


def test_al_dia_y_en_orden_no_repara_nada(monkeypatch):
    base = _BaseViva(monkeypatch)
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "al_dia"
    assert base.finalizadas == 0 and base.embeddings == []
    assert base.latidos == [_SERIE.identidad]


def test_reconciliar_en_dry_run_no_toca_nada(monkeypatch):
    base = _BaseViva(monkeypatch)
    base.catalogo = {"estado": "permanently_failed", "filas": 8643}
    res = _procesar(_ApiFalsa(_filas_diarias(8643)), dry_run=True)
    assert res.estado == "simulada" and res.motivo == "reconciliar"
    assert base.finalizadas == 0 and base.latidos == [] and base.embeddings == []
    assert base.catalogo["estado"] == "permanently_failed"


def test_una_excepcion_despues_del_reemplazo_se_alerta(monkeypatch):
    # Antes caía como motivo "error", que `_alertas_de_ingesta` no alerta:
    # tabla nueva, metadata vieja y cero avisos.
    base = _BaseViva(monkeypatch, filas=1000, max_fecha=date(2005, 9, 27))
    base.fallas_finalize.append(RuntimeError("se cortó la conexión"))
    res = _procesar(_ApiFalsa(_filas_diarias(8643)))
    assert res.estado == "fallida" and res.motivo == "metadatos", res
    (alerta,) = st._alertas_de_ingesta([_SERIE], {_SERIE.clave: res})
    assert alerta.kind == "series_ingest_failed"
    assert alerta.key == f"{_SERIE.identidad}:metadatos"


def test_el_resumen_cuenta_las_reconciliadas_como_resueltas():
    resumen = st._resumen(
        {
            "tipo_cambio": st.ResultadoSerie(estado="reconciliada", consulto_api=True),
            "emae": st.ResultadoSerie(estado="al_dia", consulto_api=True),
        }
    )
    assert resumen["reconciliadas"] == 1 and resumen["fallidas"] == 0
    assert resumen["resueltas"] == 2


# ── el reemplazo: la tabla vieja queda como `__previa` (H025/H057) ────────


class _ConexionGrabadora:
    """Anota cada sentencia del reemplazo y contesta lo poco que pregunta.

    `permisos` va por tabla calificada (``raw."<tabla>"``) y lista los
    `(rol, privilegio, con_grant)` que no son del dueño, como `aclexplode`.
    """

    def __init__(
        self,
        *,
        existe: bool = True,
        dependientes: tuple[str, ...] = (),
        permisos: dict[str, list[tuple[str, str, bool]]] | None = None,
    ) -> None:
        self.existe = existe
        self.dependientes = list(dependientes)
        self.permisos = permisos or {}
        self.sentencias: list[str] = []
        self.transacciones = 0

    def execute(self, clausula, params=None):
        sql = " ".join(str(clausula).split())
        self.sentencias.append(sql)
        resultado = MagicMock()
        if "aclexplode" in sql:
            filas = self.permisos.get((params or {}).get("q", ""), [])
            resultado.all.return_value = [
                SimpleNamespace(rol=r, privilegio=p, con_grant=g) for r, p, g in filas
            ]
        elif "pg_rewrite" in sql:
            resultado.scalars.return_value.all.return_value = self.dependientes
        elif "to_regclass" in sql:
            resultado.scalar.return_value = self.existe
        return resultado

    def engine(self) -> MagicMock:
        engine = MagicMock()

        def _begin():
            self.transacciones += 1
            ctx = MagicMock()
            ctx.__enter__ = MagicMock(return_value=self)
            ctx.__exit__ = MagicMock(return_value=False)
            return ctx

        engine.begin.side_effect = _begin
        return engine

    def ddl(self) -> list[str]:
        return [
            s
            for s in self.sentencias
            if s.startswith(("to_sql", "DROP", "ALTER", "GRANT", "REVOKE", "CREATE"))
        ]


@pytest.fixture
def grabadora(monkeypatch):
    def _crear(**kw: Any) -> _ConexionGrabadora:
        conn = _ConexionGrabadora(**kw)
        monkeypatch.setattr(
            pd.DataFrame,
            "to_sql",
            lambda self, name, con, **k: con.sentencias.append(
                f"to_sql {k.get('schema')}.{name} {k.get('if_exists')}"
            ),
        )
        return conn

    return _crear


_DF = pd.DataFrame({"fecha": pd.to_datetime(["2026-08-31"]), "Serie de prueba": [1.0]})
_T = "cache_series_tipo_cambio"


def test_el_reemplazo_guarda_la_tabla_vieja_como_previa(grabadora):
    """Antes: DROP de la vieja. El único retorno era un dump JSON que no se
    podía restaurar (pandas 3, fecha como TEXT, metadata desalineada)."""
    conn = grabadora()
    st.escribir_atomico(conn.engine(), _T, _DF)
    assert conn.ddl() == [
        f"to_sql raw.{_T}__nueva replace",
        f'DROP TABLE IF EXISTS raw."{_T}__previa"',
        f'ALTER TABLE raw."{_T}" RENAME TO "{_T}__previa"',
        f'ALTER TABLE raw."{_T}__nueva" RENAME TO "{_T}"',
    ]
    assert conn.transacciones == 1, "el swap sigue siendo una sola transacción"
    assert conn.sentencias[0] == "SET LOCAL lock_timeout = '15s'"


def test_sin_tabla_viva_no_se_toca_la_previa(grabadora):
    # Primera escritura, o una vuelta atrás a medio hacer: la previa es lo
    # único que hay y no se borra.
    conn = grabadora(existe=False)
    st.escribir_atomico(conn.engine(), _T, _DF)
    assert conn.ddl() == [
        f"to_sql raw.{_T}__nueva replace",
        f'ALTER TABLE raw."{_T}__nueva" RENAME TO "{_T}"',
    ]


def test_una_vista_sobre_la_tabla_frena_el_reemplazo(grabadora):
    # Con DROP, la vista hacía fallar el reemplazo. Con RENAME la vista se iría
    # con la vieja a `__previa` y serviría datos viejos sin avisar.
    conn = grabadora(dependientes=("raw.cache_series_tipo_cambio_vista",))
    with pytest.raises(st._SerieFallida) as exc:
        st.escribir_atomico(conn.engine(), _T, _DF)
    assert exc.value.motivo == "reemplazo"
    assert "cache_series_tipo_cambio_vista" in exc.value.detalle
    assert not [s for s in conn.ddl() if s.startswith(("DROP", "ALTER"))]


def test_la_tabla_nueva_hereda_los_permisos_de_la_viva(grabadora):
    conn = grabadora(
        permisos={
            f'raw."{_T}"': [("openarg_sandbox_ro", "SELECT", False)],
            f'raw."{_T}__nueva"': [],
        }
    )
    st.escribir_atomico(conn.engine(), _T, _DF)
    ddl = conn.ddl()
    grant = f'GRANT SELECT ON raw."{_T}__nueva" TO openarg_sandbox_ro'
    assert grant in ddl
    assert ddl.index(grant) < ddl.index(f'ALTER TABLE raw."{_T}__nueva" RENAME TO "{_T}"')


def test_la_tabla_nueva_no_gana_permisos_que_la_viva_no_tenia(grabadora):
    # Los privilegios por defecto del esquema pueden darle a la nueva algo que
    # a la viva le sacaron a mano.
    conn = grabadora(
        permisos={
            f'raw."{_T}"': [("openarg_sandbox_ro", "SELECT", False)],
            f'raw."{_T}__nueva"': [
                ("openarg_sandbox_ro", "SELECT", False),
                ("PUBLIC", "SELECT", False),
            ],
        }
    )
    st.escribir_atomico(conn.engine(), _T, _DF)
    permisos = [s for s in conn.ddl() if s.startswith(("GRANT", "REVOKE"))]
    assert permisos == [f'REVOKE ALL ON raw."{_T}__nueva" FROM PUBLIC']


def test_con_los_mismos_permisos_no_se_toca_nada(grabadora):
    mismos = [("openarg_sandbox_ro", "SELECT", False)]
    conn = grabadora(permisos={f'raw."{_T}"': mismos, f'raw."{_T}__nueva"': mismos})
    st.escribir_atomico(conn.engine(), _T, _DF)
    assert not [s for s in conn.ddl() if s.startswith(("GRANT", "REVOKE"))]


@pytest.mark.parametrize("clave", sorted(st.SERIES_TABLAS))
def test_la_previa_entra_en_un_identificador_de_postgres(clave):
    # Postgres trunca a 63 bytes: un nombre truncado no sería `<tabla>__previa`.
    assert len(f"cache_series_{clave}__previa".encode()) <= 63


# ── la tarea ───────────────────────────────────────────────────────────────


def _con_api(monkeypatch, api: _ApiFalsa) -> None:
    real = httpx.Client

    def _cliente(*a, **k):
        k.pop("transport", None)
        return real(*a, transport=httpx.MockTransport(api), **k)

    monkeypatch.setattr(st.httpx, "Client", _cliente)
    monkeypatch.setattr(st, "get_sync_engine", lambda: MagicMock())


def _capturar_alertas(monkeypatch) -> list[Any]:
    enviados: list[Any] = []

    def _notify(engine, alertas, heading):
        enviados.extend(alertas)
        return {"sent": len(alertas)}

    monkeypatch.setattr("app.application.quality.alerting.notify", _notify)
    return enviados


def test_sin_api_la_tarea_falla_en_vez_de_registrar_un_exito(monkeypatch):
    # El latido de la tarea sale de `task_success`: si termina bien sin haber
    # mirado nada, cuenta como sana (corrió "bien" todos los meses sin escribir).
    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    _con_api(monkeypatch, _ApiFalsa([], status=503))
    monkeypatch.setattr(st, "estado_tabla", lambda *a: pytest.fail("no debería leer la base"))
    with pytest.raises(RuntimeError, match="ninguna serie"):
        st.ingest_series_tiempo.run(claves=["tipo_cambio", "emae"])


def test_la_tarea_cuenta_por_estado(monkeypatch):
    _con_api(monkeypatch, _ApiFalsa(_filas_diarias(8643)))
    resultados = iter(
        [
            st.ResultadoSerie(estado="escrita", consulto_api=True),
            st.ResultadoSerie(estado="al_dia", consulto_api=True),
            st.ResultadoSerie(
                estado="rechazada", motivo="x", fin_api="2026-08-31", consulto_api=True
            ),
        ]
    )
    monkeypatch.setattr(st, "procesar_serie", lambda *a, **k: next(resultados))
    enviados = _capturar_alertas(monkeypatch)
    resumen = st.ingest_series_tiempo.run(claves=["tipo_cambio", "emae", "desempleo"])
    assert (resumen["escritas"], resumen["al_dia"], resumen["rechazadas"]) == (1, 1, 1)
    assert resumen["resueltas"] == 3
    (alerta,) = enviados
    assert alerta.kind == "series_cache_stale"
    assert alerta.key == "series_tiempo::series-tiempo-desempleo@2026-08-31"


def test_el_limite_de_tiempo_no_se_traga(monkeypatch):
    from celery.exceptions import SoftTimeLimitExceeded

    _con_api(monkeypatch, _ApiFalsa(_filas_diarias(3)))

    def _boom(*a, **k):
        raise SoftTimeLimitExceeded()

    monkeypatch.setattr(st, "procesar_serie", _boom)
    with pytest.raises(SoftTimeLimitExceeded):
        st.ingest_series_tiempo.run(claves=["tipo_cambio"])


# ── la alarma: tabla atrasada vs fuente atrasada ──────────────────────────

_HOY = date(2026, 10, 4)


def test_tabla_atras_de_la_api_es_bug_nuestro():
    assert st.clasificar_frescura(_meta(), _estado(), _HOY) == ["series_cache_stale"]


def test_fuente_con_is_updated_false_es_fuente_atrasada():
    estado = _estado(filas=8643, max_fecha=date(2026, 8, 31))
    assert st.clasificar_frescura(_meta(actualizada=False), estado, _HOY) == ["series_source_stale"]


def test_las_dos_a_la_vez():
    hallazgos = st.clasificar_frescura(_meta(actualizada=False), _estado(), _HOY)
    assert hallazgos == ["series_cache_stale", "series_source_stale"]


def test_al_dia_no_dice_nada():
    estado = _estado(filas=8643, max_fecha=date(2026, 8, 31))
    assert st.clasificar_frescura(_meta(), estado, _HOY) == []


def test_tabla_inexistente_u_otra_serie_es_tabla_atrasada():
    assert st.clasificar_frescura(_meta(), _estado(existe=False, max_fecha=None), _HOY) == [
        "series_cache_stale"
    ]
    otra = _estado(serie_id_previa="OTRA.1", filas=8643, max_fecha=date(2026, 8, 31))
    assert st.clasificar_frescura(_meta(), otra, _HOY) == ["series_cache_stale"]


@pytest.mark.parametrize(
    ("fin", "esperado"),
    [(date(2026, 7, 1), []), (date(2026, 3, 1), ["series_source_stale"])],
)
def test_sin_is_updated_manda_el_margen_por_frecuencia(fin, esperado):
    meta = _meta(actualizada=None, frecuencia="mensual", fin=fin)
    estado = _estado(max_fecha=fin, filas=8643)
    assert st.clasificar_frescura(meta, estado, _HOY) == esperado


def test_la_alarma_alerta_con_clases_distintas(monkeypatch):
    api = _ApiFalsa(_filas_diarias(8643), is_updated="False")
    _con_api(monkeypatch, api)
    serie_emae = st.SerieETL("emae", "emae", "143.3_NO_PR_2004_A_21")
    monkeypatch.setattr(st, "series_del_catalogo", lambda: ([_SERIE, serie_emae], {}))
    estados = {
        "tipo_cambio": _estado(),  # atrás de la API, y la fuente atrasada
        "emae": _estado(filas=8643, max_fecha=date(2026, 8, 31), serie_id_previa=None),
    }
    monkeypatch.setattr(st, "estado_tabla", lambda engine, serie: estados[serie.clave])
    enviados = _capturar_alertas(monkeypatch)

    informe = st.check_series_freshness.run()

    assert [f["serie"] for f in informe["series_cache_stale"]] == ["tipo_cambio"]
    assert [f["serie"] for f in informe["series_source_stale"]] == ["tipo_cambio", "emae"]
    clases = sorted((a.kind, a.key) for a in enviados)
    assert clases == [
        ("series_cache_stale", "series_tiempo::series-tiempo-tipo_cambio@2026-08-31"),
        ("series_source_stale", "series_tiempo::series-tiempo-emae@2026-08-31"),
        ("series_source_stale", "series_tiempo::series-tiempo-tipo_cambio@2026-08-31"),
    ]


def test_la_alarma_en_dry_run_no_alerta(monkeypatch):
    _con_api(monkeypatch, _ApiFalsa(_filas_diarias(8643)))
    monkeypatch.setattr(st, "series_del_catalogo", lambda: ([_SERIE], {}))
    monkeypatch.setattr(st, "estado_tabla", lambda engine, serie: _estado())
    monkeypatch.setattr(
        "app.application.quality.alerting.notify",
        lambda *a, **k: pytest.fail("dry_run no alerta"),
    )
    informe = st.check_series_freshness.run(dry_run=True)
    assert informe["series_cache_stale"]


def test_la_alarma_sin_api_falla(monkeypatch):
    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    _con_api(monkeypatch, _ApiFalsa([], status=503))
    monkeypatch.setattr(st, "series_del_catalogo", lambda: ([_SERIE], {}))
    with pytest.raises(RuntimeError, match="no respondió"):
        st.check_series_freshness.run()


# ── agenda ─────────────────────────────────────────────────────────────────


def _entrada(tarea: str) -> dict[str, Any]:
    from app.infrastructure.celery.app import celery_app

    (entrada,) = [e for e in celery_app.conf.beat_schedule.values() if e["task"] == tarea]
    return entrada


def test_la_ingesta_corre_todos_los_dias_a_las_1845_hora_argentina():
    from app.infrastructure.celery.app import celery_app

    assert celery_app.conf.timezone == "America/Argentina/Buenos_Aires"
    cron = _entrada("openarg.ingest_series_tiempo")["schedule"]
    assert cron.hour == {18} and cron.minute == {45}
    assert len(cron.day_of_month) == 31 and len(cron.day_of_week) == 7
    assert _entrada("openarg.ingest_series_tiempo")["options"]["queue"] == "ingest"


def test_la_alarma_corre_despues_de_la_ingesta_en_la_misma_cola():
    ingesta = _entrada("openarg.ingest_series_tiempo")["schedule"]
    alarma = _entrada("openarg.check_series_freshness")["schedule"]
    assert min(alarma.hour) * 60 + min(alarma.minute) > min(ingesta.hour) * 60 + min(ingesta.minute)
    assert _entrada("openarg.check_series_freshness")["options"]["queue"] == "ingest"
