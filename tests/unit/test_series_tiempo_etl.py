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
  menos filas.
- Una corrida que no pudo consultar la API no termina en éxito.
- La alarma separa "la tabla está atrás de la API" (bug nuestro) de "la API
  está atrás" (la fuente).

La API va en memoria con `httpx.MockTransport`; la base, con dobles por
función. El camino contra Postgres de verdad está en
`tests/integration/test_series_tiempo_etl_db.py`.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from app.infrastructure.celery.tasks import series_tiempo_tasks as st

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


def test_si_cambio_la_serie_achicarse_es_esperable():
    # El catálogo cambió el id (p. ej. actividad_industrial a otra serie): la
    # comparación de filas contra la serie anterior no dice nada.
    assert (
        st.motivo_para_rechazar(
            filas_nuevas=120, fin_nuevo=date(2026, 7, 1), estado=_estado(), misma_serie=False
        )
        is None
    )


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
        self.registros: list[dict[str, Any]] = []
        self.latidos: list[str] = []
        self.embeddings: list[str] = []
        self.finalizadas: list[dict[str, Any]] = []
        self.registro_ok = True
        monkeypatch.setattr(st, "estado_tabla", lambda engine, serie: estado)
        monkeypatch.setattr(
            st,
            "escribir_atomico",
            lambda engine, tabla, df: self.escrituras.append((tabla, len(df))),
        )
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


def _procesar(api: _ApiFalsa, **kw: Any) -> st.ResultadoSerie:
    with api.cliente() as c:
        return st.procesar_serie(MagicMock(), c, _SERIE, **kw)


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
    base = _Base(monkeypatch, _estado(filas=8643, max_fecha=date(2026, 8, 31)))
    api = _ApiFalsa(_filas_diarias(8643))
    res = _procesar(api)
    assert res.estado == "al_dia"
    assert base.latidos == ["series_tiempo::series-tiempo-tipo_cambio"]
    assert base.escrituras == [] and api.paginas == [], "sólo el pedido liviano de metadata"


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
