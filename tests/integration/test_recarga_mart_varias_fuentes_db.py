"""Lo que un mart de varias fuentes puede perder sin quedar vacío, y lo que ve quien lee.

`mart.inflacion_argentina` une tres tablas. Si una se pierde el mart no queda en
cero, así que mirar sólo si quedó vacío no alcanza. La otra mitad es la decisión
central de la recarga: `DELETE` y no `TRUNCATE`, porque quien lee durante la carga
tiene que seguir viendo las filas de antes.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pytest
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.quality import alerting
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import indec_tasks as indec
from app.infrastructure.celery.tasks._db import (
    TableReplaceRefused,
    replace_table_keeping_dependents,
)


def _engine_or_skip() -> Engine:
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _filas(n: int, valor: str = "1.5") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "periodo": [f"20{10 + i // 12:02d}-{i % 12 + 1:02d}-01 00:00:00" for i in range(n)],
            "valor": [valor] * n,
        }
    )


@dataclass
class TresFuentes:
    engine: Engine
    tablas: dict[str, str]
    mart: str

    def por_fuente(self) -> dict[str, int]:
        with self.engine.connect() as conn:
            filas = conn.execute(
                text(f'SELECT fuente, count(*) FROM mart."{self.mart}" GROUP BY 1')  # noqa: S608
            ).fetchall()
            conn.rollback()
        return {str(f[0]): int(f[1]) for f in filas}

    def filas_de(self, fuente: str) -> int:
        with self.engine.connect() as conn:
            n = conn.execute(
                text(f'SELECT count(*) FROM raw."{self.tablas[fuente]}"')  # noqa: S608
            ).scalar()
            conn.rollback()
        return int(n or 0)


@pytest.fixture
def fuentes() -> Iterator[TresFuentes]:
    """Tres tablas de 100 filas y un mart que las une, con el filtro del mart real."""
    engine = _engine_or_skip()
    sufijo = uuid.uuid4().hex[:8]
    tablas = {f: f"cache_indec_zz_{sufijo}_{f}" for f in "abc"}
    mart = f"zz_mart_{sufijo}"
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        for tabla in tablas.values():
            _filas(100).to_sql(tabla, conn, schema="raw", index=False)
        union = " UNION ALL ".join(
            f"SELECT '{f}'::text AS fuente, periodo::timestamp AS periodo, valor::numeric AS valor "
            f"FROM raw.\"{t}\" WHERE valor ~ '^-?[0-9]+(\\.[0-9]+)?$'"
            for f, t in tablas.items()
        )
        conn.execute(text(f'CREATE MATERIALIZED VIEW mart."{mart}" AS {union}'))
    yield TresFuentes(engine, tablas, mart)
    with engine.begin() as conn:
        conn.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS mart."{mart}"'))
        for tabla in tablas.values():
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))


def test_rechaza_que_el_mart_pierda_una_fuente_entera(fuentes: TresFuentes) -> None:
    """`3,4` no pasa el filtro del mart: la tabla sigue llena y la fuente desaparece."""
    with pytest.raises(TableReplaceRefused, match="would lose"):
        replace_table_keeping_dependents(
            fuentes.engine, _filas(100, valor="3,4"), fuentes.tablas["a"]
        )

    assert fuentes.por_fuente() == {"a": 100, "b": 100, "c": 100}
    with fuentes.engine.connect() as conn:
        valores = conn.execute(
            text(f'SELECT DISTINCT valor FROM raw."{fuentes.tablas["a"]}"')  # noqa: S608
        ).scalars()
        assert list(valores) == ["1.5"]
        conn.rollback()


def test_rechaza_que_una_fuente_se_duplique(fuentes: TresFuentes) -> None:
    with pytest.raises(TableReplaceRefused, match="rows would replace"):
        replace_table_keeping_dependents(fuentes.engine, _filas(200), fuentes.tablas["a"])

    assert fuentes.por_fuente() == {"a": 100, "b": 100, "c": 100}
    assert fuentes.filas_de("a") == 100


def test_rechaza_un_archivo_que_trae_bastante_menos(fuentes: TresFuentes) -> None:
    """Con el piso anterior, del 50 %, estas 60 filas se cargaban."""
    with pytest.raises(TableReplaceRefused, match="rows would replace"):
        replace_table_keeping_dependents(fuentes.engine, _filas(60), fuentes.tablas["a"])

    assert fuentes.por_fuente() == {"a": 100, "b": 100, "c": 100}


def test_rechaza_que_el_mart_gane_mas_de_lo_que_gano_la_tabla(fuentes: TresFuentes) -> None:
    """La tabla no cambia de tamaño, pero antes el filtro dejaba afuera a la mitad."""
    a_medias = _filas(100)
    a_medias.loc[a_medias.index[:50], "valor"] = "s/d"
    with fuentes.engine.begin() as conn:
        conn.execute(text(f'DELETE FROM raw."{fuentes.tablas["a"]}"'))  # noqa: S608
        a_medias.to_sql(fuentes.tablas["a"], conn, schema="raw", if_exists="append", index=False)
        conn.execute(text(f'REFRESH MATERIALIZED VIEW mart."{fuentes.mart}"'))
    assert fuentes.por_fuente()["a"] == 50

    with pytest.raises(TableReplaceRefused, match="would gain"):
        replace_table_keeping_dependents(fuentes.engine, _filas(100), fuentes.tablas["a"])

    assert fuentes.por_fuente()["a"] == 50


def test_acepta_el_crecimiento_normal_y_las_fechas_corregidas(fuentes: TresFuentes) -> None:
    """La forma de la recarga real del IPC: unas filas más y un mes que cambia de fecha."""
    with fuentes.engine.begin() as conn:
        conn.execute(
            text(
                f"""UPDATE raw."{fuentes.tablas["a"]}" SET periodo = '2018-04-26 00:00:00'
                    WHERE periodo = '2018-04-01 00:00:00'"""  # noqa: S608
            )
        )
        conn.execute(text(f'REFRESH MATERIALIZED VIEW mart."{fuentes.mart}"'))

    replace_table_keeping_dependents(fuentes.engine, _filas(106), fuentes.tablas["a"])

    assert fuentes.por_fuente() == {"a": 106, "b": 100, "c": 100}
    with fuentes.engine.connect() as conn:
        rezagadas = conn.execute(
            text(
                f'SELECT count(*) FROM mart."{fuentes.mart}" '  # noqa: S608
                "WHERE extract(day from periodo) <> 1"
            )
        ).scalar()
        conn.rollback()
    assert rezagadas == 0


def test_acepta_que_el_filtro_descarte_unas_pocas_filas(fuentes: TresFuentes) -> None:
    """Un `s/d` suelto en el origen no alcanza para frenar la carga del mes."""
    con_huecos = _filas(100)
    con_huecos.loc[con_huecos.index[:5], "valor"] = "s/d"

    replace_table_keeping_dependents(fuentes.engine, con_huecos, fuentes.tablas["a"])

    assert fuentes.por_fuente()["a"] == 95


def test_quien_lee_durante_la_carga_sigue_viendo_lo_de_antes(
    fuentes: TresFuentes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Falla si `DELETE` pasa a ser `TRUNCATE`.

    `TRUNCATE` toma ACCESS EXCLUSIVE hasta el commit: este lector esperaría su
    turno y el `lock_timeout` lo cortaría. Con `DELETE` no espera nada y lee las
    filas de antes, porque la carga todavía no se confirmó.
    """
    original = pd.DataFrame.to_sql
    vistos: list[tuple[int, int]] = []

    def lee_en_medio_de_la_carga(self, *args, **kwargs):
        resultado = original(self, *args, **kwargs)
        with fuentes.engine.connect() as lector:
            lector.execute(text("SET LOCAL lock_timeout = '500ms'"))
            tabla = lector.execute(
                text(f'SELECT count(*) FROM raw."{fuentes.tablas["a"]}"')  # noqa: S608
            ).scalar()
            mart = lector.execute(
                text(f"SELECT count(*) FROM mart.\"{fuentes.mart}\" WHERE fuente = 'a'")  # noqa: S608
            ).scalar()
            lector.rollback()
        vistos.append((int(tabla or 0), int(mart or 0)))
        return resultado

    monkeypatch.setattr(pd.DataFrame, "to_sql", lee_en_medio_de_la_carga)

    replace_table_keeping_dependents(fuentes.engine, _filas(106), fuentes.tablas["a"])

    assert vistos == [(100, 100)]
    assert fuentes.por_fuente()["a"] == 106


# ── los avisos ───────────────────────────────────────────────────────────────


@pytest.fixture
def canal(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """`alert_log` de verdad; lo único de mentira es el envío a Telegram."""
    engine = _engine_or_skip()
    que = f"zz_ipc_{uuid.uuid4().hex[:8]} [hoja]"
    estado: dict[str, Any] = {"engine": engine, "que": que, "enviados": [], "acepta": True}

    def enviar(cuerpo: str) -> bool:
        estado["enviados"].append(cuerpo)
        return bool(estado["acepta"])

    monkeypatch.setattr(alerting, "_send", enviar)
    yield estado
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM public.alert_log WHERE key LIKE :p"),
            {"p": f"{que}%"},
        )


def _un_mes(monkeypatch: pytest.MonkeyPatch, canal: dict[str, Any], mes: int, motivo: str) -> int:
    """Corre el aviso en un mes dado y dice cuántos mensajes salieron."""
    monkeypatch.setattr(
        indec, "_utc_now", lambda: datetime(2027, mes, 15, tzinfo=UTC), raising=False
    )
    antes = len(canal["enviados"])
    indec._report_failures(canal["engine"], [(canal["que"], motivo)])
    return len(canal["enviados"]) - antes


def test_la_misma_falla_avisa_todos_los_meses(
    canal: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Con la clave fija avisaba en los meses 1, 3 y 10: el resto, en silencio."""
    envios = [_un_mes(monkeypatch, canal, mes, "cambiaron las columnas") for mes in range(1, 13)]

    assert envios == [1] * 12


def test_un_motivo_distinto_en_el_mismo_mes_avisa(
    canal: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _un_mes(monkeypatch, canal, 3, "cambiaron las columnas") == 1
    assert _un_mes(monkeypatch, canal, 3, "se cortó la conexión") == 1


def test_el_mismo_motivo_en_el_mismo_mes_no_repite(
    canal: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Un reintento del mismo mes no es una noticia nueva."""
    assert _un_mes(monkeypatch, canal, 3, "cambiaron las columnas") == 1
    assert _un_mes(monkeypatch, canal, 3, "cambiaron las columnas") == 0


def test_un_envio_fallido_no_silencia_el_mes_siguiente(
    canal: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    canal["acepta"] = False
    assert _un_mes(monkeypatch, canal, 3, "cambiaron las columnas") == 1

    canal["acepta"] = True
    assert _un_mes(monkeypatch, canal, 4, "cambiaron las columnas") == 1


def test_el_motivo_va_escapado_para_telegram(
    canal: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Un `<` sin escapar hace que Telegram rechace el mensaje entero."""
    _un_mes(monkeypatch, canal, 3, "valor <class 'float'> & otras")

    cuerpo = canal["enviados"][-1]
    assert "<class" not in cuerpo
    assert "&lt;class" in cuerpo


def test_las_demas_alertas_siguen_con_su_clave_estable(canal: dict[str, Any]) -> None:
    """El arreglo es de la clave de INDEC: `alerting` no cambia para nadie más."""
    alerta = alerting.Alert(kind="zz_otra", key=f"{canal['que']}:estable", title="t")

    enviados = [alerting.notify(canal["engine"], [alerta], heading="h")["sent"] for _ in range(12)]

    assert [i + 1 for i, n in enumerate(enviados) if n] == [1, 3, 10]


# ── el límite de tiempo ──────────────────────────────────────────────────────


@pytest.fixture
def corrida(fuentes: TresFuentes, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """`ingest_indec` con una hoja que falla, una que carga y una que agota el tiempo."""
    estado: dict[str, Any] = {"alertas": [], "refrescos": 0}
    original = indec._ingest_sheet

    def por_hoja(engine, ds_info, sheet_name, df):
        if sheet_name == "falla":
            raise RuntimeError("cambiaron las columnas")
        if sheet_name == "tiempo":
            raise SoftTimeLimitExceeded()
        return original(engine, ds_info, sheet_name, df)

    def refrescar() -> None:
        estado["refrescos"] += 1

    monkeypatch.setattr(indec, "get_sync_engine", lambda: fuentes.engine)
    monkeypatch.setattr(indec, "_ingest_sheet", por_hoja)
    monkeypatch.setattr(indec, "_refresh_indec_marts", refrescar)
    monkeypatch.setattr(indec, "_register_dataset", lambda *a, **k: None)
    monkeypatch.setattr(indec, "register_via_b_table", lambda *a, **k: True)
    monkeypatch.setattr(fuentes.engine, "dispose", lambda: None)
    monkeypatch.setattr(
        alerting,
        "notify",
        lambda engine, alerts, heading: estado["alertas"].extend(alerts),
    )
    monkeypatch.setattr(
        indec,
        "INDEC_DATASETS",
        [{"id": "zz", "name": "Prueba", "url": "http://x/y.xls"}],
    )
    monkeypatch.setattr(
        indec,
        "_download_and_parse",
        lambda url: {
            "falla": _filas(1),
            fuentes.tablas["a"].removeprefix("cache_indec_zz_"): _filas(106),
            "tiempo": _filas(1),
        },
    )
    monkeypatch.setattr(
        indec,
        "_sanitize_table_name",
        lambda sufijo: fuentes.tablas["a"] if sufijo.endswith("_a") else f"cache_{sufijo}",
    )
    return estado


def test_el_limite_de_tiempo_no_se_lleva_el_aviso_ni_el_refresco(
    fuentes: TresFuentes, corrida: dict[str, Any]
) -> None:
    with pytest.raises(SoftTimeLimitExceeded):
        indec.ingest_indec.run()

    motivos = [a.title for a in corrida["alertas"]]
    assert any("falla" in m for m in motivos), "la falla de antes del corte tiene que salir"
    assert any("tiempo" in m for m in motivos), "el corte también es una falla"
    assert corrida["refrescos"] == 1, "la hoja que sí cargó tiene que llegar al catálogo"
    assert fuentes.por_fuente()["a"] == 106
