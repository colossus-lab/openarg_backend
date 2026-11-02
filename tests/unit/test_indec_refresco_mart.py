"""Después de recargar en el lugar, el mart se refresca una vez y al final.

Lo que hace Postgres con la recarga está en
`tests/integration/test_indec_recarga_con_mart.py`. Acá va lo que decide la
tarea: cuándo pide el refresco.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.infrastructure.celery.tasks import indec_tasks


@pytest.fixture
def tarea(monkeypatch: pytest.MonkeyPatch) -> dict:
    """`ingest_indec` con un archivo de dos hojas y sin base ni red."""
    orden: list[str] = []
    refresco = MagicMock(return_value=["inflacion_argentina"])

    def guardar(_engine, _df, tabla: str) -> bool:
        orden.append(tabla)
        return tabla.endswith("_a")

    def refrescar(engine, tablas: list[str]) -> list[str]:
        orden.append("refresco")
        return refresco(engine, tablas)

    monkeypatch.setattr(indec_tasks, "get_sync_engine", MagicMock())
    monkeypatch.setattr(indec_tasks, "INDEC_DATASETS", [{"id": "ipc", "name": "IPC", "url": "u"}])
    monkeypatch.setattr(indec_tasks, "resolve_dated_url", lambda d: d["url"])
    monkeypatch.setattr(
        indec_tasks,
        "_download_and_parse",
        lambda _u: {"a": pd.DataFrame({"x": [1]}), "b": pd.DataFrame({"x": [2]})},
    )
    monkeypatch.setattr(indec_tasks, "_register_dataset", lambda *a, **k: None)
    monkeypatch.setattr(
        "app.infrastructure.celery.tasks._db.register_via_b_table", lambda *a, **k: True
    )
    monkeypatch.setattr(indec_tasks, "_store_sheet", guardar)
    monkeypatch.setattr(indec_tasks, "_refresh_marts_reading", refrescar)
    return {"orden": orden, "refresco": refresco}


def test_refresca_despues_de_la_ultima_hoja(tarea: dict) -> None:
    resultado = indec_tasks.ingest_indec.run()

    assert tarea["orden"] == ["cache_indec_ipc_a", "cache_indec_ipc_b", "refresco"]
    assert tarea["refresco"].call_args.args[1] == ["cache_indec_ipc_a"]
    assert resultado["reloaded_in_place"] == ["cache_indec_ipc_a"]
    assert resultado["marts_refreshing"] == ["inflacion_argentina"]
    assert resultado["errors"] == 0


def test_sin_recargas_en_el_lugar_no_pide_refresco(
    tarea: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(indec_tasks, "_store_sheet", lambda *a: False)

    resultado = indec_tasks.ingest_indec.run()

    tarea["refresco"].assert_not_called()
    assert "reloaded_in_place" not in resultado


def test_pide_el_refresco_con_demora_y_solo_de_los_marts_que_leen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """La demora es para no toparse con el lock de un refresco a mitad de la carga."""
    from app.infrastructure.celery.tasks import mart_tasks

    conn = MagicMock()
    conn.execute.return_value = [("inflacion_argentina",)]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    refresh_mart = MagicMock()
    monkeypatch.setattr(mart_tasks, "refresh_mart", refresh_mart)
    monkeypatch.setattr(
        "app.application.marts.mart.load_all_marts",
        lambda _d: [
            SimpleNamespace(id="inflacion_argentina", view_name="inflacion_argentina"),
            SimpleNamespace(id="otro", view_name="otro"),
        ],
    )

    assert indec_tasks._refresh_marts_reading(engine, ["cache_indec_ipc_a"]) == [
        "inflacion_argentina"
    ]

    refresh_mart.apply_async.assert_called_once()
    llamada = refresh_mart.apply_async.call_args.kwargs
    assert llamada["args"] == ["inflacion_argentina"]
    assert llamada["countdown"] == indec_tasks._FINAL_MART_REFRESH_DELAY_SECONDS


@pytest.mark.parametrize(
    ("exc", "esperado"),
    [
        (SimpleNamespace(orig=SimpleNamespace(sqlstate="2BP01")), True),
        (Exception("cannot drop table x because other objects depend on it"), True),
        (Exception("connection refused"), False),
    ],
)
def test_reconoce_el_rechazo_del_drop(exc, esperado: bool) -> None:
    assert indec_tasks._blocked_by_dependents(exc) is esperado
