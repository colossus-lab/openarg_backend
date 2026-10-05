"""Backfill de tablas rotas por el colector: qué lista y qué despacha."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.celery.tasks import collector_tasks as ct
from app.infrastructure.celery.tasks import header_backfill as hb


def _row(dataset_id, table_name, *, row_count=0, columns_json=None, url="https://x/y.csv"):
    return SimpleNamespace(
        dataset_id=dataset_id,
        table_name=table_name,
        row_count=row_count,
        columns_json=columns_json,
        portal="diputados",
        format="csv",
        download_url=url,
    )


def _phys(columns, approx):
    return hb._Physical(schema_name="raw", columns=columns, approx_rows=approx)


def test_lista_encabezados_rotos_y_tablas_truncadas():
    rows = [
        _row("d-leyes", "diputados__leyes_sancionadas__11b91158__v2"),
        # La forma que deja el DROP + recreate con un último chunk sano:
        # nombres buenos y 7.048 de 185.618 filas.
        _row(
            "d-cyt",
            "datos_gob_ar__personal_de_ciencia_y_tecnologia__6422ae1d__v1",
            row_count=185618,
        ),
        _row("d-ok", "diputados__leyes_sancionadas__7ce083ad__v1", row_count=1321),
    ]
    physical = {
        ("raw", "diputados__leyes_sancionadas__11b91158__v2"): _phys(
            ["HCDN285290 / HCDN287440", "Senado", "2025-12-26T00:00:00", "2025-12-17T00:00:00_2"],
            1319,
        ),
        ("raw", "datos_gob_ar__personal_de_ciencia_y_tecnologia__6422ae1d__v1"): _phys(
            ["persona_id", "nombre", "apellido", "_source_dataset_id"], 7048
        ),
        ("raw", "diputados__leyes_sancionadas__7ce083ad__v1"): _phys(
            ["PROYECTO_ID", "CAMARA_SANCIONADORA", "LEY", "_source_dataset_id"], 1321
        ),
    }
    counted: list[str] = []

    def _exact(schema, bare):
        counted.append(bare)
        return 7048

    candidates, skipped = hb.classify(rows, physical, _exact)

    assert [(c.dataset_id, c.reason) for c in candidates] == [
        ("d-leyes", hb.REASON_HEADER),
        ("d-cyt", hb.REASON_DEFICIT),
    ]
    assert candidates[1].detail == "7048 de 185618 filas"
    # Sólo se cuenta lo que la estimación ya hace sospechar.
    assert counted == ["datos_gob_ar__personal_de_ciencia_y_tecnologia__6422ae1d__v1"]
    assert not skipped


def test_el_conteo_exacto_desmiente_a_la_estimacion():
    rows = [_row("d1", "t__v1", row_count=50000)]
    physical = {("raw", "t__v1"): _phys(["a", "b"], 100)}  # reltuples viejo
    candidates, _ = hb.classify(rows, physical, lambda s, b: 50000)
    assert candidates == []


def test_salteadas_se_cuentan_y_no_se_despachan():
    rows = [
        _row("d-nourl", "t1__v1", url=""),
        _row("d-nostats", "t2__v1", row_count=20000),
        _row("d-chica", "t3__v1", row_count=900),
        _row("d-sin", "t4__v1"),
    ]
    physical = {
        ("raw", "t1__v1"): _phys(["2025-12-26T00:00:00", "HCDN285290"], 10),
        ("raw", "t2__v1"): _phys(["a", "b"], -1),
        ("raw", "t3__v1"): _phys(["a", "b"], 10),
    }
    candidates, skipped = hb.classify(rows, physical, lambda s, b: 0)
    assert candidates == []
    assert skipped == {"sin_url": 1, "sin_estimacion": 1, "sin_tabla_fisica": 1}


def test_una_candidata_por_dataset_y_nombres_calificados():
    rows = [
        _row("d1", "raw.t1__v2"),
        _row("d1", "t1__v1"),
    ]
    broken = ["2025-12-26T00:00:00", "HCDN285290"]
    physical = {("raw", "t1__v2"): _phys(broken, 10), ("raw", "t1__v1"): _phys(broken, 10)}
    candidates, _ = hb.classify(rows, physical, lambda s, b: 0)
    assert [c.table_name for c in candidates] == ["raw.t1__v2"]


def test_dry_run_no_despacha(capsys):
    candidate = hb.Candidate("d1", "t__v1", "diputados", "csv", hb.REASON_HEADER, "x")
    with (
        patch("app.infrastructure.celery.tasks._db.get_sync_engine"),
        patch.object(hb, "find_candidates", return_value=([candidate], {})),
        patch.object(hb, "dispatch") as dispatch,
    ):
        assert hb.main(["--json"]) == 0

    dispatch.assert_not_called()
    out = json.loads(capsys.readouterr().out)
    assert out["resumen"]["dry_run"] is True
    assert out["resumen"]["candidatas"] == 1
    assert out["candidatas"][0]["dataset_id"] == "d1"


def test_execute_despacha_con_force_reparse():
    candidates = [
        hb.Candidate("d1", "t1__v1", "p", "csv", hb.REASON_HEADER, ""),
        hb.Candidate("d2", "t2__v1", "p", "csv", hb.REASON_DEFICIT, ""),
    ]
    with patch(
        "app.infrastructure.celery.tasks.collector_tasks.collect_dataset.apply_async"
    ) as apply_async:
        assert hb.dispatch(candidates, step_seconds=7) == 2

    calls = apply_async.call_args_list
    assert calls[0].kwargs == {"args": ["d1"], "kwargs": {"force_reparse": True}, "countdown": 0}
    assert calls[1].kwargs["countdown"] == 7


# ── backfill: parser_version y force_reparse ───────────────────────────────


class _Conn:
    def __init__(self, first_row):
        self.first_row = first_row
        self.calls: list[tuple[str, dict]] = []

    def execute(self, stmt, params=None):
        self.calls.append((str(stmt), dict(params or {})))
        result = MagicMock()
        result.fetchone.return_value = self.first_row if len(self.calls) == 1 else (1,)
        return result

    def rollback(self):
        return None


def _engine_with(conn):
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def test_el_chequeo_de_archivo_sin_cambios_puede_exigir_el_mismo_parser():
    row = SimpleNamespace(schema_name="raw", table_name="t__v1", row_count=10)
    conn = _Conn(row)

    found = ct._unchanged_since_last_collect(
        _engine_with(conn), resource_identity="p::s", file_hash="abc", parser_version="p:new"
    )

    assert found == "t__v1"
    sql, params = conn.calls[0]
    assert "v.parser_version = :pv" in sql
    assert params["pv"] == "p:new"


def test_sin_parser_version_la_clave_sigue_siendo_solo_el_archivo():
    row = SimpleNamespace(schema_name="raw", table_name="t__v1", row_count=10)
    conn = _Conn(row)

    ct._unchanged_since_last_collect(_engine_with(conn), resource_identity="p::s", file_hash="abc")

    sql, params = conn.calls[0]
    assert "parser_version" not in sql
    assert "pv" not in params


def test_reparsear_por_cambio_de_parser_esta_apagado_por_defecto(monkeypatch):
    monkeypatch.delenv("OPENARG_REPARSE_ON_PARSER_CHANGE", raising=False)
    assert ct._reparse_on_parser_change() is False
    monkeypatch.setenv("OPENARG_REPARSE_ON_PARSER_CHANGE", "1")
    assert ct._reparse_on_parser_change() is True


def _collect_mocks(dataset_row):
    retry_row = SimpleNamespace(retry_count=0)
    prev_cached_row = SimpleNamespace(
        is_cached=False,
        table_name=None,
        cached_row_count=None,
        columns_json=None,
        s3_key=None,
        error_message=None,
    )
    conn = MagicMock()

    def execute_side_effect(stmt, params=None):
        query = str(stmt)
        result = MagicMock()
        if "SELECT title, download_url, format, portal, source_id" in query:
            result.fetchone.return_value = dataset_row
        elif "SELECT retry_count FROM raw.cached_datasets" in query:
            result.fetchone.return_value = retry_row
        elif "WHERE table_name = :tn AND status = 'ready'" in query:
            result.fetchone.return_value = None
        elif "SELECT d.is_cached," in query:
            result.fetchone.return_value = prev_cached_row
        return result

    conn.execute.side_effect = execute_side_effect
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.mark.parametrize("force_reparse", [False, True])
def test_force_reparse_parsea_aunque_el_archivo_no_haya_cambiado(force_reparse):
    dataset_row = SimpleNamespace(
        title="Proyectos parlamentarios",
        download_url="https://example.com/proyectos.csv",
        format="csv",
        portal="diputados",
        source_id="proy-1",
    )
    engine = _collect_mocks(dataset_row)
    with (
        patch.object(ct, "get_sync_engine", return_value=engine),
        patch.object(ct, "_has_temp_space", return_value=True),
        patch.object(ct, "_stream_download", return_value=123),
        patch.object(ct, "_upload_file_to_s3", return_value="k"),
        patch.object(ct, "_ensure_cached_entry"),
        patch.object(ct, "_ensure_postgis_geom"),
        patch.object(ct, "_settle_reserved_row"),
        patch.object(ct, "_unchanged_since_last_collect", return_value="t__v1") as unchanged,
        patch.object(ct, "_load_csv_chunked", return_value=(10, ["col1"], False)) as load,
        patch("app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding.delay"),
        patch("app.infrastructure.celery.tasks.catalog_enrichment_tasks.enrich_single_table.delay"),
    ):
        result = ct.collect_dataset.run(
            "11111111-1111-1111-1111-111111111111", force_reparse=force_reparse
        )

    if force_reparse:
        unchanged.assert_not_called()
        load.assert_called_once()
        assert result["rows"] == 10
    else:
        load.assert_not_called()
        assert result["status"] == "unchanged"


def test_el_desvio_a_la_cola_pesada_conserva_force_reparse():
    dataset_row = SimpleNamespace(
        title="ENNyS2 encuesta",
        download_url="https://example.com/ennys.zip",
        format="zip",
        portal="datos_gob_ar",
        source_id="ennys-1",
    )
    engine = _collect_mocks(dataset_row)
    with (
        patch.object(ct, "get_sync_engine", return_value=engine),
        patch.object(ct, "_try_advisory_lock", return_value=True),
        patch.object(ct, "_ensure_cached_entry"),
        patch.object(ct.collect_dataset, "apply_async") as apply_async,
    ):
        result = ct.collect_dataset.run("11111111-1111-1111-1111-111111111111", force_reparse=True)

    assert result["status"] == "rerouted_heavy"
    assert apply_async.call_args.kwargs["kwargs"] == {"force_heavy": True, "force_reparse": True}
