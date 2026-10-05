"""El re-embed diario de ~18.000 datasets que no habían cambiado.

Medido en prod el 04-oct: 60.042 chunks escritos en 24 h para 17.805 datasets,
de 06 a 23 UTC a ~3.300 por hora. El bucle:

1. el scrape del portal (03:00-05:50 ART) no publica columnas para la mayoría
   de los portales CKAN y mandaba ``[]``; contra las columnas reales que había
   cargado ``columns_backfill``, la firma cambiaba: re-embed y ``columns='[]'``;
2. ``columns_backfill`` (cada hora a los :20, 1.000 filas) encontraba el hueco,
   lo volvía a llenar y re-embebía otra vez.

En staging se vio igual: los datasets re-embebidos tenían ``updated_at`` a los
:20 y las columnas recién copiadas de ``raw.cached_datasets``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks import scraper_tasks
from app.infrastructure.celery.tasks.scraper_tasks import (
    _chunks_unchanged,
    _has_columns,
    _keep_known_columns,
    index_dataset_embedding,
    scrape_catalog,
)

_BACKFILLED = '["indice_tiempo", "nivel_general", "_source_dataset_id"]'


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


def _package(resources: list[dict]) -> dict:
    return {
        "result": {
            "results": [
                {
                    "title": "IPC Córdoba",
                    "notes": "Índice de precios",
                    "organization": {"title": "Dirección de Estadística"},
                    "url": "https://portal/dataset/ipc",
                    "metadata_modified": datetime(2026, 9, 1, tzinfo=UTC).isoformat(),
                    "tags": [{"name": "ipc"}],
                    "resources": resources,
                }
            ]
        }
    }


def _existing(sid: str, columns: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=f"id-{sid}",
        source_id=sid,
        title="IPC Córdoba",
        description="Índice de precios",
        organization="Dirección de Estadística",
        portal="cordoba_estadistica",
        download_url=f"https://portal/dataset/{sid}.csv",
        format="csv",
        columns=columns,
        tags="ipc",
        last_updated_at=None,
    )


def _run_scrape(resources: list[dict], existing: list[SimpleNamespace]):
    """Corre `scrape_catalog` con el portal y la base dobles; devuelve (delay, upsert_rows)."""
    count = MagicMock(headers={"content-type": "application/json"}, content=b"{}")
    count.json.return_value = {"result": {"count": 1}}
    page = MagicMock(headers={"content-type": "application/json"}, content=b"{}")
    page.json.return_value = _package(resources)
    client = MagicMock()
    client.get.side_effect = [count, page]

    conn = MagicMock()
    conn.execute.side_effect = [
        _Rows(existing),
        MagicMock(),
        _Rows([SimpleNamespace(id=e.id, source_id=e.source_id) for e in existing]),
    ]
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    cb = MagicMock(is_open=False)
    with (
        patch.object(scraper_tasks.httpx, "Client", return_value=client),
        patch.object(scraper_tasks, "get_sync_engine", return_value=engine),
        patch.object(scraper_tasks, "get_circuit_breaker", return_value=cb),
        patch.object(scraper_tasks.index_dataset_embedding, "delay") as delay,
    ):
        scrape_catalog.run(portal="cordoba_estadistica", batch_size=100)
    upsert_rows = conn.execute.call_args_list[1].args[1]
    return delay, upsert_rows


def test_a_portal_without_columns_does_not_erase_the_backfilled_ones() -> None:
    delay, upsert_rows = _run_scrape(
        [{"id": "r1", "format": "csv", "url": "https://portal/dataset/r1.csv"}],
        [_existing("r1", _BACKFILLED)],
    )

    # Nada cambió en el portal: no se re-embebe ...
    delay.assert_not_called()
    # ... y la fila se escribe con las columnas que ya se conocían, no con `[]`.
    [row] = upsert_rows
    assert row["cols"] == _BACKFILLED


def test_a_real_change_still_reembeds() -> None:
    existing = _existing("r1", _BACKFILLED)
    existing.description = "Descripción vieja"
    delay, _ = _run_scrape(
        [{"id": "r1", "format": "csv", "url": "https://portal/dataset/r1.csv"}],
        [existing],
    )
    delay.assert_called_once_with("id-r1")


def test_columns_published_by_the_portal_win() -> None:
    attrs = json.dumps({"fecha": "Fecha", "valor": "Valor"})
    _, upsert_rows = _run_scrape(
        [
            {
                "id": "r1",
                "format": "csv",
                "url": "https://portal/dataset/r1.csv",
                "attributesDescription": attrs,
            }
        ],
        [_existing("r1", _BACKFILLED)],
    )
    assert json.loads(upsert_rows[0]["cols"]) == ["fecha", "valor"]


def test_has_columns_treats_every_spelling_of_empty_as_empty() -> None:
    for empty in (None, "", "[]", " [] ", "null", '""', []):
        assert not _has_columns(empty)
    assert _has_columns('["a"]')
    assert _has_columns(["a"])


def test_keep_known_columns_only_fills_holes() -> None:
    rows = [{"sid": "a", "cols": "[]"}, {"sid": "b", "cols": '["x"]'}, {"sid": "c", "cols": "[]"}]
    _keep_known_columns(rows, {"a": '["k"]', "b": '["old"]', "c": "[]"})
    assert [r["cols"] for r in rows] == ['["k"]', '["x"]', "[]"]


# ── la guarda en index_dataset_embedding ───────────────────


def _engine_with_chunks(stored: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value = _Rows([(c,) for c in stored])
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def test_chunks_unchanged_compares_texts_regardless_of_order() -> None:
    assert _chunks_unchanged(_engine_with_chunks(["b", "a"]), "d", ["a", "b"])
    assert not _chunks_unchanged(_engine_with_chunks(["a"]), "d", ["a", "b"])
    assert not _chunks_unchanged(_engine_with_chunks(["a", "c"]), "d", ["a", "b"])
    # Sin chunks guardados no hay nada que ahorrar.
    assert not _chunks_unchanged(_engine_with_chunks([]), "d", [])


def test_chunks_unchanged_says_no_when_it_cannot_read() -> None:
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("db down")
    assert not _chunks_unchanged(engine, "d", ["a"])


_DATASET = SimpleNamespace(
    title="IPC Córdoba",
    description="Índice de precios al consumidor de la provincia",
    columns="[]",
    tags="ipc",
    organization="Dirección de Estadística",
    portal="cordoba_estadistica",
    format="csv",
    download_url="https://portal/dataset/r1.csv",
    is_cached=False,
    row_count=None,
)


def _run_index(stored: list[str] | None, *, force: bool = False):
    """Corre la tarea con la base doble; `stored=None` = los mismos textos que genera."""
    begin_conn = MagicMock()
    begin_conn.execute.return_value = _Rows([_DATASET])
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    seen: dict[str, list[str]] = {}

    def _unchanged(_engine, _did, chunks):
        seen["chunks"] = chunks
        return _chunks_unchanged(
            _engine_with_chunks(chunks if stored is None else stored), _did, chunks
        )

    bedrock = MagicMock()
    bedrock.invoke_model.side_effect = lambda **kw: {
        "body": MagicMock(
            read=lambda: json.dumps(
                {"embeddings": [[0.1] * 4 for _ in json.loads(kw["body"])["texts"]]}
            )
        )
    }
    with (
        patch.object(scraper_tasks, "get_sync_engine", return_value=engine),
        patch.object(scraper_tasks, "_get_data_statistics", return_value=None),
        patch.object(scraper_tasks, "_chunks_unchanged", side_effect=_unchanged),
        patch("boto3.client", return_value=bedrock),
    ):
        result = index_dataset_embedding.run("did-1", force=force)
    return result, bedrock, seen


def test_same_texts_skip_bedrock_and_the_rewrite() -> None:
    result, bedrock, seen = _run_index(stored=None)

    assert result == {"dataset_id": "did-1", "chunks_created": 0, "unchanged": True}
    bedrock.invoke_model.assert_not_called()
    assert seen["chunks"]  # compared the real chunk texts


def test_different_texts_are_embedded() -> None:
    result, bedrock, _ = _run_index(stored=["otro texto"])

    assert result["chunks_created"] > 0
    bedrock.invoke_model.assert_called()


def test_force_reembeds_identical_texts() -> None:
    result, bedrock, _ = _run_index(stored=None, force=True)

    assert result["chunks_created"] > 0
    bedrock.invoke_model.assert_called()
