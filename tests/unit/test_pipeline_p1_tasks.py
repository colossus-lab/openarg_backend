from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.infrastructure.celery.tasks.collector_tasks import (
    audit_cache_coverage,
    collect_large_group,
    consolidate_group_tables,
    materialize_format_duplicate_aliases,
    reconcile_cache_coverage,
    recover_stuck_tasks,
)
from app.infrastructure.celery.tasks.orchestrator_tasks import run_pipeline
from app.infrastructure.celery.tasks.scraper_tasks import scrape_catalog


class _FetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class TestScrapeCatalogP1:
    @patch("app.infrastructure.celery.tasks.scraper_tasks.get_circuit_breaker")
    @patch("app.infrastructure.celery.tasks.scraper_tasks.index_dataset_embedding.delay")
    @patch("app.infrastructure.celery.tasks.scraper_tasks.get_sync_engine")
    @patch("app.infrastructure.celery.tasks.scraper_tasks.httpx.Client")
    def test_dispatches_embeddings_only_for_new_or_changed_datasets(
        self,
        mock_client_cls,
        mock_get_engine,
        mock_delay,
        mock_get_cb,
    ):
        mock_cb = MagicMock()
        mock_cb.is_open = False
        mock_get_cb.return_value = mock_cb

        count_resp = MagicMock()
        count_resp.raise_for_status = MagicMock()
        count_resp.headers = {"content-type": "application/json"}
        count_resp.content = b'{"result":{"count":3}}'
        count_resp.json.return_value = {"result": {"count": 3}}

        package_resp = MagicMock()
        package_resp.raise_for_status = MagicMock()
        package_resp.headers = {"content-type": "application/json"}
        package_resp.content = b'{"result":{"results":[]}}'
        last_updated = datetime(2026, 4, 1, tzinfo=UTC)
        package_resp.json.return_value = {
            "result": {
                "results": [
                    {
                        "title": "IPC Nacional",
                        "notes": "Inflacion mensual",
                        "organization": {"title": "INDEC"},
                        "url": "https://portal/dataset/ipc",
                        "metadata_modified": last_updated.isoformat().replace("+00:00", "Z"),
                        "tags": [{"name": "ipc"}],
                        "resources": [
                            {
                                "id": "same",
                                "format": "csv",
                                "url": "https://portal/dataset/ipc.csv",
                            },
                            {
                                "id": "changed",
                                "format": "csv",
                                "url": "https://portal/dataset/ipc_changed.csv",
                            },
                            {
                                "id": "new",
                                "format": "csv",
                                "url": "https://portal/dataset/ipc_new.csv",
                            },
                        ],
                    }
                ]
            }
        }

        mock_client = MagicMock()
        mock_client.get.side_effect = [count_resp, package_resp]
        mock_client.close = MagicMock()
        mock_client_cls.return_value = mock_client

        existing_rows = [
            SimpleNamespace(
                id="id-same",
                source_id="same",
                title="IPC Nacional",
                description="Inflacion mensual",
                organization="INDEC",
                portal="datos_gob_ar",
                download_url="https://portal/dataset/ipc.csv",
                format="csv",
                columns="[]",
                tags="ipc",
                last_updated_at=last_updated,
            ),
            SimpleNamespace(
                id="id-changed",
                source_id="changed",
                title="IPC Nacional",
                description="Descripcion vieja",
                organization="INDEC",
                portal="datos_gob_ar",
                download_url="https://portal/dataset/ipc_changed.csv",
                format="csv",
                columns="[]",
                tags="ipc",
                last_updated_at=last_updated,
            ),
        ]
        dataset_ids = [
            SimpleNamespace(id="id-same", source_id="same"),
            SimpleNamespace(id="id-changed", source_id="changed"),
            SimpleNamespace(id="id-new", source_id="new"),
        ]

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            _FetchAllResult(existing_rows),
            MagicMock(),
            _FetchAllResult(dataset_ids),
        ]
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = scrape_catalog.run(portal="datos_gob_ar", batch_size=100)

        assert result["datasets_indexed"] == 2
        assert mock_delay.call_count == 2
        dispatched_ids = [call.args[0] for call in mock_delay.call_args_list]
        assert dispatched_ids == ["id-changed", "id-new"]


class TestRunPipelineP1:
    @patch("app.infrastructure.celery.tasks.orchestrator_tasks.celery_app.send_task")
    def test_embeddings_group_no_longer_dispatches_mass_reindex(self, mock_send_task):
        result = run_pipeline.run(groups=["embeddings"], skip_delays=True)

        assert result["total_tasks"] == 1
        assert result["dispatched"]["embeddings"] == ["openarg.index_sesiones"]
        mock_send_task.assert_called_once()
        assert mock_send_task.call_args.args[0] == "openarg.index_sesiones"


class TestCollectLargeGroupP1:
    @patch("app.infrastructure.celery.tasks.collector_tasks.materialize_format_duplicate_aliases.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.consolidate_group_tables.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_table_row_count")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_dataset")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("celery.group")
    def test_does_not_mark_overflow_resources_as_cached(
        self,
        mock_celery_group,
        mock_get_engine,
        mock_collect_dataset,
        mock_get_row_count,
        mock_consolidate_group,
        mock_materialize_aliases,
    ):
        mock_get_row_count.return_value = 0
        mock_collect_dataset.s.side_effect = lambda did: f"task-{did}"

        resources = [
            SimpleNamespace(
                id=f"id-{i}",
                format="csv",
                download_url=f"https://example.org/data_{i}.csv",
            )
            for i in range(205)
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = _FetchAllResult(resources)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_group_result = MagicMock()
        mock_celery_group.return_value = mock_group_result

        result = collect_large_group.run("Pauta Publicitaria", "caba")

        assert result["dispatched"] == 200
        assert result["dispatch_batches"] == 20
        assert result["deferred"] == 5
        assert result["consolidation_scheduled"] is True
        assert result["alias_materialization_scheduled"] is False
        assert mock_group_result.apply_async.call_count == 20
        countdowns = [
            call.kwargs["countdown"] for call in mock_group_result.apply_async.call_args_list
        ]
        assert countdowns == [i * 30 for i in range(20)]
        mock_consolidate_group.assert_called_once_with(
            args=["Pauta Publicitaria", "caba"],
            countdown=660,
        )
        mock_materialize_aliases.assert_not_called()
        mock_engine.begin.assert_not_called()

    @patch("app.infrastructure.celery.tasks.collector_tasks.materialize_format_duplicate_aliases.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.consolidate_group_tables.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_table_row_count")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_dataset")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("celery.group")
    def test_collect_large_group_ignores_group_table_full_and_still_dispatches_resources(
        self,
        mock_celery_group,
        mock_get_engine,
        mock_collect_dataset,
        mock_get_row_count,
        mock_consolidate_group,
        mock_materialize_aliases,
    ):
        mock_get_row_count.return_value = 500_000
        mock_collect_dataset.s.side_effect = lambda did: f"task-{did}"

        resources = [
            SimpleNamespace(
                id=f"id-{i}",
                format="csv",
                download_url=f"https://example.org/data_{i}.csv",
            )
            for i in range(3)
        ]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = _FetchAllResult(resources)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        mock_group_result = MagicMock()
        mock_celery_group.return_value = mock_group_result

        result = collect_large_group.run("Pauta Publicitaria", "caba")

        assert result["dispatched"] == 3
        assert result["deferred"] == 0
        assert result["consolidation_scheduled"] is True
        assert result["alias_materialization_scheduled"] is False
        mock_group_result.apply_async.assert_called_once_with(countdown=0)
        mock_consolidate_group.assert_called_once_with(
            args=["Pauta Publicitaria", "caba"],
            countdown=90,
        )
        mock_materialize_aliases.assert_not_called()


class TestBulkCollectAllP4:
    @patch("app.infrastructure.celery.tasks.collector_tasks.bulk_collect_all.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks._count_bulk_collect_remaining")
    @patch("app.infrastructure.celery.tasks.collector_tasks._revive_schema_mismatch")
    @patch("app.infrastructure.celery.tasks.collector_tasks._reconcile_cache_coverage")
    @patch("app.infrastructure.celery.tasks.collector_tasks._recycle_stuck_downloads")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_inflight_counts")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_large_group")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_dataset")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("celery.group")
    def test_bulk_collect_dispatches_in_staggered_batches(
        self,
        mock_celery_group,
        mock_get_engine,
        mock_collect_dataset,
        mock_collect_large_group,
        mock_get_inflight_counts,
        mock_recycle_stuck,
        mock_reconcile,
        mock_revive_schema,
        mock_count_remaining,
        mock_followup_apply_async,
    ):
        mock_reconcile.return_value = {
            "orphaned_ready": 0,
            "fixed_cached_flags": 0,
            "reindexed_missing_chunks": 0,
        }
        mock_revive_schema.return_value = {"revived_schema_mismatch": 7}
        mock_recycle_stuck.return_value = {
            "recovered_ready": 0,
            "recycled_error": 0,
            "recycled_failed": 0,
        }
        mock_get_inflight_counts.return_value = (0, {})
        group_rows = [
            SimpleNamespace(title="Grupo A", portal="datos_gob_ar"),
            SimpleNamespace(title="Grupo B", portal="caba"),
        ]

        mock_collect_dataset.s.side_effect = lambda did: f"task-{did}"
        mock_collect_large_group.s.side_effect = lambda title, portal: f"group-{title}-{portal}"

        conn = MagicMock()
        conn.execute.side_effect = [
            _ScalarResult(True),
            _FetchAllResult(
                [(f"id-{i}", "datos_gob_ar", "csv", f"Dataset {i}") for i in range(60)]
            ),
            _FetchAllResult(group_rows),
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        group_result = MagicMock()
        mock_celery_group.return_value = group_result

        from app.infrastructure.celery.tasks.collector_tasks import bulk_collect_all

        result = bulk_collect_all.run()

        assert result["inflight_total"] == 0
        assert result["dispatched_individual"] == 10
        assert result["deferred_individual"] == 50
        assert result["phase1_batches"] == 1
        assert result["dispatched_groups"] == 1
        assert result["deferred_groups"] == 1
        assert result["phase2_batches"] == 1
        assert result["reconciled_orphaned_ready"] == 0
        assert result["reconciled_cached_flags"] == 0
        assert result["reindexed_missing_chunks"] == 0
        assert result["revived_schema_mismatch"] == 7
        assert result["recycled_stale_ready"] == 0
        assert result["recycled_stale_error"] == 0
        assert result["recycled_stale_failed"] == 0
        assert result["remaining_eligible_individual"] == 0
        assert result["remaining_eligible_groups"] == 0
        assert result["followup_scheduled"] is True
        assert result["converged"] is False
        assert result["chain_depth"] == 0
        countdowns = [call.kwargs["countdown"] for call in group_result.apply_async.call_args_list]
        assert countdowns == [0, 0]
        mock_count_remaining.assert_not_called()
        mock_followup_apply_async.assert_called_once_with(
            kwargs={"portal": None, "chain_depth": 1},
            countdown=300,
        )

    @patch("app.infrastructure.celery.tasks.catalog_backfill.catalog_backfill_task.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.reconcile_cache_coverage.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.bulk_collect_all.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks._count_bulk_collect_remaining")
    @patch("app.infrastructure.celery.tasks.collector_tasks._reconcile_cache_coverage")
    @patch("app.infrastructure.celery.tasks.collector_tasks._revive_schema_mismatch")
    @patch("app.infrastructure.celery.tasks.collector_tasks._recycle_stuck_downloads")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_inflight_counts")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_large_group")
    @patch("app.infrastructure.celery.tasks.collector_tasks.collect_dataset")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("celery.group")
    def test_bulk_collect_respects_inflight_limits(
        self,
        mock_celery_group,
        mock_get_engine,
        mock_collect_dataset,
        mock_collect_large_group,
        mock_get_inflight_counts,
        mock_recycle_stuck,
        mock_revive_schema,
        mock_reconcile,
        mock_count_remaining,
        mock_followup_apply_async,
        mock_reconcile_apply_async,
        mock_catalog_backfill_apply_async,
    ):
        mock_reconcile.return_value = {
            "orphaned_ready": 4,
            "fixed_cached_flags": 5,
            "reindexed_missing_chunks": 6,
        }
        mock_revive_schema.return_value = {"revived_schema_mismatch": 7}
        mock_recycle_stuck.return_value = {
            "recovered_ready": 1,
            "recycled_error": 2,
            "recycled_failed": 3,
        }
        mock_get_inflight_counts.return_value = (98, {"datos_gob_ar": 9, "caba": 10})
        mock_collect_dataset.s.side_effect = lambda did: f"task-{did}"
        mock_collect_large_group.s.side_effect = lambda title, portal: f"group-{title}-{portal}"

        conn = MagicMock()
        conn.execute.side_effect = [
            _ScalarResult(True),
            _FetchAllResult(
                [
                    ("id-1", "datos_gob_ar", "csv", "Dataset 1"),
                    ("id-2", "datos_gob_ar", "csv", "Dataset 2"),
                    ("id-3", "caba", "csv", "Dataset 3"),
                    ("id-4", "mendoza", "csv", "Dataset 4"),
                ]
            ),
            _FetchAllResult(
                [
                    SimpleNamespace(title="Grupo A", portal="datos_gob_ar"),
                    SimpleNamespace(title="Grupo B", portal="caba"),
                    SimpleNamespace(title="Grupo C", portal="mendoza"),
                ]
            ),
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        group_result = MagicMock()
        mock_celery_group.return_value = group_result

        from app.infrastructure.celery.tasks.collector_tasks import bulk_collect_all

        result = bulk_collect_all.run()

        assert result["inflight_total"] == 98
        assert result["reconciled_orphaned_ready"] == 4
        assert result["reconciled_cached_flags"] == 5
        assert result["reindexed_missing_chunks"] == 6
        assert result["revived_schema_mismatch"] == 7
        assert result["recycled_stale_ready"] == 1
        assert result["recycled_stale_error"] == 2
        assert result["recycled_stale_failed"] == 3
        assert result["dispatched_individual"] == 2
        assert result["deferred_individual"] == 2
        assert result["dispatched_groups"] == 0
        assert result["deferred_groups"] == 3
        assert result["remaining_eligible_individual"] == 0
        assert result["remaining_eligible_groups"] == 0
        assert result["followup_scheduled"] is True
        assert result["converged"] is False
        mock_count_remaining.assert_not_called()
        mock_followup_apply_async.assert_called_once_with(
            kwargs={"portal": None, "chain_depth": 1},
            countdown=300,
        )
        mock_reconcile_apply_async.assert_not_called()
        mock_catalog_backfill_apply_async.assert_not_called()

    @patch("app.infrastructure.celery.tasks.catalog_backfill.catalog_backfill_task.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.reconcile_cache_coverage.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks.bulk_collect_all.apply_async")
    @patch("app.infrastructure.celery.tasks.collector_tasks._count_bulk_collect_remaining")
    @patch("app.infrastructure.celery.tasks.collector_tasks._revive_schema_mismatch")
    @patch("app.infrastructure.celery.tasks.collector_tasks._reconcile_cache_coverage")
    @patch("app.infrastructure.celery.tasks.collector_tasks._recycle_stuck_downloads")
    @patch("app.infrastructure.celery.tasks.collector_tasks._get_inflight_counts")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_bulk_collect_marks_converged_and_schedules_final_sync(
        self,
        mock_get_engine,
        mock_get_inflight_counts,
        mock_recycle_stuck,
        mock_reconcile,
        mock_revive_schema,
        mock_count_remaining,
        mock_followup_apply_async,
        mock_reconcile_apply_async,
        mock_catalog_backfill_apply_async,
    ):
        mock_reconcile.return_value = {
            "orphaned_ready": 0,
            "fixed_cached_flags": 0,
            "reindexed_missing_chunks": 0,
        }
        mock_revive_schema.return_value = {"revived_schema_mismatch": 0}
        mock_recycle_stuck.return_value = {
            "recovered_ready": 0,
            "recycled_error": 0,
            "recycled_failed": 0,
        }
        mock_get_inflight_counts.return_value = (0, {})
        mock_count_remaining.return_value = {
            "eligible_individual": 0,
            "eligible_groups": 0,
        }

        conn = MagicMock()
        conn.execute.side_effect = [
            _ScalarResult(True),
            _FetchAllResult([]),
            _FetchAllResult([]),
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        from app.infrastructure.celery.tasks.collector_tasks import bulk_collect_all

        result = bulk_collect_all.run()

        assert result["dispatched_individual"] == 0
        assert result["dispatched_groups"] == 0
        assert result["followup_scheduled"] is False
        assert result["converged"] is True
        mock_followup_apply_async.assert_not_called()
        mock_reconcile_apply_async.assert_called_once_with(countdown=60)
        mock_catalog_backfill_apply_async.assert_called_once_with(countdown=180)


class TestRecoverStuckTasksP5:
    @patch("app.infrastructure.celery.tasks.collector_tasks._recycle_stuck_downloads")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_recover_stuck_tasks_reuses_stuck_download_recycler(
        self,
        mock_get_engine,
        mock_recycle_stuck,
    ):
        mock_recycle_stuck.return_value = {
            "recovered_ready": 2,
            "recycled_error": 3,
            "recycled_failed": 1,
        }

        conn = MagicMock()
        conn.execute.side_effect = [
            _FetchAllResult([]),  # stuck user_queries
            _FetchAllResult([]),  # ready datasets
        ]
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = recover_stuck_tasks.run()

        mock_recycle_stuck.assert_called_once_with(mock_engine, redispatch=True)
        assert result["recovered_downloads"] == 6
        assert result["recovered_queries"] == 0
        assert result["orphaned_ready"] == 0


class TestFormatDuplicateAliasesP6:
    @patch("app.infrastructure.celery.tasks.collector_tasks._materialize_format_duplicate_aliases")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_materialize_format_duplicate_aliases_builds_canonical_stem_map(
        self,
        mock_get_engine,
        mock_materialize,
    ):
        resources = [
            SimpleNamespace(
                id="id-csv",
                format="csv",
                download_url="https://example.org/recurso.csv",
            ),
            SimpleNamespace(
                id="id-xlsx",
                format="xlsx",
                download_url="https://example.org/recurso.xlsx",
            ),
            SimpleNamespace(
                id="id-json",
                format="json",
                download_url="https://example.org/recurso.json",
            ),
        ]

        conn = MagicMock()
        conn.execute.return_value = _FetchAllResult(resources)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_materialize.return_value = {"created_aliases": 2, "skipped_missing_source": 0}

        result = materialize_format_duplicate_aliases.run("Grupo A", "caba")

        assert result["created_aliases"] == 2
        assert result["skipped_missing_source"] == 0
        kwargs = mock_materialize.call_args.kwargs
        assert kwargs["group_table_name"] == "cache_caba_grupo_a"
        assert kwargs["stem_to_winner"]["https://example.org/recurso"][0] == "id-csv"


class TestCacheCoverageP6:
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_audit_cache_coverage_reports_actionable_counts(self, mock_get_engine):
        conn = MagicMock()
        conn.execute.side_effect = [
            _ScalarResult(17004),
            _ScalarResult(243),
            _ScalarResult(69),
            _ScalarResult(364),
            _ScalarResult(437),
            _ScalarResult(12),
            _ScalarResult(21),
            _ScalarResult(8),
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = audit_cache_coverage.run()

        assert result == {
            "total_datasets": 17004,
            "non_cached": 243,
            "stale_downloading": 69,
            "recoverable_errors": 364,
            "permanently_failed": 437,
            "ready_missing_table": 12,
            "cached_flag_inconsistent": 21,
            "ready_missing_chunks": 8,
        }

    @patch("app.infrastructure.celery.tasks.collector_tasks._reconcile_cache_coverage")
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    def test_reconcile_cache_coverage_reuses_shared_reconciler(
        self,
        mock_get_engine,
        mock_reconcile,
    ):
        mock_reconcile.return_value = {
            "orphaned_ready": 3,
            "fixed_cached_flags": 7,
            "reindexed_missing_chunks": 4,
        }
        mock_engine = MagicMock()
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = reconcile_cache_coverage.run()

        mock_reconcile.assert_called_once_with(
            mock_engine,
            redispatch=True,
            reindex_embeddings=True,
        )
        assert result == {
            "orphaned_ready": 3,
            "fixed_cached_flags": 7,
            "reindexed_missing_chunks": 4,
        }


class TestConsolidateGroupTablesP7:
    @patch("app.infrastructure.celery.tasks.collector_tasks.get_sync_engine")
    @patch("app.infrastructure.celery.tasks.collector_tasks._table_exists")
    def test_consolidate_group_tables_merges_ready_resource_tables_by_schema(
        self,
        mock_table_exists,
        mock_get_engine,
    ):
        mock_table_exists.side_effect = [False]
        conn = MagicMock()
        conn.execute.side_effect = [
            _FetchAllResult(
                [
                    SimpleNamespace(
                        dataset_id="id-1",
                        table_name="cache_caba_pauta_publicitaria_rid1",
                        columns_json='["a","b","_source_dataset_id"]',
                    ),
                    SimpleNamespace(
                        dataset_id="id-2",
                        table_name="cache_caba_pauta_publicitaria_rid2",
                        columns_json='["a","b","_source_dataset_id"]',
                    ),
                ]
            ),
            MagicMock(),  # create table
            _FetchAllResult([]),  # loaded ids
            MagicMock(),  # insert source 1
            MagicMock(),  # insert source 2
        ]
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        mock_engine.dispose = MagicMock()
        mock_get_engine.return_value = mock_engine

        result = consolidate_group_tables.run("Pauta Publicitaria", "caba")

        assert result["schemas_seen"] == 1
        assert result["consolidated_tables"] == 1
        assert result["consolidated_sources"] == 2
