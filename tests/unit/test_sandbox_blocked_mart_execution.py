"""A serving-blocked mart must be unreachable, not merely unlisted.

Migration 0054 added `serving_blocked` so a mart with wrong rows could be
withheld without deleting it. Every enforcement point landed in *discovery*
queries — the sandbox universe, the context builder, the serving adapter —
which only controls what gets *suggested* to the NL2SQL model.

Measured on staging 2026-07-26: the model wrote
`FROM mart.presupuesto_nacional_ejecutado` on its own, the prefix-free
allowlist for the `mart` schema waved it through, and Postgres executed it.
It failed on a type error, not on the block. Had the types lined up, the
chat would have served numbers from the mart we had just declared unfit —
the exact outcome the flag exists to prevent.

The schema allowlist justifies skipping the prefix check for `mart` with
"the pipeline already gates what lands there". 0054 is the case where that
stopped being true.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import (
    _ALLOWED_SCHEMAS,
    _FORBIDDEN_TABLES,
    _PREFIX_FREE_SCHEMAS,
    _blocked_mart_error,
    _referenced_mart_views,
)


def _engine_returning(rows: list[tuple[str, str]]) -> MagicMock:
    """An engine whose `mart_definitions` lookup yields `rows`."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = rows
    return engine


class TestMartReferenceExtraction:
    def test_finds_qualified_mart_reference(self) -> None:
        sql = "SELECT SUM(credito_devengado) FROM mart.presupuesto_nacional_ejecutado"
        assert _referenced_mart_views(sql) == {"presupuesto_nacional_ejecutado"}

    def test_finds_mart_in_a_join(self) -> None:
        sql = (
            "SELECT a.anio FROM mart.presupuesto_consolidado a "
            "JOIN mart.presupuesto_nacional_ejecutado b ON a.anio = b.anio"
        )
        assert _referenced_mart_views(sql) == {
            "presupuesto_consolidado",
            "presupuesto_nacional_ejecutado",
        }

    def test_is_case_insensitive(self) -> None:
        sql = "select 1 from MART.Presupuesto_Nacional_Ejecutado"
        assert _referenced_mart_views(sql) == {"presupuesto_nacional_ejecutado"}

    def test_ignores_cache_tables(self) -> None:
        assert _referenced_mart_views("SELECT * FROM cache_presupuesto_credito_2024") == set()

    def test_ignores_a_same_named_table_in_another_schema(self) -> None:
        """Only the `mart` schema is gated by mart_definitions."""
        assert _referenced_mart_views("SELECT * FROM raw.presupuesto_nacional_ejecutado") == set()


class TestBlockedMartExecution:
    def test_blocked_mart_is_refused(self) -> None:
        engine = _engine_returning([("presupuesto_nacional_ejecutado", "montos TEXT mixtos")])
        error = _blocked_mart_error(engine, "SELECT 1 FROM mart.presupuesto_nacional_ejecutado")
        assert error is not None
        assert "presupuesto_nacional_ejecutado" in error
        assert "montos TEXT mixtos" in error, "the documented reason must reach the caller"

    def test_servable_mart_passes(self) -> None:
        engine = _engine_returning([])
        assert _blocked_mart_error(engine, "SELECT 1 FROM mart.presupuesto_consolidado") is None

    def test_query_without_marts_skips_the_lookup(self) -> None:
        """No mart reference, no round trip to mart_definitions."""
        engine = _engine_returning([])
        assert _blocked_mart_error(engine, "SELECT 1 FROM cache_diputados_1") is None
        engine.connect.assert_not_called()

    def test_unreadable_catalog_fails_closed(self) -> None:
        """A query we cannot clear is a query we do not run."""
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("catalog unavailable")
        error = _blocked_mart_error(engine, "SELECT 1 FROM mart.presupuesto_nacional_ejecutado")
        assert error is not None

    def test_static_allowlist_cannot_catch_it(self) -> None:
        """Documents why the DB-backed check is necessary at all.

        The static allowlist is pure and has no way to know a mart was
        blocked: `mart` is prefix-free by design, and the blocked view is
        a legitimate relation, not an internal table. Nothing short of a
        `mart_definitions` lookup can distinguish it.
        """
        assert "mart" in _ALLOWED_SCHEMAS
        assert "mart" in _PREFIX_FREE_SCHEMAS
        assert "presupuesto_nacional_ejecutado" not in _FORBIDDEN_TABLES
