"""Regression test: the sandbox search_path must survive pool checkouts.

Measured on staging 2026-07-26: the previous implementation set the
search_path with a ``SET`` inside a ``connect`` event listener. psycopg3
opens an implicit transaction for that statement, so the ROLLBACK
SQLAlchemy issues when the connection returns to the pool reverted it —
only 1 of 10 checkouts saw ``raw`` in ``current_schemas()``. Every
unqualified reference to a raw-only table (``cached_datasets`` and the
~4.4k ``cache_*`` data tables) then failed with "relation does not
exist", which the NL2SQL path surfaces as a no-data deflection.

The fix passes the search_path through the libpq ``options`` connection
parameter, making it part of the session's startup state.
"""

from __future__ import annotations

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import (
    _SANDBOX_SEARCH_PATH,
    PgSandboxAdapter,
)


class TestSandboxSearchPath:
    def test_search_path_passed_via_connect_args(self, monkeypatch) -> None:
        """The engine must carry the search_path in libpq connect options."""
        captured: dict = {}

        def _fake_create_engine(url, **kwargs):
            captured["url"] = url
            captured["kwargs"] = kwargs
            return object()

        monkeypatch.setattr(
            "app.infrastructure.adapters.sandbox.pg_sandbox_adapter.create_engine",
            _fake_create_engine,
        )
        monkeypatch.setenv("SANDBOX_DATABASE_URL", "postgresql+psycopg://u:p@h:5432/db")

        PgSandboxAdapter()._get_engine()

        options = captured["kwargs"].get("connect_args", {}).get("options", "")
        assert f"-csearch_path={_SANDBOX_SEARCH_PATH}" == options, (
            "search_path must ride in libpq connect options — a SET in a "
            "connect listener is reverted by the pool's ROLLBACK on return"
        )

    def test_search_path_has_no_spaces(self) -> None:
        """A space in the libpq options value starts a new option."""
        assert " " not in _SANDBOX_SEARCH_PATH

    def test_public_precedes_raw(self) -> None:
        """Tables present in both schemas must resolve to public."""
        parts = _SANDBOX_SEARCH_PATH.split(",")
        assert parts.index("public") < parts.index("raw")
