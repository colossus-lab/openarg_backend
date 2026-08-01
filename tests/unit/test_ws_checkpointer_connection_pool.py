"""A cached connection must be able to die without taking the feature with it.

`/ws/smart` builds its LangGraph checkpointer once and caches it in a module
global. It used `AsyncPostgresSaver.from_conn_string()`, which opens exactly
one `psycopg.AsyncConnection` — no pre-ping, no recycle, no reconnect — and
`_get_checkpointer()` returned that same object forever: its retry TTL only
ever covered a failed *init*, never a connection that died afterwards.

Effect on prod: the backend ran 68 days on one connection. An RDS OOM-kill on
2026-07-30 00:21 UTC dropped every connection; everything else recovered
because it sits behind a pool, and the checkpointer did not. The first chat
request seven hours later raised
`psycopg.errors.ProtocolViolation('server conn crashed?')`, and every request
after it got `OperationalError('the connection is closed')`. The UI showed
"Internal error / Respuesta parcial" for five hours, on both uvicorn workers,
until the process was restarted.

Same shape as the twelve defects of that day: the safeguard existed and was
correctly written. `pool_pre_ping=True` and `pool_recycle=300` live on the
SQLAlchemy engine in `persistence_sqla/provider.py` and could never have
covered this connection — a different engine, a different driver handle.

What made it invisible: the failure needs a connection-killing event (an
OOM-kill, a failover, a maintenance patch) *and* a request afterwards. Nothing
in the test suite kept a connection across such an event, and duration-based
suspicion pointed the wrong way — prod's runs peak at 40s, nowhere near any
timeout, because the connection dies while idle *between* requests.

Asserted against source text rather than by importing the module: the router
pulls in dishka, fastapi and langgraph, so an import-based test would be
skipped in exactly the environments that lack them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_WS_ROUTER = (
    _ROOT
    / "src"
    / "app"
    / "presentation"
    / "http"
    / "controllers"
    / "query"
    / "smart_query_v2_router.py"
)
_PROVIDER = _ROOT / "src" / "app" / "infrastructure" / "persistence_sqla" / "provider.py"
_PYPROJECT = _ROOT / "pyproject.toml"

# pgbouncer sits between the app and RDS. Prod's config sets
# `server_idle_timeout = 300` and leaves `server_lifetime` at its 3600s
# default, so anything the pool holds longer than that gets retired by
# pgbouncer instead — unannounced, which is the whole failure mode.
_PGBOUNCER_SERVER_LIFETIME_S = 3600
_PGBOUNCER_SERVER_IDLE_TIMEOUT_S = 300


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _code_only(source: str) -> str:
    """Source minus docstrings and comments.

    These tests assert on the *absence* of certain calls, and this file's own
    subject matter means the router explains `from_conn_string` and
    `pool_pre_ping` in prose. Prose about a defect is not the defect.
    """
    without_docstrings = re.sub(r'("""|\'\'\')(?:.|\n)*?\1', "", source)
    kept = []
    for line in without_docstrings.splitlines():
        code = re.sub(r"#.*$", "", line)
        if code.strip():
            kept.append(code)
    return "\n".join(kept)


def _read_code(path: Path) -> str:
    return _code_only(_read(path))


def _function_body(source: str, name: str) -> str:
    """Return the source of a top-level `async def name(...)` / `def name(...)`."""
    match = re.search(rf"^(?:async )?def {re.escape(name)}\(", source, re.MULTILINE)
    assert match, f"{name}() not found in the router"
    rest = source[match.start() :]
    following = re.search(r"\n(?:async )?def ", rest[1:])
    return rest[: following.start() + 1] if following else rest


def _constant(source: str, name: str) -> float:
    match = re.search(rf"^{re.escape(name)}\s*[:=][^=]*?=?\s*([0-9.]+)\s*$", source, re.MULTILINE)
    assert match, f"constant {name} not found"
    return float(match.group(1))


class TestCheckpointerUsesAPool:
    def test_the_single_connection_helper_is_not_used(self) -> None:
        """`from_conn_string` is the bug: one bare connection, cached forever."""
        source = _read_code(_WS_ROUTER)
        assert "from_conn_string" not in source, (
            "AsyncPostgresSaver.from_conn_string() opens a single AsyncConnection "
            "with no pre-ping, recycle or reconnect — the 2026-07-30 prod outage"
        )

    def test_the_saver_is_built_over_a_connection_pool(self) -> None:
        body = _function_body(_read_code(_WS_ROUTER), "_open_checkpointer")
        assert "AsyncConnectionPool(" in body
        assert re.search(r"AsyncPostgresSaver\(\s*conn=pool", body), (
            "the saver must be handed the pool; _ainternal.Conn accepts one and "
            "acquires per operation"
        )

    def test_connections_are_revalidated_on_checkout(self) -> None:
        """This is the pre-ping equivalent. Without it the pool hands back corpses."""
        body = _function_body(_read_code(_WS_ROUTER), "_open_checkpointer")
        assert "check=AsyncConnectionPool.check_connection" in body

    def test_the_pool_sets_what_the_saver_assumes(self) -> None:
        """`from_conn_string` set these three; a pool does not unless told."""
        body = _function_body(_read_code(_WS_ROUTER), "_open_checkpointer")
        for expected in ('"autocommit": True', '"prepare_threshold": 0', '"row_factory": dict_row'):
            assert expected in body, f"pool connections are missing {expected}"


class TestPoolTimeoutsStayUnderPgbouncer:
    """The pool must retire a connection before pgbouncer does it silently."""

    def test_max_lifetime_is_below_pgbouncer_server_lifetime(self) -> None:
        lifetime = _constant(_read(_WS_ROUTER), "_CHECKPOINTER_CONN_MAX_LIFETIME_S")
        assert 0 < lifetime < _PGBOUNCER_SERVER_LIFETIME_S, (
            f"max_lifetime={lifetime}s must stay under pgbouncer's "
            f"server_lifetime ({_PGBOUNCER_SERVER_LIFETIME_S}s)"
        )

    def test_max_idle_is_below_pgbouncer_server_idle_timeout(self) -> None:
        max_idle = _constant(_read(_WS_ROUTER), "_CHECKPOINTER_CONN_MAX_IDLE_S")
        assert 0 < max_idle < _PGBOUNCER_SERVER_IDLE_TIMEOUT_S, (
            f"max_idle={max_idle}s must stay under pgbouncer's "
            f"server_idle_timeout ({_PGBOUNCER_SERVER_IDLE_TIMEOUT_S}s)"
        )


class TestADeadCheckpointerIsRebuilt:
    def test_the_cache_hit_is_guarded_by_a_liveness_check(self) -> None:
        """A bare `if _checkpointer is not None: return` is what caused the outage."""
        body = _function_body(_read_code(_WS_ROUTER), "_get_checkpointer")
        first_return = body.find("return _checkpointer")
        assert first_return != -1, "_get_checkpointer no longer returns the cached saver"
        assert "_checkpointer_is_live()" in body[:first_return], (
            "the cached saver is returned without checking it still has a usable "
            "connection source — the 2026-07-30 regression"
        )

    def test_a_liveness_helper_exists(self) -> None:
        source = _read_code(_WS_ROUTER)
        assert re.search(r"^def _checkpointer_is_live\(\)", source, re.MULTILINE)

    def test_rebuilding_invalidates_the_compiled_graphs(self) -> None:
        """A compiled graph captures the saver, so a stale cache re-routes to the corpse."""
        body = _function_body(_read_code(_WS_ROUTER), "_teardown_checkpointer_locked")
        assert "_compiled_graphs = {}" in body, (
            "swapping the saver without clearing _compiled_graphs leaves the "
            "graph pointing at the discarded one"
        )

    def test_a_failed_rebuild_closes_the_pool_it_opened(self) -> None:
        """The old handler closed the *global* stack, which is unset on that path."""
        body = _function_body(_read_code(_WS_ROUTER), "_get_checkpointer")
        assert re.search(r"if stack is not None:\s*\n\s*with contextlib\.suppress", body), (
            "a non-benign setup failure must close the stack just opened, or the "
            "pool leaks up to max_size connections per attempt"
        )


class TestTheEngineFlagsAreNotMistakenForCoverage:
    def test_pre_ping_lives_on_a_different_engine(self) -> None:
        """Guards against 'pre-ping is already on' being read as protection here.

        If these ever move onto the checkpointer's connection, this test should
        be rewritten — until then their presence in provider.py says nothing
        about `/ws/smart`.
        """
        assert "pool_pre_ping=True" in _read_code(_PROVIDER)
        assert "pool_pre_ping" not in _read_code(_WS_ROUTER)


class TestTheDependencyIsDeclared:
    def test_psycopg_pool_is_a_direct_dependency(self) -> None:
        """We import it ourselves; it must not rely on langgraph pulling it in."""
        assert re.search(r'"psycopg-pool[>=~]', _read(_PYPROJECT)), (
            "psycopg_pool is imported directly by the router but only arrives "
            "transitively via langgraph-checkpoint-postgres"
        )


class TestPoolIsAvailableAtRuntime:
    def test_the_pool_class_supports_the_check_hook(self) -> None:
        """Cheap guard against a psycopg_pool major bump dropping `check_connection`."""
        psycopg_pool = pytest.importorskip("psycopg_pool")
        assert hasattr(psycopg_pool.AsyncConnectionPool, "check_connection")
