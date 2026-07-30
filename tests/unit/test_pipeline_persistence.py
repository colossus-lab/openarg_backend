from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from app.presentation.http.controllers.query import smart_query_v2_router as router

_DSN = "postgresql+psycopg://user:pass@host/db"
_EXPECTED_CONNINFO = "postgresql://user:pass@host/db"


def _install_fake_pool(monkeypatch, events: list[str]) -> type:
    """Replace `psycopg_pool` so the router never opens a real pool.

    Without this the checkpointer builds a live `AsyncConnectionPool` against
    the fake DSN above, and its workers retry the unresolvable host in the
    background forever — which hangs the whole test session rather than
    failing it. That is not hypothetical: it cancelled CI at 67% for 14
    minutes on 2026-07-30.
    """

    class FakePool:
        @staticmethod
        def check_connection(conn) -> None:  # noqa: ANN001
            """Stand-in for the real liveness probe; presence is what matters."""

        def __init__(self, conninfo: str, **kwargs) -> None:
            self.conninfo = conninfo
            self.kwargs = kwargs
            self.closed = False
            events.append(f"pool:{conninfo}")

        async def __aenter__(self) -> FakePool:
            events.append("open")
            return self

        async def __aexit__(self, *_exc_info) -> bool:
            self.closed = True
            events.append("close")
            return False

    monkeypatch.setitem(sys.modules, "psycopg_pool", SimpleNamespace(AsyncConnectionPool=FakePool))
    return FakePool


def _install_fake_saver(monkeypatch, saver_cls: type) -> None:
    monkeypatch.setitem(
        sys.modules,
        "langgraph.checkpoint.postgres.aio",
        SimpleNamespace(AsyncPostgresSaver=saver_cls),
    )


async def test_checkpointer_is_built_over_a_pool(monkeypatch) -> None:
    events: list[str] = []
    _install_fake_pool(monkeypatch, events)

    class FakeSaver:
        def __init__(self, conn) -> None:  # noqa: ANN001
            self.conn = conn
            events.append("saver")

        async def setup(self) -> None:
            events.append("setup")

    monkeypatch.setenv("DATABASE_URL", _DSN)
    _install_fake_saver(monkeypatch, FakeSaver)

    await router.shutdown_pipeline_persistence()

    checkpointer = await router._get_checkpointer()

    assert isinstance(checkpointer, FakeSaver)
    assert events == [f"pool:{_EXPECTED_CONNINFO}", "open", "saver", "setup"]

    # The saver must be handed the pool itself — that is what makes
    # `get_connection()` acquire (and revalidate) per operation.
    assert checkpointer.conn.conninfo == _EXPECTED_CONNINFO

    await router.shutdown_pipeline_persistence()

    assert events[-1] == "close"


async def test_pool_carries_the_settings_the_saver_assumes(monkeypatch) -> None:
    """`from_conn_string` applied these; a pool silently would not."""
    events: list[str] = []
    _install_fake_pool(monkeypatch, events)

    class FakeSaver:
        def __init__(self, conn) -> None:  # noqa: ANN001
            self.conn = conn

        async def setup(self) -> None: ...

    monkeypatch.setenv("DATABASE_URL", _DSN)
    _install_fake_saver(monkeypatch, FakeSaver)

    await router.shutdown_pipeline_persistence()
    checkpointer = await router._get_checkpointer()

    conn_kwargs = checkpointer.conn.kwargs["kwargs"]
    assert conn_kwargs["autocommit"] is True
    assert conn_kwargs["prepare_threshold"] == 0

    pool_kwargs = checkpointer.conn.kwargs
    assert pool_kwargs["check"] is not None, "connections are never revalidated on checkout"
    assert pool_kwargs["open"] is False, "the exit stack must own the pool lifecycle"

    await router.shutdown_pipeline_persistence()


async def test_a_failure_after_the_pool_opens_still_closes_it(monkeypatch) -> None:
    """An abandoned open pool reconnects forever — it hangs, it does not raise.

    `_open_checkpointer` opens the pool and *then* constructs the saver. When
    that construction raised, the pool stayed open with its background workers
    retrying, and the caller could not clean up because it never received the
    stack. This is the exact shape that froze CI on 2026-07-30.
    """
    events: list[str] = []
    _install_fake_pool(monkeypatch, events)

    class ExplodingSaver:
        def __init__(self, conn) -> None:  # noqa: ANN001
            raise TypeError("saver rejected the pool")

    monkeypatch.setenv("DATABASE_URL", _DSN)
    _install_fake_saver(monkeypatch, ExplodingSaver)

    await router.shutdown_pipeline_persistence()

    checkpointer = await router._get_checkpointer()

    assert checkpointer is None, "a failed init must leave persistence off, not half-built"
    assert "close" in events, "the opened pool was abandoned instead of closed"
    assert events == [f"pool:{_EXPECTED_CONNINFO}", "open", "close"]

    await router.shutdown_pipeline_persistence()


async def test_checkpointer_recovers_from_concurrent_setup_race(monkeypatch) -> None:
    events: list[str] = []
    _install_fake_pool(monkeypatch, events)

    labels = iter(["first", "second"])

    class FakeSaver:
        def __init__(self, conn) -> None:  # noqa: ANN001
            self.conn = conn
            self.label = next(labels)
            events.append(f"saver:{self.label}")

        async def setup(self) -> None:
            events.append(f"setup:{self.label}")
            if self.label == "first":
                raise RuntimeError(
                    'duplicate key value violates unique constraint "checkpoint_migrations_pkey"'
                )

    monkeypatch.setenv("DATABASE_URL", _DSN)
    _install_fake_saver(monkeypatch, FakeSaver)

    await router.shutdown_pipeline_persistence()

    checkpointer = await router._get_checkpointer()

    assert checkpointer.label == "second"
    assert events == [
        f"pool:{_EXPECTED_CONNINFO}",
        "open",
        "saver:first",
        "setup:first",
        "close",
        f"pool:{_EXPECTED_CONNINFO}",
        "open",
        "saver:second",
    ]

    await router.shutdown_pipeline_persistence()

    assert events[-1] == "close"


async def test_a_dead_checkpointer_is_rebuilt_rather_than_reused(monkeypatch) -> None:
    """The 2026-07-30 prod outage: a cached saver whose connection had died.

    `_get_checkpointer()` returned the same object forever, so one DB restart
    broke authenticated chat until the process was restarted.
    """
    events: list[str] = []
    _install_fake_pool(monkeypatch, events)

    class FakeSaver:
        def __init__(self, conn) -> None:  # noqa: ANN001
            self.conn = conn

        async def setup(self) -> None: ...

    monkeypatch.setenv("DATABASE_URL", _DSN)
    _install_fake_saver(monkeypatch, FakeSaver)

    await router.shutdown_pipeline_persistence()

    first = await router._get_checkpointer()
    assert await router._get_checkpointer() is first, "a live checkpointer must be reused"

    # Whatever killed it — OOM-kill, failover, maintenance patch — the pool is
    # now closed and the saver can no longer serve anything.
    first.conn.closed = True
    router._compiled_graphs = {True: object()}

    second = await router._get_checkpointer()

    assert second is not first, "the dead checkpointer was handed back again"
    assert router._compiled_graphs == {}, (
        "a compiled graph captures the saver, so it must be dropped alongside it"
    )

    await router.shutdown_pipeline_persistence()


async def test_compiled_graph_cache_distinguishes_persistence_mode(monkeypatch) -> None:
    built: list[bool] = []

    def _fake_build(_deps, checkpointer=None):
        built.append(bool(checkpointer))
        return {"persistent": bool(checkpointer), "build_no": len(built)}

    monkeypatch.setattr(router, "build_pipeline_graph", _fake_build)
    router._compiled_graphs = {}

    no_persist_1 = await router._get_or_compile_graph(object(), None)
    no_persist_2 = await router._get_or_compile_graph(object(), None)
    persist_1 = await router._get_or_compile_graph(object(), object())
    persist_2 = await router._get_or_compile_graph(object(), object())

    assert no_persist_1 is no_persist_2
    assert persist_1 is persist_2
    assert no_persist_1 is not persist_1
    assert built == [False, True]


@pytest.fixture(autouse=True)
async def _reset_checkpointer_state():
    """Module globals leak between tests otherwise."""
    yield
    await router.shutdown_pipeline_persistence()
