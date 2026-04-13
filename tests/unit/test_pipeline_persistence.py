from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

from app.presentation.http.controllers.query import smart_query_v2_router as router


async def test_checkpointer_uses_async_context_manager(monkeypatch) -> None:
    events: list[str] = []

    class FakeSaver:
        async def setup(self) -> None:
            events.append("setup")

    saver = FakeSaver()

    class FakeAsyncPostgresSaver:
        @staticmethod
        def from_conn_string(conn_str: str):
            events.append(conn_str)

            @asynccontextmanager
            async def _cm():
                events.append("enter")
                try:
                    yield saver
                finally:
                    events.append("exit")

            return _cm()

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pass@host/db")
    monkeypatch.setitem(
        sys.modules,
        "langgraph.checkpoint.postgres.aio",
        SimpleNamespace(AsyncPostgresSaver=FakeAsyncPostgresSaver),
    )

    await router.shutdown_pipeline_persistence()

    checkpointer = await router._get_checkpointer()

    assert checkpointer is saver
    assert events[:3] == ["postgresql://user:pass@host/db", "enter", "setup"]

    await router.shutdown_pipeline_persistence()

    assert events[-1] == "exit"


async def test_checkpointer_recovers_from_concurrent_setup_race(monkeypatch) -> None:
    events: list[str] = []

    class FakeUniqueViolation(Exception):
        pass

    class FakeSaver:
        def __init__(self, label: str, should_race: bool) -> None:
            self.label = label
            self.should_race = should_race

        async def setup(self) -> None:
            events.append(f"setup:{self.label}")
            if self.should_race:
                raise FakeUniqueViolation(
                    'duplicate key value violates unique constraint "checkpoint_migrations_pkey"'
                )

    savers = [
        FakeSaver("first", True),
        FakeSaver("second", False),
    ]

    class FakeAsyncPostgresSaver:
        @staticmethod
        def from_conn_string(conn_str: str):
            saver = savers.pop(0)
            events.append(f"conn:{conn_str}:{saver.label}")

            @asynccontextmanager
            async def _cm():
                events.append(f"enter:{saver.label}")
                try:
                    yield saver
                finally:
                    events.append(f"exit:{saver.label}")

            return _cm()

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pass@host/db")
    monkeypatch.setitem(
        sys.modules,
        "langgraph.checkpoint.postgres.aio",
        SimpleNamespace(AsyncPostgresSaver=FakeAsyncPostgresSaver),
    )

    await router.shutdown_pipeline_persistence()

    checkpointer = await router._get_checkpointer()

    assert checkpointer.label == "second"
    assert events == [
        "conn:postgresql://user:pass@host/db:first",
        "enter:first",
        "setup:first",
        "exit:first",
        "conn:postgresql://user:pass@host/db:second",
        "enter:second",
    ]

    await router.shutdown_pipeline_persistence()

    assert events[-1] == "exit:second"


def test_compiled_graph_cache_distinguishes_persistence_mode(monkeypatch) -> None:
    built: list[bool] = []

    def _fake_build(_deps, checkpointer=None):
        built.append(bool(checkpointer))
        return {"persistent": bool(checkpointer), "build_no": len(built)}

    monkeypatch.setattr(router, "build_pipeline_graph", _fake_build)
    router._compiled_graphs = {}

    no_persist_1 = router._get_or_compile_graph(object(), None)
    no_persist_2 = router._get_or_compile_graph(object(), None)
    persist_1 = router._get_or_compile_graph(object(), object())
    persist_2 = router._get_or_compile_graph(object(), object())

    assert no_persist_1 is no_persist_2
    assert persist_1 is persist_2
    assert no_persist_1 is not persist_1
    assert built == [False, True]
