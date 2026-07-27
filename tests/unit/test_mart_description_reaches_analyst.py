"""A mart's curated description must reach the analyst that writes the answer.

`table_descriptions` — the only channel carrying prose about a table into
the final answer — was built exclusively from `table_catalog`, which is the
`cache_*` catalog and holds zero rows for `mart.*`. So for all 71 marts the
list came out empty, and whatever the mart author wrote reached nobody.

That description is where the semantics a number cannot carry live. The
budget mart says, in capitals:

    ⚠️ UNIDADES: `credito_vigente` y `credito_devengado` están expresados
    en MILLONES DE PESOS ARGENTINOS ... NUNCA reportar el número crudo
    como pesos.

Measured on staging 2026-07-27: asked what was transferred to national
universities in 2024, the pipeline retrieved the correct value —
`3326595.47`, meaning 3,33 billones de pesos — and answered
"$3.326.595.466". Off by six orders of magnitude, stated as fact, with the
right number in hand. The instruction that would have prevented it existed,
was correct, and was never delivered.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.application.pipeline.connectors.sandbox import get_mart_descriptions

_UNITS_WARNING = (
    "⚠️ UNIDADES: `credito_vigente` y `credito_devengado` están expresados en "
    "MILLONES DE PESOS ARGENTINOS. NUNCA reportar el número crudo como pesos."
)


class _Sandbox:
    """Minimal stand-in exposing the private engine hook the helper uses."""

    def __init__(self, rows):
        self._rows = rows
        self.queries: list[str] = []
        self.params: list[dict] = []

    def _get_engine(self):
        sandbox = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_a):
                return False

            def execute(self, statement, params=None):
                sandbox.queries.append(str(statement))
                sandbox.params.append(params or {})
                return SimpleNamespace(fetchall=lambda: sandbox._rows)

            def rollback(self):
                pass

        class _Engine:
            @staticmethod
            def connect():
                return _Conn()

        return _Engine()


def _row(view: str, description: str):
    return SimpleNamespace(mart_view_name=view, description=description)


class TestMartDescriptionLookup:
    @pytest.mark.asyncio
    async def test_returns_the_description_keyed_by_qualified_name(self) -> None:
        sandbox = _Sandbox([_row("presupuesto_consolidado", _UNITS_WARNING)])
        out = await get_mart_descriptions(["mart.presupuesto_consolidado"], sandbox)
        assert out == {"mart.presupuesto_consolidado": _UNITS_WARNING}

    @pytest.mark.asyncio
    async def test_the_units_warning_survives_intact(self) -> None:
        """Truncating this would silently drop the part that matters."""
        sandbox = _Sandbox([_row("presupuesto_consolidado", _UNITS_WARNING)])
        out = await get_mart_descriptions(["mart.presupuesto_consolidado"], sandbox)
        text_out = out["mart.presupuesto_consolidado"]
        assert "MILLONES" in text_out
        assert "NUNCA reportar el número crudo" in text_out

    @pytest.mark.asyncio
    async def test_strips_the_schema_before_querying(self) -> None:
        """mart_definitions stores the bare view name, not `mart.x`."""
        sandbox = _Sandbox([])
        await get_mart_descriptions(["mart.presupuesto_consolidado"], sandbox)
        assert sandbox.params[0]["views"] == ["presupuesto_consolidado"]

    @pytest.mark.asyncio
    async def test_ignores_non_mart_tables(self) -> None:
        """`cache_*` tables are covered by table_catalog; no round trip here."""
        sandbox = _Sandbox([])
        out = await get_mart_descriptions(["cache_presupuesto_credito_2024"], sandbox)
        assert out == {}
        assert not sandbox.queries

    @pytest.mark.asyncio
    async def test_missing_sandbox_is_not_an_error(self) -> None:
        assert await get_mart_descriptions(["mart.x"], None) == {}

    @pytest.mark.asyncio
    async def test_a_failing_lookup_degrades_quietly(self) -> None:
        """Losing the description must not take the whole answer down."""

        class _Broken:
            def _get_engine(self):
                raise RuntimeError("no engine")

        assert await get_mart_descriptions(["mart.x"], _Broken()) == {}
