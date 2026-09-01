"""Unir el mismo archivo dos veces duplica filas, no agrega datos.

Medido en staging antes del arreglo: `caba_presupuesto_ejecutado` 1.350.388
filas contra 822.026 únicas (39 % duplicado) y `empleo_registrado_argentina`
947.396 contra 306.841 (68 %). En un mart de presupuesto eso es un total
inflado para cualquiera que sume.
"""

from __future__ import annotations

from typing import Any

from app.application.marts.sql_macros import _drop_identical_content, _LiveRow


class _FakeConn:
    def __init__(self, filas: list[Any]) -> None:
        self.filas = filas

    def execute(self, *_: Any, **__: Any) -> Any:
        return self

    def fetchall(self) -> list[Any]:
        return self.filas

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *_: Any) -> None:
        return None


class _FakeRow:
    def __init__(self, sch: str, tab: str, h: str | None) -> None:
        self.schema_name, self.table_name, self.source_file_hash = sch, tab, h


class _FakeEngine:
    def __init__(self, filas: list[Any]) -> None:
        self.filas = filas

    def connect(self) -> _FakeConn:
        return _FakeConn(self.filas)

    def raise_on_connect(self) -> None: ...


def _live(ident: str, tab: str) -> _LiveRow:
    return _LiveRow(resource_identity=ident, schema_name="raw", table_name=tab)


def test_descarta_el_archivo_repetido() -> None:
    lives = [_live("p::a", "t_a"), _live("p::b", "t_b")]
    eng = _FakeEngine([_FakeRow("raw", "t_a", "H1"), _FakeRow("raw", "t_b", "H1")])
    quedan, descartadas = _drop_identical_content(lives, eng)
    assert descartadas == 1
    assert len(quedan) == 1


def test_no_toca_archivos_distintos() -> None:
    lives = [_live("p::a", "t_a"), _live("p::b", "t_b")]
    eng = _FakeEngine([_FakeRow("raw", "t_a", "H1"), _FakeRow("raw", "t_b", "H2")])
    quedan, descartadas = _drop_identical_content(lives, eng)
    assert descartadas == 0
    assert len(quedan) == 2


def test_sin_hash_no_puede_hacer_nada_y_no_inventa() -> None:
    """El 90 % de las tablas vivas no tiene hash. Ante la duda, conservar: un
    mart al que le falta una fuente es peor que uno con filas de más."""
    lives = [_live("p::a", "t_a"), _live("p::b", "t_b")]
    eng = _FakeEngine([])
    quedan, descartadas = _drop_identical_content(lives, eng)
    assert descartadas == 0
    assert len(quedan) == 2


def test_es_determinista() -> None:
    """Dos builds con la misma entrada tienen que producir el mismo mart."""
    eng = _FakeEngine(
        [_FakeRow("raw", "t_a", "H1"), _FakeRow("raw", "t_b", "H1"), _FakeRow("raw", "t_c", "H1")]
    )
    a = _drop_identical_content(
        [_live("p::c", "t_c"), _live("p::a", "t_a"), _live("p::b", "t_b")], eng
    )
    b = _drop_identical_content(
        [_live("p::b", "t_b"), _live("p::c", "t_c"), _live("p::a", "t_a")], eng
    )
    assert [r.table_name for r in a[0]] == [r.table_name for r in b[0]] == ["t_a"]


def test_si_la_consulta_falla_no_rompe_el_build() -> None:
    class Rota:
        def connect(self) -> Any:
            raise RuntimeError("db caída")

    lives = [_live("p::a", "t_a"), _live("p::b", "t_b")]
    quedan, descartadas = _drop_identical_content(lives, Rota())
    assert (len(quedan), descartadas) == (2, 0)


def test_sin_engine_o_con_una_sola_tabla_es_no_op() -> None:
    lives = [_live("p::a", "t_a")]
    assert _drop_identical_content(lives, None) == (lives, 0)
    assert _drop_identical_content(lives, _FakeEngine([])) == (lives, 0)
