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


# ── hijos de expansión de un padre superado ────────────────


def test_descarta_la_hoja_de_la_version_vieja_del_padre() -> None:
    """Caso real medido en staging: las dos hojas tienen 57.673 filas y la
    MISMA huella de contenido. El mart quedaba 39 % duplicado."""
    from app.application.marts.sql_macros import _drop_stale_parent_versions

    pref = "caba::df17d1ba-2968-4ae5-9236-ebfb1ea594f5"
    t1 = "caba__presupuesto_ejecutado__d8aaa421__v1_scdf4d2ef_s_saa25a06b"
    t2 = "caba__presupuesto_ejecutado__d8aaa421__v2_scdf4d2ef_s_saa25a06b"
    lives = [
        _LiveRow(resource_identity=f"{pref}::{t1}", schema_name="raw", table_name=t1),
        _LiveRow(resource_identity=f"{pref}::{t2}", schema_name="raw", table_name=t2),
    ]
    quedan, descartadas = _drop_stale_parent_versions(lives)
    assert descartadas == 1
    assert [r.table_name for r in quedan] == [t2]  # gana la versión mayor del padre


def test_no_toca_hojas_distintas_del_mismo_padre() -> None:
    """Dos hojas DISTINTAS de la misma versión son datos distintos."""
    from app.application.marts.sql_macros import _drop_stale_parent_versions

    pref = "caba::uuid"
    a = "x__abc__v2_shoja_a"
    b = "x__abc__v2_shoja_b"
    lives = [
        _LiveRow(resource_identity=f"{pref}::{a}", schema_name="raw", table_name=a),
        _LiveRow(resource_identity=f"{pref}::{b}", schema_name="raw", table_name=b),
    ]
    assert _drop_stale_parent_versions(lives) == (lives, 0)


def test_no_toca_tablas_que_no_son_hijos_de_expansion() -> None:
    """Sin sufijo de hoja, el versionado ya lo maneja el registro."""
    from app.application.marts.sql_macros import _drop_stale_parent_versions

    a = "diputados__bloques__84ff2259__v3"
    b = "diputados__bloques__f3067840__v1"
    lives = [
        _LiveRow(resource_identity=f"p::{a}", schema_name="raw", table_name=a),
        _LiveRow(resource_identity=f"p::{b}", schema_name="raw", table_name=b),
    ]
    assert _drop_stale_parent_versions(lives) == (lives, 0)


def test_no_mezcla_padres_distintos() -> None:
    """La misma hoja de dos recursos distintos son dos datos distintos."""
    from app.application.marts.sql_macros import _drop_stale_parent_versions

    t = "x__abc__v1_shoja"
    lives = [
        _LiveRow(resource_identity=f"caba::uuid_A::{t}", schema_name="raw", table_name=t),
        _LiveRow(resource_identity=f"caba::uuid_B::{t}", schema_name="raw", table_name=t),
    ]
    assert _drop_stale_parent_versions(lives) == (lives, 0)


# ── exact_columns: la dimensión de más ─────────────────────


def test_exact_columns_descarta_la_tabla_con_una_dimension_de_mas() -> None:
    """`require_all_columns` exige que estén las esperadas; no dice nada de las
    de MÁS, y una columna de más suele ser una dimensión.

    Medido en `empleo_registrado_argentina`: 14 tablas nacionales
    (fecha, letra, puestos → 51.355 filas), 9 por `zona_prov` (740.607) y 8 por
    `provincia` (670.563), todas proyectadas a (fecha, letra, puestos) y
    apiladas. El 96,5 % de las filas era detalle provincial con la provincia
    borrada, sumado encima del total nacional.
    """
    from app.application.marts.sql_macros import _build_union

    esperadas = ["fecha", "letra", "puestos"]
    cols = {
        ("raw", "nacional"): {"fecha", "letra", "puestos", "_source_dataset_id"},
        ("raw", "por_provincia"): {"fecha", "letra", "puestos", "provincia"},
        ("raw", "por_zona"): {"fecha", "zona_prov", "letra", "puestos"},
    }

    class _Eng:
        def connect(self) -> Any:  # pragma: no cover - no se usa
            raise AssertionError("no debería consultar la DB")

    lives = [_live(f"p::{t}", t) for _, t in cols]
    import app.application.marts.sql_macros as m

    original = m._query_columns
    m._query_columns = lambda engine, pares: cols  # type: ignore[assignment]
    try:
        sql = _build_union(
            lives,
            expected_columns=esperadas,
            require_all_columns=True,
            exact_columns=True,
            engine=_Eng(),
        )
    finally:
        m._query_columns = original  # type: ignore[assignment]

    assert "nacional" in sql
    assert "por_provincia" not in sql, "una dimensión de más no puede entrar al UNION"
    assert "por_zona" not in sql


def test_sin_exact_columns_el_comportamiento_no_cambia() -> None:
    """Es opt-in: ningún mart existente cambia por esto."""
    from app.application.marts.sql_macros import _build_union

    cols = {
        ("raw", "nacional"): {"fecha", "letra", "puestos"},
        ("raw", "por_provincia"): {"fecha", "letra", "puestos", "provincia"},
    }

    class _Eng:
        def connect(self) -> Any:  # pragma: no cover
            raise AssertionError("no debería consultar la DB")

    lives = [_live(f"p::{t}", t) for _, t in cols]
    import app.application.marts.sql_macros as m

    original = m._query_columns
    m._query_columns = lambda engine, pares: cols  # type: ignore[assignment]
    try:
        sql = _build_union(
            lives,
            expected_columns=["fecha", "letra", "puestos"],
            require_all_columns=True,
            engine=_Eng(),
        )
    finally:
        m._query_columns = original  # type: ignore[assignment]

    assert "por_provincia" in sql


def test_las_columnas_internas_no_cuentan_como_dimension() -> None:
    """`_source_dataset_id` lo agrega el colector, no la fuente."""
    from app.application.marts.sql_macros import _build_union

    cols = {("raw", "nacional"): {"fecha", "letra", "puestos", "_source_dataset_id", "_x"}}

    class _Eng:
        def connect(self) -> Any:  # pragma: no cover
            raise AssertionError

    import app.application.marts.sql_macros as m

    original = m._query_columns
    m._query_columns = lambda engine, pares: cols  # type: ignore[assignment]
    try:
        sql = _build_union(
            [_live("p::nacional", "nacional")],
            expected_columns=["fecha", "letra", "puestos"],
            require_all_columns=True,
            exact_columns=True,
            engine=_Eng(),
        )
    finally:
        m._query_columns = original  # type: ignore[assignment]

    assert "nacional" in sql


# ── red por huella de contenido ────────────────────────────


class _EngHuella:
    """Engine falso: responde row_count, columnas y huellas."""

    def __init__(self, filas: dict, huellas: dict) -> None:
        self.filas, self.huellas, self.consultas = filas, huellas, 0

    def connect(self) -> Any:
        return _ConnHuella(self)


class _ConnHuella:
    def __init__(self, eng: _EngHuella) -> None:
        self.eng = eng
        self.ultimo: Any = None

    def execute(self, sql: Any, *_: Any) -> Any:
        self.ultimo = str(sql)
        self.eng.consultas += 1
        return self

    def fetchall(self) -> list[Any]:
        return [
            type("R", (), {"schema_name": s, "table_name": t, "row_count": n})()
            for (s, t), n in self.eng.filas.items()
        ]

    def scalar(self) -> Any:
        for (_s, t), h in self.eng.huellas.items():
            if f'"{t}"' in self.ultimo:
                return h
        return None

    def __enter__(self) -> _ConnHuella:
        return self

    def __exit__(self, *_: Any) -> None:
        return None


def _con_cols(monkeypatch: Any, cols: dict) -> None:
    import app.application.marts.sql_macros as m

    monkeypatch.setattr(m, "_query_columns", lambda engine, pares: cols)


def test_descarta_las_tablas_con_el_mismo_contenido(monkeypatch: Any) -> None:
    """Caso real: entre las nacionales de empleo, 3.895 filas aparece 6 veces y
    son sólo DOS datasets (huellas 759310f2… ×2 y 6ba01ebc… ×3)."""
    from app.application.marts.sql_macros import _drop_duplicate_tables

    tablas = ["a", "b", "c"]
    cols = {("raw", t): {"fecha", "letra", "puestos", "_source_dataset_id"} for t in tablas}
    _con_cols(monkeypatch, cols)
    eng = _EngHuella(
        filas={("raw", t): 3895 for t in tablas},
        huellas={("raw", "a"): "H1", ("raw", "b"): "H1", ("raw", "c"): "H2"},
    )
    quedan, descartadas = _drop_duplicate_tables([_live(f"p::{t}", t) for t in tablas], eng)
    assert descartadas == 1
    assert {r.table_name for r in quedan} == {"a", "c"}


def test_el_prefiltro_evita_huellear_lo_que_no_hace_falta(monkeypatch: Any) -> None:
    """Sin `row_count` repetido no se huellea nada: es lo que hace que esto no
    cueste minutos en cada build."""
    from app.application.marts.sql_macros import _drop_duplicate_tables

    cols = {("raw", t): {"x"} for t in ("a", "b")}
    _con_cols(monkeypatch, cols)
    eng = _EngHuella(filas={("raw", "a"): 100, ("raw", "b"): 200}, huellas={})
    quedan, descartadas = _drop_duplicate_tables([_live("p::a", "a"), _live("p::b", "b")], eng)
    assert descartadas == 0
    assert eng.consultas == 1  # sólo la de row_count


def test_row_count_igual_pero_contenido_distinto_no_se_toca(monkeypatch: Any) -> None:
    """Mismo tamaño no es lo mismo que mismo dato — medido: dentro del grupo de
    3.895 conviven dos datasets distintos."""
    from app.application.marts.sql_macros import _drop_duplicate_tables

    cols = {("raw", t): {"x"} for t in ("a", "b")}
    _con_cols(monkeypatch, cols)
    eng = _EngHuella(
        filas={("raw", "a"): 3895, ("raw", "b"): 3895},
        huellas={("raw", "a"): "H1", ("raw", "b"): "H2"},
    )
    quedan, descartadas = _drop_duplicate_tables([_live("p::a", "a"), _live("p::b", "b")], eng)
    assert descartadas == 0
    assert len(quedan) == 2


def test_la_huella_ignora_las_columnas_internas() -> None:
    """`_source_dataset_id` es distinto en cada tabla por construcción. Una
    huella que lo incluya nunca coincide — que es lo que le pasó a la primera
    versión de esta función y por lo que no descartaba nada."""
    import re as _re

    from app.application.marts.sql_macros import _huella

    capturado = {}

    class _E:
        def connect(self) -> Any:
            return self

        def execute(self, sql: Any) -> Any:
            capturado["sql"] = str(sql)
            return self

        def scalar(self) -> str:
            return "H"

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

    _huella(_E(), "raw", "t", {"fecha", "puestos", "_source_dataset_id", "_x"})
    sql = capturado["sql"]
    # la proyección es la del subquery, no la del md5 de afuera
    interno = _re.search(r"FROM \(SELECT (.+?) FROM ", sql).group(1)
    assert "_source_dataset_id" not in interno
    assert "_x" not in interno
    assert '"fecha"' in interno and '"puestos"' in interno


def test_sin_engine_es_no_op() -> None:
    from app.application.marts.sql_macros import _drop_duplicate_tables

    lives = [_live("p::a", "a"), _live("p::b", "b")]
    assert _drop_duplicate_tables(lives, None) == (lives, 0)
