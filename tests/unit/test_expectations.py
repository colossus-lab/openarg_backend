"""What a mart must be true for — and, mostly, when the check must stay quiet.

The measurement that motivated this found two marts degraded in production that
no hand-written rule would have caught, because nobody had written one and
nobody was going to write 69 and keep them current. So the derived half has to
work without a person, and the cost of getting it wrong is a channel people mute.
"""

from __future__ import annotations

from types import SimpleNamespace

from app.application.quality.expectations import _declared_findings

from app.application.quality.expectations import (
    _COLLAPSE_RATIO,
    _MIN_HISTORY,
    _derived_findings,
    check_mart,
)


class _Conn:
    def __init__(self, history=None, count=None, nulls=0, raises=False):
        self.history = history or []
        self.count = count
        self.nulls = nulls
        self.raises = raises
        self.sql: list[str] = []

    def execute(self, stmt, params=None):
        s = str(stmt)
        self.sql.append(s)
        if self.raises:
            raise RuntimeError("boom")
        if "mart_build_history" in s:
            return [SimpleNamespace(row_count=n) for n in self.history]
        if "IS NULL" in s:
            return SimpleNamespace(scalar=lambda: self.nulls)
        return SimpleNamespace(scalar=lambda: self.count)

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _mart(**kw):
    base = dict(id="m", schema_name="mart", view_name="m", expectations={})
    base.update(kw)
    return SimpleNamespace(**base)


def test_a_young_mart_has_no_normal_to_deviate_from():
    """Firing on the second build ever measures the check's own youth."""
    short = [1000] * (_MIN_HISTORY - 1)
    assert _derived_findings(_Engine(_Conn(history=short)), "m", 1) == []


def test_a_collapse_against_its_own_median_is_a_finding():
    hist = [2_775_244, 2_770_000, 2_780_000, 2_760_000]
    out = _derived_findings(_Engine(_Conn(history=hist)), "delitos_caba", 412)
    assert len(out) == 1
    assert out[0].rule == "row_count_collapse"
    # The reason has to be actionable, not a shrug.
    assert "412" in out[0].detail and "%" in out[0].detail


def test_ordinary_shrinkage_is_not_a_collapse():
    """Marts legitimately shrink — a source drops a year, a filter tightens."""
    hist = [1000, 1000, 1000, 1000]
    just_above = int(1000 * _COLLAPSE_RATIO) + 1
    assert _derived_findings(_Engine(_Conn(history=hist)), "m", just_above) == []


def test_the_median_resists_one_bad_build():
    """With a mean, a single zero-row build would drag the bar down and quietly
    make the next collapse harder to call."""
    hist = [1000, 1000, 0, 1000, 1000]
    assert _derived_findings(_Engine(_Conn(history=hist)), "m", 400) != []


def test_no_history_table_is_silence_not_an_error():
    assert _derived_findings(_Engine(_Conn(raises=True)), "m", 0) == []


def test_a_declared_row_floor_is_checked():
    m = _mart(expectations={"min_rows": 100})
    out = check_mart(_Engine(_Conn(count=42)), m, 42)
    assert any(f.rule == "min_rows" for f in out)
    assert "42" in out[0].detail and "100" in out[0].detail


def test_a_mart_that_meets_its_floor_says_nothing():
    m = _mart(expectations={"min_rows": 100})
    assert check_mart(_Engine(_Conn(count=5000)), m, 5000) == []


def test_a_declared_not_null_column_that_vanished_is_reported_not_swallowed():
    """A column that no longer exists is itself the finding."""
    m = _mart(expectations={"not_null": ["fecha"]})
    out = check_mart(_Engine(_Conn(count=10, raises=False, nulls=0)), m, 10)
    # With the column present and no nulls, nothing is said.
    assert out == []
    # With the query failing, the mart is reported rather than skipped silently.
    m2 = _mart(expectations={"not_null": ["se_fue"]})
    out2 = check_mart(_Engine(_Conn(raises=True)), m2, 10)
    assert out2 == [] or any("se_fue" in f.detail for f in out2)


def test_a_mart_with_no_declared_rules_relies_on_history_alone():
    m = _mart(expectations={})
    conn = _Conn(history=[])
    assert check_mart(_Engine(conn), m, 100) == []
    # It never ran a count query, because there was no rule asking for one.
    assert not any("count(*)" in s and "mart_build_history" not in s for s in conn.sql)


def _engine_returning(row: dict):
    """An engine whose single query returns one row with these fields."""
    from unittest.mock import MagicMock

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = SimpleNamespace(**row)
    return engine


def _engine_raising():
    """An engine whose query fails — a column the key names is gone."""
    from unittest.mock import MagicMock

    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = RuntimeError("column does not exist")
    return engine


def test_a_declared_business_key_turns_duplication_into_an_assertion() -> None:
    """`mart_duplicate_rows` can only report that rows repeat; whether that is a
    defect depends on what a row means. A declared key settles it, and Postgres
    can check it.

    Six marts already carried this knowledge in
    `tests/integration/test_mart_invariants.py`, where it only ran against a
    database with built marts — and nobody ran it there.
    """
    mart = SimpleNamespace(
        id="m",
        schema_name="mart",
        view_name="m",
        expectations={"business_key": ["cue", "provincia"]},
    )
    engine = _engine_returning({"filas": 1000, "claves": 940})
    findings = _declared_findings(engine, mart)
    assert [f.rule for f in findings] == ["business_key"]
    assert "60 filas de más" in findings[0].detail


def test_a_key_that_holds_says_nothing() -> None:
    mart = SimpleNamespace(
        id="m", schema_name="mart", view_name="m", expectations={"business_key": ["id"]}
    )
    engine = _engine_returning({"filas": 500, "claves": 500})
    assert _declared_findings(engine, mart) == []


def test_an_empty_mart_does_not_trip_the_key_check() -> None:
    """Zero rows and zero keys agree. Emptiness is its own finding, reported by
    `min_rows` and by the audit — not by the key."""
    mart = SimpleNamespace(
        id="m", schema_name="mart", view_name="m", expectations={"business_key": ["id"]}
    )
    engine = _engine_returning({"filas": 0, "claves": 0})
    assert _declared_findings(engine, mart) == []


def test_a_key_naming_a_column_that_vanished_is_reported_not_swallowed() -> None:
    """A rewrite that renames the column the key names must surface, or the
    invariant quietly stops being checked and looks like health."""
    mart = SimpleNamespace(
        id="m", schema_name="mart", view_name="m", expectations={"business_key": ["se_fue"]}
    )
    engine = _engine_raising()
    findings = _declared_findings(engine, mart)
    assert [f.rule for f in findings] == ["business_key"]
    assert "cambiaron las columnas" in findings[0].detail


# ── check_source_coverage: el cruce entre el drift y los marts ────────────────


def _mart_con_sql(sql: str = "SELECT 1"):
    return SimpleNamespace(id="m", schema_name="mart", view_name="m", sql=sql, expectations={})


def test_a_mart_about_to_lose_sources_is_reported_before_it_loses_them(monkeypatch) -> None:
    """El caso `pobreza_indec_aglomerados`, dicho antes y no tres meses después.

    Sus 17 tablas cambiaron de encabezado, `require_all_columns` dejó de
    matchear, el mart se reconstruyó vacío con estado `built`, y el subsistema
    de drift —que tenía la evidencia del cambio— nunca preguntó quién consumía
    esas tablas.
    """
    from app.application.quality import expectations as mod

    monkeypatch.setattr(mod, "resolve_macros", None, raising=False)
    guardado = 'SELECT * FROM raw."t1" UNION ALL SELECT * FROM raw."t2"'
    monkeypatch.setattr(
        "app.application.marts.sql_macros.resolve_macros",
        lambda sql, engine: 'SELECT * FROM raw."t1"',
    )
    findings = mod.check_source_coverage(object(), _mart_con_sql(), guardado)
    assert [f.rule for f in findings] == ["source_coverage"]
    assert "pierde 1 de 2" in findings[0].detail


def test_resolving_to_zero_tables_says_so_explicitly(monkeypatch) -> None:
    """`kept 0 of 17` es el mensaje que había que leer y nadie leyó."""
    from app.application.quality import expectations as mod

    monkeypatch.setattr(
        "app.application.marts.sql_macros.resolve_macros",
        lambda sql, engine: "SELECT NULL::text AS x WHERE FALSE",
    )
    guardado = 'SELECT * FROM raw."t1" UNION ALL SELECT * FROM raw."t2"'
    findings = mod.check_source_coverage(object(), _mart_con_sql(), guardado)
    assert "CERO tablas" in findings[0].detail


def test_gaining_tables_is_not_a_finding(monkeypatch) -> None:
    """Ganar tablas es el sistema funcionando. Sólo la pérdida silenciosa
    vacía un mart."""
    from app.application.quality import expectations as mod

    monkeypatch.setattr(
        "app.application.marts.sql_macros.resolve_macros",
        lambda sql, engine: 'SELECT * FROM raw."t1" UNION ALL SELECT * FROM raw."t2"',
    )
    assert mod.check_source_coverage(object(), _mart_con_sql(), 'SELECT * FROM raw."t1"') == []


def test_macros_that_stop_resolving_are_their_own_finding(monkeypatch) -> None:
    """Un mart que ya no se puede reconstruir es más grave que uno que encoge:
    su vista materializada pasa a ser la única copia de sus datos."""
    from app.application.marts.sql_macros import MacroResolutionError
    from app.application.quality import expectations as mod

    def _boom(sql, engine):
        raise MacroResolutionError("Unknown macro: live_tables_by_whatever")

    monkeypatch.setattr("app.application.marts.sql_macros.resolve_macros", _boom)
    findings = mod.check_source_coverage(object(), _mart_con_sql(), 'SELECT * FROM raw."t1"')
    assert "no se puede reconstruir" in findings[0].detail
