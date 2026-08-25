"""Tests for finding our fields in a source that renamed them.

Written against the 2026-08-10 HCDN change, both sides of it, because the point
of this module is not that it maps fields — it is knowing which kinds of change
it can absorb by itself and which it cannot. A test suite that only proves the
happy path would hide exactly that line.
"""

from __future__ import annotations

import pytest

from app.application.collection.field_mapping import (
    FieldSpec,
    Mapping,
    normalize_key,
    propose_mapping_with_llm,
    resolve_mapping,
)

_SPECS = (
    FieldSpec("legajo", identity=True),
    FieldSpec("apellido"),
    FieldSpec("nombre"),
    FieldSpec("escalafon", aliases=("escalafón",)),
    FieldSpec("area_desempeno", aliases=("Área de Desempeño", "estructura")),
    FieldSpec("convenio"),
)

_ANTES = ["Legajo", "Apellido", "Nombre", "Escalafón", "Área de Desempeño", "Convenio"]
_HOY = ["LEGAJO", "APELLIDO", "NOMBRE", "ESCALAFON", "ESTRUCTURA", "CONVENIO"]


# ── normalización ──────────────────────────────────────────────


def test_case_accents_and_punctuation_all_collapse():
    assert normalize_key("Área de Desempeño") == normalize_key("AREA DE DESEMPENO")
    assert normalize_key("area_de_desempeno") == normalize_key("Área de Desempeño")


def test_a_real_rename_does_not_collapse():
    # The honest half: `ESTRUCTURA` and `Área de Desempeño` share nothing, and
    # pretending otherwise would map fields by accident.
    assert normalize_key("ESTRUCTURA") != normalize_key("Área de Desempeño")


def test_a_number_survives_normalisation():
    assert normalize_key("Tasa 2024") == "tasa2024"


# ── el escalón determinista ────────────────────────────────────


def test_the_old_spelling_maps_completely():
    m = resolve_mapping(_SPECS, _ANTES)
    assert m.usable
    assert m.unmapped == ()


def test_the_uppercase_rename_is_absorbed_without_a_model():
    # Five of the six fields changed only in case: the deterministic tier alone
    # recovers those, which is the measurement that justifies running it first.
    m = resolve_mapping(_SPECS, _HOY)
    assert m.by_field["apellido"] == "APELLIDO"
    assert m.by_field["legajo"] == "LEGAJO"
    assert m.tier_by_field["apellido"] == "normalized"
    assert m.usable, "el legajo se recuperó, así que se puede escribir"


def test_the_genuine_rename_is_reached_through_a_known_alias():
    m = resolve_mapping(_SPECS, _HOY)
    assert m.by_field["area_desempeno"] == "ESTRUCTURA"


def test_without_the_alias_the_rename_stays_unmapped():
    # The line this module draws: strings cannot get from `Área de Desempeño`
    # to `ESTRUCTURA`. Said out loud so nobody expects them to.
    specs = (FieldSpec("area_desempeno", aliases=("Área de Desempeño",)),)
    m = resolve_mapping(specs, ["ESTRUCTURA"])
    assert m.unmapped == ("area_desempeno",)
    assert m.unused_source_keys == ("ESTRUCTURA",)


def test_an_exact_hit_wins_over_a_normalised_one():
    m = resolve_mapping((FieldSpec("nombre"),), ["NOMBRE", "nombre"])
    assert m.by_field["nombre"] == "nombre"
    assert m.tier_by_field["nombre"] == "exact"


def test_two_keys_that_differ_only_in_case_are_refused_not_guessed():
    m = resolve_mapping((FieldSpec("nombre"),), ["NOMBRE", "Nombre"])
    assert m.unmapped == ("nombre",)


def test_one_source_key_is_never_spent_twice():
    specs = (FieldSpec("a", aliases=("x",)), FieldSpec("b", aliases=("x",)))
    m = resolve_mapping(specs, ["x"])
    assert len(m.by_field) == 1


# ── la negativa ────────────────────────────────────────────────


def test_a_lost_identity_field_makes_the_batch_unusable():
    m = resolve_mapping(_SPECS, ["APELLIDO", "NOMBRE"])
    assert not m.usable
    assert m.unmapped_identity == ("legajo",)


def test_a_lost_ordinary_field_does_not_stop_the_write():
    m = resolve_mapping(_SPECS, [k for k in _HOY if k != "CONVENIO"])
    assert m.usable
    assert "convenio" in m.unmapped


# ── proyección ─────────────────────────────────────────────────


def test_applying_the_mapping_renames_the_record():
    m = resolve_mapping(_SPECS, _HOY)
    out = m.apply({"LEGAJO": 804905, "APELLIDO": " COSTA ", "ESTRUCTURA": "BLOQUE"})
    assert out["legajo"] == "804905"
    assert out["apellido"] == "COSTA"
    assert out["area_desempeno"] == "BLOQUE"


def test_an_unmapped_field_projects_to_empty_not_missing():
    m = resolve_mapping(_SPECS, [k for k in _HOY if k != "CONVENIO"])
    assert m.apply({"LEGAJO": 1})["convenio"] == ""


def test_a_null_value_projects_to_empty():
    m = resolve_mapping((FieldSpec("nombre"),), ["nombre"])
    assert m.apply({"nombre": None})["nombre"] == ""


# ── el escalón del modelo ──────────────────────────────────────


class _LLM:
    def __init__(self, content):
        self.content = content
        self.calls = 0

    async def chat_json(self, **kw):
        self.calls += 1
        return type("R", (), {"content": self.content})()


@pytest.mark.asyncio
async def test_the_model_places_what_the_strings_could_not():
    specs = (FieldSpec("area_desempeno", aliases=("Área de Desempeño",)),)
    base = resolve_mapping(specs, ["ESTRUCTURA"])
    llm = _LLM('{"mapping": {"area_desempeno": "ESTRUCTURA"}}')

    m = await propose_mapping_with_llm(specs, base, [{"ESTRUCTURA": "DIR. COMISIONES"}], llm=llm)

    assert m.by_field["area_desempeno"] == "ESTRUCTURA"
    assert m.tier_by_field["area_desempeno"] == "llm"
    assert m.unmapped == ()


@pytest.mark.asyncio
async def test_the_model_is_not_asked_when_nothing_is_missing():
    specs = (FieldSpec("nombre"),)
    base = resolve_mapping(specs, ["nombre"])
    llm = _LLM("{}")

    await propose_mapping_with_llm(specs, base, [{}], llm=llm)

    assert llm.calls == 0, "no se paga una llamada por un mapeo ya resuelto"


@pytest.mark.asyncio
async def test_a_column_the_model_invented_is_dropped():
    specs = (FieldSpec("area_desempeno"),)
    base = resolve_mapping(specs, ["ESTRUCTURA"])
    llm = _LLM('{"mapping": {"area_desempeno": "NO_EXISTE"}}')

    m = await propose_mapping_with_llm(specs, base, [{}], llm=llm)

    assert m.unmapped == ("area_desempeno",)


@pytest.mark.asyncio
async def test_the_model_cannot_overrule_a_field_already_matched():
    specs = (FieldSpec("nombre"), FieldSpec("otro"))
    base = resolve_mapping(specs, ["nombre", "SOBRA"])
    llm = _LLM('{"mapping": {"nombre": "SOBRA"}}')

    m = await propose_mapping_with_llm(specs, base, [{}], llm=llm)

    assert m.by_field["nombre"] == "nombre"


@pytest.mark.asyncio
async def test_a_model_that_fails_leaves_the_deterministic_result_intact():
    specs = (FieldSpec("legajo", identity=True), FieldSpec("area_desempeno"))
    base = resolve_mapping(specs, ["legajo", "ESTRUCTURA"])

    class _Boom:
        async def chat_json(self, **kw):
            raise TimeoutError("bedrock")

    m = await propose_mapping_with_llm(specs, base, [{}], llm=_Boom())

    assert m.by_field["legajo"] == "legajo"
    assert m.usable


def test_an_empty_mapping_is_unusable_when_identity_is_required():
    assert Mapping(unmapped_identity=("legajo",)).usable is False


# ── lo aprendido ───────────────────────────────────────────────


class _Engine:
    """An engine that records what was executed, without a database."""

    def __init__(self, rows=(), raise_on_begin=False):
        self.rows = list(rows)
        self.raise_on_begin = raise_on_begin
        self.statements: list[tuple[str, dict]] = []

    def begin(self):
        engine = self

        class _Ctx:
            def __enter__(self):
                if engine.raise_on_begin:
                    raise RuntimeError("db caída")
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                engine.statements.append((str(stmt), params or {}))

                class _Res:
                    @staticmethod
                    def fetchall():
                        return engine.rows

                return _Res()

        return _Ctx()


class _Row:
    def __init__(self, field, source_key):
        self.field = field
        self.source_key = source_key


def test_a_learned_key_is_used_on_the_next_run():
    from app.application.collection.field_mapping import learned_aliases, with_learned

    engine = _Engine(rows=[_Row("area_desempeno", "ESTRUCTURA")])
    specs = with_learned((FieldSpec("area_desempeno"),), learned_aliases(engine, "staff_hcdn"))
    m = resolve_mapping(specs, ["ESTRUCTURA"])
    assert m.by_field["area_desempeno"] == "ESTRUCTURA"
    assert m.tier_by_field["area_desempeno"] == "exact", "ya no cuesta una llamada"


def test_a_name_a_person_wrote_wins_over_one_a_model_proposed():
    from app.application.collection.field_mapping import with_learned

    specs = with_learned((FieldSpec("x", aliases=("DECLARADO",)),), {"x": ("APRENDIDO",)})
    assert specs[0].aliases == ("DECLARADO", "APRENDIDO")


def test_a_learned_key_already_declared_is_not_duplicated():
    from app.application.collection.field_mapping import with_learned

    specs = with_learned((FieldSpec("x", aliases=("A",)),), {"x": ("A",)})
    assert specs[0].aliases == ("A",)


def test_only_what_the_model_contributed_is_remembered():
    from app.application.collection.field_mapping import remember_mapping

    engine = _Engine()
    m = Mapping(
        by_field={"a": "A", "b": "B"},
        tier_by_field={"a": "normalized", "b": "llm"},
    )
    assert remember_mapping(engine, "c", m) == 1
    remembered = [p for _, p in engine.statements if p.get("field")]
    assert [p["field"] for p in remembered] == ["b"], "lo determinista ya se resuelve solo"


def test_nothing_to_remember_touches_no_database():
    from app.application.collection.field_mapping import remember_mapping

    engine = _Engine()
    m = Mapping(by_field={"a": "A"}, tier_by_field={"a": "exact"})
    assert remember_mapping(engine, "c", m) == 0
    assert engine.statements == []


def test_a_database_that_is_down_does_not_break_the_connector():
    from app.application.collection.field_mapping import learned_aliases, remember_mapping

    engine = _Engine(raise_on_begin=True)
    assert learned_aliases(engine, "c") == {}
    m = Mapping(by_field={"b": "B"}, tier_by_field={"b": "llm"})
    assert remember_mapping(engine, "c", m) == 0
