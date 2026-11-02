"""Tests for `application/repair/parse_repair.py`.

The pure proposal logic (`propose_col_n_rename`) is unit-tested here against
in-memory data. The DDL-applying orchestrator (`repair_col_n_table`) is
exercised in integration tests against staging Postgres because SQLite
doesn't expose `information_schema.columns`.

Specs/021-parser-hardening Phase 2.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock

import pytest

from app.application.repair.parse_repair import (
    propose_col_n_rename,
    propose_llm_assisted_rename,
    propose_title_as_columns_rename,
)


def test_propose_simple_buried_header():
    """col_0..col_2 + first row = real header → proposes rename + 1 row delete."""
    old_cols = ["col_0", "col_1", "col_2"]
    sample_rows = [
        ["provincia", "anio", "valor"],  # the real header
        ["BA", 2020, 1.5],
        ["Córdoba", 2021, 2.0],
    ]
    new_cols, rows_to_delete, reason = propose_col_n_rename(old_cols, sample_rows)
    assert reason == "applied"
    assert "provincia" in new_cols
    assert "anio" in new_cols
    assert "valor" in new_cols
    assert rows_to_delete == 1


def test_propose_skip_clean_input():
    """Real-named cols → garbage ratio below threshold → skip."""
    old_cols = ["provincia", "anio", "valor"]
    sample_rows = [["BA", 2020, 1.5]]
    new_cols, rows_to_delete, reason = propose_col_n_rename(old_cols, sample_rows)
    assert reason == "garbage_ratio_below_threshold"
    assert new_cols == old_cols
    assert rows_to_delete == 0


def test_propose_skip_empty_table():
    """Garbage cols but no data rows → can't infer header → skip."""
    old_cols = ["col_0", "col_1", "col_2"]
    sample_rows: list[list] = []
    new_cols, rows_to_delete, reason = propose_col_n_rename(old_cols, sample_rows)
    assert reason == "table_empty"
    assert new_cols == old_cols


def test_propose_skip_no_buried_header():
    """Garbage cols but data is purely numeric (no buried header) → skip."""
    old_cols = ["col_0", "col_1", "col_2"]
    sample_rows = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ]
    new_cols, rows_to_delete, reason = propose_col_n_rename(old_cols, sample_rows)
    # promote_buried_headers needs a header row above a data row to fire.
    # Pure-numeric data → reason will be "no_improvement" or "no_renames_needed"
    # depending on the exact pathway; either way, no rename is applied.
    assert reason in {"no_improvement", "no_renames_needed"}
    assert new_cols == old_cols


def test_propose_dedupes_collisions():
    """Two cols whose recovered names collide (long byte-equivalent) get
    different names in the proposal."""
    old_cols = ["col_0", "col_1", "col_2", "col_3"]
    long_a = "Cuadro 3.1 Población — A_" + "x" * 80
    long_b = "Cuadro 3.1 Población — A_" + "y" * 80
    sample_rows = [
        ["provincia", long_a, long_b, "valor"],
        ["BA", "1", "2", "3.5"],
    ]
    new_cols, _, reason = propose_col_n_rename(old_cols, sample_rows)
    assert reason == "applied"
    # all distinct
    assert len(set(new_cols)) == len(new_cols)
    # all fit Postgres byte budget
    assert all(len(c.encode("utf-8")) <= 63 for c in new_cols)


def test_propose_threshold_50pct_col_n():
    """50% col_N + 50% real → still triggers because matches default 0.40 threshold."""
    old_cols = ["provincia", "col_0", "anio", "col_2"]
    sample_rows = [
        ["BA_label", "anio_label", "valor_label", "categoria_label"],
        ["BA", "2020", "1.5", "A"],
        ["Córdoba", "2021", "2.0", "B"],
    ]
    new_cols, rows_to_delete, reason = propose_col_n_rename(old_cols, sample_rows)
    # 2/4 = 0.5 > 0.40 default threshold → should fire if buried header present
    assert reason == "applied"
    assert rows_to_delete == 1


def test_propose_threshold_below_skips():
    """25% col_N → below threshold → skip even if buried header present."""
    old_cols = ["provincia", "anio", "valor", "col_3"]
    sample_rows = [
        ["a_label", "b_label", "c_label", "d_label"],
        ["BA", "2020", "1.5", "X"],
    ]
    new_cols, _, reason = propose_col_n_rename(old_cols, sample_rows)
    assert reason == "garbage_ratio_below_threshold"
    assert new_cols == old_cols


# ---------- title_as_columns ----------


def _pami_like_cols(n: int = 36) -> list[str]:
    """Reproduce the PAMI fingerprint: long shared title + dedup suffix.
    The last few cols differ (URL/UUID/date) — same as real-world."""
    base = "LISTADO DE LLAMADOS DE LICITACIONES PUBLICAS - AÑO 2017 / Gcia"
    business = [base if i == 0 else f"{base[:55]}_{i + 1}" for i in range(n - 5)]
    tail = [
        "095f8efe-1400-44a2-9bf4-4e025f094027",
        "http://datos.pami.org.ar/dataset/abc",
        "http://datos.pami.org.ar/dataset/def_2",
        "http://datos.pami.org.ar/dataset/ghi_3",
        "2026-05-04",
    ]
    return business + tail


def test_propose_title_as_columns_pami_pattern():
    """Real-world PAMI fingerprint → applies, deletes 2 rows, semantic names."""
    old_cols = _pami_like_cols(36)
    sample = [
        [None] * 36,  # row 0: separator
        # row 1: real headers
        [
            "N° L.P",
            "EXPEDIENTE",
            "OBJETO",
            "DESTINO",
            "FECHA DE APERTURA",
            "ESTADO",
            "RUBRO",
            "PLAZO DE CONTRATO",
        ]
        + ["headercol"] * 23
        + [None, None, None, None, None],
        ["1", "EX-001", "Servicio X", "GESP", "2020-01-01", "ADJUDICADO", "BIENES", "30 días"]
        + ["x"] * 23
        + ["data1", "data2", "data3", "data4", "data5"],
    ]
    new_cols, rows_to_delete, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "applied"
    assert rows_to_delete == 2
    # First col 'N° L.P' becomes a normalized identifier
    assert new_cols[0] == "n_l_p"
    assert new_cols[1] == "expediente"
    assert new_cols[2] == "objeto"
    # Tail cells with NULL row1 + URL-looking original → metadata_<i>
    assert new_cols[-5].startswith("metadata_") or new_cols[-5].startswith("col_")
    # Date string starting with digit gets `col_` prefix
    assert all(not c[0].isdigit() for c in new_cols)


def test_propose_title_skips_too_few_cols():
    """Below `min_cols` threshold (default 30) → skip."""
    old_cols = ["a"] * 10
    sample = [[None] * 10, ["x"] * 10]
    _, _, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "too_few_cols"


def test_propose_title_skips_no_common_prefix():
    """Cols without shared prefix → skip (not the pattern)."""
    old_cols = ["alpha", "beta", "gamma", "delta", "epsilon"] * 7
    sample = [[None] * 35, ["x"] * 35]
    _, _, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "no_common_prefix"


def test_propose_title_skips_when_row0_has_data():
    """Row 0 not mostly NULL → not the title-as-columns pattern."""
    old_cols = _pami_like_cols(36)
    sample = [
        ["data"] * 36,  # row 0 has data — not a separator
        ["EXPEDIENTE", "OBJETO"] + ["x"] * 34,
    ]
    _, _, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "row0_not_separator"


def test_propose_title_skips_when_row1_has_no_alpha():
    """Row 1 mostly numeric → not headers."""
    old_cols = _pami_like_cols(36)
    sample = [
        [None] * 36,
        ["1", "2", "3", "4"] + [None] * 32,  # only 4 cells, no alpha
    ]
    _, _, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "row1_not_header_like"


def test_propose_title_dedupes_repeated_headers():
    """If row 1 has duplicate header values, emit `_2, _3, ...`."""
    old_cols = _pami_like_cols(36)
    sample = [
        [None] * 36,
        ["estado", "estado", "estado"] + ["filler"] * 28 + [None] * 5,
        ["adj", "adj", "adj"] + ["x"] * 28 + ["d"] * 5,
    ]
    new_cols, _, reason = propose_title_as_columns_rename(old_cols, sample)
    assert reason == "applied"
    assert new_cols[0] == "estado"
    assert new_cols[1] == "estado_2"
    assert new_cols[2] == "estado_3"


# ---------- LLM-assisted (DEBT-021-002) ----------


@pytest.mark.asyncio
async def test_propose_llm_applied_when_llm_returns_valid_proposal():
    old_cols = ["col_0", "col_1", "col_2"]
    sample = [["BA", "2024", "12345"], ["Cordoba", "2024", "67890"]]
    llm = AsyncMock()
    llm.chat_json.return_value.content = json.dumps(
        {"proposed_columns": ["provincia", "anio", "monto"]}
    )

    new_cols, rows, reason = await propose_llm_assisted_rename(old_cols, sample, llm=llm)

    assert reason == "applied"
    assert new_cols == ["provincia", "anio", "monto"]
    assert rows == 0  # LLM doesn't propose row deletions


@pytest.mark.asyncio
async def test_propose_llm_skips_when_length_mismatch():
    old_cols = ["col_0", "col_1", "col_2"]
    sample = [["a", "b", "c"]]
    llm = AsyncMock()
    llm.chat_json.return_value.content = json.dumps(
        {"proposed_columns": ["only_one"]}  # wrong length
    )

    new_cols, _, reason = await propose_llm_assisted_rename(old_cols, sample, llm=llm)

    assert reason.startswith("length_mismatch")
    assert new_cols == old_cols


@pytest.mark.asyncio
async def test_propose_llm_skips_when_too_many_cols():
    """200 cols → skip without LLM call (cost guard)."""
    old_cols = [f"c_{i}" for i in range(200)]
    llm = AsyncMock()

    _, _, reason = await propose_llm_assisted_rename(old_cols, [], llm=llm)

    assert reason == "too_many_cols"
    llm.chat_json.assert_not_called()


@pytest.mark.asyncio
async def test_propose_llm_handles_bad_json_gracefully():
    old_cols = ["col_0", "col_1"]
    llm = AsyncMock()
    llm.chat_json.return_value.content = "not valid json {{"

    _, _, reason = await propose_llm_assisted_rename(old_cols, [["a", "b"]], llm=llm)

    assert reason == "llm_bad_json"


@pytest.mark.asyncio
async def test_propose_llm_normalizes_and_dedupes_response():
    """LLM returns names with caps/duplicates → we still apply normalize+dedupe."""
    old_cols = ["col_0", "col_1", "col_2"]
    llm = AsyncMock()
    llm.chat_json.return_value.content = json.dumps(
        {"proposed_columns": ["Estado", "Estado", "ESTADO"]}
    )

    new_cols, _, reason = await propose_llm_assisted_rename(old_cols, [["a", "b", "c"]], llm=llm)

    assert reason == "applied"
    # All three normalize to `estado`, dedup to estado / estado_2 / estado_3
    assert new_cols[0] == "estado"
    assert new_cols[1] == "estado_2"
    assert new_cols[2] == "estado_3"


def test_accented_u_normalises_to_u_not_o() -> None:
    """Regression guard for a shifted accent-translation table.

    `_normalize_header_to_identifier` is shared by every repair that renames a
    column (`col_n`, `title_as_columns`, `smeared_title`, the LLM tier). Its
    table had six accented vowels mapping onto four `o` targets and two `u`
    targets, so `ú` resolved to `o`: `Común` normalised to `comon` and `Número`
    to `nomero`. Nothing failed loudly — the names were merely wrong, in every
    rename applied since May.
    """
    from app.application.repair.parse_repair import _normalize_header_to_identifier as norm

    assert norm("Común") == "comun"
    assert norm("Número") == "numero"
    assert norm("Múltiple") == "multiple"
    # The vowels that were already correct must stay correct.
    assert norm("Ámbito") == "ambito"
    assert norm("Jurisdicción") == "jurisdiccion"
    assert norm("Título") == "titulo"


def test_a_declined_repair_records_why_it_declined() -> None:
    """5.509 declines in 30 days landed with a NULL reason.

    `smeared_title` alone declined 4.397 times against 134 total applies across
    every phase, and the audit could not say why even once — so there was no way
    to tell a correctly conservative sweep from a misconfigured one. A repair
    that fails sets `error_message`; one that declines sets `reason`, and only
    the first was being written.
    """
    from unittest.mock import MagicMock

    from app.application.repair.parse_repair import RepairOutcome, _audit

    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    declined = RepairOutcome(
        table_schema="raw",
        table_name="t",
        ok=False,
        reason="garbage_ratio_below_threshold",
    )
    _audit(engine, run_id=uuid.uuid4(), outcome=declined, operation="skip")

    params = conn.execute.call_args[0][1]
    assert params["err"] == "garbage_ratio_below_threshold"


def test_a_failed_repair_still_records_its_error_not_its_reason() -> None:
    """When both are set the error wins: a repair that tried and broke is a
    different event from one that declined, and the error is the more urgent
    of the two."""
    from unittest.mock import MagicMock

    from app.application.repair.parse_repair import RepairOutcome, _audit

    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    failed = RepairOutcome(
        table_schema="raw",
        table_name="t",
        ok=False,
        reason="applied",
        error_message="deadlock detected",
    )
    _audit(engine, run_id=uuid.uuid4(), outcome=failed, operation="apply")

    params = conn.execute.call_args[0][1]
    assert params["err"] == "deadlock detected"


# ---------- periodos inventados por el LLM (cuadro 1.6 de la EPH, 30-oct-2026) ----------
#
# Las columnas con las que el 1.6 llegó a prod: el título del cuadro, 34
# trimestres sin año (1T-2017 a 2T-2025, con las notas y los "º" que NFKC volvió
# "o") y la columna separadora que sobrevivió como `col_25`. Los nombres que
# están en el informe del incidente van tal cual; los sufijos de los demás se
# reconstruyeron con la misma regla de dedupe.
_CUADRO_1_6_OLD = [
    "Cuadro 1.6 Composición de la tasa de desocupación. Total 31 a",
    # 2017
    "1° trimestre",
    "2° trimestre",
    "3° trimestre",
    "4° trimestre",
    # 2018
    "1° trimestre_2",
    "2° trimestre_2",
    "3° trimestre_2",
    "4° trimestre_2",
    # 2019
    "1° trimestre_3",
    "2° trimestre_3",
    "3° trimestre (1)",
    "4° trimestre_3",
    # 2020
    "1° trimestre_4",
    "2o trimestre",
    "3o trimestre (2)",
    "4o trimestre",
    # 2021
    "1o trimestre",
    "2o trimestre_2",
    "3o trimestre",
    "4o trimestre_2",
    "col_25",
    # 2022
    "1° trimestre_5",
    "2° trimestre_3",
    "3° trimestre_3",
    "4° trimestre_4",
    # 2023
    "1° trimestre_6",
    "2o trimestre_3",
    "3° trimestre_4",
    "4° trimestre_5",
    # 2024
    "1° trimestre_7",
    "2o trimestre_4",
    "3° trimestre_5",
    "4° trimestre_6",
    # 2025
    "1° trimestre_8",
    "2° trimestre_4",
]

# Lo que propuso el modelo: los mismos 36 casilleros, fechados 2003-2011.
_CUADRO_1_6_PROPUESTA = (
    ["categoria_desocupacion"]
    + [f"q{q}_{y}" for y in range(2003, 2008) for q in range(1, 5)]
    + ["metadata_21"]
    + [f"q{q}_{y}" for y in range(2008, 2012) for q in range(1, 5)][:14]
)

# Filas de muestra reales del 1.6 (las dos primeras con datos).
_CUADRO_1_6_FILAS = [
    ["Tasa de desocupación"]
    + [9.2, 8.7, 8.3, 7.2, 9.1, 9.6, 9.0, 9.1, 10.1, 10.6, 9.0, 8.9]
    + [10.4, 13.1, 11.7, 11.0, 10.2, 9.6, 8.2, 7.0, None]
    + [7.0, 6.9, 7.1, 6.3, 6.9, 6.2, 5.7, 5.7, 7.7, 7.6, 6.9, 6.4, 7.9, 7.6],
    ["Mujeres hasta 29 años"]
    + [2.2, 2.2, 2.1, 2.0, 2.4, 2.4, 2.3, 2.4, 2.5, 2.5, 2.3, 2.0]
    + [2.6, 2.8, 2.4, 2.8, 2.5, 2.3, 1.9, 1.7, None]
    + [1.8, 1.5, 1.8, 1.4, 1.9, 1.4, 1.4, 1.4, 1.8, 1.7, 1.7, 1.5, 2.0, 1.8],
]


def test_las_fixtures_del_1_6_tienen_el_largo_real() -> None:
    assert len(_CUADRO_1_6_OLD) == 36
    assert len(_CUADRO_1_6_PROPUESTA) == 36
    assert all(len(f) == 36 for f in _CUADRO_1_6_FILAS)


@pytest.mark.asyncio
async def test_rechaza_los_anios_que_el_llm_le_invento_al_1_6() -> None:
    """El caso del hackatón: ni los nombres ni las filas dicen 2003."""
    llm = AsyncMock()
    llm.chat_json.return_value.content = json.dumps({"proposed_columns": _CUADRO_1_6_PROPUESTA})

    new_cols, rows, reason = await propose_llm_assisted_rename(
        _CUADRO_1_6_OLD, _CUADRO_1_6_FILAS, llm=llm
    )

    assert reason.startswith("invented_periods:")
    assert "2003" in reason
    assert new_cols == _CUADRO_1_6_OLD
    assert rows == 0


def test_un_anio_que_esta_en_las_filas_no_es_inventado() -> None:
    from app.application.repair.parse_repair import invented_periods

    old = ["col_0", "col_1"]
    filas = [["Año 2017", 9.2], ["Año 2018", 9.1]]

    assert invented_periods(old, filas, ["periodo", "tasa"]) == []
    assert invented_periods(old, filas, ["periodo_2017", "tasa"]) == []
    assert invented_periods(old, filas, ["periodo_2019", "tasa"]) == ["2019"]


def test_un_anio_que_esta_en_el_nombre_viejo_no_es_inventado() -> None:
    from app.application.repair.parse_repair import invented_periods

    assert invented_periods(["col_2018_0", "col_1"], [], ["anio_2018", "valor"]) == []


def test_un_trimestre_sin_evidencia_es_inventado() -> None:
    """Sin años también: `q1` donde nada habla de trimestres."""
    from app.application.repair.parse_repair import invented_periods

    assert invented_periods(["col_0", "col_1"], [["BA", 1.5]], ["provincia", "q1"]) == ["q1"]
    # Con "trimestre" en los nombres viejos, el trimestre no es inventado.
    assert invented_periods(["col_0", "1° trimestre"], [["BA", 1.5]], ["provincia", "t1"]) == []


def test_una_columna_que_no_cambia_no_se_juzga() -> None:
    from app.application.repair.parse_repair import invented_periods

    assert invented_periods(["q1_2003", "col_1"], [], ["q1_2003", "valor"]) == []


@pytest.mark.asyncio
async def test_las_tablas_del_indec_no_pasan_por_el_llm() -> None:
    """`cache_indec_*` las reescribe `ingest_indec`: el modelo no las nombra."""
    from unittest.mock import MagicMock

    from app.application.repair.parse_repair import repair_with_llm_assist

    engine = MagicMock()
    llm = AsyncMock()

    outcome = await repair_with_llm_assist(
        engine,
        llm=llm,
        table_schema="raw",
        table_name="cache_indec_eph_tasas_cuadro_1_6",
        dry_run=False,
    )

    assert outcome.reason == "excluded_table"
    assert not outcome.ok
    llm.chat_json.assert_not_called()
    engine.connect.assert_not_called()


def test_la_consulta_nocturna_excluye_las_tablas_del_indec() -> None:
    from app.infrastructure.celery.tasks.llm_repair_tasks import _CANDIDATES_SQL

    assert r"NOT LIKE 'cache\_indec\_%'" in str(_CANDIDATES_SQL)
