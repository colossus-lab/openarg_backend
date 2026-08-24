"""Tests for `application/repair/parse_repair.py`.

The pure proposal logic (`propose_col_n_rename`) is unit-tested here against
in-memory data. The DDL-applying orchestrator (`repair_col_n_table`) is
exercised in integration tests against staging Postgres because SQLite
doesn't expose `information_schema.columns`.

Specs/021-parser-hardening Phase 2.
"""

from __future__ import annotations

import json
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
