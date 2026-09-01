"""Tests for the CTE-alias bypass of the Serving Port's table blocklist.

Found by audit and confirmed by execution before the fix: naming a CTE after a
forbidden table made the reference to the **real** table be skipped. In a
non-recursive CTE the alias is not in scope inside its own body, so

    WITH api_keys AS (SELECT ... FROM mart.x JOIN api_keys ON true)
    SELECT * FROM api_keys

reads the physical `api_keys` while the validator saw only a name it had been
told to ignore — and this gate guards the Serving Port, which runs on the
privileged read-write engine.

The blocklist check now runs before the alias skip. These tests pin the order,
because the bug was invisible in the code: the comment directly under the skip
already claimed the blocklist always wins.
"""

from __future__ import annotations

import pytest

from app.application.common.sql_safety import is_pure_select_for_relation


def _check(sql: str):
    return is_pure_select_for_relation(sql, expected_schema="mart", expected_table="gastos_nacion")


@pytest.mark.parametrize("tabla", ["api_keys", "users", "conversations", "messages"])
def test_a_cte_named_after_a_forbidden_table_does_not_unlock_it(tabla):
    ok, motivo = _check(
        f"WITH {tabla} AS (SELECT t.* FROM mart.gastos_nacion m "
        f"JOIN {tabla} t ON true LIMIT 100) SELECT * FROM {tabla}"
    )
    assert not ok
    assert tabla in (motivo or "")


def test_the_direct_reference_was_always_blocked():
    # El control que hacía parecer segura la compuerta.
    ok, motivo = _check("SELECT * FROM api_keys")
    assert not ok and "api_keys" in (motivo or "")


def test_an_ordinary_cte_still_works():
    # El arreglo no puede costar las CTE legítimas, que son la forma normal de
    # escribir estas consultas.
    ok, _ = _check(
        "WITH filtrado AS (SELECT * FROM mart.gastos_nacion WHERE anio = 2024) "
        "SELECT * FROM filtrado"
    )
    assert ok


def test_several_chained_ctes_still_work():
    ok, _ = _check(
        "WITH a AS (SELECT * FROM mart.gastos_nacion), "
        "b AS (SELECT * FROM a WHERE anio = 2024) SELECT * FROM b"
    )
    assert ok


def test_a_cte_named_after_a_forbidden_table_is_blocked_even_if_never_used():
    # El bloqueo es por referencia física, no por uso.
    ok, _ = _check("WITH users AS (SELECT u.email FROM users u) SELECT * FROM mart.gastos_nacion")
    assert not ok
