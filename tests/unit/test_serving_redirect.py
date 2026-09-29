"""Redirección de marts bloqueados y muestras de ruteo del presupuesto.

Caso real (prod, 29-sep): "Mostrame el gasto publico por ministerio del ultimo
ejercicio" se respondió con el catálogo de servicios administrativos (sin
montos). El mejor candidato, `presupuesto_nacional_ejecutado`, está bloqueado
y su motivo dice "Usar presupuesto_consolidado"; el ruteo lo descartaba sin
mirar eso. Además el catálogo de servicios tenía una muestra cargada a mano
("ministerios y secretarías presupuesto") que le daba el +0.17.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.application.marts.mart import load_all_marts
from app.application.marts.serving_redirect import (
    MART_CANDIDATES_SQL,
    REDIRECT_SQL_PATTERN,
    redirect_target,
)
from app.application.pipeline.nodes.analyst import _scrub_internal_identifiers

MARTS_DIR = Path(__file__).resolve().parents[2] / "config" / "marts"


@pytest.fixture(scope="module")
def marts():  # type: ignore[no-untyped-def]
    return {m.id: m for m in load_all_marts(MARTS_DIR)}


class TestRedirectTarget:
    def test_real_reason(self, marts) -> None:  # type: ignore[no-untyped-def]
        reason = marts["presupuesto_nacional_ejecutado"].serving_blocked_reason
        assert redirect_target(reason) == "presupuesto_consolidado"

    @pytest.mark.parametrize(
        ("reason", "expected"),
        [
            ("… Usar `presupuesto_consolidado`.", "presupuesto_consolidado"),
            ("usar mart_x en su lugar", "mart_x"),
            ("Mezcla unidades; no hay reemplazo.", None),
            (None, None),
            ("", None),
        ],
    )
    def test_forms(self, reason: str | None, expected: str | None) -> None:
        assert redirect_target(reason) == expected

    def test_sql_pattern_matches_the_python_rule(self, marts) -> None:  # type: ignore[no-untyped-def]
        # Postgres usa POSIX; para esta forma simple, `re` da el mismo grupo.
        reason = marts["presupuesto_nacional_ejecutado"].serving_blocked_reason
        match = re.search(REDIRECT_SQL_PATTERN, reason)
        assert match and match.group(1) == "presupuesto_consolidado"
        assert REDIRECT_SQL_PATTERN in MART_CANDIDATES_SQL


class TestEveryRedirectIsServable:
    def test_redirects_point_to_existing_unblocked_marts(self, marts) -> None:  # type: ignore[no-untyped-def]
        checked = 0
        for mart in marts.values():
            if not mart.serving_blocked:
                continue
            target = redirect_target(mart.serving_blocked_reason)
            if target is None:
                continue
            checked += 1
            assert target in marts, f"{mart.id} redirige a {target}, que no existe"
            assert not marts[target].serving_blocked, (
                f"{mart.id} redirige a {target}, también bloqueado"
            )
        assert checked >= 1


class TestBudgetSamples:
    def test_consolidado_owns_spending_by_ministry(self, marts) -> None:  # type: ignore[no-untyped-def]
        samples = [s.lower() for s in marts["presupuesto_consolidado"].sample_queries]
        assert any("por ministerio" in s for s in samples)
        assert any("jurisdicción" in s for s in samples)

    def test_services_catalog_does_not_claim_ministry_spending(self, marts) -> None:  # type: ignore[no-untyped-def]
        mart = marts["presupuesto_servicios_administrativos"]
        assert mart.sample_queries, "sin muestras en el YAML, las viejas de la base no se borran"
        assert not any("ministerio" in s.lower() for s in mart.sample_queries)
        assert "presupuesto_nacional_ejecutado" not in mart.description


class TestTableNamesAreNotShownAsCode:
    def test_backticked_table_name_becomes_words(self) -> None:
        text = (
            "Para eso necesito datos del fact table `presupuesto_nacional_ejecutado` con columnas"
        )
        out = _scrub_internal_identifiers(text)
        assert "`" not in out
        assert "presupuesto nacional ejecutado" in out

    def test_qualified_names_become_words(self) -> None:
        assert _scrub_internal_identifiers("según mart.presupuesto_consolidado.") == (
            "según presupuesto consolidado."
        )

    def test_cache_tables_are_still_removed_not_humanized(self) -> None:
        out = _scrub_internal_identifiers("Fuente: `cache_leyes_sancionadas`.")
        assert "leyes sancionadas" not in out and "cache" not in out

    def test_short_snake_case_and_plain_prose_are_untouched(self) -> None:
        for text in ("usá `tasa_call` para eso", "un valor de 7,6 % en 2025", " que"):
            assert _scrub_internal_identifiers(text) == text
