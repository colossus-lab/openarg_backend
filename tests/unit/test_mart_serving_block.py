"""A mart can be withheld from serving without being deleted.

`last_row_count > 0` only hides marts that are EMPTY. There was no way to
say "this mart has rows, but they are wrong" — which is exactly what
`presupuesto_nacional_ejecutado` was on 2026-07-26 when it served a budget
ranking that was wrong by orders of magnitude.

The flag lives in the mart YAML, not just the DB, because `build_mart` does
DROP+CREATE and re-upserts `mart_definitions` on every run: a DB-only flag
would be silently cleared by the next rebuild.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.application.marts.mart import MartParseError, load_all_marts, load_mart

_MARTS_DIR = Path(__file__).resolve().parents[2] / "config" / "marts"

_BASE_YAML = """
id: {id_}
version: 1.0.0
description: test mart
domain: presupuesto
sources:
  portals: [test]
canonical_columns:
  - name: anio
    type: int
    description: year
sql: SELECT 1 AS anio
refresh:
  policy: manual
"""


def _write(tmp_path: Path, body: str, id_: str = "t") -> Path:
    path = tmp_path / f"{id_}.yaml"
    path.write_text(_BASE_YAML.format(id_=id_) + body, encoding="utf-8")
    return path


class TestServingBlockParsing:
    def test_defaults_to_not_blocked(self, tmp_path: Path) -> None:
        mart = load_mart(_write(tmp_path, ""))
        assert mart.serving_blocked is False
        assert mart.serving_blocked_reason is None

    def test_blocked_with_reason(self, tmp_path: Path) -> None:
        mart = load_mart(
            _write(tmp_path, "\nserving:\n  blocked: true\n  blocked_reason: datos sucios\n")
        )
        assert mart.serving_blocked is True
        assert mart.serving_blocked_reason == "datos sucios"

    def test_blocking_without_a_reason_is_rejected(self, tmp_path: Path) -> None:
        """Withholding data from users must be a documented decision."""
        with pytest.raises(MartParseError, match="blocked_reason"):
            load_mart(_write(tmp_path, "\nserving:\n  blocked: true\n"))

    def test_serving_must_be_a_mapping(self, tmp_path: Path) -> None:
        with pytest.raises(MartParseError, match="mapping"):
            load_mart(_write(tmp_path, "\nserving: true\n"))


class TestShippedMarts:
    def test_dirty_budget_mart_is_blocked(self) -> None:
        """Regression: the mart that produced the wrong ranking stays hidden."""
        marts = {m.id: m for m in load_all_marts(_MARTS_DIR)}
        target = marts.get("presupuesto_nacional_ejecutado")
        assert target is not None, "mart not found — was it renamed?"
        assert target.serving_blocked is True, (
            "presupuesto_nacional_ejecutado must stay out of serving until its "
            "TEXT amounts, corrupt anio values and 32/560 table coverage are fixed"
        )
        assert target.serving_blocked_reason

    def test_the_clean_replacement_is_servable(self) -> None:
        """Blocking the dirty mart is only acceptable because this one covers it."""
        marts = {m.id: m for m in load_all_marts(_MARTS_DIR)}
        replacement = marts.get("presupuesto_consolidado")
        assert replacement is not None
        assert replacement.serving_blocked is False

    def test_every_other_mart_stays_servable(self) -> None:
        blocked = {m.id for m in load_all_marts(_MARTS_DIR) if m.serving_blocked}
        assert blocked == {"presupuesto_nacional_ejecutado"}, (
            f"unexpected marts withheld from serving: {sorted(blocked)}"
        )
