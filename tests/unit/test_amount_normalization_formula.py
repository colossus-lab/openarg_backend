"""The prompt must not teach a normalisation that multiplies rows by 100.

Until 2026-07-31 every surface told the model to normalise a TEXT amount with
`replace(replace(col, '.', ''), ',', '.')`. That strips *every* dot on the
assumption it is a thousands separator. Where the dot is the DECIMAL separator
the row comes out 100x too large, and the expression carries no sign of having
guessed.

Measured against staging the same day, on columns that are served today:

    mart.caba_departamentos_en_venta.precio_m2_dolares   75.1% of rows
    mart.caba_terrenos_oferta.precio_m2_dolares          74.9%
    mart.caba_pauta_publicitaria.importe                 32.6%
    mart.neuquen_ejecucion_presupuestaria.*              0.1-2.6%

The prior plan's proposed fix — `CASE WHEN col ~ ',' THEN replace(col,',','.')
ELSE col END` — was derived from `neuquen`, where no value carries both
separators, and errors on `99.900,00`: 15,366 such values live in
`caba_pauta_publicitaria`. Branching on *which symbol is present* cannot work;
the branch has to be on the shape of the whole value.

These tests read the patterns out of the shipped prompt and classify the real
measured shapes with them, so the thing under test is the artifact that reaches
the model rather than a copy of it kept in sync by hope.

PostgreSQL's `~` and Python's `re` agree on everything these patterns use
(anchors, `\\s`, escaped dots, `[0-9]` classes, bounded repetition).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_PROMPTS = Path(__file__).resolve().parents[2] / "src" / "app" / "prompts"
_NL2SQL_RAW = (_PROMPTS / "nl2sql.txt").read_text(encoding="utf-8")
# The stored file is not what reaches the model. `load_prompt` runs
# `str.format()` on it, so every literal brace has to be doubled — and the
# regexes here are full of `[0-9]{1,3}`. Shipped unescaped, `.format()` read
# `{1,3}` as a field name and the whole NL2SQL path died on `KeyError('1,3')`,
# deflecting every query with a fabricated "no tenemos ese dato". Asserting on
# the raw text would have kept passing throughout. Render first, always.
_NL2SQL = _NL2SQL_RAW.format(tables_context="<TABLES>", few_shot_block="<FEWSHOT>")
# sql_fixer is loaded with no kwargs, so `load_prompt` skips `.format()` and
# its braces must stay single. The asymmetry is the loader's, not a style
# choice — see TestTheTemplateSurvivesLoading.
_SQL_FIXER = (_PROMPTS / "sql_fixer.txt").read_text(encoding="utf-8")

# `WHEN col ~ '<pattern>' THEN <transform>` as written in the rule block.
_BRANCH_RE = re.compile(r"WHEN col ~ '(?P<pattern>\^[^']+\$)'\s*THEN\s*(?P<transform>[^\n`]+)")


def _branches(prompt: str) -> list[tuple[str, str]]:
    return [(m.group("pattern"), m.group("transform").strip()) for m in _BRANCH_RE.finditer(prompt)]


def _normalise(value: str, branches: list[tuple[str, str]]) -> float | None:
    """Apply the prompt's CASE to `value`, mirroring what Postgres would do."""
    for pattern, transform in branches:
        if not re.match(pattern, value):
            continue
        out = value.strip()
        # The transforms are compositions of replace(); read them right to left
        # the way the nesting evaluates.
        if "replace(replace(" in transform:
            out = out.replace(".", "").replace(",", ".")
        elif "','," in transform.replace(" ", "") or "),',','.')" in transform:
            out = out.replace(",", ".")
        elif "'.',''" in transform.replace(" ", ""):
            out = out.replace(".", "")
        return float(out)
    return None  # CASE with no ELSE yields NULL


# Every shape measured on staging 2026-07-31, plus the boundary cases.
_MEASURED = [
    ("983700", 983700.0, "entero pelado"),
    ("600600,00000", 600600.0, "coma decimal"),
    ("1086738.69", 1086738.69, "punto decimal"),
    ("99.900,00", 99900.0, "notacion AR completa"),
    ("1.234.567,89", 1234567.89, "AR, miles multiples"),
    ("1.234", 1234.0, "punto como miles"),
    ("998.9", 998.9, "punto como decimal"),
    ("0,00", 0.0, "cero con coma"),
    ("-1.234,50", -1234.50, "negativo AR"),
    ("  4200  ", 4200.0, "con espacios"),
    ("-", None, "guion solo"),
    ("§", None, "simbolo no numerico"),
    ("", None, "vacio"),
    ("12,5,3", None, "malformado"),
]


class TestPromptShipsTheBranchingFormula:
    def test_the_rule_block_has_four_branches(self) -> None:
        assert len(_branches(_NL2SQL)) == 4, (
            "the NUMERIC CASTS rule must branch on all four shapes: AR full, "
            "comma decimal, dot thousands, dot decimal / plain"
        )

    def test_the_repair_prompt_teaches_the_same_thing(self) -> None:
        """A model that only ever sees sql_fixer must not learn the old form."""
        assert _branches(_SQL_FIXER) == _branches(_NL2SQL), (
            "sql_fixer.txt and nl2sql.txt must agree; a query corrected by the "
            "repair node would otherwise undo the rule the generator followed"
        )

    @pytest.mark.parametrize(("raw", "expected", "label"), _MEASURED, ids=[c[2] for c in _MEASURED])
    def test_classifies_every_measured_shape(
        self, raw: str, expected: float | None, label: str
    ) -> None:
        got = _normalise(raw, _branches(_NL2SQL))
        if expected is None:
            assert got is None, f"{label}: unparseable input must yield NULL, not {got}"
        else:
            assert got == pytest.approx(expected), f"{label}: {raw!r} normalised wrong"

    def test_dot_decimals_are_not_inflated(self) -> None:
        """The regression itself, stated as a number rather than a shape."""
        branches = _branches(_NL2SQL)
        assert _normalise("1086738.69", branches) == pytest.approx(1086738.69)
        assert _normalise("998.9", branches) == pytest.approx(998.9)
        # What the old formula produced for the same inputs.
        assert _normalise("1086738.69", branches) != pytest.approx(108673869.0)


class TestTheDiscreditedFormsAppearOnlyAsProhibitions:
    """Both discredited forms are still named in the prompt, deliberately.

    The model's prior for Argentine amounts *is* the blanket
    `replace(replace(...))`; staying silent about it does not unlearn it. So the
    prompt names each one and says not to use it. What these tests defend is the
    polarity — that the string never appears as a recommendation.
    """

    _NEGATION_CUES = ("do not", "don't", "never", "must not", "wrong", "no uses", "nunca")

    def _occurrences_are_all_negated(self, haystack: str, needle: str) -> bool:
        for match in re.finditer(re.escape(needle), haystack):
            window = haystack[max(0, match.start() - 260) : match.start()].lower()
            if not any(cue in window for cue in self._NEGATION_CUES):
                return False
        return True

    def test_blanket_dot_stripping_is_only_ever_forbidden(self) -> None:
        for prompt, label in ((_NL2SQL, "nl2sql"), (_SQL_FIXER, "sql_fixer")):
            assert self._occurrences_are_all_negated(prompt, "replace(replace(col"), (
                f"{label}.txt presents blanket dot-stripping without forbidding it"
            )

    def test_to_number_with_locale_mask_is_only_ever_forbidden(self) -> None:
        """`G`/`D` resolve from lc_numeric, not from the mask.

        Under the server's `C`/`en_US` locale they mean the English convention —
        the opposite of what the old prose claimed — so the prompt used to
        recommend it for precisely the mixed columns it gets wrong.
        """
        for prompt, label in ((_NL2SQL, "nl2sql"), (_SQL_FIXER, "sql_fixer")):
            assert self._occurrences_are_all_negated(prompt, "999G999G999D99"), (
                f"{label}.txt still offers to_number with a locale-dependent mask"
            )

    def test_worked_examples_do_not_reteach_the_old_pattern(self) -> None:
        """Examples are demonstration, and demonstration outweighs instruction."""
        examples = _NL2SQL.split("Examples:", 1)[1]
        assert "replace(replace(" in examples, "examples should show the real thing"
        for line in examples.splitlines():
            if not line.startswith("SQL:") or "replace(replace(" not in line:
                continue
            assert "CASE WHEN" in line, (
                f"example normalises without branching on shape: {line[:120]}"
            )


class TestHistoryStopsReteachingTheOldForm:
    """Few-shot examples come from queries that ran, and the old form always ran.

    `successful_queries` feeds `{few_shot_block}`, so fixing the prompt while
    leaving the history alone lets worked examples keep demonstrating the
    discredited normalisation. Measured 2026-07-31: 2 such rows in staging.
    """

    @staticmethod
    def _predicate():
        history = pytest.importorskip("app.application.pipeline.history")
        return history.teaches_discredited_normalisation

    def test_flags_unguarded_dot_stripping(self) -> None:
        assert self._predicate()(
            "SELECT SUM(CAST(NULLIF(replace(replace(credito_vigente, '.', ''), ',', '.'), '') "
            "AS NUMERIC)) FROM t"
        )

    def test_accepts_dot_stripping_inside_a_shape_branch(self) -> None:
        """The correct formula strips dots too — only after proving the shape."""
        branches = _branches(_NL2SQL)
        good = (
            "SELECT SUM(CASE "
            + " ".join(f"WHEN monto ~ '{p}' THEN {t}" for p, t in branches).replace("col", "monto")
            + " END::numeric) FROM t"
        )
        assert not self._predicate()(good)

    def test_ignores_queries_with_no_normalisation_at_all(self) -> None:
        assert not self._predicate()("SELECT anio, monto FROM t WHERE anio = '2024'")

    def test_empty_sql_is_not_flagged(self) -> None:
        assert not self._predicate()("")


class TestTheGuardrailAcceptsTheNewForm:
    def test_shape_tests_inside_a_case_are_not_lossy_filters(self) -> None:
        """Otherwise the safety net refuses every correctly-written query.

        `find_lossy_numeric_filters` looks for `col ~ '^…$'`, which is exactly
        what the new formula is built from — but inside a CASE those classify a
        value, they never remove a row.
        """
        nl2sql = pytest.importorskip("app.application.pipeline.subgraphs.nl2sql")
        branches = _branches(_NL2SQL)
        case_sql = (
            "SELECT SUM(CASE "
            + " ".join(f"WHEN monto ~ '{p}' THEN {t}" for p, t in branches).replace("col", "monto")
            + " END::numeric) AS total FROM t"
        )
        assert nl2sql.find_lossy_numeric_filters(case_sql) == []

    def test_a_real_shape_filter_in_where_is_still_caught(self) -> None:
        """The 2026-07-26 prod failure must keep tripping it."""
        nl2sql = pytest.importorskip("app.application.pipeline.subgraphs.nl2sql")
        bad = (
            "SELECT SUM(CAST(monto AS NUMERIC)) AS total FROM t "
            r"WHERE monto ~ '^\-?\d+\.?\d*$'"
        )
        assert nl2sql.find_lossy_numeric_filters(bad)


class TestTheTemplateSurvivesLoading:
    """The prompt is a `str.format()` template, and regexes are full of braces.

    Shipped with `[0-9]{1,3}` unescaped, `load_prompt("nl2sql", ...)` raised
    `KeyError('1,3')` before the model saw anything. The sandbox swallowed it as
    a failed step and the pipeline answered with a deflection that invented a
    reason ("el dataset cubre valores en pesos, no en dólares") about a mart
    whose column is literally `precio_m2_dolares`.

    Every test above reads the file; none of them called the loader, so all of
    them passed against a prompt that could not be loaded at all.
    """

    def test_nl2sql_loads_with_its_real_kwargs(self) -> None:
        prompts = pytest.importorskip("app.prompts")
        rendered = prompts.load_prompt(
            "nl2sql", tables_context="<TABLES>", few_shot_block="<FEWSHOT>"
        )
        assert "<TABLES>" in rendered and "<FEWSHOT>" in rendered

    def test_the_regex_reaches_the_model_with_single_braces(self) -> None:
        """Escaping is a transport concern; the model must not see `{{1,3}}`."""
        prompts = pytest.importorskip("app.prompts")
        rendered = prompts.load_prompt("nl2sql", tables_context="", few_shot_block="")
        assert "[0-9]{1,3}" in rendered
        assert "[0-9]{3}" in rendered
        assert "{{" not in rendered and "}}" not in rendered

    def test_every_formatted_prompt_in_the_directory_loads(self) -> None:
        """Guards the next prompt that grows a brace, not just this one."""
        prompts = pytest.importorskip("app.prompts")
        import re as _re

        for path in sorted(_PROMPTS.glob("*.txt")):
            raw = path.read_text(encoding="utf-8")
            fields = set(_re.findall(r"(?<!\{)\{([a-z_][a-z0-9_]*)\}(?!\})", raw))
            if not fields:
                continue  # loaded verbatim, braces are literal by contract
            prompts.load_prompt(path.stem, **dict.fromkeys(fields, ""))
