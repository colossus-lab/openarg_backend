"""Tests for measuring what the drift cascade misses.

The reason this module exists: the shadow run measured precision and never
measured recall, and in a cascade whose only action is to *remove* items from
the alert set, every mistake is silent by construction.

So the first thing these tests check is that the suite has teeth. A suite where
everything passes on the first attempt is indistinguishable from a suite that
cannot fail, and the second one is worse than no suite at all.
"""

from __future__ import annotations

from app.application.drift import DriftVerdict, Verdict
from app.application.drift.seeded_faults import measure, seeded_cases


def _always(verdict: Verdict):
    def _classifier(before, after, context):
        return DriftVerdict(verdict=verdict)

    return _classifier


# ── ¿la suite puede fallar? ────────────────────────────────────


def test_a_classifier_that_explains_everything_away_scores_zero_recall():
    # El modo de falla que importa: si algo empieza a exonerar de más, esto lo
    # tiene que ver. Si no puede, no mide nada.
    m = measure(classifier=_always(Verdict.EXONERATED))
    assert m.recall == 0.0
    assert len(m.missed) == m.planted


def test_a_classifier_that_flags_everything_scores_perfect_recall_and_bad_precision():
    m = measure(classifier=_always(Verdict.UNEXPLAINED))
    assert m.recall == 1.0
    assert m.precision < 1.0
    assert m.wrongly_flagged, "las benignas tienen que aparecer marcadas de más"


def test_a_classifier_that_raises_counts_against_it():
    def _boom(before, after, context):
        raise RuntimeError("x")

    m = measure(classifier=_boom)
    assert m.recall == 0.0, "un caso inmedible es exactamente lo que esto viene a evitar"


# ── la suite en sí ─────────────────────────────────────────────


def test_the_suite_has_both_kinds():
    casos = seeded_cases()
    assert any(c.should_be_actionable for c in casos)
    assert any(not c.should_be_actionable for c in casos)


def test_every_case_is_named():
    assert all(c.name for c in seeded_cases())


def test_the_real_cascade_catches_every_planted_fault():
    m = measure()
    assert m.recall == 1.0, f"no detectadas: {m.missed}"


def test_the_real_cascade_explains_away_every_benign_change():
    m = measure()
    assert m.precision == 1.0, f"marcadas de más: {m.wrongly_flagged}"


# ── el reporte ─────────────────────────────────────────────────


def test_the_report_names_what_was_missed_not_only_how_much():
    # Una tasa dice qué tan mal; la lista dice cuál, y sólo lo segundo se arregla.
    m = measure(classifier=_always(Verdict.EXONERATED))
    d = m.as_dict()
    assert d["no_detectadas"]
    assert d["recall"] == 0.0


def test_an_empty_suite_does_not_divide_by_zero():
    m = measure(cases=())
    assert m.recall == 1.0 and m.precision == 1.0
