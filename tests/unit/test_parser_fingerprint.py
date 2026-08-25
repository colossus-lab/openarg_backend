"""Tests for the derived parser fingerprint.

The property that matters is discrimination in both directions: it must move
when behaviour moves, and it must hold still when only prose does. A
fingerprint that moves on every comment edit would hand G1 exonerations nobody
earned, which is a worse failure than the stale constant it replaces — because
it would be invisible.
"""

from __future__ import annotations

from app.application.catalog.parser_fingerprint import (
    UNKNOWN,
    _fingerprint,
    _structural_digest,
    is_real_provenance,
    normalization_fingerprint,
    parser_fingerprint,
)


def test_a_fingerprint_is_produced_and_is_shaped_like_a_version():
    p = parser_fingerprint()
    n = normalization_fingerprint()

    assert p.startswith("p:") and len(p) == 14
    assert n.startswith("n:") and len(n) == 14
    assert p != n


def test_it_is_stable_across_calls():
    assert parser_fingerprint() == parser_fingerprint()


# ── discriminación ─────────────────────────────────────────────


def test_rewriting_a_docstring_does_not_move_it():
    """This codebase writes long docstrings and edits them often. If that moved
    the fingerprint, G1 would exonerate changes that nothing caused."""
    before = _structural_digest('def f():\n    """One thing."""\n    return 1\n')
    after = _structural_digest('def f():\n    """Quite another thing entirely."""\n    return 1\n')

    assert before == after


def test_rewriting_a_comment_does_not_move_it():
    before = _structural_digest("def f():\n    # why\n    return 1\n")
    after = _structural_digest("def f():\n    # a much longer why\n    return 1\n")

    assert before == after


def test_reformatting_does_not_move_it():
    before = _structural_digest("def f(a,b):\n    return a+b\n")
    after = _structural_digest("def f(\n    a,\n    b,\n):\n    return a + b\n")

    assert before == after


def test_changing_a_literal_does_move_it():
    """Thresholds live in literals. A changed threshold is exactly the kind of
    parser change G1 exists to notice."""
    before = _structural_digest("THRESHOLD = 0.6\n")
    after = _structural_digest("THRESHOLD = 0.8\n")

    assert before != after


def test_changing_a_branch_does_move_it():
    before = _structural_digest("def f(x):\n    if x:\n        return 1\n    return 2\n")
    after = _structural_digest("def f(x):\n    if not x:\n        return 1\n    return 2\n")

    assert before != after


def test_renaming_a_function_does_move_it():
    """A rename is not cosmetic here — callers bind to the name, so the set of
    behaviour reachable from the pipeline changed."""
    assert _structural_digest("def parse():\n    pass\n") != _structural_digest(
        "def parse_v2():\n    pass\n"
    )


# ── degradación ────────────────────────────────────────────────


def test_a_missing_module_moves_the_fingerprint_rather_than_being_skipped():
    """A module that was deleted or moved is a real change to how parsing works.
    Silently skipping it would report the same version for different code."""
    present = _fingerprint(("app.application.pipeline.parsers.time_pivot",), "p")
    with_missing = _fingerprint(
        ("app.application.pipeline.parsers.time_pivot", "app.does.not.exist"), "p"
    )

    assert present != with_missing


def test_no_readable_module_yields_unknown_not_a_confident_hash():
    """Hashing five error markers would produce a stable, meaningless value that
    looks exactly like a real version."""
    assert _fingerprint(("app.nope.one", "app.nope.two"), "p") == UNKNOWN


# ── placeholders ───────────────────────────────────────────────


def test_placeholders_are_not_real_provenance():
    """These are the values the corpus is actually full of. Counting them made
    coverage read 26,435 when the figure G1 could use was zero."""
    assert not is_real_provenance(None)
    assert not is_real_provenance("")
    assert not is_real_provenance("legacy:unknown")
    assert not is_real_provenance("2026-05-04")
    assert not is_real_provenance(UNKNOWN)


def test_a_derived_fingerprint_is_real_provenance():
    assert is_real_provenance(parser_fingerprint())
    assert is_real_provenance("phase4")
