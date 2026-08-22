"""Decide whether a proposed repair is an improvement, before it is allowed to run.

This is the gate between self-repair and self-harm. Its job is not to check that
a repair *executes* — that is trivial and worthless — but that the table it
leaves behind is closer to what the resource is supposed to look like than the
one it found. Without it, a plausible-looking proposal silently rewrites data,
which is the failure mode the automated-repair literature calls patch
overfitting: a fix that satisfies the check it was written against and is wrong
about everything the check does not see.

**It does not mutate anything to reach a verdict.** Every repair this project
has is a column rename, and a rename does not touch values: the column that was
called `col_1` holds exactly the same data once it is called `departamento`. So
the profile after the repair is knowable from the profile before it plus the
proposed mapping, and the whole question can be settled as a pure function over
two stored snapshots. That is not a shortcut — it means verification cannot
itself corrupt a table, and it means the decision is reproducible long after
both tables are gone.

**The reference is the caller's choice, and choosing it wrongly inverts the
answer.** Verification compares against a snapshot held to be correct, and
nothing in a profile says which of two versions that is. The four PAMI findings
of 2026-08-21 are the case in point: `v1` carried `destino`, `estado`,
`expediente`, and `v2` carried a title row promoted to headers — the *older*
version was the right one. A verifier pointed at `v2` would have confirmed the
damage. So the reference is an explicit argument with no default, and
[DEBT-025-003] stays open until something can determine direction on its own.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from app.application.catalog.schema_snapshot import (
    ColumnProfile,
    TableSnapshot,
    is_identifiable,
    profile_similarity,
)

logger = logging.getLogger(__name__)

# A repair must move the alignment by more than noise. Two columns of the same
# type with a couple of overlapping values score non-zero on their own, so a
# threshold of "any improvement at all" would accept proposals that changed
# nothing meaningful. Not calibrated against real repairs yet — see the module
# note on DEBT-025-003 — so it is deliberately conservative.
MIN_IMPROVEMENT = 0.10

# `profile_similarity` is `0.7·overlap + 0.2·same_type + 0.1·null_agreement`, so
# two columns that share a type and a null fraction score 0.30 with **no
# overlapping values whatsoever**. Every text column in a table clears that.
#
# The threshold therefore has to sit above that floor, and it is derived rather
# than chosen: an alignment only counts when part of it came from the values.
# 0.45 requires an overlap of roughly 0.21 — a fifth of the sampled values in
# common — which is little to ask of a column that is genuinely the same one.
#
# Found by a test: a proposal that put the right names on the wrong columns
# scored exactly 0.30 and was accepted. Names alone would never have caught it;
# neither did a threshold set at the floor.
_TYPE_AND_NULL_FLOOR = 0.2 + 0.1
MIN_ALIGNMENT = _TYPE_AND_NULL_FLOOR + 0.15


@dataclass
class ColumnVerdict:
    """How one proposed name fared against the reference."""

    position: int
    current_name: str
    proposed_name: str
    reference_name: str | None
    score_before: float
    score_after: float

    @property
    def improved(self) -> bool:
        return self.score_after > self.score_before


@dataclass
class VerificationOutcome:
    """Whether the repair may run, and the evidence for the answer."""

    accepted: bool
    reason: str
    score_before: float = 0.0
    score_after: float = 0.0
    columns: list[ColumnVerdict] = field(default_factory=list)

    @property
    def improvement(self) -> float:
        return self.score_after - self.score_before

    def as_log_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "reason": self.reason,
            "score_before": round(self.score_before, 4),
            "score_after": round(self.score_after, 4),
            "improvement": round(self.improvement, 4),
            "columns_improved": sum(1 for c in self.columns if c.improved),
            "columns_total": len(self.columns),
        }


def _by_name(snapshot: TableSnapshot) -> dict[str, ColumnProfile]:
    return {c.name: c for c in snapshot.columns}


def _alignment(name: str, profile: ColumnProfile, reference: dict[str, ColumnProfile]) -> float:
    """How well a column called `name` matches the reference column of that name.

    Zero when the reference has no such column: a name the reference never used
    is not evidence of anything, and scoring it generously is how a proposal that
    invents plausible names gets accepted.
    """
    target = reference.get(name)
    if target is None:
        return 0.0
    return profile_similarity(profile, target)


def verify_rename(
    *,
    current: TableSnapshot,
    proposed_names: list[str],
    reference: TableSnapshot,
) -> VerificationOutcome:
    """Would renaming `current`'s columns to `proposed_names` improve alignment?

    `reference` is the snapshot held to be correct. It is required and has no
    default, because a verifier pointed at the wrong version confirms the damage
    it was built to prevent.
    """
    if len(proposed_names) != len(current.columns):
        # A mapping that does not cover the table cannot be evaluated, and
        # applying half of it would leave the table in a state no snapshot
        # describes.
        return VerificationOutcome(
            accepted=False,
            reason="proposal_does_not_cover_the_table",
        )

    ref_by_name = _by_name(reference)
    if not ref_by_name:
        return VerificationOutcome(accepted=False, reason="reference_has_no_columns")

    # A reference with no sampled values cannot support any comparison —
    # `profile_similarity` correctly returns 0.0 throughout, and a verdict built
    # on that would be arithmetic, not evidence.
    if not any(is_identifiable(c) for c in reference.columns):
        return VerificationOutcome(
            accepted=False,
            reason="reference_has_no_identifiable_columns",
        )

    verdicts: list[ColumnVerdict] = []
    for position, (column, proposed) in enumerate(
        zip(current.columns, proposed_names, strict=True)
    ):
        # The same data under two names: the profile does not move, only which
        # reference column it is asked to match.
        before = _alignment(column.name, column, ref_by_name)
        after = _alignment(proposed, column, ref_by_name)
        verdicts.append(
            ColumnVerdict(
                position=position,
                current_name=column.name,
                proposed_name=proposed,
                reference_name=proposed if proposed in ref_by_name else None,
                score_before=before,
                score_after=after,
            )
        )

    # Averaged over the table rather than summed, so a wide table cannot clear
    # the bar on volume alone.
    score_before = sum(v.score_before for v in verdicts) / len(verdicts)
    score_after = sum(v.score_after for v in verdicts) / len(verdicts)
    outcome = VerificationOutcome(
        accepted=False,
        reason="",
        score_before=score_before,
        score_after=score_after,
        columns=verdicts,
    )

    if score_after < MIN_ALIGNMENT:
        outcome.reason = "repaired_table_still_does_not_match_the_reference"
        return outcome

    if outcome.improvement < MIN_IMPROVEMENT:
        # Includes the case where the proposal makes things worse, and the case
        # where it shuffles names without moving the alignment — both are
        # refusals for the same reason: no demonstrated gain.
        outcome.reason = "no_demonstrated_improvement"
        return outcome

    regressions = [v for v in verdicts if v.score_before - v.score_after > MIN_IMPROVEMENT]
    if regressions:
        # A net gain that breaks a column which was already right is not an
        # improvement worth taking automatically. The average would have hidden
        # it; this is what stops a repair trading a good column for two mediocre
        # ones.
        outcome.reason = f"would_regress_{len(regressions)}_already_matching_column(s)"
        return outcome

    outcome.accepted = True
    outcome.reason = "improves_alignment"
    return outcome


def verify_against_previous_version(
    *,
    current: TableSnapshot,
    proposed_names: list[str],
    candidates: list[TableSnapshot],
) -> VerificationOutcome:
    """Pick a reference from earlier snapshots of the resource, then verify.

    Chooses the **earliest** snapshot that has identifiable columns. That is a
    deliberate bias and it is only right for the failure this project actually
    sees: `title_as_columns` and `col_n` both move a table *away* from clean
    names over time, so the oldest usable version is the one closest to correct.

    It is the wrong bias for a resource whose upstream genuinely improved, and
    there is nothing here that can tell those apart. Stated so a caller can
    override rather than inherit it silently.
    """
    usable = [
        s
        for s in sorted(candidates, key=lambda s: s.version or 0)
        if any(is_identifiable(c) for c in s.columns)
    ]
    if not usable:
        return VerificationOutcome(
            accepted=False,
            reason="no_earlier_snapshot_can_serve_as_a_reference",
        )
    return verify_rename(current=current, proposed_names=proposed_names, reference=usable[0])


# ── verification without a reference ───────────────────────────
#
# `verify_rename` compares a proposal against a snapshot held to be correct.
# Measured on production 2026-08-22, that reference does not exist for the
# tables that most need repairing: of 1,118 tables carrying `col_N` or a
# title-row header, **26** have another version of the same resource and
# **none** have a second snapshot. They were parsed badly the first time and
# there is no correct past to compare with.
#
# So this class needs a verifier that judges a proposal on its own merits.
# The question stops being "does it match what was right before" and becomes
# "is it measurably less broken than what is there now" — answerable from the
# project's own definition of a bad column name, which the parser already owns.

# A repair must clear most of the garbage, not shuffle it. Set below 1.0 because
# a table can legitimately keep one odd name; set high because a proposal that
# only half-works on a header row is usually a proposal that misread it.
MIN_GARBAGE_CLEARED = 0.8


def _garbage_ratio(names: list[str]) -> float:
    from app.application.pipeline.parsers.column_normalization import is_garbage_column

    if not names:
        return 0.0
    return sum(1 for n in names if is_garbage_column(n)) / len(names)


def verify_intrinsic(
    *, current_names: list[str], proposed_names: list[str]
) -> VerificationOutcome:
    """Is the proposal measurably less broken than what the table has now?

    No reference, because for this class there is none. What it checks instead:

    - the proposal covers the table
    - it introduces no garbage of its own — inventing `col_1` to replace
      `Unnamed: 1` is motion, not repair
    - it clears most of the garbage that was there
    - the names stay distinct, since two columns that collapse to one name lose
      a column's worth of meaning
    - and the table was actually broken to begin with

    Deliberately strict. A repair that runs unattended on 1,118 tables should
    decline the ambiguous ones and leave them for a person, because the cost of
    a wrong rename is a column that silently means something else.
    """
    if len(proposed_names) != len(current_names):
        return VerificationOutcome(
            accepted=False, reason="proposal_does_not_cover_the_table"
        )

    before = _garbage_ratio(current_names)
    if before == 0.0:
        # Nothing to fix. Renaming a healthy table is how a repair sweep turns
        # into damage.
        return VerificationOutcome(
            accepted=False, reason="nothing_wrong_with_the_current_names"
        )

    after = _garbage_ratio(proposed_names)
    outcome = VerificationOutcome(
        accepted=False, reason="", score_before=1.0 - before, score_after=1.0 - after
    )

    if after > 0.0:
        # Not "fewer garbage names" but "none". A proposal that still contains a
        # placeholder did not recover the header; it rearranged it.
        outcome.reason = "proposal_still_contains_garbage_names"
        return outcome

    if len(set(proposed_names)) != len(proposed_names):
        outcome.reason = "proposal_collapses_two_columns_into_one_name"
        return outcome

    if any(not n or not n.strip() for n in proposed_names):
        outcome.reason = "proposal_contains_an_empty_name"
        return outcome

    # The collector's own columns are not the parser's to rename.
    # `_source_dataset_id` is how a table links back to its dataset, and the
    # model tier's first production dry run proposed renaming all five of them
    # to `metadata_<i>` — which would have cut every repaired table loose from
    # its origin. The proposer holds them out now; this refuses the proposal
    # outright, because a second line of defence on a data-integrity invariant
    # is worth more than the duplication costs.
    renamed_internal = [
        old
        for old, new in zip(current_names, proposed_names, strict=True)
        if old.startswith("_") and old != new
    ]
    if renamed_internal:
        outcome.reason = f"proposal_renames_collector_columns:{','.join(renamed_internal[:3])}"
        return outcome

    cleared = (before - after) / before
    if cleared < MIN_GARBAGE_CLEARED:
        outcome.reason = f"only_cleared_{cleared:.0%}_of_the_bad_names"
        return outcome

    outcome.accepted = True
    outcome.reason = "removes_all_garbage_names"
    return outcome
