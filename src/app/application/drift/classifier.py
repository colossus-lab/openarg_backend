"""Decide what a shape change between two snapshots actually was.

The naive version of this module would be a classifier that answers "is this
upstream drift?". That design cannot be made safe: every increase in its
sensitivity buys false positives, and a false positive here eventually
triggers a repair on data that was fine.

So it is built inverted. Each gate can only ever prove that a diff is **not**
upstream drift, or say that it cannot tell. None of them can assert that a
diff *is* drift. A component that can only prove the negative can only produce
false negatives — and a false negative here means "we did not flag something we
could have", which is exactly the status quo. The asymmetry is the whole point.

What survives every gate is not a conclusion. It is the small set of changes
that nothing could explain away, which is the only set worth a human's — or a
model's — attention.

Gates implemented here (numbering follows the design doc):

  G1  provenance   did OUR parser change between the two snapshots?
  G3  pipeline     did the parse take a different path through the same file?
  G5  sufficiency  is the column identifiable at all?   (in `diff_snapshots`)
  G6  uniqueness   is the rename match unambiguous?     (in `diff_snapshots`)

G0 (stable identity) and G2 (sibling vs version) need data that does not live
in a snapshot — the resource's canonical URL and content hash. They are modelled
as an optional `context` the caller supplies, so the verdict can record that
they were not evaluated instead of silently assuming they passed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from app.application.catalog.schema_snapshot import TableSnapshot, diff_snapshots


class Verdict(StrEnum):
    """What the cascade concluded."""

    NO_CHANGE = "no_change"
    # A gate proved the diff was not upstream drift. `exonerated_by` says which.
    EXONERATED = "exonerated"
    # Nothing could explain it away. This is the only value that should ever
    # reach a human or a model.
    UNEXPLAINED = "unexplained"


class ChangeClass(StrEnum):
    """The shape of an unexplained change, for routing.

    Ordered by how safe each class is to act on. `ADDITIVE` is the only one
    where a wrong call is inert — a column nobody reads. `SEMANTIC` is the
    dangerous one: the schema is identical and the meaning moved, which is
    precisely the change that produces confident wrong answers.
    """

    ADDITIVE = "additive"
    REMOVAL = "removal"
    TYPE_CHANGE = "type_change"
    RENAME = "rename"
    RESHAPE = "reshape"
    SEMANTIC = "semantic"


@dataclass(frozen=True)
class DriftContext:
    """What the caller knows that a snapshot cannot.

    Every field is optional and `None` means "not evaluated" — never "passed".
    A gate that cannot run says so in the verdict rather than waving the case
    through, because a silent assumption is how a cascade like this stops being
    trustworthy.
    """

    # G0 — do both snapshots describe the same logical resource? Production has
    # 5.485 URLs registered under more than one `source_id` after the CKAN 2.11
    # migration, so identity cannot be taken for granted.
    same_identity: bool | None = None
    # G2 — same file over time, or two files of the same bundle? Of 78 measured
    # multi-variant families, 29 were born the same day: siblings, not drift.
    same_source_url: bool | None = None


@dataclass
class DriftVerdict:
    verdict: Verdict
    change_class: ChangeClass | None = None
    exonerated_by: str | None = None
    # Why the gate fired, in terms a person can check.
    reason: str = ""
    diff: dict[str, Any] = field(default_factory=dict)
    # Gates that could not run because the caller did not supply their input.
    gates_not_evaluated: list[str] = field(default_factory=list)

    @property
    def is_actionable(self) -> bool:
        return self.verdict is Verdict.UNEXPLAINED


def classify_change(
    before: TableSnapshot,
    after: TableSnapshot,
    context: DriftContext | None = None,
) -> DriftVerdict:
    """Run the exoneration cascade over two snapshots of the same table.

    Pure: no database access, no clock, no network. It takes two snapshots and
    returns a verdict, so it can run over stored rows long after both tables
    ceased to exist — which is the entire reason the snapshots are stored.
    """
    ctx = context or DriftContext()
    diff = diff_snapshots(before, after)
    not_evaluated: list[str] = []

    # ── G0 — identity ────────────────────────────────────────────
    if ctx.same_identity is False:
        return DriftVerdict(
            verdict=Verdict.EXONERATED,
            exonerated_by="G0_identity",
            reason=(
                "Los dos snapshots no describen el mismo recurso lógico. "
                "Comparar identidades distintas no dice nada sobre el formato."
            ),
            diff=diff,
        )
    if ctx.same_identity is None:
        not_evaluated.append("G0_identity")

    # ── G2 — sibling vs version ──────────────────────────────────
    if ctx.same_source_url is False:
        return DriftVerdict(
            verdict=Verdict.EXONERATED,
            exonerated_by="G2_sibling",
            reason=(
                "Son archivos distintos del mismo dataset, no el mismo archivo "
                "en el tiempo. Que difieran entre sí es heterogeneidad, no deriva."
            ),
            diff=diff,
        )
    if ctx.same_source_url is None:
        not_evaluated.append("G2_sibling")

    # ── Nothing changed ──────────────────────────────────────────
    # Checked after G0/G2 on purpose: "no change" between two things that are
    # not comparable is not a meaningful answer.
    if not diff["schema_changed"] and not diff["type_changed"]:
        return DriftVerdict(
            verdict=Verdict.NO_CHANGE,
            reason="Misma forma y mismos tipos.",
            diff=diff,
            gates_not_evaluated=not_evaluated,
        )

    # ── G1 — provenance ──────────────────────────────────────────
    # The single most productive gate. Our own parser rewrites schemas for
    # reasons that have nothing to do with the portal.
    provenance_changed = diff.get("provenance_changed") or []
    parser_fields = {"parser_version", "normalization_version"}
    if parser_fields.intersection(provenance_changed):
        return DriftVerdict(
            verdict=Verdict.EXONERATED,
            exonerated_by="G1_provenance",
            reason=(
                "Cambió nuestro parser entre las dos capturas "
                f"({', '.join(sorted(parser_fields.intersection(provenance_changed)))}). "
                "El cambio de forma es nuestro, no del origen."
            ),
            diff=diff,
            gates_not_evaluated=not_evaluated,
        )

    # ── G3 — pipeline behaviour ──────────────────────────────────
    # Same parser version, different path through it: a threshold flipped.
    # `unpivot_if_time_pivoted` fires at ≥50 % time columns, so one extra year
    # of data turns a wide table into a long one with no upstream change at all.
    pipeline_fields = {"layout_profile", "header_quality", "is_truncated"}
    pipeline_changed = pipeline_fields.intersection(provenance_changed)
    if pipeline_changed:
        return DriftVerdict(
            verdict=Verdict.EXONERATED,
            exonerated_by="G3_pipeline",
            reason=(
                "El mismo parser tomó un camino distinto sobre el archivo "
                f"({', '.join(sorted(pipeline_changed))}). Explica el diff sin "
                "que el origen haya cambiado."
            ),
            diff=diff,
            gates_not_evaluated=not_evaluated,
        )

    # ── Survived. Classify what it is. ───────────────────────────
    return DriftVerdict(
        verdict=Verdict.UNEXPLAINED,
        change_class=_classify(diff),
        reason="Ninguna compuerta pudo descartarlo.",
        diff=diff,
        gates_not_evaluated=not_evaluated,
    )


def _classify(diff: dict[str, Any]) -> ChangeClass:
    """Name the shape of a change that survived the cascade.

    Order matters: a rename is reported as such even though it also shows up as
    an addition plus a removal, because the two call for different handling.
    """
    if diff.get("renamed_candidates"):
        return ChangeClass.RENAME
    added, removed = diff.get("added") or [], diff.get("removed") or []
    if added and removed:
        # Both directions at once with no confident rename between them: the
        # table was restructured, not edited.
        return ChangeClass.RESHAPE
    if added:
        return ChangeClass.ADDITIVE
    if removed:
        return ChangeClass.REMOVAL
    if diff.get("type_changed"):
        return ChangeClass.TYPE_CHANGE
    # Same columns, same types, and we still got here — the values moved.
    # Not detectable from the shape alone; recorded so the caller knows the
    # cascade cannot speak to it. See spec 024 DEBT-024-002.
    return ChangeClass.SEMANTIC


def summarize(verdicts: list[DriftVerdict]) -> dict[str, Any]:
    """Aggregate a batch, keeping the per-gate counts.

    The per-gate breakdown is the point. If G1 exonerates 90 % of the diffs,
    that is not a statistic — it is the discovery that the noise was ours, and
    it changes what to fix. Without it the cascade is a black box that cannot
    be tuned.
    """
    by_verdict: dict[str, int] = {}
    by_gate: dict[str, int] = {}
    by_class: dict[str, int] = {}
    for v in verdicts:
        by_verdict[v.verdict.value] = by_verdict.get(v.verdict.value, 0) + 1
        if v.exonerated_by:
            by_gate[v.exonerated_by] = by_gate.get(v.exonerated_by, 0) + 1
        if v.change_class:
            by_class[v.change_class.value] = by_class.get(v.change_class.value, 0) + 1
    actionable = [v for v in verdicts if v.is_actionable]
    return {
        "evaluated": len(verdicts),
        "actionable": len(actionable),
        "by_verdict": by_verdict,
        "exonerated_by_gate": by_gate,
        "actionable_by_class": by_class,
    }
