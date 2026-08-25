"""Measure what the drift cascade misses, by handing it changes we planted.

The shadow run reported 2 actionable pairs out of 1,223 and that number was
presented as a calibrated noise rate. It is not. Two events out of 1,223 carry a
95% confidence interval of roughly **0.02%–0.6%**: what was measured is that the
rate is *small*, not what it is.

Worse, only precision was measured at all. **A cascade whose only action is to
remove items from the alert set has an unmeasurable false-negative rate**, because
every mistake it makes is a thing that quietly did not get reported. Precision is
the one metric whose failures are visible; optimising it alone is how a detector
comes to look excellent while missing everything.

So this plants faults with known answers. Pairs that *must* survive every gate —
a column dropped with identical provenance, a type narrowed, a rename with no
explanation — and pairs that *must* be exonerated, where our own parser version
moved or the two sides are different files of one dataset. Running both together
gives recall and precision on the same suite, which is the number the shadow mode
was supposed to produce before anything got turned on.

Deliberately synthetic. Real production pairs have no ground truth — that is the
entire problem — and waiting for labelled reality means never measuring recall.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from app.application.catalog.schema_snapshot import ColumnProfile, Provenance, TableSnapshot
from app.application.drift import DriftContext, Verdict, classify_change

logger = logging.getLogger(__name__)

# A provenance both sides share: our pipeline did not move, so nothing here can
# be blamed on us. Every fault seeded on top of it must survive the cascade.
_SAME = Provenance(
    parser_version="p:phase4-v1",
    normalization_version="n:v3",
    layout_profile="simple_tabular",
    header_quality="good",
    is_truncated=False,
)


def _cols(*specs: tuple[str, str]) -> list[ColumnProfile]:
    return [
        ColumnProfile(name=name, ordinal=i + 1, pg_type=pg_type, null_frac=0.0)
        for i, (name, pg_type) in enumerate(specs)
    ]


_BASE = _cols(
    ("provincia", "text"),
    ("anio", "bigint"),
    ("monto", "numeric"),
    ("poblacion", "bigint"),
)


def _snapshot(columns: list[ColumnProfile], *, provenance: Provenance = _SAME) -> TableSnapshot:
    return TableSnapshot(
        schema_name="raw",
        table_name="t",
        columns=columns,
        row_count_estimate=1000,
        stats_available=True,
        resource_identity="portal::x",
        version=1,
        provenance=provenance,
    )


@dataclass(frozen=True)
class SeededCase:
    """One planted change, and what the cascade is supposed to say about it."""

    name: str
    before: TableSnapshot
    after: TableSnapshot
    context: DriftContext
    should_be_actionable: bool


def _same_identity() -> DriftContext:
    return DriftContext(same_identity=True, same_source_url=True)


def seeded_cases() -> tuple[SeededCase, ...]:
    """The suite. Half must be flagged, half must be explained away."""
    otro_parser = replace(_SAME, parser_version="p:phase5-v2")

    faltante = [c for c in _BASE if c.name != "monto"]
    agregada = [*_BASE, ColumnProfile(name="tasa", ordinal=5, pg_type="numeric")]
    retipada = [replace(c, pg_type="text") if c.name == "monto" else c for c in _BASE]
    renombrada = [replace(c, name="importe") if c.name == "monto" else c for c in _BASE]
    vaciada = _cols(("col_1", "text"), ("col_2", "text"))

    return (
        # ── deben sobrevivir todas las compuertas ──
        SeededCase(
            "columna eliminada, mismo parser",
            _snapshot(_BASE),
            _snapshot(faltante),
            _same_identity(),
            True,
        ),
        SeededCase(
            "tipo cambiado a text, mismo parser",
            _snapshot(_BASE),
            _snapshot(retipada),
            _same_identity(),
            True,
        ),
        SeededCase(
            "columna renombrada, mismo parser",
            _snapshot(_BASE),
            _snapshot(renombrada),
            _same_identity(),
            True,
        ),
        SeededCase(
            "esquema reemplazado por marcadores",
            _snapshot(_BASE),
            _snapshot(vaciada),
            _same_identity(),
            True,
        ),
        SeededCase(
            "columna agregada, mismo parser",
            _snapshot(_BASE),
            _snapshot(agregada),
            _same_identity(),
            True,
        ),
        # ── deben quedar explicadas ──
        SeededCase(
            "sin cambios",
            _snapshot(_BASE),
            _snapshot(_BASE),
            _same_identity(),
            False,
        ),
        SeededCase(
            "nuestro parser cambió",
            _snapshot(_BASE),
            _snapshot(faltante, provenance=otro_parser),
            _same_identity(),
            False,
        ),
        SeededCase(
            "dos archivos distintos del mismo dataset",
            _snapshot(_BASE),
            _snapshot(faltante),
            DriftContext(same_identity=True, same_source_url=False),
            False,
        ),
        SeededCase(
            "el nombre físico se reusó para otro recurso",
            _snapshot(_BASE),
            _snapshot(vaciada),
            DriftContext(same_identity=False, same_source_url=None),
            False,
        ),
    )


@dataclass(frozen=True)
class Measurement:
    """Recall and precision on the seeded suite."""

    detected: int
    planted: int
    false_alarms: int
    benign: int
    missed: tuple[str, ...]
    wrongly_flagged: tuple[str, ...]

    @property
    def recall(self) -> float:
        return self.detected / self.planted if self.planted else 1.0

    @property
    def precision(self) -> float:
        total = self.detected + self.false_alarms
        return self.detected / total if total else 1.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "recall": round(self.recall, 3),
            "precision": round(self.precision, 3),
            "detectadas": self.detected,
            "plantadas": self.planted,
            "falsas_alarmas": self.false_alarms,
            "benignas": self.benign,
            # The two lists are the whole point: a rate says how bad, these say
            # which, and only the second kind can be fixed.
            "no_detectadas": list(self.missed),
            "marcadas_de_mas": list(self.wrongly_flagged),
        }


def measure(
    classifier: Callable[..., Any] = classify_change,
    cases: tuple[SeededCase, ...] | None = None,
) -> Measurement:
    """Run the suite and report both rates. Never raises on one bad case."""
    suite = cases if cases is not None else seeded_cases()
    detected = false_alarms = planted = benign = 0
    missed: list[str] = []
    wrongly: list[str] = []

    for case in suite:
        try:
            verdict = classifier(case.before, case.after, case.context)
            actionable = verdict.verdict is Verdict.UNEXPLAINED
        except Exception:
            # A case the classifier cannot handle counts against it rather than
            # disappearing: an unmeasurable case is exactly what this exists to
            # stop.
            logger.warning("seeded faults: %s raised", case.name, exc_info=True)
            actionable = False

        if case.should_be_actionable:
            planted += 1
            if actionable:
                detected += 1
            else:
                missed.append(case.name)
        else:
            benign += 1
            if actionable:
                false_alarms += 1
                wrongly.append(case.name)

    return Measurement(
        detected=detected,
        planted=planted,
        false_alarms=false_alarms,
        benign=benign,
        missed=tuple(missed),
        wrongly_flagged=tuple(wrongly),
    )
