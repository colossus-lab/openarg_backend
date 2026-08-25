"""Ask the model something whose answer we already know.

The LLM repair tier has written to 33 tables in production, renaming columns a
heuristic could not name. Every proposal answers to `verify_intrinsic`, so a
badly-wrong name is usually caught — but the verifier checks *shape*, not
*meaning*. It can tell that `col_3` became something identifier-like and
distinct; it cannot tell that a column of CUITs was named `fecha`.

Nothing else watched the model at all. Its tests mock the response, which
proves the plumbing and says nothing about the answers. A model that degrades —
a version change, a throttled endpoint answering with a stub, a prompt that
stops fitting — would keep producing well-formed names and nobody would know
until someone read a mart and found the wrong column heading.

So this asks a question we know the answer to. The fixtures are three columns
whose content is unambiguous to a person: Argentine tax IDs, ISO dates, and
peso amounts. If the model cannot name those, it should not be renaming
anything.

Deliberately not a unit test. A test asserts the model was fine when someone
ran CI; this runs where the model actually runs, on the schedule the repair
tier runs, and says so when the answer stops being right.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Each fixture is a column a person would name without hesitating, paired with
# the substrings any acceptable name contains. Several are allowed because
# "cuit" and "identificador_fiscal" are both right, and pinning one exact string
# would make this a test of the model's vocabulary rather than its competence.
_FIXTURES: tuple[tuple[str, list[str], tuple[str, ...]], ...] = (
    # Dígitos verificadores reales, no cadenas con forma de CUIT. Las primeras
    # fixtures tenían el dígito mal y nadie lo notó hasta que
    # `value_grounding` —que sí hace la aritmética— las refutó.
    (
        "col_1",
        ["20-12345678-6", "27-22222222-8", "30-71234567-1", "23-45678901-3"],
        ("cuit", "cuil", "fiscal", "identificador", "tributar"),
    ),
    (
        "col_2",
        ["2024-01-15", "2024-02-20", "2023-11-03", "2024-07-09"],
        ("fecha", "date", "dia", "periodo"),
    ),
    (
        "col_3",
        ["1250000.50", "890000.00", "2340500.75", "15000.00"],
        ("monto", "importe", "precio", "valor", "pesos", "total", "cantidad"),
    ),
)

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass(frozen=True)
class CanaryResult:
    """What the model answered, and whether it is still usable."""

    ok: bool
    reason: str
    answered: tuple[str, ...] = ()
    expected_any: tuple[tuple[str, ...], ...] = ()

    @property
    def detail(self) -> str:
        if self.ok:
            return f"el modelo nombró {len(self.answered)} columnas correctamente"
        pares = ", ".join(
            f"{got!r} (esperaba algo con {'/'.join(exp)})"
            for got, exp in zip(self.answered, self.expected_any)
        )
        return f"{self.reason}: {pares}" if pares else self.reason


def _acceptable(name: str, expected: tuple[str, ...]) -> bool:
    lowered = (name or "").lower()
    return any(hint in lowered for hint in expected)


async def run_canary(llm: Any, proposer: Any) -> CanaryResult:
    """Put the three fixtures through the real proposer and judge the answer.

    `proposer` is injected rather than imported so the caller decides which
    proposal path is under test — the canary should exercise the same function
    the repair tier calls, and that function moving should break this loudly
    rather than silently leave the canary testing something else.
    """
    old_cols = [name for name, _, _ in _FIXTURES]
    # One row per sample position, columns in fixture order — the shape the
    # proposer reads.
    filas = [
        [valores[i] for _, valores, _ in _FIXTURES]
        for i in range(min(len(v) for _, v, _ in _FIXTURES))
    ]

    try:
        propuestos, renamed, reason = await proposer(old_cols, filas, llm=llm)
    except Exception as exc:  # pragma: no cover — environmental
        logger.warning("model canary: the proposer raised", exc_info=True)
        return CanaryResult(ok=False, reason=f"el proposer falló: {str(exc)[:120]}")

    # `applied` es el éxito de estos proposers, no `ok`. Lo digo acá porque lo
    # asumí mal la primera vez y el canario reportó que el modelo declinaba
    # cuando en realidad había respondido bien: un vigía que se equivoca sobre
    # lo que vigila es peor que ninguno, porque enseña a ignorarlo.
    if reason not in ("applied", "ok") or not propuestos:
        # Declining is a legitimate answer, but not to these three. A model that
        # will not name a column of CUITs has no business naming the ambiguous
        # ones it is pointed at in production.
        return CanaryResult(ok=False, reason=f"el modelo declinó las tres ({reason})")

    esperados = tuple(exp for _, _, exp in _FIXTURES)
    answered = tuple(str(x) for x in propuestos[: len(esperados)])

    malformados = [n for n in answered if not _IDENT_RE.match(n)]
    if malformados:
        return CanaryResult(
            ok=False,
            reason=f"devolvió nombres que no son identificadores válidos: {malformados}",
            answered=answered,
            expected_any=esperados,
        )

    if len(set(answered)) != len(answered):
        return CanaryResult(
            ok=False,
            reason="devolvió nombres repetidos",
            answered=answered,
            expected_any=esperados,
        )

    fallados = [
        i for i, (got, exp) in enumerate(zip(answered, esperados)) if not _acceptable(got, exp)
    ]
    if fallados:
        return CanaryResult(
            ok=False,
            reason=f"{len(fallados)} de {len(esperados)} columnas mal nombradas",
            answered=answered,
            expected_any=esperados,
        )

    return CanaryResult(ok=True, reason="ok", answered=answered, expected_any=esperados)
