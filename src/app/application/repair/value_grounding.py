"""Check a proposed column name against the values in the column.

The canary asks the model three questions with known answers and gates the whole
run on the result. That catches a model that has collapsed; it cannot catch the
error that actually happens. With three fixtures and an all-must-pass rule, a
model degraded to 90% per-item accuracy still passes **73%** of the time, and
three successes out of three give a 95% Wilson interval of **[0.44, 1.00]** —
formally consistent with a model that is wrong more often than not.

And the dominant failure is not collapse. It is the *semantically adjacent*
answer: `latitud` on a longitude column, `fecha_fin` on a start date, `monto` on
a quantity. Every one of those is a valid identifier, distinct from its
neighbours, correctly typed, and completely wrong — so it passes
`verify_intrinsic`, passes the canary, and passes the "did the batch come out
non-degenerate" check, which is a smoke test rather than a verification.

This is the check that can catch it, because it is the only one that looks at
what is actually in the column. If a name claims the column holds CUITs, the
check digits either work out or they do not. That is arithmetic, not judgement:
free, deterministic, immune to whatever the provider does to the model this
month, and — unlike a gate — it rejects **one rename** rather than a whole run.

**It only ever contradicts.** A name it has no opinion about is accepted, because
most column names are not of a kind anything can verify and refusing those would
make the tier useless. The value is in the small set it can call a lie.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime

logger = logging.getLogger(__name__)

# How much of a sample must contradict the name before the name is refused.
# Not 100%: real columns carry blanks, sentinels and the occasional bad row, and
# a rule that demands perfection would only ever fire on toy data.
CONTRADICTION_SHARE = 0.70

# Below this there is not enough evidence to call anything a lie.
MIN_SAMPLE = 4


@dataclass(frozen=True)
class Grounding:
    """What the values say about a proposed name."""

    column: str
    verdict: str  # "ok" | "contradicted" | "unknown"
    detail: str = ""

    @property
    def contradicted(self) -> bool:
        return self.verdict == "contradicted"


def _norm(text: object) -> str:
    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return t.lower()


def _clean(values: Sequence[object]) -> list[str]:
    return [str(v).strip() for v in values if v is not None and str(v).strip()]


# ── los verificadores ──────────────────────────────────────────

_CUIT_WEIGHTS = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)


def is_valid_cuit(value: str) -> bool:
    """The Argentine tax id carries its own check digit. Either it works or it doesn't."""
    digits = re.sub(r"\D", "", value)
    if len(digits) != 11:
        return False
    total = sum(int(d) * w for d, w in zip(digits[:10], _CUIT_WEIGHTS, strict=True))
    resto = total % 11
    dv = 11 - resto
    if dv == 11:
        dv = 0
    elif dv == 10:
        dv = 9
    return dv == int(digits[10])


_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y%m%d", "%m/%d/%Y")


def looks_like_date(value: str) -> bool:
    v = value.strip()[:19].replace("T", " ").split(" ")[0]
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(v, fmt)
            return True
        except ValueError:
            continue
    return False


_NUM_RE = re.compile(r"^-?[\d.,\s]+$")


def looks_numeric(value: str) -> bool:
    return bool(_NUM_RE.match(value.strip())) and any(c.isdigit() for c in value)


# The 23 provinces plus the autonomous city. Accent- and case-insensitive, and
# matched by containment so "Provincia de Buenos Aires" counts.
_PROVINCIAS = tuple(
    _norm(p)
    for p in (
        "buenos aires",
        "catamarca",
        "chaco",
        "chubut",
        "cordoba",
        "corrientes",
        "entre rios",
        "formosa",
        "jujuy",
        "la pampa",
        "la rioja",
        "mendoza",
        "misiones",
        "neuquen",
        "rio negro",
        "salta",
        "san juan",
        "san luis",
        "santa cruz",
        "santa fe",
        "santiago del estero",
        "tierra del fuego",
        "tucuman",
        "caba",
        "ciudad autonoma de buenos aires",
        "capital federal",
        "nacional",
        "total",
        "pais",
    )
)


def looks_like_provincia(value: str) -> bool:
    v = _norm(value)
    return any(p in v or v in p for p in _PROVINCIAS)


def looks_like_year(value: str) -> bool:
    v = value.strip()
    if not re.fullmatch(r"\d{4}(\.0)?", v):
        return False
    return 1800 <= int(float(v)) <= date.today().year + 5


# Name fragment -> the test its values must survive. When a name contains more
# than one, the fragment appearing **earliest in the name** decides: `fecha_cuit`
# is a date, `cuit_fecha` is a tax id. In these names the head noun comes first,
# and picking by list order instead made the answer depend on how this tuple
# happened to be sorted.
_CLAIMS: tuple[tuple[tuple[str, ...], str, Callable[[str], bool]], ...] = (
    (("cuit", "cuil"), "identificadores fiscales con dígito verificador válido", is_valid_cuit),
    (("fecha", "date"), "fechas parseables", looks_like_date),
    (("anio", "año", "ejercicio"), "años entre 1800 y hoy", looks_like_year),
    (("provincia", "jurisdiccion"), "nombres de provincia argentina", looks_like_provincia),
    (
        (
            "monto",
            "importe",
            "precio",
            "valor",
            "cantidad",
            "total",
            "tasa",
            "porcentaje",
            "latitud",
            "longitud",
            "poblacion",
            "superficie",
            "edad",
        ),
        "valores numéricos",
        looks_numeric,
    ),
)


def ground_name(column: str, values: Sequence[object]) -> Grounding:
    """Do the values support this name? Only ever contradicts, never invents."""
    muestra = _clean(values)
    if len(muestra) < MIN_SAMPLE:
        return Grounding(column, "unknown", "muestra insuficiente")

    nombre = _norm(column)
    candidatos = []
    for fragments, descripcion, test in _CLAIMS:
        posiciones = [nombre.index(f) for f in fragments if f in nombre]
        if posiciones:
            candidatos.append((min(posiciones), descripcion, test))
    for _pos, descripcion, test in sorted(candidatos, key=lambda c: c[0]):
        fallan = sum(1 for v in muestra if not test(v))
        if fallan >= len(muestra) * CONTRADICTION_SHARE:
            return Grounding(
                column,
                "contradicted",
                f"el nombre promete {descripcion} y {fallan} de {len(muestra)} no lo son "
                f"(ej. {muestra[0][:40]!r})",
            )
        return Grounding(column, "ok", descripcion)
    return Grounding(column, "unknown", "el nombre no promete nada verificable")


def reject_contradicted(
    old_columns: Sequence[str],
    new_columns: Sequence[str],
    sample_rows: Sequence[Sequence[object]],
) -> tuple[list[str], list[Grounding]]:
    """Keep the old name wherever the values contradict the proposed one.

    Returns the columns to apply and the contradictions found. A single bad
    rename reverts on its own; the rest of the proposal stands. Rejecting the
    whole table for one wrong column would throw away work that is correct, and
    the table would come back next run with the same odds.
    """
    if len(old_columns) != len(new_columns):
        return list(new_columns), []

    por_indice: list[list[object]] = [[] for _ in old_columns]
    for row in sample_rows:
        for i, value in enumerate(row):
            if i < len(por_indice):
                por_indice[i].append(value)

    aplicar: list[str] = []
    rechazos: list[Grounding] = []
    for i, (viejo, nuevo) in enumerate(zip(old_columns, new_columns, strict=True)):
        if viejo == nuevo:
            aplicar.append(nuevo)
            continue
        g = ground_name(nuevo, por_indice[i])
        if g.contradicted:
            rechazos.append(g)
            aplicar.append(viejo)
        else:
            aplicar.append(nuevo)
    if rechazos:
        logger.warning(
            "value grounding: %d renombre(s) contradichos por los valores: %s",
            len(rechazos),
            [r.detail for r in rechazos][:3],
        )
    return aplicar, rechazos
