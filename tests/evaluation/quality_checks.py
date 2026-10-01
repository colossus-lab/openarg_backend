"""Qué tan buena es una respuesta, medido sin depender del motor que la dio.

La batería anterior aprobaba respuestas malas por tres agujeros:

- **Fuentes por título.** `retrieval_precision` buscaba "Series de Tiempo" en
  el título de la fuente: 27 de 53 casos no esperaban ninguna y sacaban 1,0
  automáticamente, y un mart que respondía bien sacaba 0.
- **Palabras clave.** "pobreza" y "canasta" aparecen igual en una respuesta
  que confunde la línea de pobreza con la tasa. Y una deflexión que repite la
  pregunta ("no encontré datos de inflación") aprueba "inflación".
- **Ningún número verificado.** Reservas en pesos, 256 bancas, PBI respondido
  con el EMAE, 7.944 filas de muestra presentadas como personas: todas
  pasaban.

Este módulo agrega lo que faltaba. Cada chequeo es una función pura sobre el
texto de la respuesta y la lista de fuentes, así que se aplica igual al
pipeline actual y al agente, y se prueba sin red ni LLM.

Campos del dataset que se leen acá (todos opcionales):

``expected_answer_contains``
    Palabras clave. Cada elemento es un texto o una **lista de alternativas**
    (alcanza con una): el saludo tiene tres variantes y sólo una dice "datos
    abiertos".
``expected_values``
    ``[{value, tolerance?, rel_tolerance?, unit_patterns?, label?}]``. Cada uno
    tiene que aparecer en la respuesta. ``value`` puede ser una lista de
    alternativas correctas: Pinamar admite 3.675.564 (todas las edades) y
    3.571.983 (6 años y más, la cifra oficial). La tolerancia efectiva es la mayor
    entre la absoluta, la relativa y el redondeo con que la respuesta escribió
    el número ("3,7 millones" cubre 3.675.564).
``forbidden_values``
    Mismo formato. Ninguno puede aparecer: las 7.944 filas de Pinamar.
``expected_source_by``
    Tipos de fuente (ver ``SOURCE_KINDS``), con la misma regla de alternativas
    que las palabras clave. Se compara por conector o portal, nunca por título.
``must_deflect``
    ``true``: la respuesta tiene que decir que no tiene el dato. ``false``: no
    puede esquivar. Ausente: no se mira.
``expected_answer_patterns`` / ``forbidden_answer_patterns``
    Expresiones regulares (sin distinguir mayúsculas) que tienen que aparecer
    o no pueden aparecer. Para lo que no es un valor fijo: las reservas
    cambian todos los días, pero siempre son en dólares.
``forbidden_answer_contains`` / ``forbidden_sources``
    Los de antes, sin cambios.

Más dos detectores que corren sobre todos los casos: errores internos
filtrados al usuario (falla) e identificadores internos de tablas (aviso).
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from app.application.pipeline.citation_guard import (
    _DATE_DMY_RE,
    _DATE_RE,
    _DATE_TEXT_RE,
    _NUM_RE,
    _TIME_RE,
    _parse_numeric_token,
)

# ── números ────────────────────────────────────────────────

# Un año suelto ("en 2024") no es una cifra de la respuesta. citation_guard no
# lo saca porque compara contra evidencia que también tiene años; acá sí hace
# falta: si no, "no tengo datos de 2024" contaría como una respuesta con datos.
_YEAR_ALONE_RE = re.compile(
    r"(?<![\d.,])\b(?:19|20)\d{2}\b(?![\d.,]*\d)"
    # "2000 millones" o "1950 %" son cifras, no años.
    r"(?!\s*(?:%|millones|millón|miles|mil\b|billones|billón))",
    re.IGNORECASE,
)
# "1. Foo" / "2) Bar": numeración de listas, no datos.
_LIST_MARKER_RE = re.compile(r"(?m)^\s*\d{1,2}[.)]\s")


def _blank(match: re.Match[str]) -> str:
    """Reemplaza por espacios del mismo largo: las posiciones no se corren."""
    return " " * len(match.group(0))


@dataclass(frozen=True)
class NumberInText:
    value: float
    rounding: float
    start: int
    end: int
    raw: str


def numbers_in_answer(text: str) -> list[NumberInText]:
    """Las cifras de la respuesta con su posición, sin fechas, horas ni años."""
    stripped = text or ""
    for pattern in (
        _DATE_RE,
        _DATE_DMY_RE,
        _DATE_TEXT_RE,
        _TIME_RE,
        _YEAR_ALONE_RE,
        _LIST_MARKER_RE,
    ):
        stripped = pattern.sub(_blank, stripped)
    out: list[NumberInText] = []
    for m in _NUM_RE.finditer(stripped):
        parsed = _parse_numeric_token(m.group(0))
        if parsed is None:
            continue
        out.append(NumberInText(parsed[0], parsed[1], m.start(), m.end(), m.group(0).strip()))
    return out


# Ventana alrededor de la cifra donde se busca la unidad: alcanza para
# "US$ 44.516 millones" y "44.516 millones de dólares", no para agarrar la
# unidad de la oración siguiente.
_UNIT_WINDOW = 40


def _targets(spec: dict[str, Any]) -> list[float]:
    value = spec["value"]
    return [float(v) for v in value] if isinstance(value, list) else [float(value)]


def _close(target: float, spec: dict[str, Any], number: NumberInText) -> bool:
    tolerance = max(
        float(spec.get("tolerance") or 0.0),
        float(spec.get("rel_tolerance") or 0.0) * abs(target),
        number.rounding,
    )
    return abs(number.value - target) <= tolerance + 1e-9


def _value_matches(spec: dict[str, Any], number: NumberInText, text: str) -> bool:
    if not any(_close(t, spec, number) for t in _targets(spec)):
        return False
    patterns = spec.get("unit_patterns") or []
    if not patterns:
        return True
    window = text[max(0, number.start - _UNIT_WINDOW) : number.end + _UNIT_WINDOW]
    return any(re.search(p, window, re.IGNORECASE) for p in patterns)


def find_value(spec: dict[str, Any], text: str) -> NumberInText | None:
    """La primera cifra de la respuesta que coincide con el valor esperado."""
    return next((n for n in numbers_in_answer(text) if _value_matches(spec, n, text)), None)


def _value_label(spec: dict[str, Any]) -> str:
    return str(spec.get("label") or spec["value"])


# ── el cuerpo de la respuesta ──────────────────────────────

# Las sugerencias de seguimiento ("¿Querés profundizar? - ¿Cómo evolucionó la
# tasa de pobreza…?") nombran justo lo que la respuesta no dio. Caso real del
# 01-oct: complex_004 contestó con la línea de pobreza y aprobó el chequeo de
# "tasa de pobreza" porque la tasa aparecía en una sugerencia.
_QUESTION_LINE_RE = re.compile(r"^[\s>*_\-•\d.)]*¿[^\n]*\?[\s*_]*$")


def answer_body(text: str) -> str:
    """La respuesta sin las líneas que son preguntas (sugerencias de seguimiento).

    Sólo para lo que la respuesta tiene que decir. Lo prohibido se busca en el
    texto entero: una cifra inventada no deja de serlo por estar en una
    pregunta.
    """
    return "\n".join(
        line for line in (text or "").splitlines() if not _QUESTION_LINE_RE.match(line)
    )


# ── deflexión y filtraciones ───────────────────────────────

# Lo que dice una respuesta que no responde. Varias son las mismas que
# `_drop_apologetic_preface` borra del analista actual: si una llega igual al
# usuario, es que no había otra cosa que decir.
DEFLECTION_PHRASES = (
    "no tengo",
    "no tenemos",
    "no cuento con",
    "no contamos con",
    "no dispongo",
    "no encontré",
    "no encontre",
    "no pude encontrar",
    "no se encontr",
    "no logré",
    "no pude obtener",
    "no pude acceder",
    "no hay datos",
    "no hay información",
    "no está disponible",
    "no esta disponible",
    "no están disponibles",
    "no disponible",
    "no puedo responder",
    "no puedo ayudarte con",
    "fuera de mi alcance",
    "te recomiendo consultar",
    "te sugiero consultar",
    "lamentablemente",
)


def deflection_phrases(text: str) -> list[str]:
    lower = (text or "").lower()
    return [p for p in DEFLECTION_PHRASES if p in lower]


def is_deflection(text: str) -> bool:
    """Dice que no tiene el dato y no da ninguna cifra.

    Las dos condiciones juntas, a propósito. "No hay dato por partido; a nivel
    nacional son 3.675.564" es una buena respuesta y tiene la frase. Y una
    respuesta con cifras pero sin la frase no esquivó.
    """
    return bool(deflection_phrases(text)) and not numbers_in_answer(text)


# Errores internos que llegaron al texto del usuario. Cada uno es una falla
# dura: el usuario lee "operación no permitida" y entiende "error de acceso"
# (caso real, arreglado en #109).
_LEAK_PATTERNS = (
    r"internal error",
    r"traceback",
    r"\bexception\b",
    r"psycopg",
    r"sqlalchemy",
    r"asyncpg",
    r"botocore",
    r'\brelation "',
    r"does not exist",
    r"syntax error",
    r"permission denied",
    r"statement timeout",
    r"undefined(?:table|column)",
    r"\bkeyerror\b",
    r"\bnonetype\b",
    r"pipeline_error",
    r"ocurrió un error",
    r"error al analizar",
    r"operación no permitida",
    r"error de acceso",
)
_LEAK_RE = re.compile("|".join(_LEAK_PATTERNS), re.IGNORECASE)

# Nombres internos de tablas. Sólo aviso: el agente puede citar la tabla, pero
# el usuario tendría que leer el título del dataset, no
# `raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1`.
_IDENTIFIER_RE = re.compile(
    r"\b(?:raw|mart|staging)\.[a-z_][a-z0-9_]{3,}|\bcache_[a-z0-9_]{4,}|__v\d+\b",
    re.IGNORECASE,
)


def leaked_errors(text: str) -> list[str]:
    return sorted({m.group(0).lower() for m in _LEAK_RE.finditer(text or "")})


def leaked_identifiers(text: str) -> list[str]:
    return sorted({m.group(0) for m in _IDENTIFIER_RE.finditer(text or "")})


# ── fuentes por tipo ───────────────────────────────────────

# De qué conector o portal salió una fuente, por su portal y su URL. Nunca por
# el título: el título de un dataset no dice nada sobre si la cifra es la
# serie oficial o una tabla cacheada.
SOURCE_KINDS: dict[str, tuple[str, ...]] = {
    "series_tiempo": ("series de tiempo", "datos.gob.ar/series", "apis.datos.gob.ar/series"),
    # El conector de ArgentinaDatos sirve las cotizaciones del día desde
    # DolarApi (`dolarapi.com`), del mismo autor.
    "argentina_datos": ("argentinadatos", "dolarapi"),
    "bcra": ("banco central", "bcra.gob.ar"),
    "ddjj": ("declaraciones juradas", "anticorrupcion"),
    "sesiones": ("diario de sesiones",),
    "staff": ("nómina de personal",),
    "georef": ("georef",),
    "indec_live": ("descarga en vivo",),
    "cache_local": ("cache local", "base de datos local"),
    "indec": ("indec.gob.ar",),
    "datos_gob_ar": ("datos.gob.ar",),
    "ckan": ("ckan",),
}


def source_kinds(sources: list[dict[str, Any]] | list[str]) -> list[str]:
    """Los tipos de fuente presentes, en el orden de ``SOURCE_KINDS``."""
    haystacks: list[str] = []
    for s in sources or []:
        if isinstance(s, dict):
            haystacks.append(f"{s.get('portal') or ''} {s.get('url') or ''}".lower())
        else:
            haystacks.append(str(s).lower())
    found: list[str] = []
    for kind, needles in SOURCE_KINDS.items():
        if any(n in h for h in haystacks for n in needles):
            found.append(kind)
    # La serie oficial también vive en datos.gob.ar; no contarla dos veces
    # como si fuera un dataset del portal.
    if "series_tiempo" in found and "datos_gob_ar" in found:
        rest = [h for h in haystacks if not any(n in h for n in SOURCE_KINDS["series_tiempo"])]
        if not any("datos.gob.ar" in h for h in rest):
            found.remove("datos_gob_ar")
    return found


# ── grupos de alternativas ─────────────────────────────────


def _groups(spec: list[Any] | None) -> list[list[str]]:
    """Normaliza ``["a", ["b", "c"]]`` a ``[["a"], ["b", "c"]]``."""
    return [[g] if isinstance(g, str) else list(g) for g in spec or []]


def keyword_hits(answer: str, spec: list[Any] | None) -> tuple[int, list[str]]:
    """``(grupos cumplidos, grupos que faltan)``."""
    lower = (answer or "").lower()
    missing = [" | ".join(g) for g in _groups(spec) if not any(k.lower() in lower for k in g)]
    return len(_groups(spec)) - len(missing), missing


def keyword_score(answer: str, spec: list[Any] | None) -> float:
    groups = _groups(spec)
    if not groups:
        return 1.0
    hit, _ = keyword_hits(answer, spec)
    return hit / len(groups)


# ── veredicto ──────────────────────────────────────────────


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Quality:
    """El resultado de mirar una respuesta. ``passed`` es el veredicto."""

    checks: list[Check] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    deflected: bool = False
    source_kinds: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def failures(self) -> list[str]:
        return [f"{c.name}: {c.detail}" if c.detail else c.name for c in self.checks if not c.ok]

    def get(self, name: str) -> Check | None:
        return next((c for c in self.checks if c.name == name), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failures": self.failures,
            "checks": [asdict(c) for c in self.checks],
            "warnings": self.warnings,
            "deflected": self.deflected,
            "source_kinds": self.source_kinds,
        }


def assess(
    entry: dict[str, Any],
    answer: str,
    sources: list[dict[str, Any]] | list[str],
    error: str | None = None,
) -> Quality:
    """Aplica a una respuesta todos los chequeos que el caso declara."""
    q = Quality(deflected=is_deflection(answer), source_kinds=source_kinds(sources))

    if error:
        q.checks.append(Check("sin_error", False, error[:200]))
        return q

    leaks = leaked_errors(answer)
    q.checks.append(Check("sin_errores_filtrados", not leaks, ", ".join(leaks)))
    ids = leaked_identifiers(answer)
    if ids:
        q.warnings.append(f"identificadores internos en la respuesta: {', '.join(ids[:3])}")

    body = answer_body(answer)

    if entry.get("expected_answer_contains"):
        _, missing = keyword_hits(body, entry["expected_answer_contains"])
        q.checks.append(Check("palabras_clave", not missing, "faltan: " + "; ".join(missing)))

    for spec in entry.get("expected_values") or []:
        hit = find_value(spec, body)
        q.checks.append(
            Check(
                f"valor:{_value_label(spec)}",
                hit is not None,
                f"encontrado {hit.raw!r}" if hit else f"no aparece {spec['value']}",
            )
        )

    for spec in entry.get("forbidden_values") or []:
        hit = find_value(spec, answer)
        q.checks.append(
            Check(
                f"valor_prohibido:{_value_label(spec)}",
                hit is None,
                f"aparece {hit.raw!r}" if hit else "",
            )
        )

    if entry.get("expected_source_by"):
        missing = [
            " | ".join(g)
            for g in _groups(entry["expected_source_by"])
            if not set(g) & set(q.source_kinds)
        ]
        q.checks.append(
            Check(
                "fuente_por_tipo",
                not missing,
                f"faltan {missing}, hay {q.source_kinds}" if missing else "",
            )
        )

    must = entry.get("must_deflect")
    if must is not None:
        if must:
            q.checks.append(Check("deflecta", q.deflected, "" if q.deflected else "contestó"))
        else:
            q.checks.append(
                Check(
                    "no_deflecta",
                    not q.deflected,
                    ", ".join(deflection_phrases(answer)) if q.deflected else "",
                )
            )

    for pattern in entry.get("expected_answer_patterns") or []:
        ok = re.search(pattern, body, re.IGNORECASE) is not None
        q.checks.append(Check(f"patron:{pattern}", ok, "" if ok else "no aparece"))

    for pattern in entry.get("forbidden_answer_patterns") or []:
        m = re.search(pattern, answer or "", re.IGNORECASE)
        q.checks.append(
            Check(f"patron_prohibido:{pattern}", m is None, f"aparece {m.group(0)!r}" if m else "")
        )

    lower = (answer or "").lower()
    said = [p for p in entry.get("forbidden_answer_contains") or [] if p.lower() in lower]
    if entry.get("forbidden_answer_contains"):
        q.checks.append(Check("frases_prohibidas", not said, ", ".join(said)))

    if entry.get("forbidden_sources"):
        names = [
            f"{s.get('name') or ''} {s.get('portal') or ''}" if isinstance(s, dict) else str(s)
            for s in sources or []
        ]
        hit_src = [
            p for p in entry["forbidden_sources"] if any(p.lower() in n.lower() for n in names)
        ]
        q.checks.append(Check("fuentes_prohibidas", not hit_src, ", ".join(hit_src)))

    return q
