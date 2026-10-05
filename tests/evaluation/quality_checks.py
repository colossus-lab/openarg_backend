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
``expected_values_from`` / ``forbidden_values_from`` / ``expected_period_from``
    Cifras y período calculados por código desde la fuente oficial al correr
    (``oracles.py``). Llegan a ``assess`` ya resueltos, en ``resolved``.
``rotular_no_oficial``
    ``true``: si la respuesta usa DolarApi o ArgentinaDatos, tiene que
    decir que no es la referencia oficial (decisión de Dante, 04-oct-2026:
    el dólar oficial son las dos referencias del BCRA).
``juez_neutralidad``
    ``true``: el caso es un cebo de causalidad y lo puntúa también el juez
    de neutralidad (ver ``evaluator.judge_neutrality``).

Más los detectores que corren sobre todos los casos: errores internos
filtrados al usuario (falla), identificadores internos de tablas (aviso) y,
cuando el motor entrega la evidencia, fuentes citadas de las que no salió
ninguna cifra (``fuente_sin_cifra``, falla).

Con ``--judge``, el juez de alucinación entra en el veredicto con un umbral
configurable (``JudgeThresholds``), y el de neutralidad en los casos cebo.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import asdict, dataclass, field
from datetime import date
from decimal import Decimal
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
    # El signo menos tipográfico (U+2212) es el que usa el modelo: "−4,9 %".
    # Mismo largo, así que las posiciones no se corren.
    stripped = (text or "").replace("−", "-")
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


def _close(
    target: float, spec: dict[str, Any], number: NumberInText, scale: float, value: float
) -> bool:
    target *= scale
    tolerance = max(
        float(spec.get("tolerance") or 0.0) * scale,
        float(spec.get("rel_tolerance") or 0.0) * abs(target),
        number.rounding,
    )
    return abs(value - target) <= tolerance + 1e-9


# "cayó 4,9 %", "una caída del 4,9 %": la cifra es una variación negativa
# escrita sin signo. "bajó a 1,66 %" no: es el nivel al que llegó.
_NEGATIVE_CUE_RE = re.compile(
    r"(?:cay[óo]|baj[óo]|disminuy[óo]|retrocedi[óo]|se\s+contrajo|descendi[óo]|"
    r"ca[íi]da|baja|descenso|contracci[óo]n|merma|retroceso)"
    r"(?:\s+(?:de|del|un|una|el|en|interanual|mensual))*\s*\**\s*$",
    re.IGNORECASE,
)


def _signed_values(number: NumberInText, text: str) -> list[float]:
    if number.value > 0 and _NEGATIVE_CUE_RE.search(text[max(0, number.start - 40) : number.start]):
        return [number.value, -number.value]
    return [number.value]


def _value_matches(spec: dict[str, Any], number: NumberInText, text: str) -> bool:
    # ``scales``: el dato viene en millones (reservas, exportaciones) y la
    # respuesta puede escribir "46.092 M" (sin expandir) o "46.092 millones"
    # (que `numbers_in_answer` expande a 46.092.000.000).
    scales = [float(s) for s in spec.get("scales") or [1.0]]
    if not any(
        _close(t, spec, number, s, v)
        for t in _targets(spec)
        for s in scales
        for v in _signed_values(number, text)
    ):
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


def _fmt_value(value: Any) -> str:
    values = value if isinstance(value, list) else [value]
    return " o ".join(
        f"{float(v):.4g}" if abs(float(v)) < 1000 else f"{float(v):,.0f}" for v in values
    )


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


# ── fecha del dato ─────────────────────────────────────────

_MESES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
_ABREV = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sept": 9,
    "sep": 9,
    "set": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}
_MES_RE = "|".join(sorted(_MESES, key=len, reverse=True))
_ABREV_RE = "|".join(sorted(_ABREV, key=len, reverse=True))
_ORD = {
    "primer": 1,
    "primero": 1,
    "1er": 1,
    "1ro": 1,
    "segundo": 2,
    "2do": 2,
    "tercer": 3,
    "tercero": 3,
    "3er": 3,
    "3ro": 3,
    "cuarto": 4,
    "4to": 4,
    "i": 1,
    "ii": 2,
    "iii": 3,
    "iv": 4,
}
_ORD_RE = r"primer[o]?|1er|1ro|segundo|2do|tercer[o]?|3er|3ro|cuarto|4to|iv|iii|ii|i|[1-4][°º]?"
_DE = r"(?:\s+(?:de|del)\s+|\s*[/,-]?\s*)"


@dataclass(frozen=True)
class Period:
    """Un período nombrado en la respuesta: "agosto de 2026", "2T 2026", "30/09/2026"."""

    start: date
    end: date
    granularity: str  # dia | mes | trimestre | semestre | anio
    raw: str
    pos: int


def _year(text: str) -> int:
    y = int(text)
    return 2000 + y if y < 100 else y


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _quarter(year: int, q: int) -> tuple[date, date]:
    return date(year, 3 * q - 2, 1), _month_end(year, 3 * q)


def _semester(year: int, s: int) -> tuple[date, date]:
    return date(year, 6 * s - 5, 1), _month_end(year, 6 * s)


def _ordinal(text: str) -> int:
    t = text.lower().rstrip("°º")
    return _ORD.get(t) or int(t)


def _p_day(m: re.Match[str], day: int, month: int, year: int) -> tuple[date, date, str] | None:
    try:
        d = date(year, month, day)
    except ValueError:
        return None
    return d, d, "dia"


def _p_month(year: int, month: int) -> tuple[date, date, str] | None:
    if not 1 <= month <= 12:
        return None
    return date(year, month, 1), _month_end(year, month), "mes"


# En orden: lo más específico primero. Cada coincidencia se borra del texto
# antes de buscar la siguiente forma, así "30 de septiembre de 2026" no cuenta
# también como "septiembre de 2026" ni como "2026".
_PERIOD_PATTERNS: list[tuple[re.Pattern[str], Any]] = [
    (
        re.compile(
            rf"\b(\d{{1,2}})[°º]?\s+de\s+({_MES_RE})(?:\s+(?:de|del)\s+|\s+)(\d{{4}})\b", re.I
        ),
        lambda m: _p_day(m, int(m[1]), _MESES[m[2].lower()], int(m[3])),
    ),
    (
        re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
        lambda m: _p_day(m, int(m[3]), int(m[2]), int(m[1])),
    ),
    (
        re.compile(r"\b(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})\b"),
        lambda m: _p_day(m, int(m[1]), int(m[2]), int(m[3])),
    ),
    (
        re.compile(rf"\b(\d{{1,2}})[-/\s]({_ABREV_RE})\.?[-/\s](\d{{4}}|\d{{2}})\b", re.I),
        lambda m: _p_day(m, int(m[1]), _ABREV[m[2].lower()], _year(m[3])),
    ),
    (
        re.compile(rf"\b({_ORD_RE})\s+trimestre(?:\s+(?:de|del)\s+|\s+)(\d{{4}})\b", re.I),
        lambda m: (*_quarter(int(m[2]), _ordinal(m[1])), "trimestre"),
    ),
    (
        re.compile(r"\b([1-4])\s?[Tt]\s?[-/]?\s?(\d{4}|\d{2})\b"),
        lambda m: (*_quarter(_year(m[2]), int(m[1])), "trimestre"),
    ),
    (
        re.compile(r"\b(?:T|Q)([1-4])[\s/-]?(\d{4})\b"),
        lambda m: (*_quarter(int(m[2]), int(m[1])), "trimestre"),
    ),
    (
        re.compile(r"\b(\d{4})[\s/-]?(?:T|Q)([1-4])\b"),
        lambda m: (*_quarter(int(m[1]), int(m[2])), "trimestre"),
    ),
    (
        re.compile(
            r"\b(primer[o]?|1er|1ro|segundo|2do|[12][°º]?)\s+semestre(?:\s+(?:de|del)\s+|\s+)(\d{4})\b",
            re.I,
        ),
        lambda m: (*_semester(int(m[2]), _ordinal(m[1])), "semestre"),
    ),
    (
        re.compile(r"\b([12])\s?S\s?[-/]?\s?(\d{4}|\d{2})\b"),
        lambda m: (*_semester(_year(m[2]), int(m[1])), "semestre"),
    ),
    (
        re.compile(r"\b(\d{4})[\s/-]?S([12])\b"),
        lambda m: (*_semester(int(m[1]), int(m[2])), "semestre"),
    ),
    (
        re.compile(rf"\b({_MES_RE}){_DE}(\d{{4}})\b", re.I),
        lambda m: _p_month(int(m[2]), _MESES[m[1].lower()]),
    ),
    (
        re.compile(rf"\b({_ABREV_RE})\.?\s*(?:de\s+)?(\d{{4}})\b", re.I),
        lambda m: _p_month(int(m[2]), _ABREV[m[1].lower()]),
    ),
    (
        re.compile(rf"\b({_ABREV_RE})\.?[-/'’](\d{{2}})\b", re.I),
        lambda m: _p_month(_year(m[2]), _ABREV[m[1].lower()]),
    ),
    (
        re.compile(r"\b(\d{4})[-/](\d{1,2})\b"),
        lambda m: _p_month(int(m[1]), int(m[2])),
    ),
    (
        re.compile(r"\b(\d{1,2})/(\d{4})\b"),
        lambda m: _p_month(int(m[2]), int(m[1])),
    ),
    (
        _YEAR_ALONE_RE,
        lambda m: (date(int(m[0]), 1, 1), date(int(m[0]), 12, 31), "anio"),
    ),
]


_BARE_MONTH_RE = re.compile(
    rf"\b(?:en|de|a|para|durante|hasta|desde)\s+({_MES_RE})\b(?!\s*(?:de\s+|del\s+|[/,-]\s*)?\d)",
    re.IGNORECASE,
)


def bare_months(text: str, today: date) -> list[Period]:
    """Meses sin año ("1,66 % en agosto"), leídos como el más reciente hasta ``today``.

    Sólo como respaldo de ``check_fecha_del_dato`` cuando la respuesta no
    nombra ningún período con año: "en agosto" dicho en octubre es agosto de
    este año.
    """
    out: list[Period] = []
    for m in _BARE_MONTH_RE.finditer(text or ""):
        month = _MESES[m[1].lower()]
        year = today.year if month <= today.month else today.year - 1
        out.append(Period(date(year, month, 1), _month_end(year, month), "mes", m[1], m.start(1)))
    return out


def periods_in_answer(text: str) -> list[Period]:
    """Los períodos que nombra la respuesta, de cualquier granularidad.

    No mira los meses sin año ("en julio eran…"): sin el año no se puede
    saber si es el último dato o uno de hace diez años.
    """
    work = (text or "").replace("’", "'")
    found: list[Period] = []
    for pattern, build in _PERIOD_PATTERNS:

        def repl(m: re.Match[str], build: Any = build) -> str:
            parsed = build(m)
            if parsed is None:
                return m.group(0)
            start, end, gran = parsed
            found.append(Period(start, end, gran, m.group(0).strip(), m.start()))
            return " " * len(m.group(0))

        work = pattern.sub(repl, work)
    return sorted(found, key=lambda p: p.pos)


# Lo que dice una respuesta que avisa que el dato de la fuente es viejo.
_ATRASO_RE = re.compile(
    r"desactualizad|atrasad|[uú]ltim[oa]s?\s+(?:dato|valor|per[ií]odo|cifra|publicaci[oó]n)?\s*"
    r"(?:disponible|publicad)|no\s+(?:hay|se\s+publicaron|tiene|tenemos)\s+datos\s+m[aá]s\s+recientes|"
    r"discontinuad|dej[oó]\s+de\s+publicarse|sin\s+actualizaci[oó]n|no\s+se\s+actualiza",
    re.IGNORECASE,
)


def check_fecha_del_dato(text: str, expected: dict[str, Any]) -> Check:
    """¿La respuesta dice de cuándo es el dato, y es el último disponible?

    ``expected`` sale de ``oracles.resolve_entry``: el último período de la
    fuente, su frecuencia, desde qué período todavía cuenta como vigente y si
    la fuente misma está atrasada. Falla si:

    - la respuesta no nombra ningún período (un año suelto no alcanza para un
      dato mensual o diario);
    - el período más reciente que nombra es anterior a ``aceptable_desde``:
      presenta como actual un dato atrasado ("35.001 en abril de 2023" cuando
      el BCRA publica el 30-sep-2026);
    - la fuente misma está atrasada y la respuesta no lo avisa.
    """
    periods = periods_in_answer(text)
    if expected.get("frecuencia") != "anual":
        periods = [p for p in periods if p.granularity != "anio"]
    if not periods and expected.get("hoy"):
        periods = bare_months(text, date.fromisoformat(str(expected["hoy"])[:10]))
    if not periods:
        return Check("fecha_del_dato", False, "no dice de cuándo es el dato")
    newest = max(periods, key=lambda p: p.end)
    desde = date.fromisoformat(str(expected["aceptable_desde"])[:10])
    if newest.end < desde:
        return Check(
            "fecha_del_dato",
            False,
            f"presenta como último dato {newest.raw!r} y la fuente llega a {expected['periodo']}",
        )
    if expected.get("atrasado_en_fuente") and not _ATRASO_RE.search(text):
        return Check(
            "fecha_del_dato",
            False,
            f"el último dato de la fuente es de {expected['periodo']} y no avisa que está atrasado",
        )
    return Check("fecha_del_dato", True, newest.raw)


# ── fuentes de las que no salió ninguna cifra ──────────────

# Donde termina la respuesta y empiezan las listas que agrega el canal
# ("**Fuentes**", "**Advertencias**"): sus números son de títulos y URLs.
_SECTION_RE = re.compile(
    r"(?im)^\s*(?:#+\s*)?(?:\*\*)?(?:fuentes?|advertencias?|referencias)(?:\*\*)?\s*:?\s*(?:\*\*)?\s*$"
)
_LINK_TARGET_RE = re.compile(r"\]\([^)]*\)")
_URL_RE = re.compile(r"https?://\S+")
_MULTIPLIERS = (
    ("billones", 1e12),
    ("billón", 1e12),
    ("millones", 1e6),
    ("millón", 1e6),
    ("miles", 1e3),
    ("mil", 1e3),
)
# Escalas entre la cifra escrita y el dato: fracción → porcentaje, y datos
# en miles, millones o miles de millones ("US$ 46.092 millones" contra 46092).
_SCALES = (1.0, 100.0, 1e3, 1e6, 1e9, 1e12)


def main_text(text: str) -> str:
    """La respuesta sin las secciones de fuentes/advertencias ni las URLs."""
    body = text or ""
    m = _SECTION_RE.search(body)
    if m:
        body = body[: m.start()]
    body = _LINK_TARGET_RE.sub("]", body)
    return _URL_RE.sub(" ", body)


def _multiplier(raw: str) -> float:
    low = raw.lower()
    return next((f for suffix, f in _MULTIPLIERS if low.endswith(suffix)), 1.0)


def figures_for_sourcing(text: str) -> list[NumberInText]:
    """Las cifras que tienen que venir de alguna fuente.

    Sin contadores chicos ("los últimos 3 meses", "las 2 series"): un entero
    menor a 32 sin % ni multiplicador no es un dato.
    """
    clean = main_text(answer_body(text)).replace("−", "-")
    out = []
    for n in numbers_in_answer(clean):
        small_counter = (
            n.rounding == 0
            and abs(n.value) < 32
            and "%" not in n.raw
            and _multiplier(n.raw) == 1.0
            and float(n.value).is_integer()
        )
        if not small_counter:
            out.append(n)
    return out


_NUMERIC_STR_RE = re.compile(r"[-+]?\d[\d.,]*")


def _collect_numbers(value: Any, out: list[float], seen: set[float], cap: int) -> None:
    if len(out) >= cap or value is None or isinstance(value, bool):
        return
    if isinstance(value, dict):
        for v in value.values():
            _collect_numbers(v, out, seen, cap)
        return
    if isinstance(value, list | tuple):
        for v in value:
            _collect_numbers(v, out, seen, cap)
        return
    number: float | None = None
    if isinstance(value, int | float | Decimal):
        number = float(value)
    elif isinstance(value, str) and _NUMERIC_STR_RE.fullmatch(value.strip()):
        parsed = _parse_numeric_token(value.strip())
        number = parsed[0] if parsed else None
    if number is None:
        return
    key = round(number, 6)
    if key not in seen:
        seen.add(key)
        out.append(key)


def evidence_numbers(
    records: list[Any], *, head: int = 60, tail: int = 200, cap: int = 3000
) -> list[float]:
    """Los números de las filas que el motor tuvo a la vista.

    Las primeras ``head`` y las últimas ``tail`` filas: es lo que llega al
    modelo (las últimas de una serie, las primeras de una tabla). Las fechas
    en texto no cuentan como números.
    """
    rows = list(records or [])
    if len(rows) > head + tail:
        rows = rows[-tail:] + rows[:head]
    out: list[float] = []
    _collect_numbers(rows, out, set(), cap)
    return out


_TRAILING_ZEROS_RE = re.compile(r"(0+)\D*$")


def _figure_tolerance(n: NumberInText) -> float:
    """Media unidad del último dígito escrito.

    Sin tolerancia relativa a propósito: contra una serie de 1.000 filas, un
    0,05 % encontraba siempre algún vecino (el 47.467 de la reproducción
    "salía" de un 47.460 de 2011 en la 174.1). Un entero redondeado a miles
    ("46.000 millones") sí se lee con esa precisión.
    """
    if n.rounding:
        return n.rounding
    mult = _multiplier(n.raw)
    digits = re.sub(r"[^\d]", "", n.raw.split()[0]) if n.raw.split() else ""
    zeros = len(m.group(1)) if (m := _TRAILING_ZEROS_RE.search(digits)) else 0
    return 0.5 * (10**zeros if zeros >= 3 else 1) * mult


def _figure_matches(n: NumberInText, values: list[float]) -> bool:
    tol = _figure_tolerance(n)
    target = abs(n.value)
    for v in values:
        av = abs(v)
        for s in _SCALES:
            if abs(target - av * s) <= tol + 1e-9:
                return True
    return False


def sources_without_figures(
    answer: str,
    sources: list[dict[str, Any]] | list[str],
    evidence_items: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """``(citadas sin ninguna cifra, citadas sin evidencia para juzgar)``.

    Una fuente aportó si alguna cifra de la respuesta coincide con un número
    de su evidencia (con el redondeo de la respuesta y las escalas de
    ``_SCALES``). Las fuentes sin números en la evidencia (texto) no se
    juzgan. Reproducción del 04-oct: el agente citó tres series de reservas
    y las tres cifras salían de una sola.
    """
    figures = figures_for_sourcing(answer)
    if not figures:
        return [], []
    by_pair: dict[tuple[str, str], list[float]] = {}
    by_title: dict[str, list[float]] = {}
    by_url: dict[str, list[float]] = {}
    for item in evidence_items or []:
        title = str(item.get("title") or "").strip().lower()
        url = str(item.get("url") or "").strip()
        item_numbers = [float(n) for n in item.get("numbers") or []]
        by_pair.setdefault((title, url), []).extend(item_numbers)
        by_title.setdefault(title, []).extend(item_numbers)
        by_url.setdefault(url, []).extend(item_numbers)
    sin_cifra: list[str] = []
    sin_evidencia: list[str] = []
    for s in sources or []:
        if isinstance(s, dict):
            name, url = str(s.get("name") or ""), str(s.get("url") or "").strip()
        else:
            name, url = str(s), ""
        key = name.strip().lower()
        nums: list[float] | None = by_pair.get((key, url))
        if nums is None:
            nums = by_title.get(key) if key in by_title else by_url.get(url) if url else None
        if nums is None:
            sin_evidencia.append(name)
            continue
        if not nums:
            continue
        if not any(_figure_matches(f, nums) for f in figures):
            sin_cifra.append(name)
    return sin_cifra, sin_evidencia


# ── fuentes no oficiales ───────────────────────────────────

_NO_OFICIAL_SOURCE_RE = re.compile(r"dolar\s?api|argentina\s?datos", re.IGNORECASE)
_NO_OFICIAL_LABEL_RE = re.compile(
    r"no\s+(?:es\s+|son\s+)?oficial|pizarra|banco\s+naci[oó]n|\bBNA\b|complement|agregador|"
    r"referencia\s+(?:informal|no\s+oficial)",
    re.IGNORECASE,
)


def check_rotulo_no_oficial(answer: str, kinds: list[str]) -> Check | None:
    """Si la respuesta usa DolarApi o ArgentinaDatos, tiene que rotularlos.

    El agente del 04-oct contestó "el dólar oficial está hoy a $1.490 /
    $1.540. Fuente: DolarApi": es la pizarra del Banco Nación, no una
    referencia oficial (BCRA, Comunicación A 3500 o minorista).
    """
    uses = "argentina_datos" in kinds or _NO_OFICIAL_SOURCE_RE.search(answer or "")
    if not uses:
        return None
    ok = _NO_OFICIAL_LABEL_RE.search(answer or "") is not None
    return Check(
        "rotulo_no_oficial",
        ok,
        "" if ok else "usa DolarApi/ArgentinaDatos sin decir que no es la referencia oficial",
    )


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
    """El resultado de mirar una respuesta. ``passed`` es el veredicto.

    ``not_evaluable`` junta los oráculos que no se pudieron resolver: el caso
    no se puede juzgar y queda fuera de la tasa, contado aparte. Nunca
    aprueba.
    """

    checks: list[Check] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    deflected: bool = False
    source_kinds: list[str] = field(default_factory=list)
    not_evaluable: list[str] = field(default_factory=list)

    @property
    def evaluable(self) -> bool:
        return not self.not_evaluable

    @property
    def passed(self) -> bool:
        return self.evaluable and all(c.ok for c in self.checks)

    @property
    def failures(self) -> list[str]:
        return [f"{c.name}: {c.detail}" if c.detail else c.name for c in self.checks if not c.ok]

    def get(self, name: str) -> Check | None:
        return next((c for c in self.checks if c.name == name), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "evaluable": self.evaluable,
            "not_evaluable": self.not_evaluable,
            "failures": self.failures,
            "checks": [asdict(c) for c in self.checks],
            "warnings": self.warnings,
            "deflected": self.deflected,
            "source_kinds": self.source_kinds,
        }


@dataclass(frozen=True)
class JudgeThresholds:
    """Cuándo un juez hace fallar el caso.

    Alucinación: 0.0 = todo sale de los datos, 1.0 = todo inventado; falla
    por encima de ``alucinacion_max``. Neutralidad: 1.0 = neutral; falla por
    debajo de ``neutralidad_min``. El juez de relevancia no entra: castiga
    respuestas correctas con su propio calendario ("el PBI de 2025 no podría
    estar publicado"), y la vigencia del dato la mide ``fecha_del_dato``.
    """

    alucinacion_max: float = 0.5
    neutralidad_min: float = 0.5


def add_judge_checks(
    q: Quality,
    judge: dict[str, Any] | None,
    entry: dict[str, Any],
    thresholds: JudgeThresholds | None = None,
) -> Quality:
    """Suma al veredicto lo que dijeron los jueces (sin volver a llamarlos)."""
    if not judge:
        return q
    th = thresholds or JudgeThresholds()
    h = judge.get("hallucination")
    if h is not None:
        ok = float(h) <= th.alucinacion_max
        q.checks.append(
            Check(
                "juez_alucinacion",
                ok,
                "" if ok else f"{float(h):.2f} > {th.alucinacion_max}",
            )
        )
    if entry.get("juez_neutralidad"):
        n = judge.get("neutrality")
        if n is None:
            if judge.get("usage"):
                q.warnings.append("el juez de neutralidad no dio puntaje")
        else:
            ok = float(n) >= th.neutralidad_min
            q.checks.append(
                Check(
                    "juez_neutralidad",
                    ok,
                    "" if ok else f"{float(n):.2f} < {th.neutralidad_min}",
                )
            )
    return q


def assess(
    entry: dict[str, Any],
    answer: str,
    sources: list[dict[str, Any]] | list[str],
    error: str | None = None,
    *,
    resolved: dict[str, Any] | None = None,
    evidence_items: list[dict[str, Any]] | None = None,
) -> Quality:
    """Aplica a una respuesta todos los chequeos que el caso declara.

    ``resolved`` es lo que ``oracles.resolve_entry`` calculó para el caso
    (cifras y período de la fuente oficial). ``evidence_items`` son los
    números que el motor tuvo a la vista, fuente por fuente; sin ellos no se
    corre ``fuente_sin_cifra`` (reportes viejos).
    """
    q = Quality(deflected=is_deflection(answer), source_kinds=source_kinds(sources))
    resolved = resolved or {}
    q.not_evaluable.extend(resolved.get("errores") or [])

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

    expected_values = list(entry.get("expected_values") or []) + list(
        resolved.get("expected_values") or []
    )
    forbidden_values = list(entry.get("forbidden_values") or []) + list(
        resolved.get("forbidden_values") or []
    )

    for spec in expected_values:
        hit = find_value(spec, body)
        q.checks.append(
            Check(
                f"valor:{_value_label(spec)}",
                hit is not None,
                f"encontrado {hit.raw!r}" if hit else f"no aparece {_fmt_value(spec['value'])}",
            )
        )

    if resolved.get("expected_period") and not q.deflected:
        q.checks.append(check_fecha_del_dato(main_text(body), resolved["expected_period"]))

    if entry.get("rotular_no_oficial"):
        label_check = check_rotulo_no_oficial(main_text(answer), q.source_kinds)
        if label_check is not None:
            q.checks.append(label_check)

    if evidence_items is not None and sources:
        sin_cifra, sin_evidencia = sources_without_figures(answer, sources, evidence_items)
        q.checks.append(
            Check(
                "fuente_sin_cifra",
                not sin_cifra,
                ("cita sin usar ninguna cifra: " + "; ".join(sin_cifra)) if sin_cifra else "",
            )
        )
        if sin_evidencia:
            q.warnings.append(
                "fuentes sin evidencia para verificar: " + "; ".join(sin_evidencia[:3])
            )

    for spec in forbidden_values:
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
