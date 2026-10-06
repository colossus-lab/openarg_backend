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


_MULTIPLIERS = (
    ("billones", 1e12),
    ("billón", 1e12),
    ("millones", 1e6),
    ("millón", 1e6),
    ("miles", 1e3),
    ("mil", 1e3),
)
_TRAILING_ZEROS_RE = re.compile(r"(0+)\D*$")


def _multiplier(raw: str) -> float:
    low = raw.lower()
    return next((f for suffix, f in _MULTIPLIERS if low.endswith(suffix)), 1.0)


def _rounding_unit(n: NumberInText) -> tuple[float, bool]:
    """``(media unidad del último dígito escrito, si es un redondeo explícito)``.

    Un entero con tres ceros o más al final ("46.000 millones") o con
    multiplicador ("46 mil millones", "4 millones") está redondeado: "46 mil"
    vale lo mismo que "46.000". Un entero corto sin multiplicador ("2 %",
    "1.540") no: su media unidad es 0,5, pero no dice que se haya redondeado.
    El redondeo se lee hasta los miles: "50.000 millones" es ±500, no
    ±5.000 (si no, cubría las reservas de 47.960 de "oscilaron en torno a
    los USD 50.000 millones").
    """
    mult = _multiplier(n.raw)
    digits = re.sub(r"[^\d]", "", n.raw.split()[0]) if n.raw.split() else ""
    zeros = len(m.group(1)) if (m := _TRAILING_ZEROS_RE.search(digits)) else 0
    zeros = 3 if zeros >= 3 else 0
    return 0.5 * 10**zeros * mult, zeros >= 3 or mult > 1


def implied_rounding(n: NumberInText) -> float:
    """El redondeo con que la respuesta escribió la cifra, o 0 si la dio exacta.

    Los decimales lo dicen solos ("46,1 mil" → ±50); un entero, sólo si tiene
    ceros finales o multiplicador (ver ``_rounding_unit``). Es lo mismo que
    tolera ``fuente_sin_cifra``, para que las dos vistas de una cifra
    coincidan, salvo la media unidad de los enteros cortos: "2 %" no cubre un
    1,66 % esperado con tolerancia 0,15.
    """
    if n.rounding:
        return n.rounding
    unit, explicit = _rounding_unit(n)
    return unit if explicit else 0.0


def _close(
    target: float, spec: dict[str, Any], number: NumberInText, scale: float, value: float
) -> bool:
    target *= scale
    tolerance = max(
        float(spec.get("tolerance") or 0.0) * scale,
        float(spec.get("rel_tolerance") or 0.0) * abs(target),
        implied_rounding(number),
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


# Sufijo con que ``oracles.resolve_entry`` rotula las cifras de una cuenta mal
# hecha (sumar tasas, promediar en vez de sumar, acumular desde enero).
CUENTA_MAL_HECHA = " (cuenta mal hecha)"


def _excused_by(spec: dict[str, Any]) -> str | None:
    """La cifra esperada cuya presencia disculpa a esta cifra prohibida.

    Una cuenta mal hecha sólo es un error si se presenta en lugar de la
    correcta: "87.111 millones en 2025, unos 7.259 por mes" está bien. Los
    reportes congelados antes del 05-oct no tienen ``salvo_si_aparece``: se
    reconoce por el rótulo.
    """
    if spec.get("salvo_si_aparece"):
        return str(spec["salvo_si_aparece"])
    label = str(spec.get("label") or "")
    return label[: -len(CUENTA_MAL_HECHA)] if label.endswith(CUENTA_MAL_HECHA) else None


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
    """Un período nombrado en la respuesta: "agosto de 2026", "2T 2026", "30/09/2026".

    ``inferido``: la respuesta no dijo el año ("en agosto", "al 30/9") y se
    tomó el más reciente hasta hoy.
    """

    start: date
    end: date
    granularity: str  # dia | mes | trimestre | semestre | anio
    raw: str
    pos: int
    inferido: bool = False

    @property
    def endpos(self) -> int:
        return self.pos + len(self.raw)


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


def _month_number(text: str) -> int:
    t = text.lower().rstrip(".")
    return _MESES.get(t) or _ABREV[t]


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
    # "2T 2026", "1T26", "4T-2025": la T en mayúscula y pegada al número.
    # Con espacios y sin distinguir mayúsculas, "pagó 1 t 26 kilos" era el
    # primer trimestre de 2026.
    (
        re.compile(r"\b([1-4])[°º]?T(?:\s?[-/]\s?|\s)?(\d{4}|\d{2})\b"),
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
    # "1S26", "2S 2025": ídem ("US$ 2 S 50" era el segundo semestre de 2050).
    (
        re.compile(r"\b([12])[°º]?S(?:\s?[-/]\s?|\s)?(\d{4}|\d{2})\b"),
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
    # "IPC agosto 26: 1,66 %", "| Ago 26 |": mes y año de dos dígitos, sólo
    # si lo que sigue cierra el rótulo. "En agosto 15 provincias…" no es 2015.
    (
        re.compile(
            rf"\b({_MES_RE}|{_ABREV_RE})\.?\s+(\d{{2}})\b(?=\s*(?:[:|)\],;]|$))",
            re.I | re.M,
        ),
        lambda m: _p_month(_year(m[2]), _month_number(m[1])),
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


def _scan(text: str) -> tuple[list[Period], str]:
    """Los períodos con año y el texto con esos períodos borrados."""
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
    return sorted(found, key=lambda p: p.pos), work


def periods_in_answer(text: str) -> list[Period]:
    """Los períodos que nombra la respuesta con su año, de cualquier granularidad.

    Los que no dicen el año ("en julio", "al 30/9") los agrega
    ``dated_periods``, que sabe qué día es hoy.
    """
    return _scan(text)[0]


_SIN_ANIO = r"(?!\s*(?:de\s+|del\s+|[/,-]\s*)?\d)"
# "al 30 de septiembre", "el 2 de octubre": día y mes, sin año.
_BARE_DAY_RE = re.compile(rf"\b(\d{{1,2}})[°º]?\s+de\s+({_MES_RE})\b{_SIN_ANIO}", re.I)
# "(dato del 30/9)": día/mes sin año, sólo después de "al", "del", "el"…; si
# no, "1/2" o "24/7" serían fechas.
_BARE_DM_RE = re.compile(
    r"\b(?:al|del|el|hasta|desde|d[ií]a)\s+(\d{1,2})/(\d{1,2})\b(?!\s*[/.-]\s*\d)", re.I
)
# "1,66 % en agosto", "la inflación de agosto".
_BARE_MONTH_RE = re.compile(
    rf"\b(?:en|de|a|al|para|durante|hasta|desde)\s+({_MES_RE})\b{_SIN_ANIO}", re.I
)


def _latest(today: date, month: int, day: int | None = None) -> tuple[date, date] | None:
    """El ``month`` (o el día) más reciente que no sea posterior a ``today``."""
    for year in (today.year, today.year - 1):
        try:
            start = date(year, month, day or 1)
        except ValueError:
            return None
        if start <= today:
            return (start, start) if day else (start, _month_end(year, month))
    return None


_YEAR_CONTEXT_CHARS = 300
_ESTE_ANIO_RE = re.compile(r"\s+(?:de|del)\s+(?:este|corriente)\s+a[nñ]o", re.IGNORECASE)


def _year_context(text: str, pos: int, explicit: list[Period]) -> int | None:
    """El año del que viene hablando la respuesta, si lo hay.

    "Exportó USD 79.703 millones en 2024. … Ese total resulta de sumar los
    doce meses, con picos en mayo": ese mayo es de 2024, no el último mayo
    (respuesta real del agente, 05-oct). Cuenta sólo si el período anterior
    más cercano (a menos de 300 caracteres) es un año suelto y no la base de
    una comparación: "en diciembre de 2025 eran 30.000; al 30 de
    septiembre, 46.092" no hace de septiembre un mes de 2025, ni "comparado
    con 2023… en agosto" un agosto de 2023.
    """
    before = [p for p in explicit if p.endpos <= pos and pos - p.endpos <= _YEAR_CONTEXT_CHARS]
    if not before:
        return None
    last = max(before, key=lambda p: p.pos)
    if last.granularity != "anio" or _is_base(text, last):
        return None
    return last.start.year


def _bare_span(
    text: str,
    span: tuple[int, int],
    explicit: list[Period],
    today: date,
    month: int,
    day: int | None = None,
) -> tuple[date, date] | None:
    """Fechas de un día o mes sin año (``span``: dónde está en el texto)."""
    year = None if _ESTE_ANIO_RE.match(text, span[1]) else _year_context(text, span[0], explicit)
    if year is None:
        return _latest(today, month, day)
    try:
        start = date(year, month, day or 1)
    except ValueError:
        return None
    return (start, start) if day else (start, _month_end(year, month))


def bare_periods(text: str, today: date) -> list[Period]:
    """Días y meses sin año: del año del que viene hablando la respuesta o, si
    no, los más recientes hasta ``today``.

    "En agosto" dicho en octubre es agosto de este año; "al 30 de
    septiembre", el último 30 de septiembre; "en 2024… en mayo", mayo de
    2024 (``_year_context``). Se leen sobre el texto sin los períodos que sí
    tienen año (``_scan``), así "30 de septiembre de 2026" no cuenta además
    como "30 de septiembre".
    """
    explicit, work = _scan(text)
    out: list[Period] = []
    for m in _BARE_DAY_RE.finditer(work):
        span = _bare_span(work, m.span(), explicit, today, _MESES[m[2].lower()], int(m[1]))
        if span:
            out.append(Period(*span, "dia", m.group(0), m.start(), inferido=True))
    for m in _BARE_DM_RE.finditer(work):
        if not 1 <= int(m[2]) <= 12:
            continue
        span = _bare_span(work, (m.start(1), m.end()), explicit, today, int(m[2]), int(m[1]))
        if span:
            out.append(Period(*span, "dia", f"{m[1]}/{m[2]}", m.start(1), inferido=True))
    taken = [(p.pos, p.endpos) for p in out]
    for m in _BARE_MONTH_RE.finditer(work):
        if any(a <= m.start(1) < b for a, b in taken):
            continue
        span = _bare_span(work, m.span(1), explicit, today, _MESES[m[1].lower()])
        if span:
            out.append(Period(*span, "mes", m[1], m.start(1), inferido=True))
    return sorted(out, key=lambda p: p.pos)


def dated_periods(text: str, today: date) -> list[Period]:
    """Todos los períodos de la respuesta: con año y sin año (inferido)."""
    return sorted(periods_in_answer(text) + bare_periods(text, today), key=lambda p: p.pos)


# ── qué período es el del dato ─────────────────────────────

# Fin de oración: un punto seguido de espacio (no el de "46.092") o un salto
# de línea (cada fila de una tabla es su propia "oración").
_SENTENCE_END_RE = re.compile(r"[.!?](?=\s|$)|\n")
# El período es la fecha de hoy o de la consulta, no la del dato: "Hoy, 5 de
# octubre de 2026, el último dato es de abril de 2023".
_HOY_CUE_RE = re.compile(
    r"(?:\bhoy|\bconsultad[oa]s?(?:\s+(?:el|al))?|\bfecha\s+de\s+(?:hoy|consulta)|"
    r"\bal\s+d[ií]a\s+de\s+hoy)[\s,:(]*"
    r"(?:(?:es|lunes|martes|mi[ée]rcoles|jueves|viernes|s[áa]bado|domingo|el|del)[\s,]+){0,2}$",
    re.IGNORECASE,
)
# El período es justo el que no hay: "No tengo datos para septiembre 2026",
# "aún no se publicó el dato de septiembre". "No hay datos posteriores a
# abril de 2023" no entra: ahí abril de 2023 sí es el dato.
_SIN_DATO_CUE_RE = re.compile(
    r"\b(?:no|sin|tampoco)\b(?:\s+\w+){0,4}?\s+(?:datos?|informaci[oó]n|cifras?|registros?|"
    r"valor(?:es)?|publicad[oa]s?|publicaci[oó]n|disponibles?)\b(?:\s+\w+){0,2}?\s+"
    r"(?:para|de|del|sobre|en|correspondientes?\s+a)\s+(?:(?:el|la|los|las)\s+)?"
    r"(?:mes\s+de\s+|a[nñ]o\s+|per[ií]odo\s+)?$",
    re.IGNORECASE,
)
# Proyecciones: "el REM proyecta 1,8 % para septiembre de 2026" no es un dato
# publicado (y septiembre ya terminó, así que el filtro de futuro no alcanza).
_PROYECCION_CUE_RE = re.compile(
    r"\b(?:proyect\w*|prev[eé]n?|pronostic\w*|se\s+espera\w*|REM|expectativas?)\b",
    re.IGNORECASE,
)
_CLAUSE_CHARS = 100


def _clause_before(text: str, pos: int) -> str:
    """Lo que viene antes de ``pos`` en la misma oración (hasta 100 caracteres)."""
    start = max(0, pos - _CLAUSE_CHARS)
    cut = [m.end() for m in _SENTENCE_END_RE.finditer(text, start, pos)]
    return text[cut[-1] if cut else start : pos]


def descarte(text: str, p: Period, hoy: date, frecuencia: str | None) -> str | None:
    """Por qué ``p`` no puede ser el período del dato, o None si puede serlo."""
    if p.start > hoy or (p.end > hoy and frecuencia != "diaria"):
        return "futuro"
    before = _clause_before(text, p.pos)
    if _HOY_CUE_RE.search(before):
        return "hoy"
    if _SIN_DATO_CUE_RE.search(before):
        return "sin dato"
    if _PROYECCION_CUE_RE.search(before):
        return "proyección"
    return None


def _sentence_span(text: str, pos: int) -> tuple[int, int]:
    """Dónde empieza y termina la oración que contiene ``pos``."""
    start = 0
    for m in _SENTENCE_END_RE.finditer(text, 0, pos):
        start = m.end()
    end = _SENTENCE_END_RE.search(text, pos)
    return start, (end.start() if end else len(text))


def _distance(p: Period, n: NumberInText) -> int:
    if p.endpos <= n.start:
        return n.start - p.endpos
    if p.pos >= n.end:
        return p.pos - n.end
    return 0


# Cuánto se mira hacia atrás (títulos, encabezados de tabla) y hacia adelante
# ("…USD 46.092 millones. El dato es del 30 de septiembre") cuando la
# oración de la cifra no nombra ningún período.
_LOOKBACK_CHARS = 300
_LOOKAHEAD_CHARS = 150


# La base de una comparación: "cayó 4,9 % interanual respecto de julio de
# 2025" habla de julio de 2026, no de 2025.
_BASE_CUE_RE = re.compile(
    r"(?:respecto\s+(?:de|a|al|del)|contra|frente\s+a[l]?|comparad[oa]s?\s+con|"
    r"en\s+comparaci[oó]n\s+con|vs\.?|versus|con\s+relaci[oó]n\s+a[l]?|desde|"
    r"que\s+en)\s+(?:(?:el|la|los|las|del|al|igual\s+mes\s+de|mismo\s+mes\s+de)\s+)?$",
    re.IGNORECASE,
)


def _is_base(text: str, p: Period) -> bool:
    return _BASE_CUE_RE.search(text[max(0, p.pos - 40) : p.pos]) is not None


def period_of(n: NumberInText, periods: list[Period], text: str) -> Period | None:
    """El período al que se refiere una cifra.

    El más cercano en la misma oración ("La inflación de agosto fue 1,66 %,
    contra 3,5 % en agosto de 2025": el 1,66 es de agosto y el 3,5 de
    2025), salvo que sea la base de una comparación ("respecto de julio de
    2025"). Si la oración no nombra ninguno, el más reciente de los
    alrededores: un título o el encabezado de una tabla.
    """
    s0, s1 = _sentence_span(text, n.start)
    same = [p for p in periods if p.pos < s1 and p.endpos > s0]
    if same:
        return min(same, key=lambda p: (_is_base(text, p), _distance(p, n), p.pos > n.start))
    near = [
        p
        for p in periods
        if 0 <= n.start - p.endpos <= _LOOKBACK_CHARS or 0 <= p.pos - n.end <= _LOOKAHEAD_CHARS
    ]
    return max(near, key=lambda p: p.end) if near else None


# Lo que dice una respuesta que avisa que el dato de la fuente es viejo.
_ATRASO_RE = re.compile(
    r"desactualizad|atrasad|[uú]ltim[oa]s?\s+(?:dato|valor|per[ií]odo|cifra|publicaci[oó]n)?\s*"
    r"(?:disponible|publicad)|no\s+(?:hay|se\s+publicaron|tiene|tenemos)\s+datos\s+m[aá]s\s+recientes|"
    r"discontinuad|dej[oó]\s+de\s+publicarse|sin\s+actualizaci[oó]n|no\s+se\s+actualiza",
    re.IGNORECASE,
)


def check_fecha_del_dato(
    text: str, expected: dict[str, Any], anchors: list[dict[str, Any]] | None = None
) -> Check:
    """¿La respuesta dice de cuándo es el dato, y es el último disponible?

    ``expected`` sale de ``oracles.resolve_entry``: el último período de la
    fuente, su frecuencia, desde qué período todavía cuenta como vigente y si
    la fuente misma está atrasada. ``anchors`` son las cifras esperadas del
    caso (``expected_values`` ya resueltos), si las tiene.

    El período del dato es el que acompaña a la cifra, no el más nuevo del
    texto: "llega solo hasta abril de 2023 (USD 35.001 millones)… No tengo
    datos para septiembre 2026" aprobaba por "septiembre 2026" (respuesta
    real del agente, 05-oct). Por eso:

    - se descartan los períodos que no pueden ser el del dato (futuros, la
      fecha de hoy, los que la respuesta dice no tener y las proyecciones;
      ver ``descarte``);
    - si la respuesta tiene la cifra esperada, cuenta el período que la
      acompaña (``period_of``): una coincidencia casual de valor con fecha
      vieja falla;
    - si no, el más reciente entre los que acompañan a alguna cifra (una
      comparación con el año anterior no tapa el dato actual);
    - y si ninguna cifra tiene período cerca, el más reciente del texto.

    Falla si no nombra ningún período (un año suelto no alcanza para un dato
    mensual o diario), si el período del dato es anterior a
    ``aceptable_desde`` ("35.001 en abril de 2023" cuando el BCRA publica el
    30-sep-2026) o si la fuente misma está atrasada y la respuesta no lo
    avisa.
    """
    hoy = date.fromisoformat(str(expected.get("hoy") or date.today().isoformat())[:10])
    frecuencia = expected.get("frecuencia")
    periods = dated_periods(text, hoy)
    if frecuencia != "anual":
        periods = [p for p in periods if p.granularity != "anio"]
    periods = [p for p in periods if descarte(text, p, hoy, frecuencia) is None]
    if not periods:
        return Check("fecha_del_dato", False, "no dice de cuándo es el dato")
    figures = [n for n in numbers_in_answer(text) if not _small_counter(n)]
    anchored = [n for n in figures if any(_value_matches(s, n, text) for s in anchors or [])]
    dated = [p for n in anchored if (p := period_of(n, periods, text))]
    if not dated:
        dated = [p for n in figures if (p := period_of(n, periods, text))] or periods
    newest = max(dated, key=lambda p: p.end)
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


def _small_counter(n: NumberInText) -> bool:
    """ "Los últimos 3 meses", "las 2 series": un entero menor a 32 sin % ni
    multiplicador no es un dato."""
    return (
        n.rounding == 0
        and abs(n.value) < 32
        and "%" not in n.raw
        and _multiplier(n.raw) == 1.0
        and float(n.value).is_integer()
    )


def figures_for_sourcing(text: str) -> list[NumberInText]:
    """Las cifras que tienen que venir de alguna fuente (sin contadores chicos)."""
    clean = main_text(answer_body(text)).replace("−", "-")
    return [n for n in numbers_in_answer(clean) if not _small_counter(n)]


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


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float | Decimal):
        return float(value)
    return None


# Variaciones que el modelo puede calcular de una serie que vino sin columna
# de variación: contra la fila anterior y contra 12 filas antes (la
# interanual de una serie mensual), sobre las últimas 13 filas. Más rezagos o
# más filas y casi cualquier porcentaje "saldría" de alguna fuente.
_DERIVED_TAIL = 13
_DERIVED_LAGS = (1, 12)


def derived_variations(records: list[Any]) -> list[float]:
    """Variaciones % calculables desde la cola de una serie, columna por columna.

    Además de los rezagos de ``_DERIVED_LAGS``, la acumulada en el año
    (contra el diciembre anterior) cuando las filas traen ``fecha``. Con
    esto ``fuente_sin_cifra`` no marca como "citada sin cifra" la serie de
    la que el modelo calculó la interanual.
    """
    rows = [r for r in (records or []) if isinstance(r, dict)]
    window = rows[-(_DERIVED_TAIL + max(_DERIVED_LAGS)) :]
    if len(window) < 2:
        return []
    columns = [k for k in window[-1] if k != "fecha" and _number(window[-1][k]) is not None]
    out: set[float] = set()

    def add(a: Any, b: Any) -> None:
        x, y = _number(a), _number(b)
        if x is not None and y:
            out.add(round((x / y - 1.0) * 100.0, 6))

    first = max(0, len(window) - _DERIVED_TAIL)
    for col in columns:
        for i in range(first, len(window)):
            for lag in _DERIVED_LAGS:
                if i - lag >= 0:
                    add(window[i].get(col), window[i - lag].get(col))
            fecha = str(window[i].get("fecha") or "")
            dic = f"{int(fecha[:4]) - 1}-12" if fecha[:4].isdigit() else None
            base = next(
                (r for r in window[:i] if dic and str(r.get("fecha") or "").startswith(dic)), None
            )
            if base is not None:
                add(window[i].get(col), base.get(col))
    return sorted(out)


def _figure_tolerance(n: NumberInText) -> float:
    """Media unidad del último dígito escrito.

    Sin tolerancia relativa a propósito: contra una serie de 1.000 filas, un
    0,05 % encontraba siempre algún vecino (el 47.467 de la reproducción
    "salía" de un 47.460 de 2011 en la 174.1). Un entero redondeado a miles
    ("46.000 millones", "46 mil millones") sí se lee con esa precisión, igual
    que en el chequeo de valor (``implied_rounding``).
    """
    if n.rounding:
        return n.rounding
    return _rounding_unit(n)[0]


def _figure_matches(n: NumberInText, values: list[float]) -> bool:
    tol = _figure_tolerance(n)
    target = abs(n.value)
    for v in values:
        av = abs(v)
        for s in _SCALES:
            if abs(target - av * s) <= tol + 1e-9:
                return True
    return False


_Evidence = tuple[list[float], list[float]]  # (números, variaciones derivadas)


def sources_without_figures(
    answer: str,
    sources: list[dict[str, Any]] | list[str],
    evidence_items: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """``(citadas sin ninguna cifra, citadas sin evidencia para juzgar)``.

    Una fuente aportó si alguna cifra de la respuesta coincide con un número
    de su evidencia (con el redondeo de la respuesta y las escalas de
    ``_SCALES``), o si un porcentaje de la respuesta es una variación
    calculable desde la cola de la serie (``derivadas``, ver
    ``derived_variations``). Las fuentes sin números en la evidencia (texto)
    no se juzgan. Reproducción del 04-oct: el agente citó tres series de
    reservas y las tres cifras salían de una sola.
    """
    figures = figures_for_sourcing(answer)
    if not figures:
        return [], []
    percents = [f for f in figures if "%" in f.raw]
    by_pair: dict[tuple[str, str], _Evidence] = {}
    by_title: dict[str, _Evidence] = {}
    by_url: dict[str, _Evidence] = {}
    for item in evidence_items or []:
        title = str(item.get("title") or "").strip().lower()
        url = str(item.get("url") or "").strip()
        item_numbers = [float(n) for n in item.get("numbers") or []]
        item_derived = [float(n) for n in item.get("derivadas") or []]
        for nums, derived in (
            by_pair.setdefault((title, url), ([], [])),
            by_title.setdefault(title, ([], [])),
            by_url.setdefault(url, ([], [])),
        ):
            nums.extend(item_numbers)
            derived.extend(item_derived)
    sin_cifra: list[str] = []
    sin_evidencia: list[str] = []
    for s in sources or []:
        if isinstance(s, dict):
            name, url = str(s.get("name") or ""), str(s.get("url") or "").strip()
        else:
            name, url = str(s), ""
        key = name.strip().lower()
        ev: _Evidence | None = by_pair.get((key, url))
        if ev is None:
            ev = by_title.get(key) if key in by_title else by_url.get(url) if url else None
        if ev is None:
            sin_evidencia.append(name)
            continue
        nums, derived = ev
        if not nums:
            continue
        if any(_figure_matches(f, nums) for f in figures):
            continue
        if any(
            abs(abs(f.value) - abs(d)) <= _figure_tolerance(f) + 1e-9
            for f in percents
            for d in derived
        ):
            continue
        sin_cifra.append(name)
    return sin_cifra, sin_evidencia


# ── fuentes no oficiales ───────────────────────────────────

_NO_OFICIAL_SOURCE_RE = re.compile(r"dolar\s?api|argentina\s?datos", re.IGNORECASE)
# El rótulo que pidió Dante ("pizarra del Banco Nación vía DolarApi", no
# oficial) o cualquier forma de decir que no es la referencia oficial.
# "Banco Nación" solo no alcanza: "el dólar oficial en el Banco Nación" no
# rotula nada.
_NO_OFICIAL_LABEL_RE = re.compile(
    r"\bno\s+(?:es\s+|son\s+|constituye\s+|representa\s+)?(?:una?\s+|la\s+)?"
    r"(?:referencia\s+|cotizaci[oó]n\s+|fuente\s+|dato\s+)?oficial|pizarra|complement|"
    r"agregador|extraoficial",
    re.IGNORECASE,
)
# Quién publica cada cotización, para saber de cuál se dice "oficial".
_MENCION_NO_OFICIAL_RE = re.compile(
    r"dolar\s?api|argentina\s?datos|banco\s+(?:de\s+la\s+)?naci[oó]n|\bBNA\b|pizarra",
    re.IGNORECASE,
)
_MENCION_OFICIAL_RE = re.compile(
    r"\bBCRA\b|banco\s+central|\bA\s?3500\b|comunicaci[oó]n\s+a\b|minorista\s+promedio|"
    r"mayorista\s+de\s+referencia",
    re.IGNORECASE,
)
_OFICIAL_RE = re.compile(r"\boficial(?:es)?\b", re.IGNORECASE)
# "no es la referencia oficial", "ni oficial": a menos de 30 caracteres.
_NEGACION_RE = re.compile(r"\b(?:no|ni)\b[^.;\n]{0,30}$", re.IGNORECASE)


def _oficial_de_dolarapi(text: str, kinds: list[str]) -> str | None:
    """La primera vez que se llama "oficial" a la cotización de DolarApi.

    Un "oficial" sin negar cuenta como dicho de la cotización de DolarApi si,
    en su oración, la fuente más cercana es DolarApi / Banco Nación / la
    pizarra y no el BCRA: "El dólar oficial en el Banco Nación está a $1.540"
    falla; "El dólar oficial del BCRA (A3500) es $1.480; la pizarra del Banco
    Nación (no oficial) marca $1.540" aprueba. Si la oración no nombra
    ninguna fuente y la respuesta no usa el BCRA en ningún lado, la única
    cotización que hay es la de DolarApi.
    """
    sin_bcra = "bcra" not in kinds and not _MENCION_OFICIAL_RE.search(text)
    for m in _OFICIAL_RE.finditer(text):
        if _NEGACION_RE.search(text[max(0, m.start() - 40) : m.start()]):
            continue
        s0, s1 = _sentence_span(text, m.start())
        sentence = text[s0:s1]
        offset = m.start() - s0
        menciones = [
            (abs(x.start() - offset), "no_oficial")
            for x in _MENCION_NO_OFICIAL_RE.finditer(sentence)
        ] + [(abs(x.start() - offset), "oficial") for x in _MENCION_OFICIAL_RE.finditer(sentence)]
        if (menciones and min(menciones)[1] == "no_oficial") or (not menciones and sin_bcra):
            return sentence.strip()[:120]
    return None


def check_rotulo_no_oficial(answer: str, kinds: list[str]) -> Check | None:
    """Si la respuesta usa DolarApi o ArgentinaDatos, tiene que rotularlos.

    El agente del 04-oct contestó "el dólar oficial está hoy a $1.490 /
    $1.540. Fuente: DolarApi": es la pizarra del Banco Nación, no una
    referencia oficial (BCRA, Comunicación A 3500 o minorista). Falla si no
    dice que no es la referencia oficial, o si igual la llama "oficial".
    """
    text = answer or ""
    uses = "argentina_datos" in kinds or _NO_OFICIAL_SOURCE_RE.search(text)
    if not uses:
        return None
    if _NO_OFICIAL_LABEL_RE.search(text) is None:
        return Check(
            "rotulo_no_oficial",
            False,
            "usa DolarApi/ArgentinaDatos sin decir que no es la referencia oficial",
        )
    dicho = _oficial_de_dolarapi(text, kinds)
    if dicho:
        return Check("rotulo_no_oficial", False, f"llama oficial a la de DolarApi: {dicho!r}")
    return Check("rotulo_no_oficial", True)


# ── patrones ───────────────────────────────────────────────

_LINE_START_GUARD = "(?m)^[^¿\\n]*?"
_LOOKBEHIND_RE = re.compile(r"\(\?<![^)]*\)")


def pattern_label(pattern: str) -> str:
    """El patrón sin las guardas (inicio de línea, negaciones), para el reporte.

    Los cebos de neutralidad comparten 250 caracteres de guardas; sin
    sacarlas, todos los chequeos se ven iguales.
    """
    label = pattern.removeprefix(_LINE_START_GUARD)
    return _LOOKBEHIND_RE.sub("", label)


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

    found_labels: set[str] = set()
    for spec in expected_values:
        hit = find_value(spec, body)
        if hit is not None:
            found_labels.add(_value_label(spec))
        q.checks.append(
            Check(
                f"valor:{_value_label(spec)}",
                hit is not None,
                f"encontrado {hit.raw!r}" if hit else f"no aparece {_fmt_value(spec['value'])}",
            )
        )

    if resolved.get("expected_period") and not q.deflected:
        q.checks.append(
            check_fecha_del_dato(
                main_text(body),
                resolved["expected_period"],
                list(resolved.get("expected_values") or []),
            )
        )

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
        unless = _excused_by(spec)
        if hit is not None and unless in found_labels:
            # "USD 87.111 millones en 2025; en promedio, 7.259 por mes": la
            # cuenta mal hecha aparece, pero al lado de la correcta.
            q.checks.append(
                Check(
                    f"valor_prohibido:{_value_label(spec)}",
                    True,
                    f"aparece {hit.raw!r}, pero también la cifra correcta",
                )
            )
            continue
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
            Check(
                f"patron_prohibido:{pattern_label(pattern)}",
                m is None,
                # Los patrones que arrancan en el principio de la línea (para no
                # mirar preguntas) agarran la línea entera: alcanza con el final.
                f"aparece {m.group(0)[-90:].strip()!r}" if m else "",
            )
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
