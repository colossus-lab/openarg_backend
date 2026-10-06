"""Las cifras de la respuesta, una por una, contra lo que leyeron las herramientas.

El agente no llena citas estructuradas, y el chequeo del pipeline viejo
(``citation_guard``) sólo verifica las cifras que el modelo elige citar. Con
``citations=[]`` el runner terminaba pegando "verificación parcial" en toda
respuesta con números, incluida la falsa ("abril de 2023: USD 35.001 M"):
un aviso que no distingue una respuesta correcta de una falsa.

Este módulo toma cada cifra del TEXTO y le busca respaldo en los
``DataResult`` del turno:

- **Normaliza el formato.** "1.234,5" y "1,234.5"; "33,5 %" contra la
  fracción 0,3354 que devuelve la API para la interanual; "49.700 M" contra
  49.700,26 "Millones de dólares"; media unidad del último dígito escrito
  ("49.700" admite 49.700,26), y más si la cifra dice "aproximadamente".
- **Respeta el signo.** "-0,22 %" o "cayó 1,7 %" piden un valor negativo.
- **Sólo cuenta lo que el modelo vio.** La evidencia trae la serie entera;
  el modelo, las últimas filas. Una cifra corrida encontraría por azar un
  valor viejo de la serie.
- **Acepta lo derivado que se reproduce.** Una diferencia, una variación % o
  una participación entre cifras cercanas de la misma respuesta, el saldo de
  dos columnas de una misma fila, la variación de un período en una serie o
  la suma anual de una serie mensual. Nunca una suma o una resta de tasas:
  así salió el "−0,22 %" interanual (1,66 − 1,88) del pipeline viejo.
- **Acepta lo leído fuera de la evidencia** ("contexto"): un número que el
  modelo leyó tal cual en describir_tabla o en un fragmento de sesión.

Calibración en staging (04-oct, Sonnet, 46 respuestas con cifras): de 5
cifras marcadas, 4 de verdad no tenían respaldo; con errores inyectados
(±3 %, signo invertido, "-0,22 %", resta y suma de tasas) detecta el 93 % de
1.073. Pocas marcas para afirmar el 90 % de precisión: por eso el modo por
defecto es ``shadow``.

Con eso se deciden tres cosas: qué fuentes se citan (las que aportaron una
cifra o se nombran en el texto; las demás sólo se consultaron), qué citas
estructuradas lleva la respuesta y qué cifras no tienen respaldo.

Qué se hace con eso lo decide ``ANSWERS_VERIFY_MODE``:

- ``off``: nada; ni se verifica. Es el interruptor: la respuesta sale como
  antes del verificador (toda la evidencia citada, sin citas).
- ``shadow`` (por defecto): se registra en el log (``answers.verify``) y la
  respuesta sale igual, también sus fuentes y sus citas: toda la evidencia
  citada y sin citas. Es para medir la precisión antes de actuar. Lo único
  que usa es lo respaldado: si todas las cifras lo están, el aviso de atraso
  deja afuera lo que no aportó cifras y se llama igual que algo que sí, y
  pone primero lo que las aportó (``dated_evidence``); si no, mira todo lo
  leído.
- ``correct``: se citan sólo las fuentes usadas, con sus citas, y el agente
  hace UNA vuelta correctiva con la lista de cifras (``agent_engine``); si
  después sigue habiendo cifras sin respaldo, la respuesta lleva un aviso
  arriba que las nombra. Nunca se borra una cifra del texto: con la
  precisión medida, borrar se llevaría cifras correctas y rompería la
  oración.

Esto no es un control de verdad: una cifra vieja o de una serie truncada
está en la evidencia y pasa. Para eso están el aviso de atraso y la
paginación de los conectores.
"""

from __future__ import annotations

import logging
import math
import os
import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)

# ── el modo ────────────────────────────────────────────────

VERIFY_ENV = "ANSWERS_VERIFY_MODE"
VERIFY_OFF = "off"
VERIFY_SHADOW = "shadow"
VERIFY_CORRECT = "correct"
DEFAULT_VERIFY_MODE = VERIFY_SHADOW
_MODES = frozenset({VERIFY_OFF, VERIFY_SHADOW, VERIFY_CORRECT})


def verify_mode() -> str:
    """El modo que pide ``ANSWERS_VERIFY_MODE``, o ``shadow``.

    Un valor desconocido no apaga ni prende nada por accidente: queda en
    ``shadow`` y lo deja en el log como ERROR.
    """
    raw = (os.getenv(VERIFY_ENV) or DEFAULT_VERIFY_MODE).strip().lower()
    if raw not in _MODES:
        logger.error("%s=%r no existe; uso %r", VERIFY_ENV, raw, DEFAULT_VERIFY_MODE)
        return DEFAULT_VERIFY_MODE
    return raw


# ── las cifras del texto ───────────────────────────────────

_MONTHS = (
    "enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|"
    "noviembre|diciembre"
)
_MONTHS_SHORT = "ene|feb|mar|abr|may|jun|jul|ago|sep|sept|set|oct|nov|dic"

# Lo que se blanquea antes de buscar cifras: no son datos de la respuesta.
# Se reemplaza por espacios del mismo largo para no correr las posiciones.
_BLANK_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Enlaces: la URL de una serie tiene ids con números.
    re.compile(r"https?://\S+"),
    re.compile(r"\]\([^)]*\)"),
    # Identificadores: ids de series (148.3_INIVELNAL_DICI_M_26), nombres de
    # tablas, "A3500", "COVID-19", "T2".
    re.compile(r"\b[^\W_]+(?:\.[^\W_]+)*_\w[\w.\-]*"),
    re.compile(r"\b[A-Za-zÁÉÍÓÚáéíóúñÑ]{1,8}-?\d+[\w]*\b"),
    # Fechas: 2026-08-01, 2026/08, 02/10/2026, "2 de octubre de 2026",
    # "dic-2016", "oct. 2026".
    re.compile(r"\b(?:19|20)\d{2}[-/](?:0?[1-9]|1[0-2])(?:[-/](?:0?[1-9]|[12]\d|3[01]))?\b"),
    re.compile(r"\b\d{1,2}[-/]\d{1,2}[-/](?:\d{4}|\d{2})\b"),
    re.compile(rf"(?i)\b\d{{1,2}}\s+de\s+(?:{_MONTHS})(?:\s+(?:de\s+)?(?:19|20)\d{{2}})?\b"),
    re.compile(rf"(?i)\b(?:{_MONTHS_SHORT})[a-z]*\.?[-/ ]?(?:de\s+)?(?:19|20)?\d{{2}}\b"),
    # Horas.
    re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b"),
    # "base 2016=100", "(dic-2016=100)".
    re.compile(r"=\s*100\b"),
    # Ordinales y períodos: "1er semestre", "2.º trimestre", "4T 2024", "II trimestre".
    re.compile(r"(?i)\b\d{1,2}\s?(?:er|ro|do|to|vo|no|mo|°|º|ª)(?![\w])"),
    re.compile(r"(?i)\b\d{1,2}\.\s?(?:º|°|ª|er)"),
    re.compile(r"(?i)\b\d\s?[TS]\b"),
    re.compile(r"(?i)\b[IV]{1,3}\s+(?:trimestre|semestre)"),
    # Números de normas, artículos, expedientes, documentos.
    re.compile(
        r"(?i)\b(?:ley(?:es)?|decreto|dnu|resoluci[oó]n|res\.|disposici[oó]n|art[ií]culos?|"
        r"art\.|expediente|expte\.?|n[°º]|nro\.?|n[uú]mero|c[oó]digo|cuit|cuil|dni|inciso|"
        r"cap[ií]tulo|cuadro|serie|id)\s*(?:n[°º]\.?|nro\.?)?\s*[\d.,/\-]+"
    ),
    # Numeración de listas.
    re.compile(r"(?m)^\s*\d{1,2}[.)]\s"),
)

# Un año suelto no es una cifra ("en 2024"), salvo que lleve unidad.
_YEAR_ALONE_RE = re.compile(
    r"(?<![\d.,])\b(?:19|20)\d{2}\b(?![\d.,]*\d)"
    r"(?!\s*(?:%|por\s?ciento|millones|mill[oó]n|miles|mil\b|billones|bill[oó]n|M\b))",
    re.IGNORECASE,
)

_NUMBER_RE = re.compile(
    r"(?<![\w.,])(?P<sign>[-−–+]?)"
    r"(?P<num>\d{1,3}(?:\.\d{3})+(?:,\d+)?"  # 1.234.567,89
    r"|\d{1,3}(?:,\d{3})+(?:\.\d+)?"  # 1,234,567.89
    r"|\d+(?:[.,]\d+)?)"  # 1234 / 33,5 / 0.3354
    r"(?![\d])"
)

_UNIT_RE = re.compile(
    r"\s*(?:"
    r"(?P<pct>%|por\s?ciento)"
    r"|(?P<pts>puntos?\s+porcentuales|p\.\s?p\.|pp\b|puntos?\b)"
    r"|(?P<e12>billones|bill[oó]n)"
    r"|(?P<e9>mil\s+millones|miles\s+de\s+millones)"
    r"|(?P<e6>millones|mill[oó]n|mill\.|(?-i:MM?)\b)"
    r"|(?P<e3>miles|mil\b)"
    r"|(?P<veces>veces)"
    r")",
    re.IGNORECASE,
)
_TIME_UNIT_RE = re.compile(
    r"\s*(?:d[ií]as?|mes(?:es)?|a[nñ]os?|semanas?|trimestres?|semestres?|horas?|minutos?|"
    r"segundos?|d[eé]cadas?|siglos?|veces\s+por)\b",
    re.IGNORECASE,
)
_CURRENCY_BEFORE_RE = re.compile(r"(?:US\$|U\$S|u\$s|USD|ARS|\$|€)\s*$", re.IGNORECASE)
_SIGN_BEFORE_CURRENCY_RE = re.compile(
    r"(?:^|[\s(:|])[-−–]\s?(?:US\$|U\$S|u\$s|USD|ARS|\$|€)\s*$", re.IGNORECASE
)
_APPROX_BEFORE_RE = re.compile(
    r"(?:aproximadamente|aprox\.?|unos|unas|cerca\s+de|alrededor\s+de|casi|≈|~|en\s+torno\s+a|"
    r"del\s+orden\s+de|rond[a-z]*)\s*(?:de\s+)?(?:los|las|el|la)?\s*(?:US\$|U\$S|USD|\$)?\s*$",
    re.IGNORECASE,
)
_BOUND_GT_RE = re.compile(
    r"(?:m[aá]s\s+de|superior(?:es)?\s+al?|por\s+encima\s+del?|super[oó]|superar(?:on)?|"
    r"supera(?:n|ron)?|mayor(?:es)?\s+(?:al?|que))\s*(?:los|las|el|la)?\s*"
    r"(?:US\$|U\$S|USD|\$)?\s*$",
    re.IGNORECASE,
)
_BOUND_LT_RE = re.compile(
    r"(?:menos\s+de|inferior(?:es)?\s+al?|por\s+debajo\s+del?|menor(?:es)?\s+(?:al?|que))"
    r"\s*(?:los|las|el|la)?\s*(?:US\$|U\$S|USD|\$)?\s*$",
    re.IGNORECASE,
)
# "entre $40,5 y $43,3 billones", "de 6,4 a 7,9 %": la unidad del segundo
# número vale para el primero.
_RANGE_GAP_RE = re.compile(r"^\s*(?:y|a|al|o|–|-|/)\s*(?:US\$|U\$S|USD|\$)?\s*$", re.IGNORECASE)
# Verbos y sustantivos que le dan signo a la cifra que sigue: "cayó 1,7 %",
# "una suba de 3 %". No cuentan si entre la palabra y la cifra hay otra cosa
# ("bajó a 1,66 %" es un nivel, no una variación negativa).
_NEGATIVE_WORDS = (
    r"cay[oó]|caen|cayeron|ca[ií]da|baj[oó]|bajaron|descendi[oó]|descenso|disminuy[oó]|"
    r"disminuci[oó]n|retrocedi[oó]|retroceso|se\s+contrajo|contracci[oó]n|perdi[oó]|"
    r"p[eé]rdida|se\s+redujo|reducci[oó]n|negativa\s+de|negativo\s+de"
)
_POSITIVE_WORDS = (
    r"subi[oó]|subieron|suba|aument[oó]|aumentaron|aumento|creci[oó]|crecieron|crecimiento|"
    r"increment[oó]|incremento|alza|avanz[oó]|avance|gan[oó]|se\s+expandi[oó]|expansi[oó]n|"
    r"repunt[oó]|repunte|positiva\s+de|positivo\s+de"
)
_POLARITY_RE = re.compile(
    rf"(?P<neg>\b(?:{_NEGATIVE_WORDS}))|(?P<pos>\b(?:{_POSITIVE_WORDS}))", re.IGNORECASE
)
# "de"/"del" no van: "bajó del 52,9 % al 38,1 %" nombra el nivel de partida,
# no el tamaño de la caída. "Una caída de 1,2 %" queda cubierta por la señal
# negativa del contexto (`_NEGATIVE_CUE_RE`).
_POLARITY_GAP_RE = re.compile(
    r"^(?:\s|\*|\(|,|un|una|el|la|en|total|interanual|mensual|anual|acumulad[ao]|"
    r"aproximadamente|casi|cerca|unos|unas|us\$|u\$s|usd|\$)*$",
    re.IGNORECASE,
)
_NEGATIVE_CUE_RE = re.compile(
    r"negativ|d[eé]ficit|ca[ií]d|cay|baj[oóa]|descen|disminu|retroce|contrac|p[eé]rdid|perdi|"
    r"reduc|por\s+debajo|−",
    re.IGNORECASE,
)

_MULTIPLIERS = {"e12": 1e12, "e9": 1e9, "e6": 1e6, "e3": 1e3}


@dataclass(frozen=True)
class Figure:
    """Una cifra de la respuesta, como está escrita."""

    raw: str
    start: int
    end: int
    # Lecturas posibles del número sin signo ni multiplicador, con su
    # tolerancia de redondeo: "46.092" es 46092 (o, en formato inglés, 46,092).
    readings: tuple[tuple[float, float], ...]
    multiplier: float = 1.0
    percent: bool = False
    points: bool = False
    times: bool = False
    currency: bool = False
    sign: int = 0  # -1 / +1 si el signo está escrito
    polarity: int = 0  # -1 "cayó", +1 "subió"
    negative_cue: bool = False
    approx: bool = False
    bound: str | None = None  # "gt" ("más de"), "lt" ("menos de")

    @property
    def value(self) -> float:
        bare = self.readings[0][0]
        return (-bare if self.sign < 0 else bare) * self.multiplier

    @property
    def bare_integer(self) -> bool:
        """Un entero sin unidad: "257 bancas"."""
        bare, tol = self.readings[0]
        return (
            tol >= 0.5
            and not (self.percent or self.points or self.currency or self.times)
            and self.multiplier == 1.0
            and float(bare).is_integer()
        )


def _blank(match: re.Match[str]) -> str:
    return " " * len(match.group(0))


def _readings(num: str) -> list[tuple[float, float]]:
    """Las lecturas posibles de un número escrito, la más probable primero.

    Formato argentino por defecto. Un solo separador con tres dígitos detrás
    es ambiguo: "46.092" son 46 mil (AR) o 46,092 (EN); "1,234" es 1,234 (AR)
    o 1.234 (EN). Se prueban las dos lecturas.

    Un token mal formado ("1.1,1.2", de la descripción de un dataset) no tiene
    lecturas: antes tiraba ValueError y se llevaba la respuesta entera.
    """
    try:
        return _parse_readings(num)
    except ValueError:
        return []


def _parse_readings(num: str) -> list[tuple[float, float]]:
    def _make(int_part: str, dec_part: str) -> tuple[float, float]:
        value = float(f"{int_part}.{dec_part}" if dec_part else int_part)
        tol = 0.5 * 10 ** -len(dec_part) if dec_part else 0.5
        return value, tol

    if "." in num and "," in num:
        if num.rfind(",") > num.rfind("."):  # 1.234,5
            int_part, _, dec = num.partition(",")
            return [_make(int_part.replace(".", ""), dec)]
        int_part, _, dec = num.partition(".")  # 1,234.5
        return [_make(int_part.replace(",", ""), dec)]
    for sep, thousands_first in ((".", True), (",", False)):
        if sep not in num:
            continue
        parts = num.split(sep)
        if len(parts) > 2:  # 1.234.567
            return [_make("".join(parts), "")]
        int_part, dec = parts
        if len(dec) == 3 and int_part != "0":
            thousands = _make(int_part + dec, "")
            decimal = _make(int_part, dec)
            return [thousands, decimal] if thousands_first else [decimal, thousands]
        return [_make(int_part, dec)]
    return [_make(num, "")]


def _trailing_zero_tolerance(value: float) -> float:
    """Media unidad del último dígito no nulo: "50.000" admite 49.700."""
    if value == 0 or not float(value).is_integer():
        return 0.0
    n = int(abs(value))
    place = 1
    while n and n % 10 == 0:
        n //= 10
        place *= 10
    return 0.5 * place


def extract_figures(text: str) -> list[Figure]:
    """Las cifras de la respuesta: sin fechas, años, ids, enumeraciones ni normas.

    Tampoco cuentan los enteros chicos sin unidad ("los 5 diputados", "12
    meses"): casi nunca son el dato y coinciden por azar con cualquier cosa.
    """
    source = text or ""
    blanked = source
    for pattern in _BLANK_PATTERNS:
        blanked = pattern.sub(_blank, blanked)
    blanked = _YEAR_ALONE_RE.sub(_blank, blanked)

    matches = list(_NUMBER_RE.finditer(blanked))
    kinds: list[str | None] = []
    ends: list[int] = []
    for m in matches:
        unit = _UNIT_RE.match(blanked[m.end() : m.end() + 40])
        kind = next((k for k, v in (unit.groupdict() if unit else {}).items() if v), None)
        kinds.append(kind)
        ends.append(m.end() + (unit.end() if unit and kind else 0))
    # Un rango comparte la unidad: "entre $40,5 y $43,3 billones".
    inherited = list(kinds)
    for i in range(len(matches) - 2, -1, -1):
        nxt = inherited[i + 1]
        if (
            kinds[i] is None
            and nxt not in (None, "veces")
            and _RANGE_GAP_RE.match(blanked[ends[i] : matches[i + 1].start()])
        ):
            inherited[i] = nxt

    figures: list[Figure] = []
    for i, m in enumerate(matches):
        num = m.group("num")
        readings = _readings(num)
        if not readings:
            continue
        after = blanked[m.end() : m.end() + 40]
        # Sin las marcas de Markdown: "más de **$31.218 millones**".
        before = blanked[max(0, m.start() - 60) : m.start()].replace("*", " ")
        kind = inherited[i]
        end = ends[i]
        multiplier = _MULTIPLIERS.get(kind or "", 1.0)
        percent = kind == "pct"
        points = kind == "pts"
        times = kind == "veces"
        currency = bool(_CURRENCY_BEFORE_RE.search(before[-10:]))
        if not kind and _TIME_UNIT_RE.match(after):
            continue
        bare, tol = readings[0]
        if (
            not kind
            and not currency
            and tol >= 0.5
            and float(bare).is_integer()
            and abs(bare) < 100
        ):
            continue
        sign_char = m.group("sign")
        sign = -1 if sign_char in ("-", "−", "–") else (1 if sign_char == "+" else 0)
        # "1.490–1.540": un guion entre dos cifras es un rango, no un signo.
        if sign and m.start() > 0 and blanked[: m.start()].rstrip()[-1:].isdigit():
            sign = 0
        # El signo antes de la moneda: "-$73.441.683", "–US$ 5 millones".
        if not sign and _SIGN_BEFORE_CURRENCY_RE.search(before[-12:]):
            sign = -1
        polarity = 0
        if not sign:
            window = before[-45:]
            last = None
            for pm in _POLARITY_RE.finditer(window):
                last = pm
            if last is not None and _POLARITY_GAP_RE.match(window[last.end() :]):
                polarity = -1 if last.group("neg") else 1
        context = source[max(0, m.start() - 70) : min(len(source), end + 20)]
        figures.append(
            Figure(
                raw=source[m.start() : end].strip(),
                start=m.start(),
                end=end,
                readings=tuple(readings),
                multiplier=multiplier,
                percent=percent,
                points=points,
                times=times,
                currency=currency,
                sign=sign,
                polarity=polarity,
                negative_cue=bool(_NEGATIVE_CUE_RE.search(context)),
                approx=bool(_APPROX_BEFORE_RE.search(before[-30:])),
                bound=(
                    "gt"
                    if _BOUND_GT_RE.search(before[-30:])
                    else ("lt" if _BOUND_LT_RE.search(before[-30:]) else None)
                ),
            )
        )
    return figures


# ── la evidencia ───────────────────────────────────────────

# Columnas que no son datos: fechas, códigos, identificadores.
_SKIP_KEY_RE = re.compile(
    r"^(?:_.*|fecha.*|indice_tiempo|periodo|period|date|time|anio|año|year|mes|month|dia|"
    r"trimestre|semestre|ejercicio.*|.*_id|id_.*|id|codigo.*|cod_.*|.*_cod|cuit|cuil|dni|"
    r"latitud|longitud|lat|lon|lng)$",
    re.IGNORECASE,
)
_RATE_KEY_RE = re.compile(
    r"tasa|porcentaje|pct|%|variaci[oó]n|var_|inflaci[oó]n|interanual|percent", re.IGNORECASE
)


@dataclass(frozen=True)
class EvidenceValue:
    """Un número que devolvió una herramienta."""

    result: int  # índice en la lista de evidencia
    # fila; -1 = cantidad de filas, -2 = agregado anual, -3 = descripción
    record: int
    key: str
    value: float
    rate: bool  # es una tasa o porcentaje (o su fracción)
    scale: float = 1.0  # "Millones de dólares" → 1e6
    # El valor tal como se escribió en la respuesta (redondeado), para las
    # cuentas: 6.140 − 5.864 = 276, aunque 6.139,70 − 5.864,29 dé 275,42.
    written: bool = False
    # Posición de la columna en la fila: el orden natural de una resta entre
    # columnas (exportaciones − importaciones, bienes − deudas).
    col: int = 0
    # En qué escala vienen las tasas del resultado (``_percent_scale``):
    # "porcentaje" (1,66 es 1,66 %), "fraccion" (0,3354 es 33,54 %) o "" si no
    # se sabe.
    pct: str = ""
    # De un valor ``written``, el que trajo la fuente: es el que va en la cita.
    source_value: float | None = None

    @property
    def path(self) -> str:
        if self.record >= 0:
            return f"records[{self.record}].{self.key}"
        return self.key


def _to_float(raw: Any) -> list[float]:
    if isinstance(raw, bool) or raw is None:
        return []
    if isinstance(raw, int | float | Decimal):
        value = float(raw)
        return [value] if math.isfinite(value) else []
    if isinstance(raw, str):
        text = raw.strip().replace(" ", "")
        m = re.fullmatch(r"([-−+]?)\s*(?:US\$|USD|\$)?\s*([\d.,]+)\s*%?", text)
        if not m:
            return []
        try:
            sign = -1.0 if m.group(1) in ("-", "−") else 1.0
            return [sign * v for v, _ in _readings(m.group(2))]
        except ValueError:
            return []
    return []


def _unit_scale(units: str) -> float:
    text = (units or "").lower()
    if "miles de millones" in text:
        return 1e9
    if "millones" in text or "millón" in text or "millon" in text:
        return 1e6
    if "miles" in text or re.search(r"\bmil\b", text):
        return 1e3
    return 1.0


def _is_rate_result(meta: dict[str, Any]) -> bool:
    units = str(meta.get("units") or meta.get("unidades") or "").lower()
    representation = str(meta.get("representation") or "")
    return (
        meta.get("unidad") == "porcentaje"
        or meta.get("unit") == "percent"
        or meta.get("value_scale") == "percentage_points"
        or representation.startswith("percent_change")
        or "%" in units
        or "porcentaje" in units
    )


PCT_POINTS = "porcentaje"
PCT_FRACTION = "fraccion"


def _percent_scale(meta: dict[str, Any]) -> str:
    """En qué escala vienen las tasas de un resultado, si se sabe.

    - "porcentaje": el contrato de metadatos (``unidad``) o la marca del
      adaptador de series (``value_scale``/``unit``) dicen que los valores ya
      están multiplicados por 100. "166 %" contra 1,66 es un error.
    - "fraccion": una representación ``percent_change*`` sin esa marca llega
      tal como la da la API (0,3354 es 33,54 %). "0,34 %" contra 0,3354 es un
      error.
    - "": no se sabe (una columna «tasa» de una tabla cualquiera): se aceptan
      las dos lecturas.
    """
    if (
        meta.get("unidad") == "porcentaje"
        or meta.get("value_scale") == "percentage_points"
        or meta.get("unit") == "percent"
    ):
        return PCT_POINTS
    if str(meta.get("representation") or "").startswith("percent_change"):
        return PCT_FRACTION
    return ""


def _parse_day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    m = re.match(r"^((?:19|20)\d{2})-(\d{2})(?:-(\d{2}))?", text)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3) or 1))
    except ValueError:
        return None


def _year_aggregates(i: int, records: list[dict[str, Any]], scale: float) -> list[EvidenceValue]:
    """Sumas y promedios por año calendario de una serie mensual o trimestral.

    Exportaciones 2025 = 87.111 sale de sumar los doce meses que trajo la
    herramienta: es una cifra reproducible, no inventada. Las tasas no se
    suman (la acumulada no es la suma de las mensuales): para columnas de
    tasas no hay agregado.
    """
    by_year: dict[tuple[str, int], list[float]] = {}
    days = [_parse_day(r.get("fecha")) for r in records]
    if len(records) < 4 or any(d is None for d in days):
        return []
    for record, day in zip(records, days, strict=True):
        assert day is not None
        for key, raw in record.items():
            if _SKIP_KEY_RE.match(str(key)) or _RATE_KEY_RE.search(str(key)):
                continue
            values = _to_float(raw)
            if values:
                by_year.setdefault((str(key), day.year), []).append(values[0])
    out: list[EvidenceValue] = []
    for (key, year), values in by_year.items():
        if len(values) not in (4, 12):
            continue
        total = sum(values)
        out.append(EvidenceValue(i, -2, f"{key}:suma_{year}", total, False, scale))
        out.append(
            EvidenceValue(i, -2, f"{key}:promedio_{year}", total / len(values), False, scale)
        )
    return out


_SEEN_TOKEN_RE = re.compile(r"\d+(?:\.\d+)?[eE][-+]?\d+|\d[\d.,]*\d|\d")


def _key(value: float) -> float:
    return round(abs(value), 9)


def seen_numbers(text: str) -> frozenset[float]:
    """Los números que aparecen en lo que leyó el modelo (en valor absoluto).

    La evidencia trae la serie entera (hasta 1.000 filas), pero el modelo ve
    las últimas 24: una cifra mal escrita encontraría por azar algún valor
    viejo de la serie. Con esto sólo cuenta lo que el modelo tuvo a la vista.
    """
    out: set[float] = set()
    for token in _SEEN_TOKEN_RE.findall(text or ""):
        if re.fullmatch(r"\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", token):
            out.add(_key(float(token)))
        try:
            out.update(_key(v) for v, _ in _readings(token))
        except ValueError:
            continue
    return frozenset(out)


_DESCRIPTION_NUMBER_RE = re.compile(r"(?<![\w.,])\d[\d.,]*\d|(?<![\w.,])\d")


def _step_lags(records: list[dict[str, Any]]) -> tuple[int, ...]:
    """Los saltos que se toman como cuentas posibles en una serie: el período
    anterior y, si es mensual o trimestral, el mismo período del año anterior."""
    days = [d for d in (_parse_day(r.get("fecha")) for r in records[-3:]) if d]
    if len(days) >= 2:
        gap = (days[-1] - days[-2]).days
        if 27 <= gap <= 32:
            return (1, 12)
        if 88 <= gap <= 93:
            return (1, 4)
    return (1,)


@dataclass
class EvidenceIndex:
    values: list[EvidenceValue]
    by_record: dict[tuple[int, int], list[EvidenceValue]]
    # Pares (anterior, posterior) de una misma columna de una serie, entre
    # valores que el modelo vio: la variación de un mes, de un año.
    steps: list[tuple[EvidenceValue, EvidenceValue]] = field(default_factory=list)

    @classmethod
    def build(
        cls, results: Sequence[Any], seen: Sequence[frozenset[float] | None] | None = None
    ) -> EvidenceIndex:
        """``seen[i]``: los números que el modelo vio de ``results[i]`` (None = todos)."""
        values: list[EvidenceValue] = []
        steps: list[tuple[EvidenceValue, EvidenceValue]] = []
        lags: dict[int, tuple[int, ...]] = {}
        for i, result in enumerate(results):
            meta = getattr(result, "metadata", None) or {}
            records = list(getattr(result, "records", None) or [])
            scale = _unit_scale(str(meta.get("units") or ""))
            rate_result = _is_rate_result(meta)
            pct = _percent_scale(meta)
            visible = seen[i] if seen is not None and i < len(seen) else None
            lags[i] = _step_lags([r for r in records if isinstance(r, dict)])
            shown: list[dict[str, Any]] = []
            for j, record in enumerate(records):
                if not isinstance(record, dict):
                    continue
                row_seen = False
                # Una fila que marca sus columnas de tasas (la operación de
                # variación: `variacion_pct` junto a `valor_desde`) dice cuáles
                # son; las demás son niveles aunque el resultado diga "%".
                per_key = any(str(k).endswith("_pct") for k in record)
                for col, (key, raw) in enumerate(record.items()):
                    if _SKIP_KEY_RE.match(str(key)):
                        continue
                    rate = bool(_RATE_KEY_RE.search(str(key))) or (rate_result and not per_key)
                    for value in _to_float(raw):
                        if visible is not None and _key(value) not in visible:
                            continue
                        row_seen = True
                        values.append(
                            EvidenceValue(i, j, str(key), value, rate, scale, col=col, pct=pct)
                        )
                if row_seen or visible is None:
                    shown.append(record)
            # Cantidad de filas: "257 diputados" puede salir de contar la lista.
            counts = {len(records)}
            total = meta.get("total_records")
            if isinstance(total, int) and not isinstance(total, bool):
                counts.add(total)
            for n in counts:
                if visible is None or _key(n) in visible:
                    values.append(EvidenceValue(i, -1, "filas", float(n), False, 1.0))
            values.extend(_year_aggregates(i, shown, scale))
            # Las cifras de la descripción que vio el modelo ("localidades de
            # 5.000 y más habitantes"): no son datos de la tabla, pero están a
            # la vista y no son inventadas. Un token mal formado ("1.1,1.2")
            # no tiene lecturas y no cuenta (``_readings``).
            description = str(meta.get("description") or "")[:400]
            for token in _DESCRIPTION_NUMBER_RE.findall(description):
                for value, _ in _readings(token) if re.fullmatch(r"[\d.,]+", token) else []:
                    if visible is None or _key(value) in visible:
                        values.append(EvidenceValue(i, -3, "descripcion", value, False, 1.0))
        by_record: dict[tuple[int, int], list[EvidenceValue]] = {}
        by_column: dict[tuple[int, str], list[EvidenceValue]] = {}
        for v in values:
            if v.record >= 0:
                by_record.setdefault((v.result, v.record), []).append(v)
                by_column.setdefault((v.result, v.key), []).append(v)
        for (result_idx, _), column in by_column.items():
            # Los valores de la columna son los que el modelo vio: el salto se
            # mide en filas de la serie, no en posiciones de la lista.
            by_row = {c.record: c for c in column}
            for lag in lags.get(result_idx, (1,)):
                for c in column:
                    earlier = by_row.get(c.record - lag)
                    if earlier is not None:
                        steps.append((earlier, c))
        return cls(values, by_record, steps)


# ── la comparación ─────────────────────────────────────────


def _close(a: float, b: float, tol: float) -> bool:
    return abs(a - b) <= tol + 1e-9 * max(1.0, abs(a), abs(b))


def _tolerance(fig: Figure, bare: float, tol: float) -> float:
    """ "Unos 50.000" admite hasta media unidad del último dígito no nulo.

    Sin un margen relativo: con "≈ 5.723" un 1 % ya alcanzaba otro valor de la
    misma tabla (medido con errores inyectados en la calibración del 04-oct).
    """
    if fig.approx:
        return max(tol, _trailing_zero_tolerance(bare))
    return tol


def _magnitude_matches(
    fig: Figure, magnitude: float, *, rate_ok: bool, scale: float, pct: str = ""
) -> bool:
    """¿La cifra escrita (sin signo) corresponde a ``magnitude`` (sin signo)?

    ``magnitude`` es el valor de la evidencia en sus propias unidades;
    ``scale`` lo lleva a unidades completas ("Millones de…" → ×1e6).
    ``pct``: la escala de una tasa (``_percent_scale``). Una cifra en % contra
    una tasa ya en puntos no se lee ×100 ("166 %" contra 1,66), y contra una
    fracción sólo se lee ×100 ("0,34 %" contra 0,3354 es un error de unidad).
    """
    for bare, raw_tol in fig.readings:
        tol = _tolerance(fig, bare, raw_tol)
        candidates: list[tuple[float, float, float]] = []
        if fig.percent or fig.points:
            if rate_ok and pct != PCT_POINTS:
                # La API devuelve la interanual como fracción: 0,3354 → 33,5 %.
                candidates.append((bare, tol, magnitude * 100))
            if not (rate_ok and pct == PCT_FRACTION):
                candidates.append((bare, tol, magnitude))
        else:
            full, full_tol = bare * fig.multiplier, tol * fig.multiplier
            candidates.append((full, full_tol, magnitude * scale))
            if fig.multiplier == 1.0 and scale != 1.0:
                # "las reservas son 49.700" con la serie en millones.
                candidates.append((bare, tol, magnitude))
            if fig.multiplier != 1.0 and scale == 1.0:
                # "49.700 millones" contra una columna que ya está en millones.
                candidates.append((bare, tol, magnitude))
        for target, t, got in candidates:
            if fig.bound == "gt":
                if target < got <= target * (1 + _BOUND_MARGIN) + t:
                    return True
            elif fig.bound == "lt":
                if target * (1 - _BOUND_MARGIN) - t <= got < target:
                    return True
            elif _close(target, got, t):
                return True
    return False


# "Más de 46.000 millones" con 46.092: la cota se escribe redondeando hacia
# abajo, cerca del valor. Un margen más ancho deja pasar cifras inventadas.
_BOUND_MARGIN = 0.15


def _sign_ok(fig: Figure, value: float) -> bool:
    """El signo escrito, o el que le da el verbo, tiene que coincidir."""
    if value == 0:
        return True
    if fig.sign:
        return (value < 0) == (fig.sign < 0)
    if fig.polarity:
        return (value < 0) == (fig.polarity < 0)
    # Sin signo ni verbo, un valor negativo necesita alguna señal en el texto
    # ("déficit", "negativa", "cayó"): presentarlo como positivo es un error.
    return value > 0 or fig.negative_cue


def _value_matches(fig: Figure, ev: EvidenceValue) -> bool:
    if ev.record == -1 and not fig.bare_integer:
        return False
    if ev.record == -3 and (fig.sign or fig.polarity):
        return False
    if not _sign_ok(fig, ev.value):
        return False
    return _magnitude_matches(
        fig, abs(ev.value), rate_ok=True, scale=ev.scale, pct=ev.pct if ev.rate else ""
    )


@dataclass(frozen=True)
class FigureCheck:
    figure: Figure
    status: str  # "directa" | "derivada" | "contexto" | "sin_respaldo"
    matches: tuple[EvidenceValue, ...] = ()
    how: str = ""

    @property
    def supported(self) -> bool:
        return self.status != "sin_respaldo"


@dataclass
class Verification:
    checks: list[FigureCheck] = field(default_factory=list)

    @property
    def figures(self) -> list[Figure]:
        return [c.figure for c in self.checks]

    @property
    def unsupported(self) -> list[Figure]:
        return [c.figure for c in self.checks if not c.supported]

    @property
    def supported(self) -> list[FigureCheck]:
        return [c for c in self.checks if c.supported]

    def used_results(self) -> set[int]:
        return {m.result for c in self.checks if c.supported for m in c.matches}

    def summary(self) -> dict[str, Any]:
        return {
            "cifras": len(self.checks),
            "directas": sum(1 for c in self.checks if c.status == "directa"),
            "derivadas": sum(1 for c in self.checks if c.status == "derivada"),
            "contexto": sum(1 for c in self.checks if c.status == "contexto"),
            "sin_respaldo": [c.figure.raw for c in self.checks if not c.supported],
        }


_MAX_OPERANDS = 40
# Las cuentas se buscan entre las cifras escritas cerca: la misma oración, la
# misma fila de una tabla o las inmediatamente anteriores. Con todas las
# cifras de la respuesta, una tabla de 36 números da miles de diferencias y
# sumas posibles, y una cifra inventada encuentra alguna por azar.
_OPERAND_WINDOW_BEFORE = 400
_OPERAND_WINDOW_AFTER = 120


def _operands(
    fig: Figure,
    figures: list[Figure],
    direct: dict[int, tuple[EvidenceValue, ...]],
    index: EvidenceIndex,
) -> list[EvidenceValue]:
    """Los operandos posibles de una cuenta: las cifras respaldadas cercanas,
    los demás valores de sus filas (exportaciones e importaciones → saldo) y
    las mismas cifras tal como se escribieron."""
    pool: list[EvidenceValue] = []
    in_pool: set[EvidenceValue] = set()
    # Las más cercanas primero: en una tabla, la fila de la cifra.
    near = sorted(direct.items(), key=lambda item: abs(figures[item[0]].start - fig.start))
    for n, hits in near:
        other = figures[n]
        if not (
            fig.start - _OPERAND_WINDOW_BEFORE <= other.start <= fig.end + _OPERAND_WINDOW_AFTER
        ):
            continue
        for v in hits[:3]:
            if v.record == -3:
                continue
            for w in (v, *index.by_record.get((v.result, v.record), []), _written(other, v)):
                if w not in in_pool and w.value != 0:
                    in_pool.add(w)
                    pool.append(w)
    return pool[:_MAX_OPERANDS]


def _signed(fig: Figure) -> bool:
    return bool(fig.sign or fig.polarity)


def _natural_order(a: EvidenceValue, b: EvidenceValue) -> bool | None:
    """¿``a − b`` es el orden natural de la resta? None = no se sabe.

    En una misma columna, el posterior menos el anterior; en una misma fila,
    la columna de la izquierda menos la de la derecha (exportaciones −
    importaciones, bienes − deudas). Entre filas o resultados distintos no
    hay orden: se aceptan los dos.
    """
    if a.result == b.result and a.key == b.key and a.record != b.record:
        return a.record > b.record
    if a.result == b.result and a.record == b.record and a.key != b.key:
        return a.col < b.col
    return None


def _sign_fits(fig: Figure, r: float, a: EvidenceValue, b: EvidenceValue) -> bool:
    """Una cifra con signo ("-2,58 %", "cayó 142,9 millones") sólo sale de una
    cuenta en su orden natural y con el mismo signo."""
    if not _signed(fig):
        return True
    if _natural_order(a, b) is False:
        return False
    return _sign_ok(fig, r)


def _derived_step(
    fig: Figure, steps: list[tuple[EvidenceValue, EvidenceValue]]
) -> tuple[EvidenceValue, ...] | None:
    """¿La cifra es la variación de un período en una serie que el modelo vio?

    "En julio subieron 1.195 millones", "la mensual de marzo fue 3,38 %" con
    los índices de febrero y marzo a la vista. Variación % sólo entre niveles,
    diferencia en puntos sólo entre tasas: nunca una resta de tasas como %.
    """
    if fig.bound:
        return None
    for prev, cur in steps:
        if prev.value == 0:
            continue
        if fig.percent and not fig.points:
            if prev.rate or cur.rate:
                continue
            r = (cur.value / prev.value - 1) * 100
            ok = _magnitude_matches(fig, abs(r), rate_ok=False, scale=1.0)
        elif fig.points:
            r = cur.value - prev.value
            ok = (
                prev.rate
                and cur.rate
                and _magnitude_matches(fig, abs(r), rate_ok=True, scale=1.0, pct=cur.pct)
            )
        elif not fig.times and not prev.rate and not cur.rate:
            r = cur.value - prev.value
            ok = _magnitude_matches(fig, abs(r), rate_ok=False, scale=prev.scale)
        else:
            continue
        if ok and (not _signed(fig) or _sign_ok(fig, r)):
            return (prev, cur)
    return None


def _written(fig: Figure, ev: EvidenceValue) -> EvidenceValue:
    """La cifra respaldada tal como está escrita, en las unidades de la evidencia."""
    target = abs(ev.value)
    options = [
        bare * m for bare, _ in fig.readings for m in (fig.multiplier / ev.scale, 1.0, 0.01, 100.0)
    ]
    best = min(options, key=lambda x: abs(x - target)) if options else target
    return EvidenceValue(
        ev.result,
        ev.record,
        ev.key,
        math.copysign(best, ev.value) if ev.value else best,
        ev.rate,
        ev.scale,
        written=True,
        col=ev.col,
        pct=ev.pct,
        source_value=ev.source_value if ev.written else ev.value,
    )


def _derived(fig: Figure, pool: list[EvidenceValue]) -> tuple[EvidenceValue, ...] | None:
    """¿La cifra sale de una cuenta simple entre valores ya respaldados?

    - variación % y participación (``a/b``) para una cifra en %;
    - diferencia entre tasas para "puntos porcentuales";
    - diferencia o suma para una cifra sin %, y cociente si dice "veces".

    Nunca una diferencia entre tasas presentada como %: es exactamente el
    "−0,22 %" interanual que sale de restar dos inflaciones mensuales. Y una
    cota ("más de 31.250 millones") no se busca en cuentas: con su margen,
    alguna suma caería adentro por azar.
    """
    if fig.bound:
        return None
    for a in pool:
        for b in pool:
            if (a.result, a.record, a.key) == (b.result, b.record, b.key):
                continue
            candidates: list[tuple[float, bool, float]] = []  # (r, rate_ok, scale)
            if fig.percent and not fig.points:
                candidates = [
                    ((a.value / b.value - 1) * 100, False, 1.0),
                    (a.value / b.value * 100, False, 1.0),
                ]
            elif fig.points:
                # Una tasa en fracción menos una en puntos no es una cuenta.
                if a.pct == b.pct:
                    candidates = [(a.value - b.value, a.rate and b.rate, 1.0)]
            elif fig.times:
                candidates = [(a.value / b.value, False, 1.0)]
            elif not (a.rate or b.rate or a.scale != b.scale):
                candidates = [
                    (a.value - b.value, False, a.scale),
                    (a.value + b.value, False, a.scale),
                ]
            for r, rate_ok, scale in candidates:
                if _magnitude_matches(
                    fig, abs(r), rate_ok=rate_ok, scale=scale, pct=a.pct if rate_ok else ""
                ) and _sign_fits(fig, r, a, b):
                    return (a, b)
    return None


def _in_context(fig: Figure, context: frozenset[float]) -> bool:
    """¿La cifra figura tal cual en algo que el modelo leyó (una descripción,
    la muestra de una tabla, la cantidad de filas de una búsqueda)?"""
    # El contexto guarda los números sin signo: uno negativo no se puede
    # confirmar. "+5 puntos" sí (sesiones: "un aumento de 5 puntos del PBI").
    if fig.sign < 0 or fig.polarity < 0 or fig.bound:
        return False
    for bare, _ in fig.readings:
        if _key(bare) in context or _key(bare * fig.multiplier) in context:
            return True
    return False


def verify_figures(
    answer: str,
    evidence: Sequence[Any],
    seen: Sequence[frozenset[float] | None] | None = None,
    context: frozenset[float] | None = None,
) -> Verification:
    """Cada cifra de ``answer`` contra la evidencia del turno.

    ``seen``, alineada con ``evidence``: los números que el modelo tuvo a la
    vista de cada resultado (``seen_numbers`` del contenido de la
    herramienta). Sin ella cuenta toda la evidencia.

    ``context``: los números de todo lo que leyó el modelo, también de las
    herramientas que no devuelven datos (describir_tabla, las búsquedas).
    Una cifra que figura ahí tal cual no es inventada, aunque no se cite como
    fuente ("localidades de 5.000 y más habitantes", de la descripción del
    estudio): queda como "contexto".
    """
    figures = extract_figures(answer)
    if not figures:
        return Verification()
    index = EvidenceIndex.build(evidence, seen)
    direct: dict[int, tuple[EvidenceValue, ...]] = {}
    for n, fig in enumerate(figures):
        hits = tuple(v for v in index.values if _value_matches(fig, v))
        if hits:
            direct[n] = hits

    checks: list[FigureCheck] = []
    for n, fig in enumerate(figures):
        if n in direct:
            checks.append(FigureCheck(fig, "directa", direct[n]))
            continue
        operands = _derived(fig, _operands(fig, figures, direct, index)) or _derived_step(
            fig, index.steps
        )
        if operands is not None:
            checks.append(FigureCheck(fig, "derivada", operands, how="cuenta entre valores"))
        elif context and _in_context(fig, context):
            checks.append(FigureCheck(fig, "contexto"))
        else:
            checks.append(FigureCheck(fig, "sin_respaldo"))
    return Verification(checks)


# ── las fuentes que se citan ───────────────────────────────


def _norm(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", folded.lower()).split())


def _title_key(title: str) -> str:
    """El título sin lo que agrega la herramienta ("— conteo ponderado por …")."""
    base = re.split(r"\s+[—–]\s+", title or "", maxsplit=1)[0]
    base = re.sub(r"\s*\((?:API de Series de Tiempo|DolarApi|ArgentinaDatos[^)]*)\)\s*$", "", base)
    return _norm(base)


def select_evidence(
    answer: str, evidence: Sequence[Any], verification: Verification | None = None
) -> tuple[list[Any], list[Any]]:
    """Las evidencias que se citan y las que sólo se consultaron.

    Se cita una evidencia si alguna cifra suya está en la respuesta, o si su
    título aparece en el texto (lo que no tiene cifras: fragmentos de
    sesiones, listados). Si dos evidencias tienen el mismo título y una ya
    aportó cifras, la otra no entra sólo por el título: en la reproducción del
    04-oct, 92.1 y 92.2 se llaman igual ("Reservas internacionales y pasivos
    del BCRA") y sólo una aportó las cifras.

    El título tiene que aparecer en palabras completas, y un título genérico
    contenido en el de una evidencia que aportó cifras no alcanza: "tipo de
    cambio" en una respuesta hecha con «Tipo de cambio mayorista Comunicación
    A 3500» no cita además una serie vieja llamada «Tipo de cambio».

    Si la respuesta no tiene cifras ni nombra ninguna evidencia, no hay forma
    de saber qué se usó: se citan todas, como antes.
    """
    items = list(evidence)
    if not items:
        return [], []
    check = verification if verification is not None else verify_figures(answer, items)
    by_figures = check.used_results()
    text = f" {_norm(answer)} "
    keys = [_title_key(str(getattr(r, "dataset_title", "") or "")) for r in items]
    figure_keys = [f" {keys[i]} " for i in by_figures]
    by_title = {
        i
        for i, key in enumerate(keys)
        if i not in by_figures
        and len(key) >= 10
        and f" {key} " in text
        and not any(f" {key} " in fk for fk in figure_keys)
    }
    used_idx = by_figures | by_title
    if not used_idx:
        return items, []
    used = [r for i, r in enumerate(items) if i in used_idx]
    consulted = [r for i, r in enumerate(items) if i not in used_idx]
    return used, consulted


def figure_evidence(evidence: Sequence[Any], verification: Verification | None) -> list[Any]:
    """Las evidencias que aportaron alguna cifra respaldada.

    De acá sale el aviso de atraso: una evidencia citada sólo porque su título
    aparece en el texto no tiene que poner "**Dato atrasado:**" arriba de una
    respuesta hecha con datos frescos de otra fuente.
    """
    if verification is None:
        return []
    used = verification.used_results()
    return [r for i, r in enumerate(evidence) if i in used]


def dated_evidence(evidence: Sequence[Any], verification: Verification) -> list[Any]:
    """Sobre qué evidencias se calcula el aviso de atraso fuera de ``correct``.

    Todo lo leído, menos lo que no aportó cifras y se llama igual que una
    evidencia que sí aportó: 92.1 y 92.2 son las dos «Reservas internacionales
    y pasivos del BCRA», y con 92.1 al día y 92.2 consultada y vieja el aviso
    parecía hablar de la cifra de la respuesta (revisión de #146).

    No alcanza con lo que aportó cifras (``figure_evidence``): el respaldo es
    por coincidencia de valor, y una serie vieja que la respuesta sí usó queda
    afuera si su cifra (truncada) coincide con un valor o un salto de otra
    serie leída, o si la usa para una afirmación sin cifra propia.

    Primero lo que aportó cifras y después lo demás, en el orden en que se
    leyó: el runner se queda con los dos primeros avisos y con la primera
    tabla del catálogo, y lo leído antes y no usado no puede desplazar a lo
    que puso una cifra en la respuesta.
    """
    used = verification.used_results()
    keys = [_title_key(str(getattr(r, "dataset_title", "") or "")) for r in evidence]
    used_keys = {keys[i] for i in used if keys[i]}
    first = [r for i, r in enumerate(evidence) if i in used]
    rest = [r for i, r in enumerate(evidence) if i not in used and keys[i] not in used_keys]
    return first + rest


# ── las citas estructuradas ────────────────────────────────

_MAX_GROUNDING = 3


def claim_for(answer: str, fig: Figure) -> str:
    """La oración (o el renglón) donde está la cifra, corta."""
    dot = answer.rfind(". ", 0, fig.start)
    start = max(dot + 2 if dot != -1 else 0, answer.rfind("\n", 0, fig.start) + 1)
    ends = [i for i in (answer.find(". ", fig.end), answer.find("\n", fig.end)) if i != -1]
    end = min(ends) if ends else len(answer)
    claim = " ".join(answer[start:end].replace("*", "").split())
    return claim if len(claim) <= 200 else claim[:199].rstrip() + "…"


def build_citations(
    answer: str, verification: Verification, evidence: Sequence[Any]
) -> list[dict[str, Any]]:
    """Una cita por cifra respaldada, con la fila de donde sale.

    Misma forma que las del pipeline viejo (``claim``, ``source``,
    ``verified``, ``grounding``, ``unsupported_numbers``): los clientes de
    ``/ask`` ya la conocen.
    """
    items = list(evidence)
    citations: list[dict[str, Any]] = []
    for check in verification.supported:
        grounding = []
        for ev in check.matches[:_MAX_GROUNDING]:
            if not 0 <= ev.result < len(items):
                continue
            r = items[ev.result]
            meta = getattr(r, "metadata", None) or {}
            # De una cuenta hecha con las cifras redondeadas de la respuesta
            # (6.140 − 5.864), la cita muestra lo que trajo la fuente (6.139,70).
            value = ev.source_value if ev.written and ev.source_value is not None else ev.value
            grounding.append(
                {
                    "source_name": getattr(r, "dataset_title", ""),
                    "portal": getattr(r, "portal_name", ""),
                    "url": getattr(r, "portal_url", ""),
                    "accessed_at": str(meta.get("fetched_at", "")),
                    "path": ev.path,
                    "value": value,
                }
            )
        if not grounding:
            continue
        citations.append(
            {
                "claim": claim_for(answer, check.figure),
                "source": grounding[0]["source_name"],
                "verified": True,
                "derived": check.status == "derivada",
                "grounding": grounding,
                "unsupported_numbers": [],
            }
        )
    return citations


# ── lo que se le dice al modelo y al lector ────────────────

_MAX_LISTED = 8


def _listed(figures: Iterable[Figure]) -> list[str]:
    out: list[str] = []
    for fig in figures:
        if fig.raw not in out:
            out.append(fig.raw)
    return out[:_MAX_LISTED]


def correction_note(unsupported: Sequence[Figure]) -> str:
    """El pedido de la vuelta correctiva: qué cifras no salen de los resultados."""
    listed = "; ".join(_listed(unsupported))
    return (
        "Revisé tu respuesta contra los resultados de las herramientas y estas cifras no "
        f"aparecen en ellos ni salen de una cuenta simple con ellos: {listed}. Reescribí la "
        "respuesta completa: sacá esas cifras, o calculalas con una herramienta (por ejemplo "
        "series_tiempo con `representacion`, o calcular) y usá lo que devuelva. No hagas "
        "cuentas de cabeza y no sumes ni restes tasas. Si alguna sí sale de los resultados, "
        "escribila tal como figura ahí. No menciones esta revisión."
    )


def unverified_notice(unsupported: Sequence[Figure]) -> str:
    """El aviso que va arriba si, después de la vuelta correctiva, quedan cifras sin respaldo."""
    listed = ", ".join(_listed(unsupported))
    return (
        f"**Aviso:** no pude verificar con las fuentes consultadas {'esta cifra' if len(_listed(unsupported)) == 1 else 'estas cifras'}"
        f" de la respuesta: {listed}. Tomalas con cautela."
    )


def confidence_for(verification: Verification | None, ceiling: float = 1.0) -> float:
    """La confianza interna (no sale al cliente) a partir de la verificación."""
    if verification is None or not verification.checks:
        return min(0.8, ceiling)
    if verification.unsupported:
        return min(0.6, ceiling)
    return min(0.9, ceiling)
