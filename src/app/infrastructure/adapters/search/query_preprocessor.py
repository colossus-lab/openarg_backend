"""Query preprocessor — acronym expansion, temporal normalization, province aliases, and LLM reformulation."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)

# ── Acronym expansion map ──────────────────────────────────

ACRONYM_MAP: dict[str, str] = {
    "PBI": "Producto Bruto Interno (PBI)",
    "IPC": "Índice de Precios al Consumidor (IPC)",
    "BCRA": "Banco Central de la República Argentina (BCRA)",
    "INDEC": "Instituto Nacional de Estadística y Censos (INDEC)",
    "EMAE": "Estimador Mensual de Actividad Económica (EMAE)",
    "CBT": "Canasta Básica Total (CBT)",
    "CBA": "Canasta Básica Alimentaria (CBA)",
    "DDJJ": "Declaraciones Juradas (DDJJ)",
    "HCDN": "Honorable Cámara de Diputados de la Nación (HCDN)",
    "ANSES": "Administración Nacional de la Seguridad Social (ANSES)",
    "AFIP": "Administración Federal de Ingresos Públicos (AFIP)",
    "ARCA": "Agencia de Recaudación y Control Aduanero (ARCA)",
    "CCL": "Contado Con Liquidación (CCL)",
    "MEP": "Mercado Electrónico de Pagos (MEP)",
    "LELIQ": "Letras de Liquidez del BCRA (LELIQ)",
    "EMI": "Estimador Mensual Industrial (EMI)",
    "FMI": "Fondo Monetario Internacional (FMI)",
    "EMBI": "Emerging Markets Bond Index (EMBI)",
    "EPH": "Encuesta Permanente de Hogares (EPH)",
    "UVA": "Unidad de Valor Adquisitivo (UVA)",
    "INTA": "Instituto Nacional de Tecnología Agropecuaria (INTA)",
    "CONICET": "Consejo Nacional de Investigaciones Científicas y Técnicas (CONICET)",
}

# Pre-compiled acronym patterns (compiled once at module load)
_ACRONYM_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b" + re.escape(acronym) + r"\b", re.IGNORECASE), expansion)
    for acronym, expansion in ACRONYM_MAP.items()
]
# Fast pre-filter: set of lowercase acronym words for O(1) check before regex
_ACRONYM_WORDS = frozenset(k.lower() for k in ACRONYM_MAP)

# ── Temporal patterns (single combined regex — 1 pass instead of 9) ──

_TEMPORAL_COMBINED = re.compile(
    r"\b(?:"
    r"(?P<ultimo_mes>[úu]ltimo\s+mes)"
    r"|(?P<ultimos_n_meses>[úu]ltimos?\s+(?P<n_meses>\d+)\s+meses?)"
    r"|(?P<ultimo_ano>[úu]ltimo\s+a[ñn]o)"
    r"|(?P<ultimos_n_anos>[úu]ltimos?\s+(?P<n_anos>\d+)\s+a[ñn]os?)"
    r"|(?P<este_ano>este\s+a[ñn]o)"
    r"|(?P<hoy>hoy)"
    r"|(?P<ayer>ayer)"
    r"|(?P<esta_semana>esta\s+semana)"
    r"|(?P<este_mes>este\s+mes)"
    r")\b",
    re.IGNORECASE,
)

# ── Province aliases (sorted longest-first to avoid partial matches) ──

_PROVINCE_ALIASES: dict[str, str] = {
    "capital federal": "Ciudad Autónoma de Buenos Aires",
    "cap fed": "Ciudad Autónoma de Buenos Aires",
    "bs.as.": "Buenos Aires",
    "bs as": "Buenos Aires",
    "bsas": "Buenos Aires",
    "baires": "Buenos Aires",
    "caba": "Ciudad Autónoma de Buenos Aires",
    "lrioja": "La Rioja",
    "chaco": "Chaco",
    "jujuy": "Jujuy",
    "salta": "Salta",
    "mnes": "Misiones",
    "ctes": "Corrientes",
    "nqn": "Neuquén",
    "tdf": "Tierra del Fuego",
    "sgo": "Santiago del Estero",
    "sfe": "Santa Fe",
    "mza": "Mendoza",
    "tuc": "Tucumán",
    "cordoba": "Córdoba",
    "chu": "Chubut",
    "fsa": "Formosa",
    "cba": "Córdoba",
}

# Pre-compiled province patterns (compiled once, longest-first order preserved)
_PROVINCE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b" + re.escape(alias) + r"\b", re.IGNORECASE), full_name)
    for alias, full_name in _PROVINCE_ALIASES.items()
]

# ── Synonym expansion ─────────────────────────────────────

SYNONYM_MAP: dict[str, list[str]] = {
    # ── Economía ──
    "inflación": ["ipc", "precios", "costo de vida", "aumento de precios", "suba de precios"],
    "ipc": ["inflación", "precios", "costo de vida"],
    "salario": ["sueldo", "remuneración", "ingreso", "haber", "salarios", "sueldos"],
    "sueldo": ["salario", "remuneración", "ingreso", "haber"],
    "remuneración": ["salario", "sueldo", "ingreso"],
    "desempleo": ["desocupación", "sin trabajo", "sin empleo", "desocupados"],
    "desocupación": ["desempleo", "sin trabajo", "desocupados"],
    "empleo": ["trabajo", "ocupación", "mercado laboral", "empleo formal"],
    "trabajo": ["empleo", "ocupación", "mercado laboral"],
    "laburo": ["trabajo", "empleo", "ocupación"],
    "pobreza": ["indigencia", "canasta básica", "línea de pobreza", "vulnerabilidad", "pobres"],
    "indigencia": ["pobreza", "canasta básica alimentaria", "necesidades básicas insatisfechas"],
    "pbi": ["producto bruto", "producto interno", "producto bruto interno", "pib", "gdp"],
    "producto bruto interno": ["pbi", "pib", "gdp"],
    "actividad económica": ["emae", "actividad", "nivel de actividad", "producción"],
    "emae": ["actividad económica", "nivel de actividad"],
    # ── Finanzas ──
    "dólar": ["tipo de cambio", "divisa", "cotización dólar", "moneda extranjera"],
    "tipo de cambio": ["dólar", "divisa", "cotización"],
    "reservas": ["reservas internacionales", "reservas bcra", "reservas del central"],
    "deuda": ["deuda pública", "deuda externa", "deuda interna", "endeudamiento", "pasivos"],
    "deuda pública": ["deuda", "endeudamiento", "deuda externa"],
    "endeudamiento": ["deuda", "deuda pública", "pasivos"],
    "tasa de interés": ["tasa", "tasa de referencia", "tasa política monetaria"],
    "base monetaria": ["emisión", "emisión monetaria", "dinero en circulación", "circulante"],
    "emisión": ["base monetaria", "emisión monetaria", "circulante"],
    "exportaciones": ["expo", "ventas al exterior", "comercio exterior exportación"],
    "importaciones": ["impo", "compras al exterior", "comercio exterior importación"],
    "plata": ["dinero", "fondos"],
    "guita": ["dinero", "fondos"],
    # ── Presupuesto ──
    "presupuesto": ["gasto público", "ejecución presupuestaria", "partidas", "crédito vigente"],
    "gasto": ["erogación", "ejecución presupuestaria"],
    "gasto público": ["presupuesto", "ejecución presupuestaria", "gasto estatal"],
    "recaudación": [
        "ingresos fiscales",
        "recursos tributarios",
        "recaudación impositiva",
        "recaudación tributaria",
    ],
    "ingresos fiscales": ["recaudación", "recursos tributarios"],
    # ── Social ──
    "educación": [
        "escuela",
        "universidad",
        "estudiantes",
        "matrícula",
        "docentes",
        "sistema educativo",
    ],
    "salud": ["hospital", "sanitario", "epidemiología", "vacunación", "mortalidad"],
    "mortalidad": ["defunciones", "fallecimientos"],
    "seguridad": [
        "delito",
        "crimen",
        "homicidio",
        "robo",
        "inseguridad",
        "violencia",
        "hechos delictivos",
    ],
    "delito": ["seguridad", "crimen", "inseguridad"],
    "vivienda": ["inmueble", "propiedad", "construcción", "déficit habitacional"],
    "jubilación": [
        "pensión",
        "haber jubilatorio",
        "anses",
        "adulto mayor",
        "pami",
        "prestación previsional",
    ],
    "pensión": ["jubilación", "haber jubilatorio", "anses"],
    "nacimientos": ["natalidad"],
    "obra pública": ["inversión pública", "obra de infraestructura"],
    # ── Infraestructura ──
    "energía": ["petróleo", "gas", "electricidad", "hidrocarburos", "combustible"],
    "transporte": ["tránsito", "ruta", "autopista", "ferrocarril", "tren", "subte", "colectivo"],
    "agricultura": ["agro", "ganadería", "campo", "cosecha", "siembra", "soja", "trigo", "maíz"],
    "agro": ["agricultura", "ganadería", "campo", "cosecha"],
    # ── Gobierno ──
    "diputados": ["congreso", "cámara baja", "legisladores", "parlamentarios"],
    "congreso": ["diputados", "cámara baja", "legisladores", "senado"],
    "declaración jurada": ["ddjj", "patrimonio", "bienes", "declaración patrimonial"],
    "ddjj": ["declaración jurada", "patrimonio", "bienes"],
    "funcionario": ["funcionarios públicos", "servidor público", "cargo público"],
}

# Pre-built inverted index: single-word terms → synonyms (O(1) lookup)
# Multi-word terms still use substring matching but are a much smaller set.
_SYNONYM_SINGLE_WORDS: dict[str, list[str]] = {}
_SYNONYM_PHRASES: dict[str, list[str]] = {}
for _term, _syns in SYNONYM_MAP.items():
    if " " in _term:
        _SYNONYM_PHRASES[_term] = _syns
    else:
        _SYNONYM_SINGLE_WORDS[_term] = _syns


def expand_synonyms(query: str) -> str:
    """Append relevant synonyms to the query for broader search coverage.

    Uses an inverted word→terms index for single-word terms (O(words) vs O(map_size)),
    and only falls back to substring scan for multi-word terms.
    """
    lower = query.lower()
    words = set(lower.split())
    additions: list[str] = []
    # O(1) per word for single-word terms via inverted index
    for w in words:
        if w in _SYNONYM_SINGLE_WORDS:
            additions.extend(_SYNONYM_SINGLE_WORDS[w])
    # O(phrases) for multi-word terms (much smaller set)
    for phrase, syns in _SYNONYM_PHRASES.items():
        if phrase in lower:
            additions.extend(syns)
    if additions:
        unique = list(dict.fromkeys(additions))[:4]
        return query + " (" + ", ".join(unique) + ")"
    return query


def expand_acronyms(query: str) -> str:
    """Expand known Argentine acronyms in the query. Deterministic, fast.

    Uses a set pre-filter to skip all 21 regex patterns when no acronym
    words are present in the query (the common case).
    """
    # Fast path: if no known acronym word appears, skip all regex
    query_words = set(query.lower().split())
    if not query_words & _ACRONYM_WORDS:
        return query
    result = query
    for pattern, expansion in _ACRONYM_PATTERNS:
        if pattern.search(result):
            result = pattern.sub(expansion, result)
    return result


def normalize_temporal(query: str) -> tuple[str, dict[str, str]]:
    """Replace temporal expressions with concrete dates. Returns (query, metadata).

    Uses a single combined regex (1 pass) instead of 9 separate patterns.
    """
    match = _TEMPORAL_COMBINED.search(query)
    if not match:
        return query, {}

    now = datetime.now(UTC)
    metadata: dict[str, str] = {}
    end = now.strftime("%Y-%m-%d")

    if match.group("ultimo_mes"):
        start = (now - relativedelta(months=1)).strftime("%Y-%m-%d")
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("ultimos_n_meses"):
        n = int(match.group("n_meses"))
        start = (now - relativedelta(months=n)).strftime("%Y-%m-%d")
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("ultimo_ano"):
        start = (now - relativedelta(years=1)).strftime("%Y-%m-%d")
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("ultimos_n_anos"):
        n = int(match.group("n_anos"))
        start = (now - relativedelta(years=n)).strftime("%Y-%m-%d")
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("este_ano"):
        start = f"{now.year}-01-01"
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("hoy"):
        metadata["date"] = end
        replacement = f"(fecha: {end})"
    elif match.group("ayer"):
        yesterday = (now - relativedelta(days=1)).strftime("%Y-%m-%d")
        metadata["date"] = yesterday
        replacement = f"(fecha: {yesterday})"
    elif match.group("esta_semana"):
        start = (now - relativedelta(days=now.weekday())).strftime("%Y-%m-%d")
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    elif match.group("este_mes"):
        start = f"{now.year}-{now.month:02d}-01"
        metadata.update(start_date=start, end_date=end)
        replacement = f"(desde {start} hasta {end})"
    else:
        return query, {}

    result = query[:match.start()] + replacement + query[match.end():]
    return result, metadata


def normalize_provinces(query: str) -> str:
    """Replace province abbreviations with full names. Deterministic."""
    result = query
    for pattern, full_name in _PROVINCE_PATTERNS:
        if pattern.search(result):
            result = pattern.sub(full_name, result)
    return result


class QueryPreprocessor:
    def __init__(self, llm: ILLMProvider) -> None:
        self._llm = llm

    async def reformulate(self, query: str) -> str:
        """Reformulate a user query for better search relevance.

        Pipeline: expand_acronyms -> normalize_temporal -> normalize_provinces -> expand_synonyms -> LLM reformulation.
        All queries are processed (no skip for short queries).
        Falls back to the preprocessed query on any LLM error.
        """
        # Deterministic preprocessing
        processed = expand_acronyms(query)
        processed, _ = normalize_temporal(processed)
        processed = normalize_provinces(processed)
        processed = expand_synonyms(processed)

        try:
            response = await self._llm.chat(
                messages=[
                    LLMMessage(role="system", content=load_prompt("preprocessor")),
                    LLMMessage(role="user", content=processed),
                ],
                temperature=0.3,
                max_tokens=256,
            )
            reformulated = response.content.strip()
            return reformulated or processed
        except Exception:
            logger.debug("Query reformulation failed, using preprocessed query")
            return processed

    def preprocess_sync(self, query: str) -> str:
        """Synchronous preprocessing without LLM — for use in decomposer and other sync contexts."""
        result = expand_acronyms(query)
        result, _ = normalize_temporal(result)
        result = normalize_provinces(result)
        result = expand_synonyms(result)
        return result
