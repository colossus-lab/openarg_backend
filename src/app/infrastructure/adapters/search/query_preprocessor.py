"""Query preprocessor — acronym expansion, temporal normalization, province aliases, and LLM reformulation."""
from __future__ import annotations

import logging
import re
from datetime import UTC, datetime

from dateutil.relativedelta import relativedelta

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Reformulá esta consulta para maximizar la relevancia de búsqueda en datos abiertos argentinos. "
    "Expandí siglas, agregá contexto temporal si es implícito, y normalizá nombres geográficos. "
    "Solo retorná la consulta reformulada, sin explicaciones."
)

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

# ── Temporal patterns ──────────────────────────────────────

_TEMPORAL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b[úu]ltimo\s+mes\b", re.IGNORECASE), "último_mes"),
    (re.compile(r"\b[úu]ltimos?\s+(\d+)\s+meses?\b", re.IGNORECASE), "últimos_n_meses"),
    (re.compile(r"\b[úu]ltimo\s+a[ñn]o\b", re.IGNORECASE), "último_año"),
    (re.compile(r"\b[úu]ltimos?\s+(\d+)\s+a[ñn]os?\b", re.IGNORECASE), "últimos_n_años"),
    (re.compile(r"\beste\s+a[ñn]o\b", re.IGNORECASE), "este_año"),
    (re.compile(r"\bhoy\b", re.IGNORECASE), "hoy"),
    (re.compile(r"\bayer\b", re.IGNORECASE), "ayer"),
    (re.compile(r"\besta\s+semana\b", re.IGNORECASE), "esta_semana"),
    (re.compile(r"\beste\s+mes\b", re.IGNORECASE), "este_mes"),
]

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
    "cba": "Córdoba",
    "chu": "Chubut",
    "fsa": "Formosa",
}

# Pre-compiled province patterns (compiled once, longest-first order preserved)
_PROVINCE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b" + re.escape(alias) + r"\b", re.IGNORECASE), full_name)
    for alias, full_name in _PROVINCE_ALIASES.items()
]

# ── Synonym expansion ─────────────────────────────────────

SYNONYM_MAP: dict[str, list[str]] = {
    "sueldo": ["salario", "remuneración"],
    "salario": ["sueldo", "remuneración"],
    "plata": ["dinero", "fondos"],
    "guita": ["dinero", "fondos"],
    "laburo": ["trabajo", "empleo"],
    "desocupación": ["desempleo"],
    "desempleo": ["desocupación"],
    "deuda": ["endeudamiento", "pasivos"],
    "gasto": ["erogación", "ejecución presupuestaria"],
    "recaudación": ["ingresos fiscales", "recaudación tributaria"],
    "obra pública": ["inversión pública", "obra de infraestructura"],
    "jubilación": ["haber jubilatorio", "prestación previsional"],
    "educación": ["sistema educativo", "establecimientos educativos"],
    "seguridad": ["delitos", "hechos delictivos"],
    "pobreza": ["indigencia", "necesidades básicas insatisfechas"],
    "mortalidad": ["defunciones", "fallecimientos"],
    "nacimientos": ["natalidad"],
}


def expand_synonyms(query: str) -> str:
    """Append relevant synonyms to the query for broader search coverage."""
    lower = query.lower()
    additions: list[str] = []
    for term, synonyms in SYNONYM_MAP.items():
        if term in lower:
            additions.extend(synonyms)
    if additions:
        # Deduplicate and append as context
        unique = list(dict.fromkeys(additions))[:4]
        return query + " (" + ", ".join(unique) + ")"
    return query


def expand_acronyms(query: str) -> str:
    """Expand known Argentine acronyms in the query. Deterministic, fast."""
    result = query
    for pattern, expansion in _ACRONYM_PATTERNS:
        if pattern.search(result):
            result = pattern.sub(expansion, result)
    return result


def normalize_temporal(query: str) -> tuple[str, dict[str, str]]:
    """Replace temporal expressions with concrete dates. Returns (query, metadata)."""
    now = datetime.now(UTC)
    metadata: dict[str, str] = {}
    result = query

    for pattern, label in _TEMPORAL_PATTERNS:
        match = pattern.search(result)
        if not match:
            continue

        if label == "último_mes":
            start = (now - relativedelta(months=1)).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "últimos_n_meses":
            n = int(match.group(1))
            start = (now - relativedelta(months=n)).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "último_año":
            start = (now - relativedelta(years=1)).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "últimos_n_años":
            n = int(match.group(1))
            start = (now - relativedelta(years=n)).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "este_año":
            start = f"{now.year}-01-01"
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "hoy":
            today = now.strftime("%Y-%m-%d")
            metadata["date"] = today
            result = pattern.sub(f"(fecha: {today})", result)
        elif label == "ayer":
            yesterday = (now - relativedelta(days=1)).strftime("%Y-%m-%d")
            metadata["date"] = yesterday
            result = pattern.sub(f"(fecha: {yesterday})", result)
        elif label == "esta_semana":
            start = (now - relativedelta(days=now.weekday())).strftime("%Y-%m-%d")
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        elif label == "este_mes":
            start = f"{now.year}-{now.month:02d}-01"
            end = now.strftime("%Y-%m-%d")
            metadata["start_date"] = start
            metadata["end_date"] = end
            result = pattern.sub(f"(desde {start} hasta {end})", result)
        break  # only process first temporal match

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
                    LLMMessage(role="system", content=_SYSTEM_PROMPT),
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
