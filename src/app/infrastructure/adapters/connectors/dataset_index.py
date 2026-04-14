"""Deterministic keyword-to-action routing index for the query planner.

Provides fast, LLM-free routing hints so the planner can choose the right
data source without guessing.  All logic is pure Python (no DB, no LLM,
no external deps) and runs in microseconds.

The production database has ~1,800 cache_* tables with 27 million rows of
pre-downloaded dataset data.  This index implements a "cache-first" strategy:
queries that match known cached datasets are routed to ``query_sandbox``
(NL2SQL) before falling back to internet APIs.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Common Spanish typos for economic/government terms.
# Keys are the misspelled form (already normalized: no accents, lowercase);
# Fuzzy typo correction vocabulary (built lazily from KEYWORD_ROUTES keys).
_FUZZY_VOCAB: list[str] | None = None
_CURRENT_VALUE_TOKENS = frozenset({"hoy", "ahora", "actual", "momento"})
_HISTORICAL_VALUE_TOKENS = frozenset(
    {"historico", "historica", "historicos", "historicas", "evolucion", "serie", "series"}
)


def _fix_typos(text: str) -> str:
    """Fix typos via fuzzy matching against KEYWORD_ROUTES vocabulary.

    Uses difflib.get_close_matches (stdlib) — no external deps.
    Words shorter than 4 chars or already in vocabulary are skipped.
    Cutoff 0.8 means ~80% character similarity required.
    """
    from difflib import get_close_matches

    global _FUZZY_VOCAB  # noqa: PLW0603
    if _FUZZY_VOCAB is None:
        vocab: set[str] = set()
        for key in KEYWORD_ROUTES:
            vocab.update(key.split())
        _FUZZY_VOCAB = sorted(vocab)

    # Common Spanish words that should never be "corrected" by fuzzy matching
    _SKIP = {
        "nacional",
        "provincial",
        "municipal",
        "federal",
        "actual",
        "general",
        "personal",
        "especial",
        "oficial",
        "total",
        "anual",
        "mensual",
        "trimestral",
        "historico",
        "publico",
        "ultimo",
        "primero",
        "segundo",
        "tercero",
        "cuarto",
        "millones",
        "argentina",
        "gobierno",
        "ministerio",
        "comparar",
        "comparado",
        "evolucion",
        "informacion",
    }

    words = text.split()
    fixed = []
    for w in words:
        if len(w) < 7 or w in _FUZZY_VOCAB or w in _SKIP:
            fixed.append(w)
            continue
        matches = get_close_matches(w, _FUZZY_VOCAB, n=1, cutoff=0.87)
        fixed.append(matches[0] if matches else w)
    return " ".join(fixed)


def normalize_query(text: str) -> str:
    """Normalize a query string for keyword matching.

    Strips accents, lowercases, removes punctuation, collapses whitespace,
    and corrects common Spanish typos for economic/government terms.
    """
    # Strip combining marks (accents)
    nfkd = unicodedata.normalize("NFKD", text)
    stripped = "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")
    # Lowercase and remove punctuation (keep alphanumeric + spaces)
    cleaned = re.sub(r"[^\w\s]", " ", stripped.lower())
    # Collapse whitespace
    result = re.sub(r"\s+", " ", cleaned).strip()
    # Fix common typos
    return _fix_typos(result)


def _wants_current_value(normalized: str) -> bool:
    tokens = set(normalized.split())
    if tokens & _CURRENT_VALUE_TOKENS:
        return True
    return "en este momento" in normalized


def _wants_historical_value(normalized: str) -> bool:
    tokens = set(normalized.split())
    if tokens & _HISTORICAL_VALUE_TOKENS:
        return True
    return "ultimo ano" in normalized or "ultimos" in tokens


# ---------------------------------------------------------------------------
# RoutingHint dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RoutingHint:
    """A deterministic routing hint for the planner."""

    action: str
    params: dict
    confidence: float
    description: str


# ---------------------------------------------------------------------------
# 1. KEYWORD_ROUTES — keyword -> RoutingHint mapping
#
# Keys are already normalized (no accents, lowercase).  Values carry the
# target action, default params, confidence [0-1], and a short description.
# ---------------------------------------------------------------------------

KEYWORD_ROUTES: dict[str, dict] = {
    # ── Economia: Inflacion / IPC / Precios ────────────────────
    "inflacion": {
        "action": "query_series",
        "params": {"series_ids": ["148.3_INIVELNAL_DICI_M_26"], "representation": "percent_change"},
        "confidence": 0.95,
        "description": "IPC Nacional variacion mensual (inflacion)",
    },
    "ipc": {
        "action": "query_series",
        "params": {"series_ids": ["148.3_INIVELNAL_DICI_M_26"], "representation": "percent_change"},
        "confidence": 0.95,
        "description": "Indice de Precios al Consumidor Nacional",
    },
    "precios": {
        "action": "query_series",
        "params": {"series_ids": ["148.3_INIVELNAL_DICI_M_26"]},
        "confidence": 0.80,
        "description": "Indice de precios nacional",
    },
    "costo de vida": {
        "action": "query_series",
        "params": {"series_ids": ["148.3_INIVELNAL_DICI_M_26"]},
        "confidence": 0.85,
        "description": "Costo de vida / IPC Nacional",
    },
    "ipc regional": {
        "action": "query_series",
        "params": {
            "series_ids": [
                "148.3_INIVELNAL_DICI_M_26",
                "103.1_I2N_2016_M_19",
                "148.3_INIVELNOA_DICI_M_21",
                "145.3_INGCUYUYO_DICI_M_11",
            ]
        },
        "confidence": 0.90,
        "description": "IPC regional: Nacional, GBA, NOA, Cuyo",
    },
    # ── Economia: Actividad ────────────────────────────────────
    "emae": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.95,
        "description": "EMAE - Estimador Mensual de Actividad Economica",
    },
    "actividad economica": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.90,
        "description": "Actividad economica (EMAE)",
    },
    "pbi": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.90,
        "description": "Producto Bruto Interno (via EMAE)",
    },
    "producto bruto": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.85,
        "description": "Producto bruto / PBI",
    },
    "crecimiento": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.75,
        "description": "Crecimiento economico (EMAE)",
    },
    "recesion": {
        "action": "query_series",
        "params": {"series_ids": ["143.3_NO_PR_2004_A_21"]},
        "confidence": 0.75,
        "description": "Recesion / contraccion economica (EMAE)",
    },
    "actividad industrial": {
        "action": "query_series",
        "params": {"series_ids": ["11.3_AGCS_2004_M_41"]},
        "confidence": 0.90,
        "description": "Actividad industrial (EMAE sector industrial)",
    },
    "produccion industrial": {
        "action": "query_series",
        "params": {"series_ids": ["11.3_AGCS_2004_M_41"]},
        "confidence": 0.90,
        "description": "Produccion industrial",
    },
    "industria": {
        "action": "query_series",
        "params": {"series_ids": ["11.3_AGCS_2004_M_41"]},
        "confidence": 0.80,
        "description": "Sector industrial",
    },
    "manufactura": {
        "action": "query_series",
        "params": {"series_ids": ["11.3_AGCS_2004_M_41"]},
        "confidence": 0.80,
        "description": "Sector manufacturero",
    },
    # ── Empleo ─────────────────────────────────────────────────
    "desempleo": {
        "action": "query_series",
        "params": {"series_ids": ["45.2_ECTDT_0_T_33"]},
        "confidence": 0.95,
        "description": "Tasa de desempleo (nacional)",
    },
    "desocupacion": {
        "action": "query_series",
        "params": {"series_ids": ["45.2_ECTDT_0_T_33"]},
        "confidence": 0.95,
        "description": "Tasa de desocupacion (nacional)",
    },
    # ── Desempleo regional (6 regiones EPH) ───────────────────
    # These composite keywords override the national-only "desempleo"
    # route when geographic comparison context is detected.
    "desempleo regional": {
        "action": "query_series",
        "params": {
            "series_ids": [
                "45.2_ECTDT_0_T_33",
                "45.2_ECTDTG_0_T_37",
                "45.2_ECTDTNO_0_T_42",
                "45.2_ECTDTNE_0_T_42",
                "45.2_ECTDTCU_0_T_38",
                "45.2_ECTDTRP_0_T_49",
                "45.2_ECTDTP_0_T_43",
            ]
        },
        "confidence": 0.95,
        "description": "Desempleo por region: Total, GBA, NOA, NEA, Cuyo, Pampeana, Patagonia",
    },
    "desempleo por region": {
        "action": "query_series",
        "params": {
            "series_ids": [
                "45.2_ECTDT_0_T_33",
                "45.2_ECTDTG_0_T_37",
                "45.2_ECTDTNO_0_T_42",
                "45.2_ECTDTNE_0_T_42",
                "45.2_ECTDTCU_0_T_38",
                "45.2_ECTDTRP_0_T_49",
                "45.2_ECTDTP_0_T_43",
            ]
        },
        "confidence": 0.95,
        "description": "Desempleo por region estadistica del INDEC",
    },
    "desempleo por provincia": {
        "action": "query_series",
        "params": {
            "series_ids": [
                "45.2_ECTDT_0_T_33",
                "45.2_ECTDTG_0_T_37",
                "45.2_ECTDTNO_0_T_42",
                "45.2_ECTDTNE_0_T_42",
                "45.2_ECTDTCU_0_T_38",
                "45.2_ECTDTRP_0_T_49",
                "45.2_ECTDTP_0_T_43",
            ]
        },
        "confidence": 0.95,
        "description": "Desempleo por region (no hay datos por provincia, se usan regiones EPH)",
    },
    "desocupacion regional": {
        "action": "query_series",
        "params": {
            "series_ids": [
                "45.2_ECTDT_0_T_33",
                "45.2_ECTDTG_0_T_37",
                "45.2_ECTDTNO_0_T_42",
                "45.2_ECTDTNE_0_T_42",
                "45.2_ECTDTCU_0_T_38",
                "45.2_ECTDTRP_0_T_49",
                "45.2_ECTDTP_0_T_43",
            ]
        },
        "confidence": 0.95,
        "description": "Desocupacion por region estadistica del INDEC",
    },
    "empleo": {
        "action": "query_series",
        "params": {"series_ids": ["45.2_ECTDT_0_T_33"]},
        "confidence": 0.80,
        "description": "Indicadores de empleo",
    },
    "trabajo": {
        "action": "query_series",
        "params": {"series_ids": ["45.2_ECTDT_0_T_33"]},
        "confidence": 0.70,
        "description": "Mercado de trabajo",
    },
    "mercado laboral": {
        "action": "query_series",
        "params": {"series_ids": ["45.2_ECTDT_0_T_33"]},
        "confidence": 0.85,
        "description": "Mercado laboral",
    },
    "salarios": {
        "action": "query_series",
        "params": {"series_ids": ["149.1_TL_INDIIOS_OCTU_0_21"]},
        "confidence": 0.95,
        "description": "Indice de Salarios",
    },
    "sueldos": {
        "action": "query_series",
        "params": {"series_ids": ["149.1_TL_INDIIOS_OCTU_0_21"]},
        "confidence": 0.90,
        "description": "Sueldos / salarios",
    },
    "remuneraciones": {
        "action": "query_series",
        "params": {"series_ids": ["149.1_TL_INDIIOS_OCTU_0_21"]},
        "confidence": 0.85,
        "description": "Remuneraciones",
    },
    "paritarias": {
        "action": "query_series",
        "params": {"series_ids": ["149.1_TL_INDIIOS_OCTU_0_21"]},
        "confidence": 0.75,
        "description": "Paritarias / negociacion salarial",
    },
    # ── Finanzas: Dolar ────────────────────────────────────────
    "dolar blue": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "blue"},
        "confidence": 0.95,
        "description": "Cotizacion dolar blue",
    },
    "dolar oficial": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "oficial"},
        "confidence": 0.95,
        "description": "Cotizacion dolar oficial",
    },
    "dolar cripto": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "cripto"},
        "confidence": 0.95,
        "description": "Cotizacion dolar cripto",
    },
    "dolar bolsa": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "bolsa"},
        "confidence": 0.95,
        "description": "Cotizacion dolar bolsa (MEP)",
    },
    "dolar mep": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "bolsa"},
        "confidence": 0.95,
        "description": "Cotizacion dolar MEP",
    },
    "dolar ccl": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "contadoconliqui"},
        "confidence": 0.95,
        "description": "Cotizacion dolar contado con liquidacion",
    },
    "contado con liquidacion": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "contadoconliqui"},
        "confidence": 0.95,
        "description": "Dolar contado con liquidacion",
    },
    "dolar solidario": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "solidario"},
        "confidence": 0.95,
        "description": "Cotizacion dolar solidario (tarjeta)",
    },
    "dolar tarjeta": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "solidario"},
        "confidence": 0.90,
        "description": "Dolar tarjeta / solidario",
    },
    "dolar mayorista": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar", "casa": "mayorista"},
        "confidence": 0.95,
        "description": "Cotizacion dolar mayorista",
    },
    "dolar": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar"},
        "confidence": 0.85,
        "description": "Cotizacion del dolar (todas las casas)",
    },
    "cotizacion dolar": {
        "action": "query_argentina_datos",
        "params": {"type": "dolar"},
        "confidence": 0.90,
        "description": "Cotizacion del dolar",
    },
    # ── Finanzas: Tipo de cambio (BCRA / series historicas) ────
    "tipo de cambio": {
        "action": "query_series",
        "params": {"series_ids": ["92.2_TIPO_CAMBIION_0_0_21_24"]},
        "confidence": 0.90,
        "description": "Tipo de cambio oficial historico",
    },
    "tipo de cambio oficial": {
        "action": "query_bcra",
        "params": {"tipo": "cotizaciones", "moneda": "USD"},
        "confidence": 0.90,
        "description": "Tipo de cambio oficial BCRA",
    },
    # ── Finanzas: Reservas / Monetarias ────────────────────────
    "reservas": {
        "action": "query_series",
        "params": {"series_ids": ["174.1_RRVAS_IDOS_0_0_36"]},
        "confidence": 0.90,
        "description": "Reservas internacionales del BCRA",
    },
    "reservas internacionales": {
        "action": "query_series",
        "params": {"series_ids": ["174.1_RRVAS_IDOS_0_0_36"]},
        "confidence": 0.95,
        "description": "Reservas internacionales del BCRA",
    },
    "reservas bcra": {
        "action": "query_series",
        "params": {"series_ids": ["174.1_RRVAS_IDOS_0_0_36"]},
        "confidence": 0.95,
        "description": "Reservas del BCRA",
    },
    "base monetaria": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_SALDO_BASERIA__15"]},
        "confidence": 0.95,
        "description": "Base monetaria",
    },
    "emision": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_SALDO_BASERIA__15"]},
        "confidence": 0.85,
        "description": "Emision monetaria / base monetaria",
    },
    "emision monetaria": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_SALDO_BASERIA__15"]},
        "confidence": 0.90,
        "description": "Emision monetaria",
    },
    "leliq": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"]},
        "confidence": 0.95,
        "description": "LELIQ del BCRA",
    },
    "pases": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"]},
        "confidence": 0.85,
        "description": "Pases pasivos del BCRA",
    },
    "pases pasivos": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"]},
        "confidence": 0.90,
        "description": "Pases pasivos del BCRA",
    },
    "pasivos remunerados": {
        "action": "query_series",
        "params": {"series_ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"]},
        "confidence": 0.85,
        "description": "Pasivos remunerados del BCRA",
    },
    "tasa de interes": {
        "action": "query_bcra",
        "params": {"tipo": "variables"},
        "confidence": 0.85,
        "description": "Tasa de interes / politica monetaria BCRA",
    },
    "tasa de politica monetaria": {
        "action": "query_bcra",
        "params": {"tipo": "variables"},
        "confidence": 0.90,
        "description": "Tasa de politica monetaria BCRA",
    },
    # ── Finanzas: Riesgo pais ──────────────────────────────────
    "riesgo pais": {
        "action": "query_argentina_datos",
        "params": {"type": "riesgo_pais"},
        "confidence": 0.95,
        "description": "Riesgo pais (EMBI+)",
    },
    "embi": {
        "action": "query_argentina_datos",
        "params": {"type": "riesgo_pais"},
        "confidence": 0.90,
        "description": "Indice EMBI+ Argentina",
    },
    # ── Finanzas: Otras monedas ────────────────────────────────
    "euro": {
        "action": "query_bcra",
        "params": {"tipo": "cotizaciones", "moneda": "EUR"},
        "confidence": 0.85,
        "description": "Cotizacion del euro",
    },
    "real brasileno": {
        "action": "query_bcra",
        "params": {"tipo": "cotizaciones", "moneda": "BRL"},
        "confidence": 0.80,
        "description": "Cotizacion del real brasileno",
    },
    "cotizacion real": {
        "action": "query_bcra",
        "params": {"tipo": "cotizaciones", "moneda": "BRL"},
        "confidence": 0.80,
        "description": "Cotizacion del real brasileno",
    },
    # ── Pobreza ────────────────────────────────────────────────
    "pobreza": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_POBREZA_0_D_13"]},
        "confidence": 0.90,
        "description": "Canasta basica total / linea de pobreza",
    },
    "indigencia": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_INDICIA_0_D_16"]},
        "confidence": 0.90,
        "description": "Canasta basica alimentaria / linea de indigencia",
    },
    "canasta basica": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_POBREZA_0_D_13"]},
        "confidence": 0.95,
        "description": "Canasta Basica Total (CBT)",
    },
    "canasta basica total": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_POBREZA_0_D_13"]},
        "confidence": 0.95,
        "description": "Canasta Basica Total (CBT)",
    },
    "canasta basica alimentaria": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_INDICIA_0_D_16"]},
        "confidence": 0.95,
        "description": "Canasta Basica Alimentaria (CBA)",
    },
    "linea de pobreza": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_POBREZA_0_D_13"]},
        "confidence": 0.95,
        "description": "Linea de pobreza (CBT)",
    },
    "linea de indigencia": {
        "action": "query_series",
        "params": {"series_ids": ["150.1_LA_INDICIA_0_D_16"]},
        "confidence": 0.95,
        "description": "Linea de indigencia (CBA)",
    },
    # ── Comercio exterior ──────────────────────────────────────
    "exportaciones": {
        "action": "query_series",
        "params": {"series_ids": ["74.3_IET_0_M_16"]},
        "confidence": 0.95,
        "description": "Exportaciones totales",
    },
    "importaciones": {
        "action": "query_series",
        "params": {"series_ids": ["74.3_IIT_0_M_25"]},
        "confidence": 0.95,
        "description": "Importaciones totales",
    },
    "balanza comercial": {
        "action": "query_series",
        "params": {"series_ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"]},
        "confidence": 0.95,
        "description": "Balanza comercial (exportaciones e importaciones)",
    },
    "comercio exterior": {
        "action": "query_series",
        "params": {"series_ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"]},
        "confidence": 0.90,
        "description": "Comercio exterior",
    },
    "saldo comercial": {
        "action": "query_series",
        "params": {"series_ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"]},
        "confidence": 0.90,
        "description": "Saldo de balanza comercial",
    },
    # ── Comercio exterior → query_sandbox (cached tables) ─────
    "exportaciones por provincia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*export*", "cache_indec_comercio_exterior*"]},
        "confidence": 0.95,
        "description": "Exportaciones por provincia (datos tabulares cacheados)",
    },
    "importaciones por provincia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*import*", "cache_indec_comercio_exterior*"]},
        "confidence": 0.95,
        "description": "Importaciones por provincia (datos tabulares cacheados)",
    },
    "exportaciones por": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*export*", "cache_indec_comercio_exterior*"]},
        "confidence": 0.92,
        "description": "Exportaciones desglosadas (datos tabulares cacheados)",
    },
    "importaciones por": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*import*", "cache_indec_comercio_exterior*"]},
        "confidence": 0.92,
        "description": "Importaciones desglosadas (datos tabulares cacheados)",
    },
    "exportaciones e importaciones": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*export*", "cache_*import*", "cache_indec_comercio_exterior*"]
        },
        "confidence": 0.92,
        "description": "Exportaciones e importaciones (datos tabulares cacheados)",
    },
    "aranceles": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*arancel*", "cache_*import*", "cache_indec_comercio_exterior*"]
        },
        "confidence": 0.90,
        "description": "Aranceles de importacion / exportacion (datos cacheados)",
    },
    "aranceles de importacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*arancel*", "cache_*import*"]},
        "confidence": 0.95,
        "description": "Aranceles de importacion (datos cacheados)",
    },
    "aranceles de exportacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*arancel*", "cache_*export*"]},
        "confidence": 0.95,
        "description": "Aranceles de exportacion (datos cacheados)",
    },
    "comercio exterior por": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*export*", "cache_*import*", "cache_indec_comercio_exterior*"]
        },
        "confidence": 0.92,
        "description": "Comercio exterior desglosado (datos tabulares cacheados)",
    },
    "aduana": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*aduana*", "cache_*export*", "cache_*import*"]},
        "confidence": 0.85,
        "description": "Datos de aduana / comercio exterior (datos cacheados)",
    },
    # ── Presupuesto → query_sandbox (cached) ───────────────────
    "presupuesto": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_*"]},
        "confidence": 0.85,
        "description": "Presupuesto nacional (datos cacheados)",
    },
    "gasto publico": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_*"]},
        "confidence": 0.85,
        "description": "Gasto publico nacional",
    },
    "ejecucion presupuestaria": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_ejecucion_presupuestaria_*"]},
        "confidence": 0.90,
        "description": "Ejecucion presupuestaria",
    },
    "credito vigente": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_credito_presupuestario_*"]},
        "confidence": 0.90,
        "description": "Credito presupuestario vigente",
    },
    "credito presupuestario": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_credito_presupuestario_*"]},
        "confidence": 0.90,
        "description": "Credito presupuestario",
    },
    "devengado": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_ejecucion_presupuestaria_*"]},
        "confidence": 0.85,
        "description": "Devengado presupuestario",
    },
    "deuda publica": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_deuda_publica_*"]},
        "confidence": 0.90,
        "description": "Deuda publica nacional",
    },
    "transferencias": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_transferencias_*"]},
        "confidence": 0.85,
        "description": "Transferencias presupuestarias",
    },
    "planta de personal": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_planta_de_personal_*"]},
        "confidence": 0.90,
        "description": "Planta de personal del Estado nacional",
    },
    "ingresos fiscales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_recursos_recaudacion_*"]},
        "confidence": 0.85,
        "description": "Ingresos fiscales / recaudacion",
    },
    "recaudacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_recursos_recaudacion_*"]},
        "confidence": 0.85,
        "description": "Recaudacion tributaria",
    },
    "coparticipacion": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_presupuesto_*"],
            "table_notes": (
                "No existe una tabla unica cache_presupuesto_coparticipacion_federal_*. "
                "Buscá en tablas de presupuesto nacional columnas o descripciones "
                "relacionadas con coparticipacion, transferencias automaticas o distribucion federal."
            ),
        },
        "confidence": 0.90,
        "description": "Coparticipacion federal de impuestos",
    },
    "ingresos tributarios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_ingresos_tributarios_*"]},
        "confidence": 0.90,
        "description": "Ingresos tributarios nacionales",
    },
    "resultado fiscal": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_presupuesto_resultado_financiero_*",
                "cache_presupuesto_resultado_primario_*",
            ]
        },
        "confidence": 0.85,
        "description": "Resultado fiscal / financiero",
    },
    "deficit fiscal": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_presupuesto_resultado_financiero_*",
                "cache_presupuesto_resultado_primario_*",
            ]
        },
        "confidence": 0.85,
        "description": "Deficit fiscal",
    },
    "superavit fiscal": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_presupuesto_resultado_financiero_*",
                "cache_presupuesto_resultado_primario_*",
            ]
        },
        "confidence": 0.85,
        "description": "Superavit fiscal",
    },
    # ── Presupuesto: Dimensiones / Clasificadores ──────────────
    "clasificador presupuestario": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_*"]},
        "confidence": 0.90,
        "description": "Clasificadores presupuestarios (dimensiones)",
    },
    "apertura programatica": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_apertura_programatica_*"]},
        "confidence": 0.90,
        "description": "Apertura programatica (programas, subprogramas, proyectos)",
    },
    "clasificador economico": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_clasificador_economico_*"]},
        "confidence": 0.90,
        "description": "Clasificador economico del gasto",
    },
    "finalidad y funcion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_finalidad_funcion_*"]},
        "confidence": 0.90,
        "description": "Finalidad y funcion del gasto",
    },
    "fuente de financiamiento": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_fuente_financiamiento_*"]},
        "confidence": 0.90,
        "description": "Fuente de financiamiento del presupuesto",
    },
    "ubicacion geografica del gasto": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_presupuesto_dim_ubicacion_geografica_*"]},
        "confidence": 0.90,
        "description": "Ubicacion geografica del gasto presupuestario",
    },
    # ── Presupuesto: Gasto publico (series de tiempo) ──────────
    "gasto publico historico": {
        "action": "query_series",
        "params": {"series_ids": ["451.3_GPNGPN_0_0_3_30"]},
        "confidence": 0.90,
        "description": "Gasto publico nacional historico (serie anual)",
    },
    # ── INDEC → query_sandbox (cached, 19 datasets) ────────────
    "indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_*"]},
        "confidence": 0.80,
        "description": "Datos tabulares del INDEC",
    },
    "ipc aperturas": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_ipc*"]},
        "confidence": 0.90,
        "description": "IPC aperturas mensuales (INDEC)",
    },
    "emae indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_emae*"]},
        "confidence": 0.90,
        "description": "EMAE mensual base 2004 (INDEC)",
    },
    "pib trimestral": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_pib*"]},
        "confidence": 0.90,
        "description": "PIB trimestral oferta y demanda (INDEC)",
    },
    "ipi manufacturero": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_ipi_manufacturero*"]},
        "confidence": 0.90,
        "description": "IPI Manufacturero - produccion industrial (INDEC)",
    },
    "construccion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_isac*"]},
        "confidence": 0.85,
        "description": "ISAC - Indicador de actividad de la construccion (INDEC)",
    },
    "isac": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_isac*"]},
        "confidence": 0.90,
        "description": "ISAC - Construccion (INDEC)",
    },
    "comercio exterior indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_comercio_exterior*"]},
        "confidence": 0.90,
        "description": "Indices de comercio exterior (INDEC)",
    },
    "supermercados": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_supermercados*"]},
        "confidence": 0.90,
        "description": "Ventas en supermercados (INDEC)",
    },
    "salarios indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_salarios*"]},
        "confidence": 0.90,
        "description": "Indice y variacion de salarios (INDEC)",
    },
    "cvs salarial": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_salarios_cvs*"]},
        "confidence": 0.90,
        "description": "Coeficiente de variacion salarial diario (INDEC)",
    },
    "canasta basica indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_canasta_basica*"]},
        "confidence": 0.90,
        "description": "CBA y CBT serie historica (INDEC)",
    },
    "pobreza indec": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_pobreza*"]},
        "confidence": 0.90,
        "description": "Pobreza e indigencia (INDEC)",
    },
    "eph": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_eph*"]},
        "confidence": 0.85,
        "description": "EPH - Encuesta Permanente de Hogares (INDEC)",
    },
    "eph hogares": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_eph_hogares*"]},
        "confidence": 0.90,
        "description": "EPH indicadores de hogares (INDEC)",
    },
    "eph tasas": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_eph_tasas*"]},
        "confidence": 0.90,
        "description": "EPH tasas e indicadores laborales (INDEC)",
    },
    "distribucion del ingreso": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_distribucion_ingreso*"]},
        "confidence": 0.90,
        "description": "Distribucion del ingreso (INDEC)",
    },
    "gini": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_distribucion_ingreso*"]},
        "confidence": 0.85,
        "description": "Coeficiente de Gini (INDEC)",
    },
    "turismo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_turismo*"]},
        "confidence": 0.85,
        "description": "Turismo receptivo y emisivo (INDEC)",
    },
    "turismo receptivo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_turismo_receptivo*"]},
        "confidence": 0.90,
        "description": "Turismo receptivo total vias (INDEC)",
    },
    "turismo emisivo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_turismo_emisivo*"]},
        "confidence": 0.90,
        "description": "Turismo emisivo total vias (INDEC)",
    },
    "balance de pagos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_balance_pagos*"]},
        "confidence": 0.90,
        "description": "Balance de pagos mensual (INDEC)",
    },
    "cuenta corriente": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_balance_pagos*"]},
        "confidence": 0.85,
        "description": "Cuenta corriente / balance de pagos (INDEC)",
    },
    "uso del tiempo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_uso_tiempo"]},
        "confidence": 0.90,
        "description": "Encuesta de uso del tiempo (INDEC)",
    },
    "pib provincial": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_indec_pib_provincial"]},
        "confidence": 0.90,
        "description": "PIB provincial (INDEC)",
    },
    # ── DDJJ ───────────────────────────────────────────────────
    "declaracion jurada": {
        "action": "query_ddjj",
        "params": {"action": "ranking"},
        "confidence": 0.95,
        "description": "Declaraciones juradas patrimoniales de diputados",
    },
    "declaraciones juradas": {
        "action": "query_ddjj",
        "params": {"action": "ranking"},
        "confidence": 0.95,
        "description": "Declaraciones juradas patrimoniales de diputados",
    },
    "ddjj": {
        "action": "query_ddjj",
        "params": {"action": "ranking"},
        "confidence": 0.95,
        "description": "DDJJ patrimoniales de diputados",
    },
    "patrimonio diputados": {
        "action": "query_ddjj",
        "params": {"action": "ranking", "sortBy": "patrimonio"},
        "confidence": 0.95,
        "description": "Patrimonio de diputados nacionales",
    },
    "bienes diputados": {
        "action": "query_ddjj",
        "params": {"action": "ranking", "sortBy": "bienes"},
        "confidence": 0.90,
        "description": "Bienes de diputados nacionales",
    },
    "patrimonio legisladores": {
        "action": "query_ddjj",
        "params": {"action": "ranking", "sortBy": "patrimonio"},
        "confidence": 0.90,
        "description": "Patrimonio de legisladores",
    },
    "diputados mas ricos": {
        "action": "query_ddjj",
        "params": {"action": "ranking", "sortBy": "patrimonio", "order": "desc"},
        "confidence": 0.95,
        "description": "Ranking de diputados con mayor patrimonio",
    },
    "diputados mas pobres": {
        "action": "query_ddjj",
        "params": {"action": "ranking", "sortBy": "patrimonio", "order": "asc"},
        "confidence": 0.95,
        "description": "Ranking de diputados con menor patrimonio",
    },
    # ── Sesiones ───────────────────────────────────────────────
    "sesion": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.85,
        "description": "Transcripciones de sesiones del Congreso",
    },
    "debate": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.80,
        "description": "Debates parlamentarios",
    },
    "discurso": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.80,
        "description": "Discursos en sesiones del Congreso",
    },
    "taquigrafica": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.90,
        "description": "Version taquigrafica de sesiones",
    },
    "diario de sesiones": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.95,
        "description": "Diario de sesiones del Congreso",
    },
    "transcripcion": {
        "action": "query_sesiones",
        "params": {},
        "confidence": 0.80,
        "description": "Transcripciones de sesiones",
    },
    # ── Personal / Staff (Diputados + Senado) ───────────────────
    # NOTE: Do NOT hardcode action=stats in params. The LLM planner
    # chooses the sub-action (get_by_legislator, count, stats, search)
    # based on the user query. The hint only tells it to use query_staff.
    "asesores": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Asesores / personal legislativo (Diputados y Senado). Usa action=get_by_legislator con name=NOMBRE para buscar por legislador, action=count para contar, action=stats para estadisticas generales.",
    },
    "personal diputados": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Personal de la Camara de Diputados",
    },
    "personal senado": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Personal del Senado de la Nacion",
    },
    "asesores del senado": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Asesores del Senado de la Nacion",
    },
    "asesores senadores": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Asesores de senadores",
    },
    "senador": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.75,
        "description": "Personal / asesores de senador. Usa action=get_by_legislator con name=NOMBRE.",
    },
    "empleados del senado": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Empleados del Senado de la Nacion",
    },
    "empleados congreso": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.85,
        "description": "Empleados del Congreso (Diputados y Senado)",
    },
    "nomina personal": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Nomina de personal legislativo",
    },
    "nomina hcdn": {
        "action": "query_staff",
        "params": {},
        "confidence": 0.90,
        "description": "Nomina de personal de HCDN",
    },
    # ── BCRA ───────────────────────────────────────────────────
    "bcra": {
        "action": "query_bcra",
        "params": {"tipo": "variables"},
        "confidence": 0.85,
        "description": "Variables monetarias del BCRA",
    },
    "variables monetarias": {
        "action": "query_bcra",
        "params": {"tipo": "variables"},
        "confidence": 0.90,
        "description": "Principales variables monetarias del BCRA",
    },
    "circulacion monetaria": {
        "action": "query_bcra",
        "params": {"tipo": "variables"},
        "confidence": 0.85,
        "description": "Circulacion monetaria",
    },
    # ── BCRA cached → query_sandbox ────────────────────────────
    "cotizaciones bcra": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bcra_cotizaciones"]},
        "confidence": 0.90,
        "description": "Cotizaciones cambiarias BCRA (datos cacheados)",
    },
    "variables bcra historicas": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bcra_variables"]},
        "confidence": 0.85,
        "description": "Variables monetarias BCRA historicas (datos cacheados)",
    },
    # ── Elecciones → query_sandbox (cached tables) ─────────────
    "elecciones": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_*"]},
        "confidence": 0.80,
        "description": "Resultados electorales",
    },
    "elecciones 2023": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_2023_*"]},
        "confidence": 0.90,
        "description": "Resultados elecciones 2023",
    },
    "elecciones 2019": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_2019_*"]},
        "confidence": 0.90,
        "description": "Resultados elecciones 2019",
    },
    "elecciones 2017": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_2017_*"]},
        "confidence": 0.90,
        "description": "Resultados elecciones 2017",
    },
    "elecciones 2015": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_2015_*"]},
        "confidence": 0.90,
        "description": "Resultados elecciones 2015",
    },
    "elecciones 2025": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_2025_*"]},
        "confidence": 0.90,
        "description": "Resultados elecciones 2025",
    },
    "resultados electorales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_*"]},
        "confidence": 0.85,
        "description": "Resultados electorales",
    },
    "votacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_elecciones_*"]},
        "confidence": 0.70,
        "description": "Resultados de votaciones",
    },
    # ── Votaciones nominales → query_sandbox ───────────────────
    "votaciones nominales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_votaciones_nominales_*"]},
        "confidence": 0.90,
        "description": "Votaciones nominales en el Congreso",
    },
    "voto nominal": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_votaciones_nominales_*"]},
        "confidence": 0.85,
        "description": "Votos nominales de diputados",
    },
    # ── Buenos Aires Compras → query_sandbox ───────────────────
    "compras publicas": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bac_*"]},
        "confidence": 0.85,
        "description": "Compras publicas de Buenos Aires (BAC)",
    },
    "buenos aires compras": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bac_*"]},
        "confidence": 0.95,
        "description": "Buenos Aires Compras (BAC)",
    },
    "licitaciones": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bac_tenders"]},
        "confidence": 0.85,
        "description": "Licitaciones de Buenos Aires Compras",
    },
    "contratos publicos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bac_contracts"]},
        "confidence": 0.85,
        "description": "Contratos publicos (BAC)",
    },
    # ── Rosario → query_sandbox ────────────────────────────────
    "datos rosario": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_rosario_*"]},
        "confidence": 0.90,
        "description": "Datos abiertos de Rosario",
    },
    # ── Senado → query_sandbox ─────────────────────────────────
    "senado": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_senado_*"]},
        "confidence": 0.85,
        "description": "Datos abiertos del Senado",
    },
    "senadores": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_senado_*"]},
        "confidence": 0.80,
        "description": "Datos de senadores",
    },
    # ── Cordoba Legislatura → query_sandbox ────────────────────
    "legislatura cordoba": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_cordoba_leg_*"]},
        "confidence": 0.90,
        "description": "Datos de la legislatura de Cordoba",
    },
    "cordoba legislatura": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_cordoba_leg_*"]},
        "confidence": 0.90,
        "description": "Datos de la legislatura de Cordoba",
    },
    # ── Siniestros viales → query_sandbox ──────────────────────
    "siniestros viales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_siniestros_viales_*"]},
        "confidence": 0.90,
        "description": "Siniestros viales",
    },
    "accidentes de transito": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_siniestros_viales_*"]},
        "confidence": 0.85,
        "description": "Accidentes de transito / siniestros viales",
    },
    # ── Educacion → query_sandbox (110 cache tables) ────────────
    "educacion": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*educaci*",
                "cache_*escuel*",
                "cache_*docent*",
                "cache_*universid*",
                "cache_*aprender*",
            ]
        },
        "confidence": 0.80,
        "description": "Datos de educacion (110 tablas cacheadas)",
    },
    "escuela": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*escuel*"]},
        "confidence": 0.80,
        "description": "Datos de escuelas",
    },
    "escuelas": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*escuel*"]},
        "confidence": 0.80,
        "description": "Datos de escuelas",
    },
    "docentes": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*docent*"]},
        "confidence": 0.80,
        "description": "Datos de docentes",
    },
    "universidad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*universid*"]},
        "confidence": 0.80,
        "description": "Datos de universidades",
    },
    "matricula escolar": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*matricul*"]},
        "confidence": 0.85,
        "description": "Matricula escolar",
    },
    # ── Salud → query_sandbox (119 cache tables) ──────────────
    "salud": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*salud*", "cache_*hospital*", "cache_*vacun*", "cache_*mortalid*"]
        },
        "confidence": 0.80,
        "description": "Datos de salud (119 tablas cacheadas)",
    },
    "vacunacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*vacun*"]},
        "confidence": 0.85,
        "description": "Datos de vacunacion",
    },
    "hospital": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*hospital*"]},
        "confidence": 0.85,
        "description": "Datos de hospitales",
    },
    "tasa de mortalidad fetal": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_tasa_de_mortalidad_fetal_por_sexo",
                "cache_tasa_de_mortalidad_fetal_total",
            ],
            "table_notes": (
                "cache_tasa_de_mortalidad_fetal_por_sexo: tasa nacional por sexo (cols: anio TEXT, total, varon, mujer). "
                "Cubre 1980-2012. Usar para consultas históricas o nacionales. "
                "IMPORTANTE: la columna anio es TEXT, filtrar con WHERE anio = '1980' (con comillas). "
                "cache_tasa_de_mortalidad_fetal_total: tasa por provincia (cols: indice_tiempo, mortalidad_fetal_total_argentina, ..._caba, ..._buenosaires, etc). "
                "Cubre 2006-2023. Usar para consultas por provincia."
            ),
        },
        "confidence": 0.95,
        "description": "Tasa de mortalidad fetal: _por_sexo (nacional 1980-2012) o _total (por provincia 2006-2023)",
    },
    "mortalidad fetal": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_tasa_de_mortalidad_fetal_por_sexo",
                "cache_tasa_de_mortalidad_fetal_total",
            ],
            "table_notes": (
                "cache_tasa_de_mortalidad_fetal_por_sexo: tasa nacional 1980-2012 (anio TEXT, total, varon, mujer). "
                "cache_tasa_de_mortalidad_fetal_total: tasa por provincia 2006-2023. "
                "Elegir según período y si pide desglose provincial."
            ),
        },
        "confidence": 0.92,
        "description": "Mortalidad fetal: elegir tabla según período y desglose",
    },
    "tasa de mortalidad infantil": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_tasa_de_mortalidad_infantil*"]},
        "confidence": 0.95,
        "description": "Tasa de mortalidad infantil (tabla pre-agregada)",
    },
    "mortalidad infantil": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*mortalidad_infantil*", "cache_tasa_de_mortalidad_infantil*"]},
        "confidence": 0.92,
        "description": "Mortalidad infantil (prefiere tablas con tasas pre-calculadas)",
    },
    "tasa de mortalidad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_tasa_de_mortalidad*"]},
        "confidence": 0.92,
        "description": "Tasas de mortalidad pre-calculadas",
    },
    "mortalidad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*mortalid*", "cache_*defunci*"]},
        "confidence": 0.85,
        "description": "Datos de mortalidad y defunciones",
    },
    "natalidad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*natalid*", "cache_*nacimiento*"]},
        "confidence": 0.85,
        "description": "Datos de natalidad y nacimientos",
    },
    "covid": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*covid*"]},
        "confidence": 0.90,
        "description": "Datos de COVID-19",
    },
    # ── Seguridad → query_sandbox (35 cache tables) ──────────
    "seguridad": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*seguridad*", "cache_*delito*", "cache_*polic*", "cache_*homicid*"]
        },
        "confidence": 0.80,
        "description": "Datos de seguridad (35 tablas cacheadas)",
    },
    "delitos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*"]},
        "confidence": 0.85,
        "description": "Datos de delitos",
    },
    "homicidios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*homicid*"]},
        "confidence": 0.85,
        "description": "Datos de homicidios",
    },
    "femicidios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*femicid*"]},
        "confidence": 0.90,
        "description": "Datos de femicidios",
    },
    "robos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*robo*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de robos y delitos contra la propiedad",
    },
    "robo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*robo*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de robos y delitos contra la propiedad",
    },
    "robos en caba": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*robo*", "cache_*seguridad*"]},
        "confidence": 0.90,
        "description": "Robos en Ciudad Autonoma de Buenos Aires",
    },
    "robos por comuna": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*robo*", "cache_*seguridad*"]},
        "confidence": 0.90,
        "description": "Robos desagregados por comuna",
    },
    "hurtos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*hurto*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de hurtos",
    },
    "hurto": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*hurto*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de hurtos",
    },
    "denuncias": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*denuncia*", "cache_*delito*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de denuncias penales",
    },
    "denuncia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*denuncia*", "cache_*delito*", "cache_*seguridad*"]},
        "confidence": 0.85,
        "description": "Datos de denuncias penales",
    },
    "denuncias penales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*denuncia*", "cache_*delito*", "cache_*seguridad*"]},
        "confidence": 0.90,
        "description": "Denuncias penales",
    },
    "crimen": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*seguridad*", "cache_*homicid*"]},
        "confidence": 0.80,
        "description": "Datos de criminalidad",
    },
    "criminalidad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*seguridad*", "cache_*homicid*"]},
        "confidence": 0.85,
        "description": "Datos de criminalidad",
    },
    "inseguridad": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*", "cache_*seguridad*", "cache_*denuncia*"]},
        "confidence": 0.80,
        "description": "Datos de inseguridad y delitos",
    },
    "delito": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*delito*"]},
        "confidence": 0.85,
        "description": "Datos de delitos",
    },
    "datasets de seguridad": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*seguridad*",
                "cache_*delito*",
                "cache_*polic*",
                "cache_*homicid*",
                "cache_*denuncia*",
            ]
        },
        "confidence": 0.90,
        "description": "Datasets de seguridad publica",
    },
    "siniestros": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_siniestros_viales_*", "cache_*siniestro*"]},
        "confidence": 0.85,
        "description": "Siniestros (viales y otros)",
    },
    "accidentes viales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_siniestros_viales_*"]},
        "confidence": 0.88,
        "description": "Accidentes viales / siniestros viales",
    },
    "policia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*polic*", "cache_*seguridad*"]},
        "confidence": 0.80,
        "description": "Datos policiales y de seguridad",
    },
    # ── Transporte → query_sandbox (103 cache tables) ─────────
    "transporte": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*transport*",
                "cache_*transit*",
                "cache_*vehicul*",
                "cache_*subte*",
                "cache_*ferrov*",
                "cache_*ferrocarril*",
                "cache_*tren*",
                "cache_*colectiv*",
                "cache_*sube*",
                "cache_*pasajero*",
            ]
        },
        "confidence": 0.80,
        "description": "Datos de transporte (subte, trenes, colectivos, SUBE, etc.)",
    },
    "transporte publico": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*transport*",
                "cache_*subte*",
                "cache_*colectiv*",
                "cache_*sube*",
                "cache_*ferrov*",
                "cache_*tren*",
                "cache_*pasajero*",
                "cache_*metrobus*",
            ]
        },
        "confidence": 0.90,
        "description": "Transporte publico de pasajeros",
    },
    "subsidios al transporte": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_subsidios_al_transporte*"]},
        "confidence": 0.95,
        "description": "Subsidios al transporte publico de pasajeros",
    },
    "ferrocarril": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*ferrov*",
                "cache_*ferrocarril*",
                "cache_*tren*",
                "cache_*estacion*ferrov*",
                "cache_*estacion*tren*",
                "cache_subte_trenes_despachados",
                "cache_cruces_ferroviarios",
            ]
        },
        "confidence": 0.90,
        "description": "Datos de ferrocarriles y trenes",
    },
    "tren": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*ferrov*",
                "cache_*ferrocarril*",
                "cache_*tren*",
                "cache_*estacion*ferrov*",
                "cache_*estacion*tren*",
                "cache_subte_trenes_despachados",
                "cache_cruces_ferroviarios",
            ]
        },
        "confidence": 0.90,
        "description": "Datos de trenes y sistema ferroviario",
    },
    "trenes": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*ferrov*",
                "cache_*ferrocarril*",
                "cache_*tren*",
                "cache_*estacion*ferrov*",
                "cache_*estacion*tren*",
                "cache_subte_trenes_despachados",
                "cache_cruces_ferroviarios",
            ]
        },
        "confidence": 0.90,
        "description": "Datos de trenes y sistema ferroviario",
    },
    "ferroviario": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*ferrov*",
                "cache_*ferrocarril*",
                "cache_*tren*",
                "cache_*estacion*ferrov*",
                "cache_*estacion*tren*",
                "cache_subte_trenes_despachados",
                "cache_cruces_ferroviarios",
            ]
        },
        "confidence": 0.90,
        "description": "Sistema ferroviario",
    },
    "subte": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*subte*",
                "cache_subte_trenes_despachados",
                "cache_subte_cronograma_de_servicio",
            ]
        },
        "confidence": 0.90,
        "description": "Datos del subte de Buenos Aires",
    },
    "colectivo": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*colectiv*",
                "cache_lineas_y_empresas_de_transporte_urbano_de_pasajero",
                "cache_l_neas_de_transporte_de_rmba",
                "cache_empresas_de_transporte_de_rmba",
            ]
        },
        "confidence": 0.85,
        "description": "Datos de colectivos y lineas urbanas",
    },
    "colectivos": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*colectiv*",
                "cache_lineas_y_empresas_de_transporte_urbano_de_pasajero",
                "cache_l_neas_de_transporte_de_rmba",
                "cache_empresas_de_transporte_de_rmba",
            ]
        },
        "confidence": 0.85,
        "description": "Datos de colectivos y lineas urbanas",
    },
    "sube": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*sube*",
                "cache_sube_operaciones_de_viaje_por_mes_de_regi_n_metrop",
                "cache_sube_cantidad_de_transacciones_usos_por_fecha",
                "cache_sube_estudiantil",
                "cache_puntos_de_carga_sube",
            ]
        },
        "confidence": 0.90,
        "description": "Datos de tarjeta SUBE (transacciones, operaciones, puntos de carga)",
    },
    "tarjeta sube": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*sube*"]},
        "confidence": 0.95,
        "description": "Datos de tarjeta SUBE",
    },
    "ecobici": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*ecobici*", "cache_*biciclet*"]},
        "confidence": 0.90,
        "description": "Datos de Ecobici / bicicletas",
    },
    "metrobus": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*metrobus*"]},
        "confidence": 0.90,
        "description": "Datos del Metrobus",
    },
    "patentamiento": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*patentamient*", "cache_*vehicul*"]},
        "confidence": 0.85,
        "description": "Datos de patentamiento de vehiculos",
    },
    # ── Energia → query_sandbox (78 cache tables) ─────────────
    "energia": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*energ*", "cache_*electri*", "cache_*petrole*", "cache_*hidrocarb*"]
        },
        "confidence": 0.80,
        "description": "Datos de energia (78 tablas cacheadas)",
    },
    "petroleo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*petrole*", "cache_*crudo*"]},
        "confidence": 0.85,
        "description": "Datos de petroleo",
    },
    "gas natural": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*gas_*"]},
        "confidence": 0.85,
        "description": "Datos de gas natural",
    },
    "nafta": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*nafta*", "cache_*combustibl*"]},
        "confidence": 0.85,
        "description": "Datos de nafta y combustibles",
    },
    "energia renovable": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*renovabl*", "cache_*eolic*", "cache_*solar*"]},
        "confidence": 0.85,
        "description": "Datos de energia renovable",
    },
    # ── Agro → query_sandbox (125 cache tables) ───────────────
    "agro": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*agro*", "cache_*ganad*", "cache_*agric*", "cache_*senasa*"]},
        "confidence": 0.80,
        "description": "Datos agropecuarios (125 tablas cacheadas)",
    },
    "agricultura": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*agric*", "cache_*cultiv*", "cache_*cosech*"]},
        "confidence": 0.85,
        "description": "Datos de agricultura",
    },
    "ganaderia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*ganad*", "cache_*bovino*", "cache_*porcin*"]},
        "confidence": 0.85,
        "description": "Datos de ganaderia",
    },
    "soja": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*soja*"]},
        "confidence": 0.90,
        "description": "Datos de soja",
    },
    "trigo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*trigo*"]},
        "confidence": 0.90,
        "description": "Datos de trigo",
    },
    "senasa": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*senasa*"]},
        "confidence": 0.90,
        "description": "Datos de SENASA",
    },
    "lecheria": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*leche*", "cache_*tambo*"]},
        "confidence": 0.85,
        "description": "Datos de lecheria y tambos",
    },
    # ── Justicia → query_sandbox (21 cache tables) ────────────
    "justicia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*justici*", "cache_*judicial*", "cache_*penal*"]},
        "confidence": 0.80,
        "description": "Datos de justicia (21 tablas cacheadas)",
    },
    "anticorrupcion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*anticorrupci*"]},
        "confidence": 0.85,
        "description": "Datos de anticorrupcion",
    },
    # ── Ambiente → query_sandbox (71 cache tables) ────────────
    "ambiente": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*ambient*", "cache_*forestal*", "cache_*bosqu*", "cache_*residuo*"]
        },
        "confidence": 0.80,
        "description": "Datos de medio ambiente (71 tablas cacheadas)",
    },
    "medio ambiente": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*ambient*", "cache_*contamin*"]},
        "confidence": 0.85,
        "description": "Datos de medio ambiente",
    },
    "contaminacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*contamin*", "cache_*ambient*", "cache_*efluent*"]},
        "confidence": 0.85,
        "description": "Datos de contaminacion ambiental",
    },
    "bosques": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*bosqu*", "cache_*forestal*"]},
        "confidence": 0.85,
        "description": "Datos de bosques y deforestacion",
    },
    "incendios forestales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*incendi*"]},
        "confidence": 0.85,
        "description": "Datos de incendios forestales",
    },
    # ── Genero → query_sandbox (47 cache tables) ──────────────
    "genero": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*genero*", "cache_*mujer*", "cache_*femeni*"]},
        "confidence": 0.80,
        "description": "Datos de genero (47 tablas cacheadas)",
    },
    "brecha de genero": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*genero*", "cache_*brecha*"]},
        "confidence": 0.85,
        "description": "Datos de brecha de genero",
    },
    "violencia de genero": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*violencia*genero*", "cache_*femicid*"]},
        "confidence": 0.85,
        "description": "Datos de violencia de genero",
    },
    # ── Telecomunicaciones → query_sandbox (60 cache tables) ──
    "telecomunicaciones": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_*teleco*",
                "cache_*internet*",
                "cache_*telefon*",
                "cache_*conectivid*",
            ]
        },
        "confidence": 0.80,
        "description": "Datos de telecomunicaciones (60 tablas cacheadas)",
    },
    "banda ancha": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*internet*", "cache_*banda_anch*", "cache_*conectivid*"]},
        "confidence": 0.85,
        "description": "Datos de banda ancha e internet",
    },
    "telefonia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*telefon*"]},
        "confidence": 0.85,
        "description": "Datos de telefonia",
    },
    "celulares": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*celular*", "cache_*telefon*movil*", "cache_*telefon*"]},
        "confidence": 0.80,
        "description": "Datos de telefonia celular",
    },
    "celular": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*celular*", "cache_*telefon*"]},
        "confidence": 0.80,
        "description": "Datos de telefonia celular",
    },
    # ── Poblacion / Censo → query_sandbox (28 cache tables) ────
    "censo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*", "cache_*poblaci*", "cache_*demograf*"]},
        "confidence": 0.85,
        "description": "Datos censales y demograficos",
    },
    "censo 2022": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*2022*", "cache_*censo*", "cache_*poblaci*"]},
        "confidence": 0.95,
        "description": "Censo Nacional 2022",
    },
    "censo 2010": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*2010*", "cache_*censo*", "cache_*poblaci*"]},
        "confidence": 0.95,
        "description": "Censo Nacional 2010",
    },
    "ultimo censo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*", "cache_*poblaci*", "cache_*demograf*"]},
        "confidence": 0.90,
        "description": "Datos del ultimo censo nacional",
    },
    "datos del censo": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*", "cache_*poblaci*", "cache_*demograf*"]},
        "confidence": 0.90,
        "description": "Datos del censo nacional",
    },
    "datos censales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*censo*", "cache_*poblaci*", "cache_*demograf*"]},
        "confidence": 0.90,
        "description": "Datos censales",
    },
    "poblacion": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*demograf*", "cache_*habitant*"]
        },
        "confidence": 0.85,
        "description": "Datos de poblacion (censos y proyecciones demograficas)",
    },
    "poblacion de": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*demograf*", "cache_*habitant*"]
        },
        "confidence": 0.90,
        "description": "Poblacion de una localidad, provincia o departamento",
    },
    "cuanta poblacion": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*demograf*", "cache_*habitant*"]
        },
        "confidence": 0.90,
        "description": "Cantidad de poblacion",
    },
    "habitantes": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*habitant*", "cache_*demograf*"]
        },
        "confidence": 0.85,
        "description": "Cantidad de habitantes (censos y proyecciones)",
    },
    "cuantos habitantes": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*habitant*", "cache_*demograf*"]
        },
        "confidence": 0.90,
        "description": "Cantidad de habitantes de una localidad o provincia",
    },
    "cantidad de habitantes": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*habitant*", "cache_*demograf*"]
        },
        "confidence": 0.90,
        "description": "Cantidad de habitantes",
    },
    "densidad poblacional": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*densidad*", "cache_*demograf*"]
        },
        "confidence": 0.90,
        "description": "Densidad poblacional",
    },
    "crecimiento poblacional": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*poblaci*", "cache_*censo*", "cache_*demograf*"]},
        "confidence": 0.85,
        "description": "Crecimiento demografico / poblacional",
    },
    "piramide poblacional": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_*poblaci*", "cache_*censo*", "cache_*demograf*", "cache_*piramide*"]
        },
        "confidence": 0.90,
        "description": "Piramide poblacional / estructura etaria",
    },
    "proyecciones de poblacion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*poblaci*", "cache_*proyecci*", "cache_*demograf*"]},
        "confidence": 0.90,
        "description": "Proyecciones de poblacion (INDEC)",
    },
    "demografia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*demograf*", "cache_*poblaci*", "cache_*censo*"]},
        "confidence": 0.85,
        "description": "Datos demograficos",
    },
    "demografico": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*demograf*", "cache_*poblaci*", "cache_*censo*"]},
        "confidence": 0.85,
        "description": "Datos demograficos",
    },
    "migracion": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*migraci*", "cache_*extranje*"]},
        "confidence": 0.85,
        "description": "Datos de migracion",
    },
    # ── Ciencia → query_sandbox (13 cache tables) ─────────────
    "ciencia y tecnologia": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*ciencia*", "cache_*tecnolog*", "cache_*conicet*"]},
        "confidence": 0.80,
        "description": "Datos de ciencia y tecnologia",
    },
    # ── Vivienda → query_sandbox (19 cache tables) ────────────
    "vivienda": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*viviend*", "cache_*construcci*", "cache_*hipotec*"]},
        "confidence": 0.80,
        "description": "Datos de vivienda y construccion",
    },
    # ── Social → query_sandbox (22 cache tables) ──────────────
    "programas sociales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*social*", "cache_*asistenci*", "cache_*beneficiari*"]},
        "confidence": 0.85,
        "description": "Datos de programas sociales",
    },
    "tarjeta alimentar": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*alimentar*"]},
        "confidence": 0.90,
        "description": "Datos de tarjeta alimentar",
    },
    # ── Derechos Humanos → query_sandbox ──────────────────────
    "derechos humanos": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*derecho*human*", "cache_*memoria*", "cache_*desaparecid*"]},
        "confidence": 0.85,
        "description": "Datos de derechos humanos",
    },
    # ── Empleo adicional → query_sandbox (66 cache tables) ────
    "empleo formal": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*empleo*", "cache_*asalariad*"]},
        "confidence": 0.85,
        "description": "Datos de empleo formal",
    },
    "informalidad laboral": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_*informal*", "cache_*laboral*"]},
        "confidence": 0.85,
        "description": "Datos de informalidad laboral",
    },
    # ── Georef ─────────────────────────────────────────────────
    "donde queda": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_georef_localidades",
                "cache_georef_municipios",
                "cache_georef_departamentos",
                "cache_georef_provincias",
            ],
            "query": "ubicación geográfica",
        },
        "confidence": 0.90,
        "description": "Ubicación geográfica (GeoRef)",
    },
    "donde esta": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_georef_localidades",
                "cache_georef_municipios",
                "cache_georef_departamentos",
                "cache_georef_provincias",
            ],
            "query": "ubicación geográfica",
        },
        "confidence": 0.90,
        "description": "Ubicación geográfica (GeoRef)",
    },
    "en que provincia": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_georef_localidades", "cache_georef_municipios"],
            "query": "provincia de localidad",
        },
        "confidence": 0.90,
        "description": "Provincia de una localidad/municipio (GeoRef)",
    },
    "ubicacion de": {
        "action": "query_sandbox",
        "params": {
            "tables": [
                "cache_georef_localidades",
                "cache_georef_municipios",
                "cache_georef_departamentos",
            ],
            "query": "ubicación geográfica",
        },
        "confidence": 0.85,
        "description": "Ubicación geográfica (GeoRef)",
    },
    "coordenadas de": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_georef_localidades", "cache_georef_municipios"],
            "query": "coordenadas geográficas",
        },
        "confidence": 0.85,
        "description": "Coordenadas geográficas (GeoRef)",
    },
    "localidades de": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_georef_localidades"], "query": "localidades de provincia"},
        "confidence": 0.85,
        "description": "Localidades de una provincia (GeoRef)",
    },
    "municipios de": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_georef_municipios"], "query": "municipios de provincia"},
        "confidence": 0.85,
        "description": "Municipios de una provincia (GeoRef)",
    },
    "departamentos de": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_georef_departamentos"], "query": "departamentos de provincia"},
        "confidence": 0.85,
        "description": "Departamentos de una provincia (GeoRef)",
    },
    "cuantas localidades": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_georef_localidades"], "query": "cantidad de localidades"},
        "confidence": 0.85,
        "description": "Cantidad de localidades (GeoRef)",
    },
    "cuantos municipios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_georef_municipios"], "query": "cantidad de municipios"},
        "confidence": 0.85,
        "description": "Cantidad de municipios (GeoRef)",
    },
    "provincia": {
        "action": "query_georef",
        "params": {},
        "confidence": 0.60,
        "description": "Normalizacion geografica (provincias)",
    },
    "municipio": {
        "action": "query_georef",
        "params": {},
        "confidence": 0.60,
        "description": "Normalizacion geografica (municipios)",
    },
    "localidad": {
        "action": "query_georef",
        "params": {},
        "confidence": 0.60,
        "description": "Normalizacion geografica (localidades)",
    },
    # ── Datasets / catalogo → search_ckan ──────────────────────
    "datasets": {
        "action": "search_ckan",
        "params": {"query": "*", "portalId": "nacional", "rows": 20},
        "confidence": 0.80,
        "description": "Catalogo de datasets nacionales",
    },
    "catalogo": {
        "action": "search_ckan",
        "params": {"query": "*", "portalId": "nacional", "rows": 20},
        "confidence": 0.80,
        "description": "Catalogo de datasets nacionales",
    },
    "datos abiertos": {
        "action": "search_ckan",
        "params": {"query": "*", "portalId": "nacional", "rows": 20},
        "confidence": 0.75,
        "description": "Catalogo de datos abiertos",
    },
    # ── Composite "datasets de [tema]" → search_ckan ─────────
    # These override per-topic sandbox routes when the user
    # explicitly asks for "datasets" of a given topic.
    # Include "datasets de X" variant (with preposition) since
    # normalize_query keeps "de" and keyword matching is contiguous.
    "datasets de educacion": {
        "action": "search_ckan",
        "params": {"query": "educación", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de educacion en datos.gob.ar",
    },
    "datasets de salud": {
        "action": "search_ckan",
        "params": {"query": "salud", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de salud en datos.gob.ar",
    },
    "datasets de transporte": {
        "action": "search_ckan",
        "params": {"query": "transporte", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de transporte en datos.gob.ar",
    },
    "datasets de energia": {
        "action": "search_ckan",
        "params": {"query": "energía", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de energia en datos.gob.ar",
    },
    "buscar datasets de seguridad": {
        "action": "search_ckan",
        "params": {"query": "seguridad", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Buscar datasets de seguridad en datos.gob.ar",
    },
    "datasets de ambiente": {
        "action": "search_ckan",
        "params": {"query": "ambiente", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de ambiente en datos.gob.ar",
    },
    "datasets de justicia": {
        "action": "search_ckan",
        "params": {"query": "justicia", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de justicia en datos.gob.ar",
    },
    "datasets de cultura": {
        "action": "search_ckan",
        "params": {"query": "cultura", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de cultura en datos.gob.ar",
    },
    "datasets de empleo": {
        "action": "search_ckan",
        "params": {"query": "empleo", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de empleo en datos.gob.ar",
    },
    "datasets de genero": {
        "action": "search_ckan",
        "params": {"query": "género", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de genero en datos.gob.ar",
    },
    "datasets de economia": {
        "action": "search_ckan",
        "params": {"query": "economía", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de economia en datos.gob.ar",
    },
    "datasets de telecomunicaciones": {
        "action": "search_ckan",
        "params": {"query": "telecomunicaciones", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de telecomunicaciones en datos.gob.ar",
    },
    "datasets de vivienda": {
        "action": "search_ckan",
        "params": {"query": "vivienda", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de vivienda en datos.gob.ar",
    },
    "datasets de presupuesto": {
        "action": "search_ckan",
        "params": {"query": "presupuesto", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets de presupuesto en datos.gob.ar",
    },
    "datasets agro": {
        "action": "search_ckan",
        "params": {"query": "agropecuario", "portalId": "nacional", "rows": 20},
        "confidence": 0.95,
        "description": "Datasets agropecuarios en datos.gob.ar",
    },
    # ── Autoridades / Gobierno PEN → query_sandbox ────────────
    "presidente": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_autoridades_pen_principales"],
            "query": "presidente de la nación",
        },
        "confidence": 0.95,
        "description": "Presidente de la Nación (Mapa del Estado)",
    },
    "presidente de argentina": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_autoridades_pen_principales"],
            "query": "presidente de la nación",
        },
        "confidence": 0.95,
        "description": "Presidente de la República Argentina",
    },
    "quien es el presidente": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_autoridades_pen_principales"],
            "query": "presidente de la nación",
        },
        "confidence": 0.95,
        "description": "Presidente de la Nación",
    },
    "vicepresidente": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "vicepresidente"},
        "confidence": 0.95,
        "description": "Vicepresidente de la Nación",
    },
    "jefe de gabinete": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "jefe de gabinete"},
        "confidence": 0.95,
        "description": "Jefe de Gabinete de Ministros",
    },
    "ministros": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "ministros"},
        "confidence": 0.90,
        "description": "Ministros del PEN",
    },
    "ministro": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "ministro"},
        "confidence": 0.90,
        "description": "Ministros del Poder Ejecutivo Nacional",
    },
    "gabinete": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "gabinete nacional"},
        "confidence": 0.90,
        "description": "Gabinete nacional (ministros y secretarios)",
    },
    "secretarios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "secretarios"},
        "confidence": 0.85,
        "description": "Secretarios del PEN",
    },
    "autoridades nacionales": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_autoridades_pen_principales"],
            "query": "autoridades principales",
        },
        "confidence": 0.90,
        "description": "Autoridades del Poder Ejecutivo Nacional",
    },
    "poder ejecutivo": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_autoridades_pen", "cache_autoridades_pen_principales"],
            "query": "poder ejecutivo nacional",
        },
        "confidence": 0.90,
        "description": "Estructura del Poder Ejecutivo Nacional",
    },
    "autoridades pen": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen_principales"], "query": "autoridades del PEN"},
        "confidence": 0.95,
        "description": "Autoridades del Poder Ejecutivo Nacional",
    },
    "estructura del estado": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen"], "query": "estructura orgánica del estado"},
        "confidence": 0.85,
        "description": "Estructura orgánica del Estado Nacional",
    },
    "organigrama": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_autoridades_pen"], "query": "organigrama del estado"},
        "confidence": 0.85,
        "description": "Organigrama del Estado Nacional",
    },
    # ── Gobernadores → query_sandbox ──────────────────────────
    "gobernador": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_gobernadores"], "query": "gobernador de provincia"},
        "confidence": 0.95,
        "description": "Gobernadores de provincias argentinas",
    },
    "gobernadores": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_gobernadores"], "query": "gobernadores de provincias"},
        "confidence": 0.95,
        "description": "Gobernadores de las 23 provincias + CABA",
    },
    "gobernador de": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_gobernadores"], "query": "gobernador de provincia"},
        "confidence": 0.95,
        "description": "Gobernador de una provincia específica",
    },
    "quien gobierna": {
        "action": "query_sandbox",
        "params": {
            "tables": ["cache_gobernadores", "cache_autoridades_pen_principales"],
            "query": "quien gobierna",
        },
        "confidence": 0.85,
        "description": "Autoridades de gobierno (gobernadores / PEN)",
    },
    # ── Diputados / Legisladores → query_sandbox ─────────────
    "diputados nacionales": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_diputados"], "query": "diputados nacionales"},
        "confidence": 0.90,
        "description": "Lista de diputados nacionales (HCDN)",
    },
    "legisladores": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_diputados", "cache_bloques"], "query": "legisladores"},
        "confidence": 0.85,
        "description": "Legisladores nacionales (diputados + bloques)",
    },
    "bloques parlamentarios": {
        "action": "query_sandbox",
        "params": {"tables": ["cache_bloques"], "query": "bloques parlamentarios"},
        "confidence": 0.90,
        "description": "Bloques parlamentarios de Diputados",
    },
}


# ---------------------------------------------------------------------------
# 2. DOMAIN_PATTERNS — regex patterns matching domain topics to portals
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DomainPattern:
    """A regex pattern that maps a domain topic to a CKAN portal or action."""

    pattern: re.Pattern[str]
    portal_id: str
    description: str


DOMAIN_PATTERNS: list[DomainPattern] = [
    # Nacional sectorial
    DomainPattern(re.compile(r"\bsalud\b", re.IGNORECASE), "salud", "Portal de datos de Salud"),
    DomainPattern(
        re.compile(r"\benerg[ií]a\b", re.IGNORECASE), "energia", "Portal de datos de Energia"
    ),
    DomainPattern(
        re.compile(r"\bjusticia\b", re.IGNORECASE), "justicia", "Portal de datos de Justicia"
    ),
    DomainPattern(
        re.compile(r"\btransporte\b", re.IGNORECASE), "transporte", "Portal de datos de Transporte"
    ),
    DomainPattern(
        re.compile(r"\bcultura\b", re.IGNORECASE), "cultura", "Portal de datos de Cultura"
    ),
    DomainPattern(
        re.compile(r"\bproduccion\b", re.IGNORECASE), "produccion", "Portal de datos de Produccion"
    ),
    DomainPattern(
        re.compile(r"\bmagyp\b|\bagricultura\b|\bganaderia\b|\bagropecuari\w*\b", re.IGNORECASE),
        "magyp",
        "Portal de Agricultura, Ganaderia y Pesca",
    ),
    DomainPattern(re.compile(r"\bacumar\b", re.IGNORECASE), "acumar", "Portal de ACUMAR"),
    DomainPattern(
        re.compile(r"\bministerio del interior\b|\binterior\b", re.IGNORECASE),
        "mininterior",
        "Portal de Interior",
    ),
    DomainPattern(re.compile(r"\bpami\b", re.IGNORECASE), "pami", "Portal de PAMI"),
    DomainPattern(
        re.compile(r"\bdesarrollo social\b|\bprogramas sociales\b", re.IGNORECASE),
        "desarrollo_social",
        "Portal de Desarrollo Social",
    ),
    DomainPattern(
        re.compile(r"\bturismo\b|\byvera\b", re.IGNORECASE), "turismo", "Portal de Turismo (Yvera)"
    ),
    DomainPattern(
        re.compile(r"\bseguro\w*\b|\bssn\b|\bsuperintendencia de seguros\b", re.IGNORECASE),
        "ssn",
        "Portal de Superintendencia de Seguros",
    ),
    # CABA
    DomainPattern(
        re.compile(r"\bcaba\b|\bciudad de buenos aires\b|\bbuenosaires\b", re.IGNORECASE),
        "caba",
        "Portal de CABA",
    ),
    DomainPattern(
        re.compile(r"\blegislatura caba\b|\blegislatura de la ciudad\b", re.IGNORECASE),
        "legislatura_caba",
        "Legislatura de CABA",
    ),
    # Provincias
    DomainPattern(
        re.compile(r"\bprovincia de buenos aires\b|\bpba\b|\bbonaerense\b", re.IGNORECASE),
        "pba",
        "Portal de Buenos Aires Provincia",
    ),
    DomainPattern(
        re.compile(r"\bcordoba estadistica\b", re.IGNORECASE),
        "cordoba_estadistica",
        "Portal de Cordoba Estadistica",
    ),
    DomainPattern(
        re.compile(r"\bcordoba\b", re.IGNORECASE), "cordoba_prov", "Portal de datos de Cordoba"
    ),
    DomainPattern(
        re.compile(r"\bmendoza\b", re.IGNORECASE), "mendoza", "Portal de datos de Mendoza"
    ),
    DomainPattern(
        re.compile(r"\bentre rios\b|\bentrerios\b", re.IGNORECASE),
        "entrerios",
        "Portal de datos de Entre Rios",
    ),
    DomainPattern(
        re.compile(r"\bneuquen\b", re.IGNORECASE), "neuquen_legislatura", "Legislatura de Neuquen"
    ),
    DomainPattern(
        re.compile(r"\btucuman\b", re.IGNORECASE), "tucuman", "Portal de datos de Tucuman"
    ),
    DomainPattern(
        re.compile(r"\bmisiones\b", re.IGNORECASE), "misiones", "Portal de datos de Misiones"
    ),
    DomainPattern(re.compile(r"\bchaco\b", re.IGNORECASE), "chaco", "Portal de datos de Chaco"),
    DomainPattern(
        re.compile(r"\bciudad de mendoza\b", re.IGNORECASE),
        "ciudad_mendoza",
        "Portal de Ciudad de Mendoza",
    ),
    DomainPattern(
        re.compile(r"\bcorrientes\b", re.IGNORECASE), "corrientes", "Portal de Ciudad de Corrientes"
    ),
    # Institucional
    DomainPattern(
        re.compile(r"\bdiputados\b|\bcongreso\b|\bcamara\b", re.IGNORECASE),
        "diputados",
        "Portal de Camara de Diputados",
    ),
    DomainPattern(re.compile(r"\barsat\b", re.IGNORECASE), "arsat", "Portal de ARSAT"),
]


# ---------------------------------------------------------------------------
# Pre-sorted keyword list (longest first) for greedy matching
# ---------------------------------------------------------------------------

_SORTED_KEYWORDS: list[str] = sorted(KEYWORD_ROUTES.keys(), key=len, reverse=True)
_KEYWORD_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (keyword, re.compile(r"\b" + re.escape(keyword) + r"\b")) for keyword in _SORTED_KEYWORDS
]


# ---------------------------------------------------------------------------
# 3. Functions
# ---------------------------------------------------------------------------

# National-only series IDs for employment/unemployment
_NATIONAL_EMPLOYMENT_SERIES = {"45.2_ECTDT_0_T_33"}

# Regional series IDs (7 series: Total + 6 regions)
_REGIONAL_EMPLOYMENT_SERIES = [
    "45.2_ECTDT_0_T_33",  # Total nacional
    "45.2_ECTDTG_0_T_37",  # GBA
    "45.2_ECTDTNO_0_T_42",  # NOA
    "45.2_ECTDTNE_0_T_42",  # NEA
    "45.2_ECTDTCU_0_T_38",  # Cuyo
    "45.2_ECTDTRP_0_T_49",  # Pampeana
    "45.2_ECTDTP_0_T_43",  # Patagonia
]

# Regex that detects geographic/comparison context in normalized queries
_GEO_COMPARISON_RE = re.compile(
    r"\b(?:provincia|provincias|region|regiones|regional|comparar|comparado"
    r"|comparacion|geografic|gba|noa|nea|cuyo|pampeana|patagonia"
    r"|otras provincias|otros paises"
    r"|entre.*(?:cordoba|mendoza|tucuman|rosario|santa fe|buenos aires|salta|jujuy))\b"
)


def _upgrade_employment_to_regional(
    normalized: str,
    seen_actions: dict[str, RoutingHint],
) -> dict[str, RoutingHint]:
    """Replace the national-only employment hint with regional series.

    When the normalized query contains both an employment keyword (desempleo,
    desocupacion, empleo, mercado laboral) AND a geographic/comparison keyword
    (provincia, comparar, region, etc.), the national-only series hint is
    replaced with the 7-region series hint.
    """
    if not _GEO_COMPARISON_RE.search(normalized):
        return seen_actions

    # Check if any matched hint is the national-only employment series
    national_key = None
    for dedup_key, hint in seen_actions.items():
        if (
            hint.action == "query_series"
            and set(hint.params.get("series_ids", [])) == _NATIONAL_EMPLOYMENT_SERIES
        ):
            national_key = dedup_key
            break

    if national_key is None:
        return seen_actions

    # Replace with regional hint
    regional_hint = RoutingHint(
        action="query_series",
        params={"series_ids": list(_REGIONAL_EMPLOYMENT_SERIES)},
        confidence=0.95,
        description=("Desempleo por region: Total, GBA, NOA, NEA, Cuyo, Pampeana, Patagonia"),
    )
    del seen_actions[national_key]
    regional_param_key = str(sorted(regional_hint.params.items()))
    regional_dedup_key = f"{regional_hint.action}:{regional_param_key}"
    seen_actions[regional_dedup_key] = regional_hint

    return seen_actions


def resolve_hints(query: str) -> list[RoutingHint]:
    """Resolve routing hints from a user query using deterministic keyword matching.

    Scans the normalized query for known keywords (longest-first to avoid
    partial matches) and returns matching ``RoutingHint`` objects sorted by
    confidence (highest first).  Duplicates by action are collapsed, keeping
    the highest-confidence match.

    Args:
        query: Raw user query in natural language (Spanish).

    Returns:
        A list of ``RoutingHint`` sorted by confidence descending.
    """
    normalized = normalize_query(query)
    if not normalized:
        return []

    # Track which actions we've already matched (keep highest confidence)
    seen_actions: dict[str, RoutingHint] = {}

    for keyword, pattern in _KEYWORD_PATTERNS:
        # Only match if keyword appears as a whole token sequence
        # Use word boundaries to prevent "gas" matching inside "gastos"
        if pattern.search(normalized):
            route = KEYWORD_ROUTES[keyword]
            hint = RoutingHint(
                action=route["action"],
                params=dict(route["params"]),
                confidence=route["confidence"],
                description=route["description"],
            )

            # Build a dedup key: action + serialized params for more
            # granular deduplication (e.g. two different dolar casas
            # should both appear)
            param_key = str(sorted(hint.params.items()))
            dedup_key = f"{hint.action}:{param_key}"

            if (
                dedup_key not in seen_actions
                or hint.confidence > seen_actions[dedup_key].confidence
            ):
                seen_actions[dedup_key] = hint

    # Also check domain patterns for portal-specific routing
    for dp in DOMAIN_PATTERNS:
        if dp.pattern.search(normalized):
            hint = RoutingHint(
                action="search_ckan",
                params={"portalId": dp.portal_id},
                confidence=0.65,
                description=dp.description,
            )
            dedup_key = f"search_ckan:{dp.portal_id}"
            if (
                dedup_key not in seen_actions
                or hint.confidence > seen_actions[dedup_key].confidence
            ):
                seen_actions[dedup_key] = hint

    # ── Geographic upgrade: when employment keywords co-occur with
    # geographic/comparison terms, replace the national-only series
    # hint with the regional one (all 7 series).
    seen_actions = _upgrade_employment_to_regional(normalized, seen_actions)

    if _wants_current_value(normalized) or _wants_historical_value(normalized):
        upgraded: dict[str, RoutingHint] = {}
        for dedup_key, hint in seen_actions.items():
            if hint.action == "query_argentina_datos" and hint.params.get("type") == "dolar":
                params = dict(hint.params)
                if _wants_current_value(normalized):
                    params["ultimo"] = True
                    params.pop("historico", None)
                elif _wants_historical_value(normalized):
                    params["historico"] = True
                    params.pop("ultimo", None)
                hint = RoutingHint(
                    action=hint.action,
                    params=params,
                    confidence=hint.confidence,
                    description=hint.description,
                )
                dedup_key = f"{hint.action}:{str(sorted(hint.params.items()))}"
            upgraded[dedup_key] = hint
        seen_actions = upgraded

    # Sort by confidence descending
    return sorted(seen_actions.values(), key=lambda h: h.confidence, reverse=True)


def format_hints_for_prompt(hints: list[RoutingHint]) -> str:
    """Format routing hints as a string to inject into the Planner prompt.

    Only includes the top 3 hints to keep the prompt concise and avoid
    confusing the LLM with too many options.

    Args:
        hints: A list of ``RoutingHint`` (typically from ``resolve_hints``).

    Returns:
        A formatted string ready to be appended to the planner system prompt,
        or an empty string if there are no hints.
    """
    if not hints:
        return ""

    top = hints[:3]
    lines = [
        "ROUTING HINTS (alta confianza, usa estas acciones si aplican):",
    ]
    for i, h in enumerate(top, 1):
        params_str = ", ".join(f"{k}={v!r}" for k, v in h.params.items())
        lines.append(
            f"  {i}. {h.action}({params_str}) [confianza={h.confidence:.0%}] — {h.description}"
        )

    # Append taxonomy context if we can derive it from hints
    taxonomy_ctx = resolve_taxonomy_context_from_hints(hints)
    if taxonomy_ctx:
        lines.append("")
        lines.append(taxonomy_ctx)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 4. Hierarchical Taxonomy — 6 domains, 34 categories
# ---------------------------------------------------------------------------

TAXONOMY: dict[str, dict] = {
    "economia": {
        "label": "Economía y Finanzas",
        "children": {
            "inflacion": {
                "label": "Inflación / Precios",
                "actions": ["query_series"],
                "cache_pattern": ["cache_*precio*", "cache_indec_ipc*"],
            },
            "actividad": {
                "label": "Actividad Económica",
                "actions": ["query_series"],
                "cache_pattern": ["cache_indec_emae*"],
            },
            "empleo": {
                "label": "Empleo y Salarios",
                "actions": ["query_series", "query_sandbox"],
                "cache_pattern": ["cache_*empleo*", "cache_*laboral*", "cache_*asalariad*"],
            },
            "comercio_exterior": {
                "label": "Comercio Exterior",
                "actions": ["query_series", "query_sandbox"],
                "cache_pattern": [
                    "cache_*export*",
                    "cache_*import*",
                    "cache_indec_comercio_exterior*",
                    "cache_*arancel*",
                    "cache_*aduana*",
                ],
            },
            "finanzas": {
                "label": "Finanzas / Dólar / BCRA",
                "actions": ["query_argentina_datos", "query_bcra", "query_series"],
                "cache_pattern": ["cache_bcra_*"],
            },
            "deuda": {
                "label": "Deuda Pública",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_presupuesto_deuda*"],
            },
            "produccion": {
                "label": "Producción Industrial",
                "actions": ["query_series", "query_sandbox"],
                "cache_pattern": ["cache_*producci*", "cache_*industrial*"],
            },
            "pobreza": {
                "label": "Pobreza e Indigencia",
                "actions": ["query_series"],
                "cache_pattern": [],
            },
            "precios": {
                "label": "Precios y Canasta Básica",
                "actions": ["query_series", "query_sandbox"],
                "cache_pattern": ["cache_*precio*", "cache_*canasta*"],
            },
        },
    },
    "gobierno": {
        "label": "Gobierno y Transparencia",
        "children": {
            "presupuesto": {
                "label": "Presupuesto Nacional",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_presupuesto_*"],
            },
            "compras_publicas": {
                "label": "Compras Públicas",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_bac_*", "cache_*compras*", "cache_*licitaci*"],
            },
            "ddjj": {
                "label": "Declaraciones Juradas",
                "actions": ["query_ddjj"],
                "cache_pattern": [],
            },
            "autoridades": {
                "label": "Autoridades Nacionales / PEN / Gobernadores",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_autoridades_pen*", "cache_gobernadores"],
            },
            "congreso": {
                "label": "Congreso / Diputados",
                "actions": ["query_sesiones", "query_staff", "query_sandbox", "search_ckan"],
                "cache_pattern": [
                    "cache_*comision*",
                    "cache_*personal*",
                    "cache_diputados",
                    "cache_bloques",
                ],
            },
            "senado": {
                "label": "Senado",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_senado_*"],
            },
            "elecciones": {
                "label": "Elecciones",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_elecciones_*"],
            },
            "gobierno": {
                "label": "Gobierno y Normativa",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*gobierno*", "cache_*decreto*"],
            },
            "justicia": {
                "label": "Justicia",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*justici*"],
            },
        },
    },
    "social": {
        "label": "Desarrollo Social y Humano",
        "children": {
            "educacion": {
                "label": "Educación",
                "actions": ["query_sandbox"],
                "cache_pattern": [
                    "cache_*educaci*",
                    "cache_*escuel*",
                    "cache_*docent*",
                    "cache_*universid*",
                ],
            },
            "salud": {
                "label": "Salud",
                "actions": ["query_sandbox"],
                "cache_pattern": [
                    "cache_*salud*",
                    "cache_*hospital*",
                    "cache_*vacun*",
                    "cache_*covid*",
                ],
            },
            "seguridad": {
                "label": "Seguridad",
                "actions": ["query_sandbox"],
                "cache_pattern": [
                    "cache_*seguridad*",
                    "cache_*delito*",
                    "cache_*homicid*",
                    "cache_*robo*",
                    "cache_*denuncia*",
                    "cache_*polic*",
                    "cache_*siniestro*",
                ],
            },
            "genero": {
                "label": "Género",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*genero*", "cache_*mujer*", "cache_*femicid*"],
            },
            "social": {
                "label": "Programas Sociales",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*social*", "cache_*asistenci*", "cache_*beneficiari*"],
            },
            "poblacion": {
                "label": "Población y Demografía",
                "actions": ["query_sandbox"],
                "cache_pattern": [
                    "cache_*censo*",
                    "cache_*poblaci*",
                    "cache_*demograf*",
                    "cache_*habitant*",
                ],
            },
            "vivienda": {
                "label": "Vivienda",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*viviend*", "cache_*construcci*"],
            },
            "derechos_humanos": {
                "label": "Derechos Humanos",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*derecho*human*", "cache_*memoria*"],
            },
            "cultura": {
                "label": "Cultura",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*cultur*", "cache_*museo*"],
            },
        },
    },
    "infraestructura": {
        "label": "Infraestructura y Servicios",
        "children": {
            "transporte": {
                "label": "Transporte",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*transport*", "cache_*subte*", "cache_*ecobici*"],
            },
            "energia": {
                "label": "Energía",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*energ*", "cache_*petrole*", "cache_*hidrocarb*"],
            },
            "telecomunicaciones": {
                "label": "Telecomunicaciones",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*teleco*", "cache_*internet*", "cache_*telefon*"],
            },
            "infraestructura": {
                "label": "Obra Pública",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*infraestr*", "cache_*obra_pub*"],
            },
        },
    },
    "recursos_naturales": {
        "label": "Recursos Naturales y Ambiente",
        "children": {
            "agro": {
                "label": "Agro y Ganadería",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*agro*", "cache_*ganad*", "cache_*senasa*"],
            },
            "ambiente": {
                "label": "Medio Ambiente",
                "actions": ["query_sandbox", "search_ckan"],
                "cache_pattern": ["cache_*ambient*", "cache_*forestal*", "cache_*residuo*"],
            },
            "turismo": {
                "label": "Turismo",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*turism*"],
            },
        },
    },
    "ciencia": {
        "label": "Ciencia, Tecnología e Indicadores",
        "children": {
            "ciencia": {
                "label": "Ciencia y Tecnología",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*ciencia*", "cache_*tecnolog*", "cache_*conicet*"],
            },
            "indicadores": {
                "label": "Indicadores y Estadísticas",
                "actions": ["query_sandbox"],
                "cache_pattern": ["cache_*indice*", "cache_*indicador*", "cache_*estadistic*"],
            },
        },
    },
}


# ---------------------------------------------------------------------------
# Pre-built reverse index: action → list of (domain_label, category_label, cache_patterns)
# ---------------------------------------------------------------------------

_ACTION_TO_TAXONOMY: dict[str, list[tuple[str, str, list[str]]]] = {}

for _domain_key, _domain in TAXONOMY.items():
    for _cat_key, _cat in _domain["children"].items():
        for _act in _cat["actions"]:
            _ACTION_TO_TAXONOMY.setdefault(_act, []).append(
                (_domain["label"], _cat["label"], _cat["cache_pattern"])
            )


def resolve_taxonomy_context(query: str) -> str:
    """Resolve taxonomy context for a user query.

    Uses the existing ``resolve_hints()`` to find matched actions, then maps
    those actions back to the hierarchical taxonomy to provide domain context.

    Args:
        query: Raw user query in natural language (Spanish).

    Returns:
        A formatted string showing the taxonomy path and related cache tables,
        or an empty string if no taxonomy match is found.
    """
    hints = resolve_hints(query)
    return resolve_taxonomy_context_from_hints(hints)


def resolve_taxonomy_context_from_hints(hints: list[RoutingHint]) -> str:
    """Resolve taxonomy context from pre-computed routing hints.

    Args:
        hints: A list of ``RoutingHint`` (typically from ``resolve_hints``).

    Returns:
        A formatted string showing the taxonomy path and related cache tables,
        or an empty string if no taxonomy match is found.
    """
    if not hints:
        return ""

    # Collect unique taxonomy entries from the matched actions.
    # When a hint has table params, prefer taxonomy categories whose
    # cache_pattern overlaps with those tables (avoids showing all 20+
    # query_sandbox categories).
    seen: set[tuple[str, str]] = set()
    entries: list[str] = []
    cache_patterns: list[str] = []

    for hint in hints[:5]:
        tax_matches = _ACTION_TO_TAXONOMY.get(hint.action, [])
        hint_tables = hint.params.get("tables", [])

        # If the hint has table patterns, filter taxonomy to matching categories
        if hint_tables and len(tax_matches) > 4:
            filtered = []
            for domain_label, cat_label, patterns in tax_matches:
                if patterns and any(hp in tp or tp in hp for hp in hint_tables for tp in patterns):
                    filtered.append((domain_label, cat_label, patterns))
            if filtered:
                tax_matches = filtered

        for domain_label, cat_label, patterns in tax_matches:
            key = (domain_label, cat_label)
            if key not in seen:
                seen.add(key)
                entries.append(f"  Dominio: {domain_label} → {cat_label}")
                cache_patterns.extend(patterns)

    if not entries:
        return ""

    lines = ["CONTEXTO TAXONOMICO:"]
    # Deduplicate entries — show up to 4
    for entry in entries[:4]:
        lines.append(entry)

    # Suggested actions
    action_descs: list[str] = []
    for hint in hints[:3]:
        params_str = ", ".join(f"{v}" for v in hint.params.values()) if hint.params else ""
        desc = f"{hint.action}"
        if params_str:
            desc += f" ({params_str})"
        action_descs.append(desc)
    if action_descs:
        lines.append(f"  Acciones sugeridas: {', '.join(action_descs)}")

    # Related cache patterns (deduplicated)
    unique_patterns = list(dict.fromkeys(cache_patterns))
    if unique_patterns:
        lines.append(f"  Tablas cache relacionadas: {', '.join(unique_patterns[:8])}")

    return "\n".join(lines)
