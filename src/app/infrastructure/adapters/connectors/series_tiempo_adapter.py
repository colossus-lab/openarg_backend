from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector

logger = logging.getLogger(__name__)

BASE_URL = "https://apis.datos.gob.ar/series/api"

# Catálogo curado de series de la API de Series de Tiempo.
#
# - ``ids``: los ids que se piden juntos.
# - ``description``: lo que ve el agente en ``buscar_series`` ("verificadas").
#   Tiene que describir la serie que de verdad es, no la que se quería: en
#   2026-02 se cargó ``11.3_AGCS_2004_M_41`` como "actividad industrial" y
#   era el EMAE de comercio (verificado contra la API el 04-oct).
# - ``expected_description``: por id, la descripción que da la API
#   (``metadata=full``, ``field.description``). La fija un test contra datos
#   grabados de la API: cambiar un id tiene que ser deliberado.
# - ``keywords``: se comparan sin acentos y por palabra completa
#   (``match_catalog``), nunca como subcadena: "emi" encontraba "emisiones".
# - ``discontinued``: la fuente dejó de actualizar la serie.
# - ``default_collapse`` / ``default_representation``: sólo los usa el
#   pipeline viejo (``pipeline/connectors/series.py``).
SERIES_CATALOG: dict[str, dict] = {
    "presupuesto": {
        "ids": ["451.3_GPNGPN_0_0_3_30"],
        "description": (
            "Gasto público nacional consolidado en millones de pesos (anual, 1980-2023). "
            "Serie discontinuada: la fuente no la actualiza desde 2023. Para el presupuesto "
            "vigente buscá tablas con buscar_datos."
        ),
        "expected_description": {"451.3_GPNGPN_0_0_3_30": "Gasto público nacional"},
        "keywords": [
            "gasto publico",
            "gasto publico nacional",
            "gasto publico consolidado",
        ],
        "discontinued": True,
    },
    "inflacion": {
        "ids": ["148.3_INIVELNAL_DICI_M_26"],
        "description": "IPC Nacional Nivel General (índice base dic-2016=100). Usar con representation=percent_change para variación % mensual.",
        "expected_description": {
            "148.3_INIVELNAL_DICI_M_26": "IPC. Nivel General Nacional. Base dic 2016. Mensual."
        },
        "keywords": ["inflacion", "ipc", "precios", "indice de precios", "costo de vida"],
        "default_collapse": "month",
        "default_representation": "percent_change",
    },
    "tipo_cambio": {
        "ids": ["92.2_TIPO_CAMBIION_0_0_21_24"],
        "description": (
            "Tipo de cambio de valuación del BCRA, pesos por dólar (diario, desde 2003; los "
            "fines de semana repiten el último dato hábil)"
        ),
        "expected_description": {
            "92.2_TIPO_CAMBIION_0_0_21_24": "Tipo de cambio de valuación (peso por dólar)"
        },
        "keywords": ["dolar", "tipo de cambio", "divisa", "cotizacion"],
        "default_collapse": "month",
    },
    "ipc_regional": {
        "ids": [
            "148.3_INIVELNAL_DICI_M_26",
            "103.1_I2N_2016_M_19",
            "148.3_INIVELNOA_DICI_M_21",
            "145.3_INGCUYUYO_DICI_M_11",
        ],
        "description": "IPC Regional: Nacional, GBA, NOA, y Cuyo (mensual)",
        "expected_description": {
            "148.3_INIVELNAL_DICI_M_26": "IPC. Nivel General Nacional. Base dic 2016. Mensual.",
            "103.1_I2N_2016_M_19": "IPC-GBA. Nivel General. Base abr 2016. Mensual",
            "148.3_INIVELNOA_DICI_M_21": "IPC. Nivel General Región noroeste. Base dic 2016. Mensual.",
            "145.3_INGCUYUYO_DICI_M_11": "IPC. Nivel General Cuyo. Base dic 2016. Mensual.",
        },
        "keywords": ["ipc regional", "precios regionales", "inflacion regional"],
        "default_collapse": "month",
    },
    "reservas": {
        "ids": ["174.1_RRVAS_IDOS_0_0_36"],
        "description": (
            "Reservas internacionales del BCRA, saldo mensual en millones de dólares (desde "
            "1940). La fuente la actualiza con meses de atraso: para el dato más reciente usá "
            "la diaria 92.2_RESERVAS_IRES_0_0_32_40."
        ),
        "expected_description": {"174.1_RRVAS_IDOS_0_0_36": "Reservas Internacionales BCRA Saldos"},
        "keywords": [
            "reservas",
            "reservas internacionales",
            "bcra reservas",
            "reservas bcra",
            "dolares bcra",
            "reservas del banco central",
        ],
        "default_collapse": "month",
    },
    "reservas_diarias": {
        "ids": ["92.2_RESERVAS_IRES_0_0_32_40"],
        "description": (
            "Reservas internacionales del BCRA, saldo diario en millones de dólares (desde 2003)"
        ),
        "expected_description": {
            "92.2_RESERVAS_IRES_0_0_32_40": "Reservas internacionales del BCRA, en millones de dólares"
        },
        "keywords": [
            "reservas",
            "reservas internacionales",
            "bcra reservas",
            "reservas bcra",
            "reservas del banco central",
        ],
    },
    "base_monetaria": {
        "ids": ["331.1_SALDO_BASERIA__15"],
        "description": "Base monetaria — saldo en millones de pesos (mensual)",
        "expected_description": {"331.1_SALDO_BASERIA__15": "Saldo de la Base Monetaria"},
        "keywords": [
            "base monetaria",
            "emision monetaria",
            "emision de pesos",
            "dinero en circulacion",
            "masa monetaria",
            "agregados monetarios",
        ],
        "default_collapse": "month",
    },
    "leliq_pases": {
        "ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"],
        "description": (
            "Pases y redescuentos: LELIQ, como factor de explicación de la variación de la base "
            "monetaria, en millones de pesos (mensual; vale 0 desde que se eliminaron las "
            "LELIQ). No es la tasa de política monetaria ni el stock de LELIQ."
        ),
        "expected_description": {
            "331.1_PASES_REDELIQ_M_MONE_0_24_24": "Pases y Redescuentos: Leliq"
        },
        "keywords": [
            "leliq",
            "pases",
            "letras de liquidez",
            "pases pasivos",
        ],
        "default_collapse": "month",
    },
    "emae": {
        "ids": ["143.3_NO_PR_2004_A_21"],
        "description": "EMAE — Estimador Mensual de Actividad Económica, índice base 2004 (mensual, desde 2004)",
        "expected_description": {"143.3_NO_PR_2004_A_21": "EMAE. Base 2004"},
        "keywords": [
            "emae",
            "actividad economica",
            "pbi mensual",
            "crecimiento economico",
            "recesion",
            "producto bruto",
        ],
        "default_collapse": "month",
    },
    "desempleo": {
        "ids": ["45.2_ECTDT_0_T_33"],
        "description": (
            "Tasa de desempleo total (trimestral, desde 2003). La API la da como fracción: "
            "0,079 es 7,9 %."
        ),
        "expected_description": {"45.2_ECTDT_0_T_33": "Tasa de desempleo total. En porcentaje."},
        "keywords": [
            "desempleo",
            "desocupacion",
            "tasa de desempleo",
            "tasa de desocupacion",
            "mercado laboral",
        ],
    },
    "salarios": {
        "ids": ["149.1_TL_INDIIOS_OCTU_0_21"],
        "description": "Índice de Salarios nivel general, base oct-2016=100 (mensual)",
        "expected_description": {"149.1_TL_INDIIOS_OCTU_0_21": "Índice de Salarios"},
        "keywords": [
            "salarios",
            "sueldos",
            "indice de salarios",
            "remuneraciones",
            "salario real",
            "paritarias",
        ],
        "default_collapse": "month",
    },
    "canasta_basica": {
        "ids": ["150.1_LA_POBREZA_0_D_13"],
        "description": "Canasta Básica Total (CBT) / Línea de pobreza por adulto equivalente en pesos (mensual, desde 2016)",
        "expected_description": {
            "150.1_LA_POBREZA_0_D_13": "Línea de pobreza desde 2016. Pesos corrientes."
        },
        "keywords": [
            "canasta basica",
            "canasta basica total",
            "cbt",
            "linea de pobreza",
            "costo de vida",
        ],
        "default_collapse": "month",
    },
    "canasta_alimentaria": {
        "ids": ["150.1_LA_INDICIA_0_D_16"],
        "description": "Canasta Básica Alimentaria (CBA) / Línea de indigencia por adulto equivalente en pesos (mensual, desde 2016)",
        "expected_description": {
            "150.1_LA_INDICIA_0_D_16": "Línea de indigencia desde 2016. Pesos corrientes."
        },
        "keywords": [
            "canasta alimentaria",
            "canasta basica alimentaria",
            "cba",
            "linea de indigencia",
            "alimentos basicos",
        ],
        "default_collapse": "month",
    },
    "exportaciones": {
        "ids": ["74.3_IET_0_M_16"],
        "description": "Exportaciones totales en millones de dólares (mensual, desde 1992)",
        "expected_description": {
            "74.3_IET_0_M_16": "Exportaciones totales. En millones de dólares."
        },
        "keywords": ["exportaciones", "expo", "ventas externas", "comercio exterior"],
        "default_collapse": "month",
    },
    "importaciones": {
        "ids": ["74.3_IIT_0_M_25"],
        "description": "Importaciones totales en millones de dólares (mensual, desde 1992)",
        "expected_description": {
            "74.3_IIT_0_M_25": "Importaciones totales. En millones de dólares."
        },
        "keywords": ["importaciones", "impo", "compras externas"],
        "default_collapse": "month",
    },
    "balanza_comercial": {
        "ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"],
        "description": "Balanza comercial: exportaciones e importaciones totales en millones de dólares (mensual)",
        "expected_description": {
            "74.3_IET_0_M_16": "Exportaciones totales. En millones de dólares.",
            "74.3_IIT_0_M_25": "Importaciones totales. En millones de dólares.",
        },
        "keywords": [
            "balanza comercial",
            "saldo comercial",
            "comercio exterior",
            "intercambio comercial",
        ],
        "default_collapse": "month",
    },
    "actividad_industrial": {
        "ids": ["453.1_SERIE_ORIGNAL_0_0_14_46"],
        "description": (
            "Índice de Producción Industrial manufacturero (IPI) del INDEC, nivel general, "
            "serie original (mensual, desde 2016)"
        ),
        "expected_description": {
            "453.1_SERIE_ORIGNAL_0_0_14_46": "IPI Nivel General Serie Original"
        },
        "keywords": [
            "industria",
            "industria manufacturera",
            "produccion industrial",
            "actividad industrial",
            "manufactura",
            "ipi",
            "emi",
            "fabrica",
        ],
        "default_collapse": "month",
    },
    "emae_comercio": {
        "ids": ["11.3_AGCS_2004_M_41"],
        "description": (
            "EMAE: comercio mayorista, minorista y reparaciones, índice base 2004=100 "
            "(mensual, desde 2004)"
        ),
        "expected_description": {
            "11.3_AGCS_2004_M_41": "EMAE. Comercio mayorista y minorista y reparaciones"
        },
        "keywords": [
            "comercio mayorista",
            "comercio minorista",
            "actividad comercial",
            "emae comercio",
        ],
        "default_collapse": "month",
    },
}


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


_WORD_RE = re.compile(r"[a-z0-9]+")


def _stem(word: str) -> str:
    """Un singular aproximado, igual para las dos puntas de la comparación.

    Alcanza para que «exportación» encuentre «exportaciones» y «dólar»
    encuentre «dólares», sin diccionario.
    """
    if len(word) > 4 and word.endswith(("ones", "res", "les", "des", "nes")):
        return word[:-2]
    if len(word) > 3 and word.endswith("s"):
        return word[:-1]
    return word


def _tokens(text: str) -> list[str]:
    return [_stem(w) for w in _WORD_RE.findall(_strip_accents(text.lower()))]


def _contains_phrase(words: list[str], phrase: tuple[str, ...]) -> bool:
    n = len(phrase)
    return n > 0 and any(tuple(words[i : i + n]) == phrase for i in range(len(words) - n + 1))


# Palabras clave tokenizadas una vez, en el orden del catálogo.
_CATALOG_NORMALIZED: list[tuple[tuple[str, ...], str, dict]] = [
    (tuple(_tokens(kw)), key, entry)
    for key, entry in SERIES_CATALOG.items()
    for kw in entry["keywords"]
]


def match_catalog(query: str) -> list[dict]:
    """Las entradas del catálogo con alguna palabra clave entera en el texto.

    Sin acentos y por palabra completa: «inflación» encuentra la inflación,
    pero «emisiones» ya no encuentra la base monetaria ni «cambio climático»
    el tipo de cambio. En el orden del catálogo, sin repetir.
    """
    words = _tokens(query)
    found: list[dict] = []
    seen: set[str] = set()
    for phrase, key, entry in _CATALOG_NORMALIZED:
        if key not in seen and _contains_phrase(words, phrase):
            seen.add(key)
            found.append(entry)
    return found


def find_catalog_match(query: str) -> dict | None:
    """La primera entrada del catálogo que corresponde al texto (pipeline viejo)."""
    matches = match_catalog(query)
    return matches[0] if matches else None


def catalog_mismatches(api_descriptions: Mapping[str, str | None]) -> list[str]:
    """Ids del catálogo cuya descripción en la API no es la esperada.

    ``api_descriptions`` va de id a ``field.description`` (``metadata=full``).
    Lo usa el test contra datos grabados; sirve igual para un chequeo en vivo.
    """
    problems: list[str] = []
    for key, entry in SERIES_CATALOG.items():
        expected = entry.get("expected_description") or {}
        for sid in entry["ids"]:
            if sid not in expected:
                problems.append(f"{key}: {sid} no tiene expected_description")
                continue
            if sid not in api_descriptions:
                problems.append(f"{key}: {sid} no está en la API")
                continue
            actual = api_descriptions[sid]
            if actual != expected[sid]:
                problems.append(f"{key}: {sid} es «{actual}», no «{expected[sid]}»")
    return problems


class SeriesTiempoAdapter(ISeriesTiempoConnector):
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http = http_client

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        try:
            resp = await self._http.get(
                f"{BASE_URL}/search",
                params={"q": query, "limit": limit},
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("data"):
                return []
            return [
                {
                    "id": item["field"]["id"],
                    "title": item["field"].get("title") or item["field"].get("description", ""),
                    "description": item["field"].get("description", ""),
                    "units": item["field"].get("units", ""),
                    "frequency": item["field"].get("frequency", ""),
                    "dataset_title": item["dataset"].get("title", ""),
                    "source": item["dataset"].get("source", ""),
                }
                for item in data["data"]
            ]
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details={"query": query[:100], "reason": str(exc)},
            ) from exc

    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
    ) -> DataResult | None:
        try:
            params: dict[str, str] = {
                "ids": ",".join(series_ids),
                "format": "json",
                "limit": str(limit),
                "metadata": "full",
            }
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
            if representation:
                params["representation_mode"] = representation
            if collapse:
                params["collapse"] = collapse

            resp = await self._http.get(f"{BASE_URL}/series", params=params)
            resp.raise_for_status()
            raw = resp.json()

            if not raw or not raw.get("data"):
                return None

            # Build human-readable labels from metadata
            # meta[0] is the time axis, meta[1..N] are series fields
            meta_list = raw.get("meta", [])
            id_to_label: dict[str, str] = {}
            field_descriptions: list[str] = []
            field_units = ""
            dataset_title = ""
            for m in meta_list[1:]:
                field = m.get("field", {})
                sid = field.get("id", "")
                label = field.get("description") or field.get("title") or sid
                if sid:
                    id_to_label[sid] = label
                if field.get("description"):
                    field_descriptions.append(field["description"])
                if not field_units and field.get("units"):
                    field_units = field["units"]
                if not dataset_title:
                    ds = m.get("dataset", {})
                    dataset_title = ds.get("title", "")

            if not dataset_title:
                dataset_title = ", ".join(series_ids)

            is_percent = representation == "percent_change"
            records = []
            for row in raw["data"]:
                record: dict = {"fecha": row[0]}
                for idx, sid in enumerate(series_ids):
                    val = row[idx + 1]
                    if val is not None and is_percent:
                        # API returns percent_change as a fraction (0.152 = 15.2%).
                        # We multiply by 100 so downstream consumers get a human
                        # number ("15.2"). The unit is signaled via metadata.unit
                        # and metadata.value_scale so the analyst prompt, charts,
                        # and UI know not to display "15.2%" as "1520%".
                        val = round(val * 100, 2)
                    label = id_to_label.get(sid, sid)
                    record[label] = val
                records.append(record)

            if not records:
                return None

            metadata: dict[str, Any] = {
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": "; ".join(field_descriptions),
                "units": field_units,
            }
            if representation:
                metadata["representation"] = representation
            if is_percent:
                # Explicit contract for downstream consumers: values are already
                # scaled to percentage points (e.g., 15.2 means 15.2%).
                metadata["unit"] = "percent"
                metadata["value_scale"] = "percentage_points"

            return DataResult(
                source="series_tiempo",
                portal_name="API de Series de Tiempo",
                portal_url=f"https://datos.gob.ar/series/api/series/?ids={','.join(series_ids)}",
                dataset_title=dataset_title,
                format="time_series",
                records=records,
                metadata=metadata,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            # `str(exc)` de un HTTPStatusError trae la URL y el código, pero
            # no el cuerpo — y el cuerpo es donde la API explica qué
            # parámetro rechazó (p. ej. "Intervalo de collapse inválido …
            # Pruebe con un intervalo mayor"). Sin esto, un 400 recuperable
            # y una serie caída se ven idénticos en los logs.
            detalle = str(exc)
            cuerpo = getattr(getattr(exc, "response", None), "text", None)
            if cuerpo:
                detalle = f"{detalle} | respuesta: {cuerpo[:300]}"
            logger.warning(
                "Series fetch falló para %s (collapse=%s, representation=%s): %s",
                series_ids,
                collapse,
                representation,
                detalle,
            )
            raise ConnectorError(
                error_code=ErrorCode.CN_SERIES_UNAVAILABLE,
                details={"series_ids": series_ids, "reason": detalle},
            ) from exc
