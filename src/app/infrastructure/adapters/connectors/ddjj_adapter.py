from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.domain.entities.connectors.data_result import DataResult

logger = logging.getLogger(__name__)

# Path relative to the backend project root
_DATA_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "ddjj_dataset.json"


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def _name_matches(nombre: str, query: str) -> bool:
    """Check if all query words appear in the name (order-independent)."""
    nombre_norm = _strip_accents(nombre.lower())
    words = _strip_accents(query.lower()).split()
    return all(w in nombre_norm for w in words)


_RE_ASSET_TYPE = re.compile(r"EN EL (?:PAIS|EXTERIOR)")


def _summarize_assets(bienes: list[dict]) -> dict[str, float]:
    summary: dict[str, float] = {}
    for b in bienes:
        cat = _RE_ASSET_TYPE.sub("", b.get("tipo", "")).strip()
        summary[cat] = summary.get(cat, 0) + b.get("importe", 0)
    return summary


# H005 (independent review, 2026-10-05): the declared closing total
# (``bienesCierre``) is checked against the declaration itself. The public part
# rarely itemizes everything (the whole dataset carries only 3 company stakes),
# so in 28 of the 192 DDJJ with itemized assets the total is more than twice the
# item sum, and several of those are real fortunes whose total was already
# declared at the start of the period. The base is therefore the larger of the
# item sum and ``bienesInicio``: a total with history is backed. Against that
# base the highest ratio in the dataset is 4.4, while the one bad load (a
# closing total of $31,275.5 M against $66.2 M itemized and $56.8 M at start)
# is 472; one order of magnitude leaves room on both sides. Downwards, a total
# under a tenth of its own items doesn't add up either (the lowest ratio in the
# dataset is 0.52, where one amount shows up twice in the detail, as a property
# and as a credit).
_MAX_TOTAL_VS_DETAIL = 10.0

_NOT_COMPARABLE = (
    "El total no coincide con la propia declaración: esta DDJJ queda fuera de "
    "rankings, promedios y variación patrimonial."
)


def _millones(value: float) -> str:
    """``31275522665.75`` → ``"$31.275,5 millones"`` (es-AR)."""
    text = f"{value / 1_000_000:,.1f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"${text} millones"


def _veces(ratio: float) -> str:
    """Truncated, never rounded up: 472.5 → ``"472"``."""
    return f"{int(ratio):,}".replace(",", ".")


def _inconsistency(r: dict) -> str | None:
    """Why the closing total doesn't add up with its own declaration, or ``None``."""
    bienes = r.get("bienes") or []
    detalle = sum(float(b.get("importe") or 0) for b in bienes)
    if detalle <= 0:
        return None  # nothing itemized: nothing to check against
    cierre = float(r.get("bienesCierre") or 0)
    inicio = float(r.get("bienesInicio") or 0)
    if cierre > _MAX_TOTAL_VS_DETAIL * max(detalle, inicio):
        motivo = (
            f"El total de bienes al cierre ({_millones(cierre)}) es {_veces(cierre / detalle)} "
            f"veces la suma de los {len(bienes)} bienes del detalle ({_millones(detalle)})"
        )
        if inicio > 0:
            motivo += f" y {_veces(cierre / inicio)} veces el total al inicio ({_millones(inicio)})"
    elif cierre * _MAX_TOTAL_VS_DETAIL < detalle:
        if cierre > 0:
            motivo = (
                f"La suma de los {len(bienes)} bienes del detalle ({_millones(detalle)}) es "
                f"{_veces(detalle / cierre)} veces el total de bienes al cierre "
                f"({_millones(cierre)})"
            )
        else:
            motivo = (
                f"El total de bienes al cierre es cero pero los {len(bienes)} bienes del "
                f"detalle suman {_millones(detalle)}"
            )
    else:
        return None
    return f"{motivo}. {_NOT_COMPARABLE}"


def _split_inconsistent(records: list[dict]) -> tuple[list[dict], list[str]]:
    """Records usable for rankings/aggregates, and names of the excluded ones."""
    usable: list[dict] = []
    excluded: list[str] = []
    for r in records:
        if _inconsistency(r) is None:
            usable.append(r)
        else:
            excluded.append(r.get("nombre", ""))
    return usable, excluded


def _declaraciones(n: int) -> str:
    return f"{n} declaración" if n == 1 else f"{n} declaraciones"


class DDJJAdapter:
    """In-memory DDJJ dataset loaded once at startup (singleton)."""

    _BACKOFF_BASE = 60
    _BACKOFF_CAP = 3600

    def __init__(self) -> None:
        self._dataset: list[dict] = []
        self._loaded = False
        self._fail_count: int = 0
        self._next_retry_at: float = 0.0

    @property
    def record_count(self) -> int:
        """Public accessor for health checks."""
        self._ensure_loaded()
        return len(self._dataset)

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if self._fail_count > 0 and time.monotonic() < self._next_retry_at:
            return
        try:
            raw = _DATA_PATH.read_text(encoding="utf-8")
            self._dataset = json.loads(raw)
            self._loaded = True
            self._fail_count = 0
            logger.info("DDJJ dataset loaded: %d records from %s", len(self._dataset), _DATA_PATH)
        except FileNotFoundError:
            self._fail_count += 1
            delay = min(self._BACKOFF_BASE * (2 ** (self._fail_count - 1)), self._BACKOFF_CAP)
            self._next_retry_at = time.monotonic() + delay
            logger.error(
                "DDJJ dataset file not found: %s (attempt %d, next retry in %ds)",
                _DATA_PATH,
                self._fail_count,
                delay,
            )
            self._dataset = []
        except Exception:
            self._fail_count += 1
            delay = min(self._BACKOFF_BASE * (2 ** (self._fail_count - 1)), self._BACKOFF_CAP)
            self._next_retry_at = time.monotonic() + delay
            logger.error(
                "Failed to load DDJJ dataset from %s (attempt %d, next retry in %ds)",
                _DATA_PATH,
                self._fail_count,
                delay,
                exc_info=True,
            )
            self._dataset = []

    def search(self, query: str, limit: int = 20) -> DataResult:
        self._ensure_loaded()
        q_clean = _strip_accents(query.lower()).replace("-", "")
        # Early-termination search instead of scanning all records then slicing
        matches: list[dict] = []
        for r in self._dataset:
            if _name_matches(r.get("nombre", ""), query) or q_clean in r.get("cuit", "").replace(
                "-", ""
            ):
                matches.append(r)
                if len(matches) >= limit:
                    break
        return self._to_data_result(f'Búsqueda DDJJ: "{query}"', matches)

    def ranking(
        self,
        sort_by: str = "patrimonio",
        top: int = 10,
        order: str = "desc",
    ) -> DataResult:
        self._ensure_loaded()
        key_map = {
            "patrimonio": "patrimonioCierre",
            "ingresos": "ingresosTrabajoNeto",
            "bienes": "bienesCierre",
        }
        sort_key = key_map.get(sort_by, "patrimonioCierre")
        # H005: a DDJJ whose total doesn't add up never enters a ranking.
        usable, excluded = _split_inconsistent(self._dataset)
        sorted_ds = sorted(
            usable,
            key=lambda r: r.get(sort_key, 0),
            reverse=(order == "desc"),
        )
        top_records = sorted_ds[:top]
        label = "mayor" if order == "desc" else "menor"
        # FR-004a: ranking rows MUST be compact. Passing ``compact=True``
        # strips ``bienes_detalle``, ``bienes`` and ``resumen_bienes`` so
        # the analyst fits all N records in its output budget. See
        # FIX-007 in ``specs/FIX_BACKLOG.md``.
        result = self._to_data_result(
            f"Ranking: {top} diputados con {label} {sort_by}",
            top_records,
            compact=True,
        )
        if excluded:
            verbo = "excluyó" if len(excluded) == 1 else "excluyeron"
            result.metadata["excluidas_por_inconsistencia"] = excluded
            result.metadata["description"] += (
                f". Se {verbo} {_declaraciones(len(excluded))} cuyo total de bienes no "
                "coincide con su propio detalle (excluidas_por_inconsistencia)."
            )
        return result

    def get_by_name(self, name: str) -> DataResult:
        self._ensure_loaded()
        matches = [r for r in self._dataset if _name_matches(r.get("nombre", ""), name)][:5]
        return self._to_data_result(f'DDJJ de "{name}"', matches)

    def stats(self) -> DataResult:
        self._ensure_loaded()
        # H005: averages, median, max and min leave out the DDJJ whose total
        # doesn't add up (a single bad load moved the average from 351 M to 509 M).
        usable, excluded = _split_inconsistent(self._dataset)
        if not usable:
            return DataResult(
                source="ddjj:oficina_anticorrupcion",
                portal_name="Declaraciones Juradas Patrimoniales — Oficina Anticorrupción",
                portal_url="https://www.argentina.gob.ar/anticorrupcion/consultar-declaraciones-juradas-de-funcionarios-publicos",
                dataset_title="Estadísticas DDJJ",
                format="json",
                records=[],
                metadata={"total_records": 0, "fetched_at": datetime.now(UTC).isoformat()},
            )

        # Single pass: collect min, max, sum, and sorted list simultaneously
        total = len(self._dataset)
        n = len(usable)
        suma = 0.0
        max_val, min_val = float("-inf"), float("inf")
        max_r = min_r = usable[0]
        patrimonios: list[float] = []
        for r in usable:
            p = r.get("patrimonioCierre", 0)
            patrimonios.append(p)
            suma += p
            if p > max_val:
                max_val, max_r = p, r
            if p < min_val:
                min_val, min_r = p, r
        patrimonios.sort()

        stats_record = {
            "total": total,
            "anio": self._dataset[0].get("anioDeclaracion", ""),
            "patrimonio_promedio": suma / n if n else 0,
            "patrimonio_mediano": patrimonios[n // 2] if n else 0,
            "patrimonio_maximo_nombre": max_r.get("nombre", ""),
            "patrimonio_maximo_monto": max_r.get("patrimonioCierre", 0),
            "patrimonio_minimo_nombre": min_r.get("nombre", ""),
            "patrimonio_minimo_monto": min_r.get("patrimonioCierre", 0),
        }
        description = f"Estadísticas agregadas de {total} declaraciones juradas patrimoniales"
        if excluded:
            stats_record["excluidas_por_inconsistencia"] = excluded
            description += (
                f". Promedio, mediana, máximo y mínimo calculados sin "
                f"{_declaraciones(len(excluded))} cuyo total de bienes no coincide con su "
                "propio detalle (excluidas_por_inconsistencia)."
            )
        return DataResult(
            source="ddjj:oficina_anticorrupcion",
            portal_name="Declaraciones Juradas Patrimoniales — Oficina Anticorrupción",
            portal_url="https://www.argentina.gob.ar/anticorrupcion/consultar-declaraciones-juradas-de-funcionarios-publicos",
            dataset_title="Estadísticas DDJJ Diputados Nacionales",
            format="json",
            records=[stats_record],
            metadata={
                "total_records": 1,
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": description,
            },
        )

    def _to_data_result(
        self,
        title: str,
        records: list[dict],
        *,
        compact: bool = False,
    ) -> DataResult:
        """Format records as a DataResult.

        When ``compact=True`` the per-asset ``bienes_detalle`` and the
        grouped ``resumen_bienes`` are omitted so each row stays under
        ~500 chars. Used by ``ranking()`` (FR-004a / FIX-007) where the
        analyst has to fit N rows in its output budget and elaborating
        every asset would blow past ``max_tokens`` after ~4 rows.
        """
        now = datetime.now(UTC).isoformat()
        formatted = []
        for r in records:
            bienes = r.get("bienes", [])
            motivo = _inconsistency(r)
            row: dict[str, Any] = {
                "cuit": r.get("cuit", ""),
                "nombre": r.get("nombre", ""),
                "sexo": r.get("sexo", ""),
                "fecha_nacimiento": r.get("fechaNacimiento", ""),
                "estado_civil": r.get("estadoCivil", ""),
                "cargo": r.get("cargo", ""),
                "organismo": r.get("organismo", ""),
                "anio_declaracion": r.get("anioDeclaracion", ""),
                "tipo_declaracion": r.get("tipoDeclaracion", ""),
                "bienes_inicio": r.get("bienesInicio", 0),
                "deudas_inicio": r.get("deudasInicio", 0),
                "bienes_cierre": r.get("bienesCierre", 0),
                "deudas_cierre": r.get("deudasCierre", 0),
                "patrimonio_cierre": r.get("patrimonioCierre", 0),
                "variacion_patrimonial": r.get("bienesCierre", 0) - r.get("bienesInicio", 0),
                "ingresos_trabajo_neto": r.get("ingresosTrabajoNeto", 0),
                "gastos_personales": r.get("gastosPersonales", 0),
                "cantidad_bienes": len(bienes),
                "inconsistente": motivo is not None,
            }
            if motivo is not None:
                # H005: the declared totals stay visible (it's what the DDJJ
                # says), but the variation of a total that doesn't add up is not
                # a variation.
                row["motivo_inconsistencia"] = motivo
                row["variacion_patrimonial"] = None
            if not compact:
                row["bienes_detalle"] = bienes
                row["resumen_bienes"] = _summarize_assets(bienes)
            formatted.append(row)

        return DataResult(
            source="ddjj:oficina_anticorrupcion",
            portal_name="Declaraciones Juradas Patrimoniales — Oficina Anticorrupción",
            portal_url="https://www.argentina.gob.ar/anticorrupcion/consultar-declaraciones-juradas-de-funcionarios-publicos",
            dataset_title=title,
            format="json",
            records=formatted,
            metadata={
                "total_records": len(formatted),
                "fetched_at": now,
                "description": "Declaraciones Juradas Patrimoniales Integrales de Diputados Nacionales — Parte Pública",
            },
        )
