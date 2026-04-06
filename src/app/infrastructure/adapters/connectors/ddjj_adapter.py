from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

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
            if _name_matches(r.get("nombre", ""), query) or q_clean in r.get("cuit", "").replace("-", ""):
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
        sorted_ds = sorted(
            self._dataset,
            key=lambda r: r.get(sort_key, 0),
            reverse=(order == "desc"),
        )
        top_records = sorted_ds[:top]
        label = "mayor" if order == "desc" else "menor"
        return self._to_data_result(
            f"Ranking: {top} diputados con {label} {sort_by}",
            top_records,
        )

    def get_by_name(self, name: str) -> DataResult:
        self._ensure_loaded()
        matches = [r for r in self._dataset if _name_matches(r.get("nombre", ""), name)][:5]
        return self._to_data_result(f'DDJJ de "{name}"', matches)

    def stats(self) -> DataResult:
        self._ensure_loaded()
        if not self._dataset:
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
        suma = 0.0
        max_val, min_val = float("-inf"), float("inf")
        max_r = min_r = self._dataset[0]
        patrimonios: list[float] = []
        for r in self._dataset:
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
            "patrimonio_promedio": suma / total if total else 0,
            "patrimonio_mediano": patrimonios[total // 2] if total else 0,
            "patrimonio_maximo_nombre": max_r.get("nombre", ""),
            "patrimonio_maximo_monto": max_r.get("patrimonioCierre", 0),
            "patrimonio_minimo_nombre": min_r.get("nombre", ""),
            "patrimonio_minimo_monto": min_r.get("patrimonioCierre", 0),
        }
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
                "description": f"Estadísticas agregadas de {total} declaraciones juradas patrimoniales",
            },
        )

    def _to_data_result(self, title: str, records: list[dict]) -> DataResult:
        now = datetime.now(UTC).isoformat()
        formatted = []
        for r in records:
            bienes = r.get("bienes", [])
            formatted.append(
                {
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
                    "bienes_detalle": bienes,
                    "resumen_bienes": _summarize_assets(bienes),
                }
            )

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
