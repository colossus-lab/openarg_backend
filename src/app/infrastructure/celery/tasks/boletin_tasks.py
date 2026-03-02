"""Scrape Boletín Oficial for HCDN/Senado staff designation resolutions."""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_BO_BASE = "https://www.boletinoficial.gob.ar"
_SEARCH_URL = f"{_BO_BASE}/busquedaAvanzada/realizarBusqueda"
_DETAIL_URL = f"{_BO_BASE}/norma/detalleSegunda"
_PAGE_SIZE = 20
_MAX_PAGES = 50  # safety limit

_SEARCH_KEYWORDS_DIPUTADOS = [
    "designar planta transitoria diputados",
    "personal transitorio honorable camara diputados",
    "asesores bloque diputados",
]
_SEARCH_KEYWORDS_SENADO = [
    "designar planta transitoria senado",
    "personal transitorio honorable senado",
    "asesores bloque senado",
]

# ── Regex patterns for parsing resolution text ──────────────

_RE_DIPUTADO = re.compile(
    r"[Dd]iputad[oa]\s+(?:[Nn]acional\s+)?"
    r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*"
    r"(?:,\s*[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)?)"
)

_RE_SENADOR = re.compile(
    r"[Ss]enador[a]?\s+(?:[Nn]acional\s+)?"
    r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*"
    r"(?:,\s*[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)?)"
)

_RE_EMPLEADO = re.compile(
    r"(?:Designar|Desígnase|Desígnese|designar)\s+"
    r".*?(?:al|a la|a el)\s+(?:agente|señor[a]?|Sr[a]?\.?)\s+"
    r"([\w\sáéíóúñÁÉÍÓÚÑ,]+?)(?:\s*\(|\s*,\s*[Ll]egajo|\s*en el cargo|\s*a partir)",
    re.DOTALL,
)

_RE_LEGAJO = re.compile(r"[Ll]egajo\s+(?:N[°º]?\s*)?(\d+)")

_RE_CARGO = re.compile(
    r"(?:en el cargo de|como|en carácter de)\s+([\w\sáéíóúñÁÉÍÓÚÑ]+?)(?:\s+del|\s+de la|\s+en|\s*,|\s*\.)",
    re.IGNORECASE,
)

# Pattern for bulk designation tables (common format)
_RE_BULK_ENTRY = re.compile(
    r"([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑA-Z\s]+?),\s*"  # APELLIDO,
    r"([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)"  # Nombre
    r"(?:\s*[-–]\s*(\d+))?"  # optional legajo
)


def _safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _search_bo(
    client: httpx.Client,
    keywords: str,
    fecha_desde: str,
    fecha_hasta: str,
) -> list[dict]:
    """Search Boletín Oficial for resolutions matching keywords."""
    results: list[dict] = []
    offset = 0

    for _ in range(_MAX_PAGES):
        try:
            resp = client.post(
                _SEARCH_URL,
                json={
                    "texto": keywords,
                    "seccion": "segunda",
                    "fechaDesde": fecha_desde,
                    "fechaHasta": fecha_hasta,
                    "offset": offset,
                    "limit": _PAGE_SIZE,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("BO search failed for '%s' offset=%d: %s", keywords, offset, exc)
            break

        items = data.get("dataList") or data.get("results") or []
        if not items:
            break

        results.extend(items)
        offset += len(items)
        if len(items) < _PAGE_SIZE:
            break

    return results


def _fetch_resolution_detail(client: httpx.Client, numero_tramite: str) -> str:
    """Fetch full text of a BO resolution."""
    try:
        resp = client.post(
            _DETAIL_URL,
            json={"numeroTramite": numero_tramite},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return _safe_str(data.get("contenido") or data.get("textoCompleto") or "")
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Failed to fetch BO detail for tramite %s: %s", numero_tramite, exc)
        return ""


def _parse_assignments(
    resolution_text: str,
    boletin_date: str,
    resolution_id: str | None = None,
    boletin_numero: str | None = None,
    camara: str = "diputados",
) -> list[dict]:
    """Parse a resolution text to extract legislator↔empleado assignments."""
    if not resolution_text:
        return []

    assignments: list[dict] = []

    # Extract legislator name(s) — try diputado first, then senador
    diputados = _RE_DIPUTADO.findall(resolution_text)
    if diputados:
        legislator = diputados[0].strip()
    else:
        senadores = _RE_SENADOR.findall(resolution_text)
        if senadores:
            legislator = senadores[0].strip()
            camara = "senado"
        else:
            legislator = None

    # Try structured single-designation pattern
    empleados = _RE_EMPLEADO.findall(resolution_text)
    legajos = _RE_LEGAJO.findall(resolution_text)
    cargos = _RE_CARGO.findall(resolution_text)

    cargo = cargos[0].strip() if cargos else None

    if empleados and legislator:
        for i, emp_raw in enumerate(empleados):
            emp = emp_raw.strip().rstrip(",")
            parts = [p.strip() for p in emp.split(",", 1)]
            apellido = parts[0] if parts else emp
            nombre = parts[1] if len(parts) > 1 else None
            legajo = legajos[i] if i < len(legajos) else None

            assignments.append({
                "employee_apellido": apellido.upper(),
                "employee_nombre": nombre.title() if nombre else None,
                "legislator_name": legislator.upper(),
                "legajo": legajo,
                "cargo": cargo,
                "boletin_date": boletin_date,
                "boletin_numero": boletin_numero,
                "resolution_id": resolution_id,
                "camara": camara,
            })

    # Try bulk table pattern (APELLIDO, Nombre - Legajo)
    if not assignments and legislator:
        for match in _RE_BULK_ENTRY.finditer(resolution_text):
            apellido = match.group(1).strip()
            nombre = match.group(2).strip()
            legajo = match.group(3)
            # Skip false positives (too short or all-caps section headers)
            if len(apellido) < 2 or apellido in ("ARTICULO", "RESOLUCION", "HONORABLE"):
                continue
            assignments.append({
                "employee_apellido": apellido.upper(),
                "employee_nombre": nombre.title() if nombre else None,
                "legislator_name": legislator.upper(),
                "legajo": legajo,
                "cargo": cargo,
                "boletin_date": boletin_date,
                "boletin_numero": boletin_numero,
                "resolution_id": resolution_id,
                "camara": camara,
            })

    return assignments


def _persist_assignments(engine, assignments: list[dict]) -> int:
    """Insert assignments with ON CONFLICT DO NOTHING. Returns count inserted."""
    if not assignments:
        return 0

    with engine.begin() as conn:
        result = conn.execute(
            text(
                "INSERT INTO boletin_staff_assignments "
                "(employee_apellido, employee_nombre, legislator_name, legajo, "
                " cargo, boletin_date, boletin_numero, resolution_id, camara) "
                "VALUES (:employee_apellido, :employee_nombre, :legislator_name, :legajo, "
                " :cargo, :boletin_date, :boletin_numero, :resolution_id, :camara) "
                "ON CONFLICT (employee_apellido, legislator_name, boletin_date, camara) DO NOTHING"
            ),
            assignments,
        )
        return result.rowcount


@celery_app.task(
    name="openarg.scrape_boletin_staff",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=900,
    time_limit=1080,
)
def scrape_boletin_staff(self):
    """Scrape Boletín Oficial for HCDN staff designation resolutions."""
    engine = get_sync_engine()

    # Determine date range
    try:
        with engine.connect() as conn:
            last_date = conn.execute(
                text("SELECT MAX(boletin_date) FROM boletin_staff_assignments")
            ).scalar()
    except Exception:
        logger.info("boletin_staff_assignments table empty or missing — full historical scrape")
        last_date = None

    if last_date:
        fecha_desde = (last_date - timedelta(days=7)).strftime("%d/%m/%Y")
    else:
        fecha_desde = (date.today() - timedelta(days=730)).strftime("%d/%m/%Y")

    fecha_hasta = date.today().strftime("%d/%m/%Y")

    logger.info("Scraping BO resolutions from %s to %s", fecha_desde, fecha_hasta)

    total_found = 0
    total_inserted = 0

    try:
        with httpx.Client(timeout=30) as client:
            for camara, keywords_list in [("diputados", _SEARCH_KEYWORDS_DIPUTADOS),
                                          ("senado", _SEARCH_KEYWORDS_SENADO)]:
                for keywords in keywords_list:
                    results = _search_bo(client, keywords, fecha_desde, fecha_hasta)
                    logger.info("BO search '%s' (%s): %d results", keywords, camara, len(results))

                    for item in results:
                        numero_tramite = _safe_str(
                            item.get("numeroTramite") or item.get("id") or ""
                        )
                        if not numero_tramite:
                            continue

                        boletin_date_raw = _safe_str(
                            item.get("fechaPublicacion") or item.get("fecha") or ""
                        )
                        # Try to parse date in dd/mm/yyyy format
                        boletin_date = fecha_hasta  # fallback
                        if boletin_date_raw:
                            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
                                try:
                                    boletin_date = date.fromisoformat(
                                        _parse_date_str(boletin_date_raw, fmt)
                                    ).isoformat()
                                    break
                                except (ValueError, AttributeError):
                                    continue

                        boletin_numero = _safe_str(item.get("numeroBoletin") or "")
                        resolution_id = _safe_str(item.get("numeroNorma") or numero_tramite)

                        detail_text = _fetch_resolution_detail(client, numero_tramite)
                        if not detail_text:
                            continue

                        assignments = _parse_assignments(
                            detail_text, boletin_date, resolution_id, boletin_numero, camara,
                        )
                        total_found += len(assignments)

                        if assignments:
                            inserted = _persist_assignments(engine, assignments)
                            total_inserted += inserted
                            logger.info(
                                "Resolution %s (%s): %d assignments parsed, %d new",
                                resolution_id, camara, len(assignments), inserted,
                            )

    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Boletin scraping failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc)

    logger.info(
        "Boletin scrape complete: %d assignments found, %d new inserts",
        total_found, total_inserted,
    )
    return {
        "status": "ok",
        "date_range": f"{fecha_desde} - {fecha_hasta}",
        "assignments_found": total_found,
        "assignments_inserted": total_inserted,
    }


def _parse_date_str(raw: str, fmt: str) -> str:
    """Parse a date string with the given format and return ISO format."""
    from datetime import datetime as dt
    return dt.strptime(raw, fmt).date().isoformat()
