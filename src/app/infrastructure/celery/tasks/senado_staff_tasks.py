"""Scrape Senado senator profiles for staff assignments."""
from __future__ import annotations

import logging
import re
import time

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

_SENATORS_URL = (
    "https://www.senado.gob.ar/micrositios/DatosAbiertos/ExportarListadoSenadores/json"
)
_PROFILE_URL = "https://www.senado.gob.ar/senadores/senador/{senator_id}"
_REQUEST_DELAY = 1.5  # seconds between requests

_RE_STAFF_ENTRY = re.compile(
    r"<td>\s*([^<]+?)\s*</td>\s*<td>\s*([A-Z]-?\d+)\s*</td>"
)


def _fetch_senators(client: httpx.Client) -> list[dict]:
    """Fetch the list of current senators from the open-data JSON endpoint."""
    try:
        resp = client.get(_SENATORS_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("table", {}).get("rows", [])
        return rows
    except (httpx.HTTPError, ValueError, KeyError) as exc:
        logger.error("Failed to fetch senators list: %s", exc)
        return []


def _parse_staff_from_html(html: str) -> list[tuple[str, str]]:
    """Extract (employee_name, categoria) pairs from a senator profile HTML.

    Looks for the ``id="Personal"`` section and matches ``<td>`` pairs
    containing name and salary category.  The HTML has missing ``<tr>`` tags
    (server bug), so the regex matches ``<td>`` pairs directly.
    """
    # Find the Personal section
    idx = html.find('id="Personal"')
    if idx == -1:
        return []

    # Isolate the table after the Personal heading
    table_start = html.find("<table", idx)
    if table_start == -1:
        return []
    table_end = html.find("</table>", table_start)
    if table_end == -1:
        table_end = len(html)
    table_html = html[table_start:table_end]

    return [(m.group(1).strip(), m.group(2).strip()) for m in _RE_STAFF_ENTRY.finditer(table_html)]


def _persist_all(engine, records: list[dict]) -> int:
    """Snapshot strategy: DELETE all + bulk INSERT in one transaction."""
    if not records:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM senado_staff"))
        return 0

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM senado_staff"))
        conn.execute(
            text(
                "INSERT INTO senado_staff "
                "(senator_id, senator_name, bloque, provincia, employee_name, categoria) "
                "VALUES (:senator_id, :senator_name, :bloque, :provincia, :employee_name, :categoria)"
            ),
            records,
        )
    return len(records)


@celery_app.task(
    name="openarg.scrape_senado_staff",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    soft_time_limit=900,
    time_limit=1080,
)
def scrape_senado_staff(self):
    """Scrape Senado senator profiles for staff assignments."""
    engine = get_sync_engine()

    senators_scraped = 0
    all_records: list[dict] = []

    try:
        with httpx.Client(timeout=30) as client:
            senators = _fetch_senators(client)
            if not senators:
                logger.warning("No senators returned from JSON endpoint")
                return {"status": "ok", "senators_scraped": 0, "staff_found": 0}

            logger.info("Fetched %d senators from open-data endpoint", len(senators))

            for senator in senators:
                senator_id = str(senator.get("ID", "")).strip()
                apellido = str(senator.get("APELLIDO", "")).strip()
                nombre = str(senator.get("NOMBRE", "")).strip()
                bloque = str(senator.get("BLOQUE", "")).strip() or None
                provincia = str(senator.get("PROVINCIA", "")).strip() or None

                if not senator_id:
                    continue

                senator_name = f"{apellido}, {nombre}".upper() if nombre else apellido.upper()

                try:
                    resp = client.get(
                        _PROFILE_URL.format(senator_id=senator_id),
                        timeout=30,
                    )
                    resp.raise_for_status()
                    staff = _parse_staff_from_html(resp.text)
                except (httpx.HTTPError, ValueError) as exc:
                    logger.warning(
                        "Failed to fetch profile for senator %s (%s): %s",
                        senator_id, senator_name, exc,
                    )
                    time.sleep(_REQUEST_DELAY)
                    continue

                for employee_name, categoria in staff:
                    all_records.append({
                        "senator_id": senator_id,
                        "senator_name": senator_name,
                        "bloque": bloque,
                        "provincia": provincia,
                        "employee_name": employee_name.upper(),
                        "categoria": categoria,
                    })

                senators_scraped += 1
                logger.debug(
                    "Senator %s (%s): %d staff members",
                    senator_id, senator_name, len(staff),
                )
                time.sleep(_REQUEST_DELAY)

        inserted = _persist_all(engine, all_records)
        logger.info(
            "Senado staff scrape complete: %d senators, %d staff inserted",
            senators_scraped, inserted,
        )

    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        logger.error("Senado staff scraping failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc)
    finally:
        engine.dispose()

    return {
        "status": "ok",
        "senators_scraped": senators_scraped,
        "staff_found": len(all_records),
    }
