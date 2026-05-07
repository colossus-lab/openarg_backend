"""
Senado — Scraper de datos abiertos del Senado de la Nación.

Scrapea las páginas HTML de datos abiertos del Senado, descarga CSVs/XLS
y cachea en PostgreSQL.
"""

from __future__ import annotations

import io
import json
import logging
import re
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.collector_tasks import _finalize_cached_dataset

logger = logging.getLogger(__name__)

SENADO_BASE_URL = "https://www.senado.gob.ar/micrositios/DatosAbiertos"

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024

# Known tabular file extensions to process
TABULAR_EXTENSIONS = (".csv", ".xls", ".xlsx")


def _sanitize_name(name: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return clean[:50]


def _register_dataset(
    engine,
    source_id: str,
    title: str,
    table_name: str,
    df: pd.DataFrame,
    url: str,
    fmt: str = "csv",
):
    """Upsert into datasets and cached_datasets tables."""
    portal = "senado"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)
    # Truncate format to fit varchar(50) column
    fmt_value = fmt[:50] if fmt else "csv"

    # Commit `datasets` upsert before `_finalize_cached_dataset` runs so
    # the FK to `datasets.id` is satisfied in the second transaction.
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO datasets
                    (source_id, title, description, organization, portal, url,
                     download_url, format, columns, tags, last_updated_at, is_cached, row_count)
                VALUES
                    (:sid, :title, :desc, :org, :portal, :url, :dl, :fmt, :cols, :tags,
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": f"Senado - {title}",
                "desc": f"Datos abiertos del Senado de la Nación: {title}",
                "org": "Senado de la Nación Argentina",
                "portal": portal,
                "url": SENADO_BASE_URL,
                "dl": url,
                "fmt": fmt_value,
                "cols": columns_json,
                "tags": f"senado,congreso,{_sanitize_name(title)}",
                "now": now,
                "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

    if dataset_id:
        finalized = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=portal,
            source_id=source_id,
            table_name=table_name,
            row_count=len(df),
            columns=list(df.columns),
            declared_format=fmt_value,
            download_url=url,
            now=now,
        )
        if not finalized["ok"]:
            return None

    return dataset_id


def _parse_response(content: bytes, url: str, fmt: str = "file") -> pd.DataFrame | None:
    """Parse a downloaded response (JSON, CSV, XLS, XLSX) into a DataFrame."""
    if fmt == "json" or url.lower().endswith("/json") or url.lower().endswith("/json/todas"):
        try:
            import json as _json

            data = _json.loads(content)
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Senado pattern: {"table": {"rows": [...]}}
                if "table" in data and isinstance(data["table"], dict):
                    records = data["table"].get("rows", [])
                else:
                    records = (
                        data.get("data") or data.get("results") or data.get("Records") or [data]
                    )
            else:
                return None
            if not records or not isinstance(records, list):
                return None
            # Flatten nested dicts if records contain them
            if records and isinstance(records[0], dict):
                return pd.json_normalize(records)
            return pd.DataFrame(records)
        except Exception:
            return None

    lower_url = url.lower()
    if lower_url.endswith((".xls", ".xlsx")) or fmt == "excel":
        try:
            return pd.read_excel(io.BytesIO(content))
        except Exception:
            return None

    # Default: CSV
    for encoding in ("utf-8", "latin-1"):
        for sep in (",", ";"):
            try:
                df = pd.read_csv(
                    io.BytesIO(content),
                    encoding=encoding,
                    sep=sep,
                    on_bad_lines="skip",
                )
                if len(df.columns) > 1:
                    return df
            except Exception:
                continue
    return None


def _extract_links(html: str) -> list[dict]:
    """Extract download links from Senado HTML page.

    The Senado doesn't serve static CSV/XLS files.  Instead, it exposes
    ``/Exportar.../json`` and ``/Exportar.../Excel`` endpoints that return
    data directly.  We prefer JSON endpoints since they're easier to parse.
    """
    links: list[dict] = []
    seen: set[str] = set()

    # Match Exportar endpoints (JSON preferred, Excel as fallback)
    pattern = re.compile(
        r'href="(/micrositios/DatosAbiertos/Exportar[^"]*)"',
        re.IGNORECASE,
    )
    json_endpoints: dict[str, str] = {}  # base_name → full path
    excel_endpoints: dict[str, str] = {}

    for match in pattern.finditer(html):
        path = match.group(1)
        # Derive a label from the path: /Exportar<Name>/<format>
        parts = path.rstrip("/").split("/")
        fmt = parts[-1].lower() if parts else ""
        # base is the Exportar part without the format suffix
        base = "/".join(parts[:-1]) if fmt in ("json", "excel") else path

        if fmt == "json":
            json_endpoints[base] = path
        elif fmt == "excel":
            excel_endpoints[base] = path

    # Prefer JSON endpoints; fall back to Excel for those without JSON
    for base, path in json_endpoints.items():
        url = f"https://www.senado.gob.ar{path}"
        # Derive label from path e.g. ExportarListadoSenadores → Listado Senadores
        label_raw = path.split("Exportar")[-1].split("/")[0]
        label = re.sub(r"([A-Z])", r" \1", label_raw).strip()
        if url not in seen:
            links.append({"url": url, "label": label, "format": "json"})
            seen.add(url)

    for base, path in excel_endpoints.items():
        if base not in json_endpoints:
            url = f"https://www.senado.gob.ar{path}"
            label_raw = path.split("Exportar")[-1].split("/")[0]
            label = re.sub(r"([A-Z])", r" \1", label_raw).strip()
            if url not in seen:
                links.append({"url": url, "label": label, "format": "excel"})
                seen.add(url)

    # Also match static file links (.csv, .xls, .xlsx)
    pattern2 = re.compile(
        r'href="([^"]*\.(?:csv|xls|xlsx))"',
        re.IGNORECASE,
    )
    for match in pattern2.finditer(html):
        href = match.group(1)
        if href.startswith("/"):
            href = f"https://www.senado.gob.ar{href}"
        if href not in seen:
            links.append({"url": href, "label": href.rsplit("/", 1)[-1], "format": "file"})
            seen.add(href)

    return links


@celery_app.task(
    name="openarg.scrape_senado",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def scrape_senado(self):
    """Scrape Senado datos abiertos and cache tabular datasets."""
    import httpx

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            # Fetch main page
            resp = client.get(SENADO_BASE_URL)
            resp.raise_for_status()
            main_html = resp.text

            # Extract download links
            links = _extract_links(main_html)

            # Also check category pages (26 categories)
            category_pattern = re.compile(
                r'href="(/micrositios/DatosAbiertos/[^"]*)"',
                re.IGNORECASE,
            )
            category_urls = set()
            for match in category_pattern.finditer(main_html):
                cat_url = f"https://www.senado.gob.ar{match.group(1)}"
                category_urls.add(cat_url)

            for cat_url in list(category_urls)[:30]:  # safety limit
                try:
                    cat_resp = client.get(cat_url)
                    if cat_resp.status_code == 200:
                        links.extend(_extract_links(cat_resp.text))
                except Exception:
                    continue

        # Deduplicate by URL
        seen_urls: set[str] = set()
        unique_links = []
        for link in links:
            if link["url"] not in seen_urls:
                seen_urls.add(link["url"])
                unique_links.append(link)

        logger.info("Senado: found %d unique download links", len(unique_links))

        for link in unique_links:
            url = link["url"]
            label = link["label"]
            fmt = link.get("format", "file")
            source_id = f"senado-{_sanitize_name(label)}"
            table_name = f"cache_senado_{_sanitize_name(label)}"

            try:
                with httpx.Client(timeout=120.0, follow_redirects=True) as client:
                    try:
                        head = client.head(url)
                        cl = int(head.headers.get("content-length", 0))
                        if cl > MAX_DOWNLOAD_BYTES:
                            continue
                    except Exception:
                        pass

                    resp = client.get(url)
                    if resp.status_code != 200:
                        continue
                    resp.raise_for_status()

                df = _parse_response(resp.content, url, fmt)
                if df is None or df.empty:
                    results["skipped"] += 1
                    continue

                if len(df) > MAX_ROWS:
                    df = df.head(MAX_ROWS)

                try:
                    df.to_sql(table_name, engine, if_exists="replace", index=False)
                except Exception as exc:
                    if "DependentObjectsStillExist" not in type(exc).__name__ \
                            and "depend on it" not in str(exc):
                        raise
                    logger.info(
                        "Senado %s: replace blocked by dependent view; "
                        "falling back to TRUNCATE + append", label,
                    )
                    from app.infrastructure.celery.tasks._db import safe_truncate_table
                    safe_truncate_table(engine, table_name)
                    df.to_sql(table_name, engine, if_exists="append", index=False)

                dataset_id = _register_dataset(engine, source_id, label, table_name, df, url, fmt)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )

                    index_dataset_embedding.delay(dataset_id)

                # Register in `raw_table_versions` so marts that target the
                # `senado` portal find these tables via `live_table()`.
                from app.infrastructure.celery.tasks._db import register_via_b_table

                register_via_b_table(
                    engine,
                    resource_identity=f"senado::{_sanitize_name(label)}",
                    table_name=table_name,
                    schema_name="public",
                    row_count=len(df),
                )

                results["ingested"] += 1
                logger.info("Senado %s: %d rows", label, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning("Senado: failed %s", url, exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("Senado scrape timed out")
        raise
    except Exception as exc:
        logger.exception("Senado scrape failed")
        raise self.retry(exc=exc, countdown=120)
