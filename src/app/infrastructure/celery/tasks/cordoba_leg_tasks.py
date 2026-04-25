"""
Córdoba Legislatura — Scraper de datos abiertos del portal legislativo.

WordPress crawler: fetch page, extract download links (CSV/XLS),
download and cache in PostgreSQL.
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

CORDOBA_LEG_URL = "https://www.legislaturacba.gob.ar/portal-de-datos-abiertos/"

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


def _sanitize_name(name: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", name.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return clean[:50]


def _register_dataset(
    engine, source_id: str, title: str, table_name: str, df: pd.DataFrame, url: str
):
    """Upsert into datasets and cached_datasets tables."""
    portal = "cordoba_legislatura"
    columns_json = json.dumps(list(df.columns))
    now = datetime.now(UTC)

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
                "title": f"Córdoba Legislatura - {title}",
                "desc": f"Datos abiertos de la Legislatura de Córdoba: {title}",
                "org": "Legislatura de la Provincia de Córdoba",
                "portal": portal,
                "url": CORDOBA_LEG_URL,
                "dl": url,
                "fmt": url.rsplit(".", 1)[-1].lower() if "." in url else "csv",
                "cols": columns_json,
                "tags": f"cordoba,legislatura,{_sanitize_name(title)}",
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
                declared_format=url.rsplit(".", 1)[-1].lower() if "." in url else "csv",
                download_url=url,
                now=now,
            )
            if not finalized["ok"]:
                return None

    return dataset_id


def _parse_file(content: bytes, url: str) -> pd.DataFrame | None:
    """Parse a downloaded file into a DataFrame."""
    lower_url = url.lower()

    if lower_url.endswith((".xls", ".xlsx")):
        try:
            return pd.read_excel(io.BytesIO(content))
        except Exception:
            return None

    # CSV
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


def _extract_download_links(html: str) -> list[dict]:
    """Extract download links to CSV/XLS from WordPress page."""
    links = []
    pattern = re.compile(
        r'href="([^"]*\.(?:csv|xls|xlsx))"',
        re.IGNORECASE,
    )
    for match in pattern.finditer(html):
        href = match.group(1)
        if href.startswith("/"):
            href = f"https://www.legislaturacba.gob.ar{href}"
        filename = href.rsplit("/", 1)[-1]
        label = re.sub(r"\.[^.]+$", "", filename).replace("-", " ").replace("_", " ")
        links.append({"url": href, "label": label})

    # Also look for wp-content/uploads links
    pattern2 = re.compile(
        r'href="(https?://[^"]*wp-content/uploads/[^"]*\.(?:csv|xls|xlsx))"',
        re.IGNORECASE,
    )
    seen = {lnk["url"] for lnk in links}
    for match in pattern2.finditer(html):
        href = match.group(1)
        if href not in seen:
            filename = href.rsplit("/", 1)[-1]
            label = re.sub(r"\.[^.]+$", "", filename).replace("-", " ").replace("_", " ")
            links.append({"url": href, "label": label})
            seen.add(href)

    return links


@celery_app.task(
    name="openarg.scrape_cordoba_legislatura",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def scrape_cordoba_legislatura(self):
    """Scrape Córdoba Legislatura datos abiertos portal."""
    import httpx

    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            resp = client.get(CORDOBA_LEG_URL)
            resp.raise_for_status()
            html = resp.text

        links = _extract_download_links(html)
        logger.info("Córdoba Legislatura: found %d download links", len(links))

        for link in links:
            url = link["url"]
            label = link["label"]
            source_id = f"cordoba-leg-{_sanitize_name(label)}"
            table_name = f"cache_cordoba_leg_{_sanitize_name(label)}"

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

                df = _parse_file(resp.content, url)
                if df is None or df.empty:
                    results["skipped"] += 1
                    continue

                if len(df) > MAX_ROWS:
                    df = df.head(MAX_ROWS)

                df.to_sql(table_name, engine, if_exists="replace", index=False)

                dataset_id = _register_dataset(engine, source_id, label, table_name, df, url)
                if dataset_id:
                    from app.infrastructure.celery.tasks.scraper_tasks import (
                        index_dataset_embedding,
                    )

                    index_dataset_embedding.delay(dataset_id)

                results["ingested"] += 1
                logger.info("Córdoba Legislatura %s: %d rows", label, len(df))

            except Exception:
                results["errors"] += 1
                logger.warning("Córdoba Legislatura: failed %s", url, exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("Córdoba Legislatura scrape timed out")
        raise
    except Exception as exc:
        logger.exception("Córdoba Legislatura scrape failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
