"""
INDEC — ETL selectivo de datasets de alto valor.

INDEC no tiene CKAN. Se predefinen URLs directas a CSV/XLS de alto valor
(IPC, EMAE, PIB, comercio exterior, empleo, etc.) y se descargan mensualmente.
"""
from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import UTC, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

INDEC_DATASETS = [
    # Verified working as of 2026-03
    {"id": "ipc", "name": "IPC mensual — Aperturas",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls"},
    {"id": "emae", "name": "EMAE mensual (base 2004)",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"},
    {"id": "pib", "name": "PIB trimestral — Oferta y Demanda",
     "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_12_24.xls"},
]

MAX_ROWS = 500_000
MAX_DOWNLOAD_BYTES = 500 * 1024 * 1024


def _sanitize_table_name(topic: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", topic.lower())
    clean = re.sub(r"_+", "_", clean).strip("_")
    return f"cache_indec_{clean}"


def _register_dataset(engine, ds_info: dict, table_name: str, df: pd.DataFrame):
    """Upsert into datasets and cached_datasets tables."""
    source_id = f"indec-{table_name}"
    portal = "indec"
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
                     :now, true, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = true, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id, "title": f"INDEC - {ds_info['name']}",
                "desc": f"Datos oficiales del INDEC: {ds_info['name']}.",
                "org": "Instituto Nacional de Estadística y Censos",
                "portal": portal,
                "url": ds_info["url"],
                "dl": ds_info["url"],
                "fmt": "xls" if ds_info["url"].endswith(".xls") else "csv",
                "cols": columns_json,
                "tags": f"indec,estadísticas,{ds_info['id']}",
                "now": now, "rows": len(df),
            },
        )
        dataset_row = conn.execute(
            text(
                "SELECT CAST(id AS text) FROM datasets "
                "WHERE source_id = :sid AND portal = :portal"
            ),
            {"sid": source_id, "portal": portal},
        ).fetchone()
        dataset_id = dataset_row[0] if dataset_row else None

        if dataset_id:
            conn.execute(
                text("""
                    INSERT INTO cached_datasets (dataset_id, table_name, status, row_count,
                                                  columns_json, updated_at)
                    VALUES (CAST(:did AS uuid), :tn, 'ready', :rows, :cols, :now)
                    ON CONFLICT (table_name) DO UPDATE SET
                        status = 'ready', row_count = EXCLUDED.row_count,
                        columns_json = EXCLUDED.columns_json, updated_at = :now
                """),
                {"did": dataset_id, "tn": table_name, "rows": len(df),
                 "cols": columns_json, "now": now},
            )

    return dataset_id


def _detect_header_row(df_raw: pd.DataFrame) -> int:
    """Find the first row that looks like a data header (many non-NaN values).

    INDEC XLS files have 3-5 rows of presentation headers (title, period,
    empty rows) before the actual column headers.  We look for the first row
    where ≥30 % of cells are non-NaN — that's the header or the start of data.
    Single-cell title rows are skipped.
    """
    ncols = df_raw.shape[1]
    threshold = max(3, ncols * 0.3)

    for idx in range(min(15, len(df_raw))):
        row = df_raw.iloc[idx]
        non_null_count = row.notna().sum()
        if non_null_count >= threshold:
            return idx
    return 0


def _parse_xls_sheet(content: bytes, sheet: str | int) -> pd.DataFrame | None:
    """Parse a single Excel sheet, auto-detecting the header row."""
    try:
        # Read with no header to inspect raw structure
        df_raw = pd.read_excel(
            io.BytesIO(content), sheet_name=sheet, header=None, engine="xlrd",
        )
    except Exception:
        try:
            df_raw = pd.read_excel(
                io.BytesIO(content), sheet_name=sheet, header=None, engine="openpyxl",
            )
        except Exception:
            return None

    if df_raw.empty:
        return None

    header_idx = _detect_header_row(df_raw)

    # Re-read with the correct header row
    try:
        df = pd.read_excel(
            io.BytesIO(content), sheet_name=sheet, header=header_idx, engine="xlrd",
        )
    except Exception:
        try:
            df = pd.read_excel(
                io.BytesIO(content), sheet_name=sheet, header=header_idx, engine="openpyxl",
            )
        except Exception:
            return None

    # Drop rows that are entirely NaN (spacer rows after header)
    df = df.dropna(how="all").reset_index(drop=True)
    # Drop columns that are entirely NaN
    df = df.dropna(axis=1, how="all")

    if df.empty or len(df) < 2:
        return None

    # Clean column names
    df.columns = [
        re.sub(r"\s+", " ", str(c)).strip()[:120] if not str(c).startswith("Unnamed") else f"col_{i}"
        for i, c in enumerate(df.columns)
    ]

    return df


def _download_and_parse(url: str) -> dict[str, pd.DataFrame]:
    """Download and parse a file. Returns {sheet_name: DataFrame} dict.

    For XLS/XLSX files, returns all parseable sheets.
    For CSV/ZIP files, returns a single entry keyed "main".
    Returns empty dict on failure.
    """
    import httpx

    with httpx.Client(timeout=120.0) as client:
        try:
            head = client.head(url, follow_redirects=True)
            if head.status_code == 404:
                return {}
            cl = int(head.headers.get("content-length", 0))
            if cl > MAX_DOWNLOAD_BYTES:
                logger.warning("INDEC file too large: %d bytes — %s", cl, url)
                return {}
        except Exception:
            pass

        resp = client.get(url, follow_redirects=True)
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()

    content = resp.content

    # Detect HTML responses (INDEC sometimes returns error pages)
    if content[:20].lstrip().startswith((b"<!DOCTYPE", b"<html", b"<HTML")):
        logger.warning("INDEC returned HTML instead of data file: %s", url)
        return {}

    lower_url = url.lower()

    if lower_url.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            data_files = [n for n in zf.namelist()
                          if n.lower().endswith((".csv", ".txt"))]
            if not data_files:
                return {}
            with zf.open(data_files[0]) as f:
                try:
                    df = pd.read_csv(f, encoding="utf-8", on_bad_lines="skip", sep=";")
                except Exception:
                    with zf.open(data_files[0]) as f2:
                        df = pd.read_csv(f2, encoding="latin-1", on_bad_lines="skip", sep=";")
            return {"main": df} if df is not None and not df.empty else {}

    if lower_url.endswith((".xls", ".xlsx")):
        try:
            xls = pd.ExcelFile(io.BytesIO(content), engine="xlrd")
            sheets = xls.sheet_names
        except Exception:
            try:
                xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
                sheets = xls.sheet_names
            except Exception:
                return {}

        result: dict[str, pd.DataFrame] = {}
        for sheet in sheets:
            df = _parse_xls_sheet(content, sheet)
            if df is not None:
                result[sheet] = df
        return result

    # Default: CSV
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip")
    except Exception:
        df = pd.read_csv(io.BytesIO(content), encoding="latin-1", on_bad_lines="skip")
    return {"main": df} if df is not None and not df.empty else {}


@celery_app.task(
    name="openarg.ingest_indec", bind=True, max_retries=3,
    soft_time_limit=600, time_limit=720,
)
def ingest_indec(self):
    """Download and cache INDEC high-value datasets.

    Each XLS file may contain multiple sheets.  Every sheet with parseable
    data is stored as a separate ``cache_indec_<id>_<sheet>`` table.
    """
    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}

    try:
        for ds_info in INDEC_DATASETS:
            try:
                sheets = _download_and_parse(ds_info["url"])
                if not sheets:
                    results["skipped"] += 1
                    logger.warning("INDEC %s: no data or unavailable", ds_info["id"])
                    continue

                for sheet_name, df in sheets.items():
                    if df.empty:
                        continue

                    # Build a table name per sheet
                    suffix = (
                        ds_info["id"]
                        if sheet_name == "main"
                        else f"{ds_info['id']}_{sheet_name}"
                    )
                    table_name = _sanitize_table_name(suffix)

                    if len(df) > MAX_ROWS:
                        df = df.head(MAX_ROWS)

                    df.to_sql(table_name, engine, if_exists="replace", index=False)

                    sheet_info = {
                        **ds_info,
                        "name": (
                            ds_info["name"]
                            if sheet_name == "main"
                            else f"{ds_info['name']} — {sheet_name}"
                        ),
                    }
                    dataset_id = _register_dataset(engine, sheet_info, table_name, df)
                    if dataset_id:
                        from app.infrastructure.celery.tasks.scraper_tasks import (
                            index_dataset_embedding,
                        )
                        index_dataset_embedding.delay(dataset_id)

                    results["ingested"] += 1
                    logger.info(
                        "INDEC %s [%s]: %d rows cached → %s",
                        ds_info["id"], sheet_name, len(df), table_name,
                    )

            except Exception:
                results["errors"] += 1
                logger.warning("Failed to ingest INDEC %s", ds_info["id"], exc_info=True)

        return results

    except SoftTimeLimitExceeded:
        logger.error("INDEC ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("INDEC ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
