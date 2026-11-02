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
import time
import zipfile
from datetime import UTC, date, datetime

import pandas as pd
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.application.pipeline.parsers import (
    dedupe_column_names,
    parse_hierarchical_headers,
    promote_buried_headers,
    unpivot_if_time_pivoted,
)
from app.application.pipeline.parsers.hierarchical_headers import ParsedHeaders, classify_row
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import (
    get_sync_engine,
    register_via_b_table,
    replace_table_keeping_dependents,
)
from app.infrastructure.celery.tasks.collector_tasks import (
    _combine_multirow_header,
    _detect_data_header_range,
    _detect_data_header_row,
    _finalize_cached_dataset,
)

logger = logging.getLogger(__name__)

INDEC_DATASETS = [
    # ── Verificados con HEAD 200 OK — Mar 2026 ──
    # URLs con nombre fijo (se actualizan in-place en el FTP del INDEC).
    # URLs con año en el nombre están marcadas con ⚠ — revisar periódicamente.
    # Las que llevan el período de publicación (`_09_25`, `1_sem_2025`) tienen
    # `url_pattern`: `resolve_dated_url` busca la edición más reciente y `url`
    # queda como la última conocida, por si no encuentra ninguna.
    # ── Precios ──
    {
        "id": "ipc",
        "name": "IPC mensual — Aperturas",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls",
    },
    # ── Actividad Económica ──
    {
        "id": "emae",
        "name": "EMAE mensual (base 2004)",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls",
    },
    {
        "id": "pib",
        "name": "PIB trimestral — Oferta y Demanda",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_12_24.xls",
    },
    # ── Industria y Construcción ──
    {
        "id": "ipi_manufacturero",
        "name": "IPI Manufacturero (producción industrial)",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipi_manufacturero_2026.xls",
    },  # ⚠ año en nombre
    {
        "id": "isac",
        "name": "ISAC — Actividad de la Construcción",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_isac_2025.xls",
    },  # ⚠ año en nombre
    # ── Comercio Exterior ──
    {
        "id": "comercio_exterior",
        "name": "Índices de Comercio Exterior",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/sh_indices_comext_04.xls",
    },
    # ── Comercio Interior ──
    {
        "id": "supermercados",
        "name": "Ventas en Supermercados",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/serie_supermercados.xlsx",
    },
    # ── Salarios ──
    {
        "id": "salarios_cvs",
        "name": "CVS — Coeficiente de Variación Salarial (diario)",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/sh_cvs_diarios_2025.xls",
    },  # ⚠ año en nombre
    {
        "id": "salarios_indice",
        "name": "Índice de Salarios",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/indice_salarios.csv",
    },
    {
        "id": "salarios_variacion",
        "name": "Variación del Índice de Salarios",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/variacion_indice_salarios.csv",
    },
    # ── Canasta Básica / Línea de Pobreza ──
    {
        "id": "canasta_basica",
        "name": "CBA y CBT — Canasta Básica (serie histórica)",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/serie_cba_cbt.xls",
    },
    # ── Pobreza e Indigencia ──
    {
        "id": "pobreza_historica",
        "name": "Pobreza e Indigencia — Serie continua",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/sh_pobrezaeindigencia_continua.xls",
    },
    {
        "id": "pobreza_informe",
        "name": "Informe de Pobreza — 31 aglomerados urbanos",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_informe_pobreza_09_25.xls",
        "url_pattern": "https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_informe_pobreza_{mm}_{yy}.xls",
    },
    # ── Mercado de Trabajo (EPH) ──
    {
        "id": "eph_tasas",
        "name": "EPH — Tasas e indicadores laborales",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_tasas_indicadores_eph_09_25.xls",
        "url_pattern": "https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_tasas_indicadores_eph_{mm}_{yy}.xls",
    },
    {
        "id": "eph_hogares",
        "name": "EPH — Indicadores de hogares",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/eph_indicadores_hogares_1_sem_2025.xls",
        "url_pattern": "https://www.indec.gob.ar/ftp/cuadros/sociedad/eph_indicadores_hogares_{s}_sem_{yyyy}.xls",
    },
    # ── Distribución del Ingreso ──
    {
        "id": "distribucion_ingreso",
        "name": "Distribución del Ingreso (EPH)",
        "url": "https://www.indec.gob.ar/ftp/cuadros/sociedad/indicadores_eph_total_urbano_ingresos_3t_2025.xls",
        "url_pattern": "https://www.indec.gob.ar/ftp/cuadros/sociedad/indicadores_eph_total_urbano_ingresos_{q}t_{yyyy}.xls",
    },
    # ── Turismo Internacional ──
    {
        "id": "turismo_receptivo",
        "name": "Turismo Receptivo — Total vías",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/serie_turismo_receptivo_total_vias.xlsx",
    },
    {
        "id": "turismo_emisivo",
        "name": "Turismo Emisivo — Total vías",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/serie_turismo_emisivo_total_vias.xlsx",
    },
    # ── Balance de Pagos ──
    {
        "id": "balance_pagos",
        "name": "Balance de Pagos mensual",
        "url": "https://www.indec.gob.ar/ftp/cuadros/economia/balanmensual.xls",
    },
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
                     :now, false, :rows)
                ON CONFLICT (source_id, portal) DO UPDATE SET
                    title = EXCLUDED.title, is_cached = false, row_count = EXCLUDED.row_count,
                    columns = EXCLUDED.columns, last_updated_at = :now, updated_at = :now
            """),
            {
                "sid": source_id,
                "title": f"INDEC - {ds_info['name']}",
                "desc": f"Datos oficiales del INDEC: {ds_info['name']}.",
                "org": "Instituto Nacional de Estadística y Censos",
                "portal": portal,
                "url": ds_info["url"],
                "dl": ds_info["url"],
                "fmt": "xlsx"
                if ds_info["url"].endswith(".xlsx")
                else "xls"
                if ds_info["url"].endswith(".xls")
                else "csv",
                "cols": columns_json,
                "tags": f"indec,estadísticas,{ds_info['id']}",
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

    # Out of the begin() block: cached_datasets INSERT inside
    # _finalize_cached_dataset opens its own tx and would otherwise race
    # the uncommitted datasets row, hitting fk_cached_datasets_dataset_id.
    if dataset_id:
        finalized = _finalize_cached_dataset(
            engine,
            dataset_id=dataset_id,
            portal=portal,
            source_id=source_id,
            table_name=table_name,
            row_count=len(df),
            columns=list(df.columns),
            declared_format=ds_info["url"].rsplit(".", 1)[-1].lower()
            if "." in ds_info["url"]
            else "csv",
            download_url=ds_info["url"],
            now=now,
        )
        if not finalized["ok"]:
            return None

    return dataset_id


def _detect_header_row(df_raw: pd.DataFrame) -> int:
    """Find the first row that looks like a real INDEC header.

    Delegates to the shared `_detect_data_header_row`, which checks for
    non-empty alphabetic tokens and rejects rows dominated by `Unnamed:N`
    placeholders. The previous implementation only counted non-NaN cells,
    so a row of `["title", "Unnamed:1", "Unnamed:2", ...]` passed (≥30 %
    non-NaN) and the WS0 validator killed it later as `placeholder_headers`.
    Pivoted layouts (`País, 2010, 2011, ...`) keep working — pure-digit
    tokens are NOT counted as placeholders.
    """
    return _detect_data_header_row(df_raw)


# Time-pivot detection, header recovery and dedup primitives now live in
# `app.application.pipeline.parsers.{time_pivot,header_recovery,column_normalization}`.
# This module imports them at the top; the inline copies that used to sit
# here were extracted in 2026-05-08 (specs/021-parser-hardening Phase 1) so
# the generic collector path can use the same logic.
def _hierarchical_frame(df_raw: pd.DataFrame, parsed: ParsedHeaders) -> pd.DataFrame:
    df = df_raw.iloc[parsed.skipped_rows :].reset_index(drop=True).copy()
    df.columns = parsed.columns
    # The spacer INDEC leaves after each year's periods. It is not always
    # empty — Cuadro 1.3 of the EPH fills it with 0.0 — so `dropna` later
    # would keep it as a `col_N` holding fake zeros.
    return df.drop(columns=[parsed.columns[j] for j in parsed.separator_columns])


def _later_year_blocks(df_raw: pd.DataFrame, parsed: ParsedHeaders) -> list[int]:
    """Rows below the data where the header starts over with another year.

    A row of years alone is not enough: it has to be followed by the same kind
    of period row the first header had, so a stray row of year-like figures
    inside the data does not cut the table in two.
    """
    if parsed.year_row_idx is None or parsed.period_kind is None:
        return []
    starts: list[int] = []
    for i in range(parsed.skipped_rows, len(df_raw) - 1):
        row = classify_row(df_raw.iloc[i].tolist())
        if row is None or row.kind != "year":
            continue
        following = classify_row(df_raw.iloc[i + 1].tolist())
        if following is not None and following.kind == parsed.period_kind:
            starts.append(i)
    return starts


def _block_titles(df_raw: pd.DataFrame, header_rows: list[int]) -> list[str]:
    """The title above each block: the nearest row holding only column 0.

    Searched upward from each block's year row to the end of the previous
    block's data. Empty string when a block has none.
    """
    titles: list[str] = []
    for k, start in enumerate(header_rows):
        floor = header_rows[k - 1] if k else -1
        title = ""
        for i in range(start - 1, floor, -1):
            row = df_raw.iloc[i]
            filled = [j for j, v in enumerate(row.tolist()) if pd.notna(v) and str(v).strip()]
            if filled == [0]:
                title = re.sub(r"\s+", " ", str(row.iloc[0])).strip()[:120]
                break
            if len(filled) > 1:
                break  # reached the previous block's data
        titles.append(title)
    return titles


_PERIOD_DATE_RE = re.compile(r"^(\d{4}-\d{2})-(\d{2})(?: 00:00:00)?$")
_MIN_PERIODS_TO_CALL_IT_MONTHLY = 12
_MIN_SHARE_OF_FIRST_OF_MONTH = 0.9


def _snap_monthly_periods_to_first(df: pd.DataFrame) -> pd.DataFrame:
    """Date a monthly series' stray columns on the first of their month.

    The IPC file's columns are real dates shown as `mmm-yy`. Someone typed
    `mar-26` for March 2026, and Excel reads that as the 26th of March of the
    current year: the cell holds 2026-03-26, displays `mar-26`, and looks right
    to anyone opening the file. Read literally, March has no `2026-03-01` and a
    query for that month finds nothing.

    Only a series that is otherwise first-of-month is touched, so a daily one
    (the CVS) keeps its days. A month with more than one column is left alone:
    that is not a typing accident, and merging them would invent duplicates.
    """
    if "periodo" not in df.columns:
        return df
    values = {str(v) for v in df["periodo"].unique()}
    if len(values) < _MIN_PERIODS_TO_CALL_IT_MONTHLY:
        return df
    by_month: dict[str, list[tuple[str, str]]] = {}
    for value in values:
        match = _PERIOD_DATE_RE.match(value)
        if match is None:
            return df
        by_month.setdefault(match.group(1), []).append((value, match.group(2)))
    fixes = {
        days[0][0]: f"{month}-01 00:00:00"
        for month, days in by_month.items()
        if len(days) == 1 and days[0][1] != "01"
    }
    if not fixes or 1 - len(fixes) / len(values) < _MIN_SHARE_OF_FIRST_OF_MONTH:
        return df
    logger.info("INDEC: monthly columns dated off the first of the month: %s", sorted(fixes))
    return df.assign(periodo=df["periodo"].astype(str).replace(fixes))


def _parse_xls_sheet(content: bytes, sheet: str | int) -> pd.DataFrame | None:
    """Parse a single Excel sheet, auto-detecting the header row."""
    try:
        # Read with no header to inspect raw structure
        df_raw = pd.read_excel(
            io.BytesIO(content),
            sheet_name=sheet,
            header=None,
            engine="xlrd",
        )
    except Exception:
        try:
            df_raw = pd.read_excel(
                io.BytesIO(content),
                sheet_name=sheet,
                header=None,
                engine="openpyxl",
            )
        except Exception:
            return None

    if df_raw.empty:
        return None

    parsed = parse_hierarchical_headers(df_raw)
    if parsed.year_row_idx is not None or parsed.period_row_idx is not None:
        starts = _later_year_blocks(df_raw, parsed)
        if starts:
            # One table per year stacked down the sheet (the CVS: `Año 2016`
            # and its months, 31 days, then `Año 2017` …). Read with the first
            # header only, every block would be dated 2016.
            bounds = [0, *starts, len(df_raw)]
            frames = []
            for a, b in zip(bounds, bounds[1:]):
                block = df_raw.iloc[a:b].reset_index(drop=True)
                frames.append(_hierarchical_frame(block, parse_hierarchical_headers(block)))
            # Blocks can also be different series that share a layout — the
            # poverty report stacks CBA, Engel's inverse and CBT, each by region
            # and month. Their titles are the only thing telling them apart.
            titles = _block_titles(df_raw, [parsed.year_row_idx, *starts])
            if all(titles) and len(set(titles)) > 1:
                frames = [f.assign(serie=t) for f, t in zip(frames, titles)]
                frames = [f[["serie", *f.columns[:-1]]] for f in frames]
            df = pd.concat(frames, ignore_index=True, sort=False)
        else:
            df = _hierarchical_frame(df_raw, parsed)
        if df.empty:
            return None
    else:
        # Use the range detector so 2-row headers (parent merged cells +
        # child sub-headers, common in INDEC `cuadros`) produce composite
        # column names like `Exportaciones_Total_mensual` instead of
        # `Unnamed: 2` from reading only the child row.
        start, end = _detect_data_header_range(df_raw)
        if end - start > 1:
            df = df_raw.iloc[end:].reset_index(drop=True).copy()
            if df.empty:
                return None
            df.columns = _combine_multirow_header(df_raw, start, end)
        else:
            try:
                df = pd.read_excel(
                    io.BytesIO(content),
                    sheet_name=sheet,
                    header=start,
                    engine="xlrd",
                )
            except Exception:
                try:
                    df = pd.read_excel(
                        io.BytesIO(content),
                        sheet_name=sheet,
                        header=start,
                        engine="openpyxl",
                    )
                except Exception:
                    return None

    # Drop rows that are entirely NaN (spacer rows after header)
    df = df.dropna(how="all").reset_index(drop=True)
    # Drop columns that are entirely NaN
    df = df.dropna(axis=1, how="all")

    if df.empty or len(df) < 2:
        return None

    if parsed.year_row_idx is None and parsed.period_row_idx is None:
        # Legacy fallback path: clean inferred header names from pandas.
        df.columns = [
            re.sub(r"\s+", " ", str(c)).strip()[:120]
            if not str(c).startswith("Unnamed")
            else f"col_{i}"
            for i, c in enumerate(df.columns)
        ]
    else:
        df.columns = [re.sub(r"\s+", " ", str(c)).strip()[:120] for c in df.columns]

    # Recover headers buried in data rows when pandas mistook a TITLE
    # for the header (most cols ended up as `col_N`). No-op when headers
    # look real. Runs before dedup so the composite names it creates
    # also get deduped if they collide.
    df = promote_buried_headers(df)

    # Drop trailing all-NaN columns introduced by forward-fill in
    # `promote_buried_headers` — when the merged-cell parent label
    # spans wider than the actual data, the fill propagates the last
    # populated header into columns that have no data underneath. The
    # original `dropna(axis=1)` above runs BEFORE the recovery, so this
    # second pass catches what the recovery introduced.
    df = df.dropna(axis=1, how="all")

    # Dedupe BEFORE unpivot. Once melted into long format the column-name
    # collision goes away, but the wide-sheet path (and the unmelted
    # branch) still has to feed `df.to_sql` which rejects duplicate names.
    df.columns = dedupe_column_names(list(df.columns))

    # Unpivot the time-pivoted INDEC layout into long format `(concepto,
    # periodo, valor)` so the downstream query pipeline can filter and
    # aggregate naturally. Idempotent: long-formatted sheets are returned
    # unchanged.
    df = unpivot_if_time_pivoted(df)

    return _snap_monthly_periods_to_first(df)


# What the INDEC's server sends for a spreadsheet. For a file that does not
# exist it answers **200 with text/html** — its own "not found" page — so a
# status check alone would take any guessed name as published.
_EXCEL_CONTENT_TYPES = frozenset(
    {
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/x-msexcel",
        "application/excel",
    }
)
# How far back to look, in months. Two years covers a release the INDEC skipped
# and still stops well short of hammering the server for a renamed series.
_RESOLVE_MONTHS_BACK = 24
_RESOLVE_TTL_SECONDS = 6 * 3600
_resolved_cache: dict[str, tuple[str, float]] = {}


def _dated_url_candidates(pattern: str, *, today: date, months_back: int) -> list[str]:
    """Every name `pattern` produces from this month back, newest first.

    Placeholders: `{yyyy}`, `{yy}`, `{mm}`, `{q}` (quarter 1-4) and `{s}`
    (semester 1-2). Walking month by month and dropping repeats gives each
    granularity for free: a quarterly pattern yields one name per quarter.
    """
    seen: list[str] = []
    year, month = today.year, today.month
    for _ in range(months_back + 1):
        url = pattern.format(
            yyyy=f"{year:04d}",
            yy=f"{year % 100:02d}",
            mm=f"{month:02d}",
            q=(month - 1) // 3 + 1,
            s=1 if month <= 6 else 2,
        )
        if url not in seen:
            seen.append(url)
        year, month = (year, month - 1) if month > 1 else (year - 1, 12)
    return seen


def _is_excel(response) -> bool:
    content_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
    return response.status_code == 200 and content_type in _EXCEL_CONTENT_TYPES


def resolve_dated_url(ds_info: dict, *, client=None, today: date | None = None) -> str:
    """The newest published edition of a file whose name carries its period.

    `cuadros_tasas_indicadores_eph_09_25.xls` was hard-coded in March 2026; by
    October the INDEC had published `_12_25`, `_03_26`, `_06_26` and `_09_26`,
    and OpenArg was still serving the September 2025 one. Each candidate is
    asked for with a HEAD, newest first, and only a spreadsheet counts.

    Falls back to `ds_info["url"]` — the last edition known to exist — when
    nothing newer answers, when the network fails, or when the entry has no
    `url_pattern`.
    """
    pattern = ds_info.get("url_pattern")
    fallback = ds_info["url"]
    if not pattern:
        return fallback

    import httpx

    candidates = _dated_url_candidates(
        pattern, today=today or datetime.now(UTC).date(), months_back=_RESOLVE_MONTHS_BACK
    )
    own_client = client is None
    client = client or httpx.Client(timeout=15.0)
    try:
        for url in candidates:
            if url == fallback:
                # Reached the edition we already knew about: nothing newer.
                break
            try:
                response = client.head(url, follow_redirects=True)
            except httpx.HTTPError:
                logger.warning("INDEC %s: no se pudo consultar %s", ds_info["id"], url)
                return fallback
            if _is_excel(response):
                logger.info("INDEC %s: edición más reciente %s", ds_info["id"], url)
                return url
    finally:
        if own_client:
            client.close()
    return fallback


def current_url(ds_info: dict) -> str:
    """`resolve_dated_url`, remembered for a few hours per process.

    The live fallback answers questions with these files; a dozen HEADs per
    question would be latency nobody asked for.
    """
    if not ds_info.get("url_pattern"):
        return ds_info["url"]
    cached = _resolved_cache.get(ds_info["id"])
    if cached and cached[1] > time.monotonic():
        return cached[0]
    url = resolve_dated_url(ds_info)
    _resolved_cache[ds_info["id"]] = (url, time.monotonic() + _RESOLVE_TTL_SECONDS)
    return url


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
            data_files = [n for n in zf.namelist() if n.lower().endswith((".csv", ".txt"))]
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

    # Default: CSV (INDEC typically uses semicolons)
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip", sep=";")
    except Exception:
        df = pd.read_csv(io.BytesIO(content), encoding="latin-1", on_bad_lines="skip", sep=";")
    return {"main": df} if df is not None and not df.empty else {}


def _registry_key(engine, table_name: str, suffix: str) -> tuple[str, int]:
    """The `(resource_identity, version)` this table is registered under.

    `raw_table_versions` allows one row per table. Most INDEC tables got theirs
    from a backfill (`indec::indec-cache_indec_…`, `backfill_postauto::…`), and
    the mart asks for `indec::ipc_variaci_n_mensual_aperturas`, while this task
    used to propose `indec::ipc_Variación mensual aperturas`. A second identity
    for a registered table hits `uq_raw_table_versions_table_name`, which
    `register_via_b_table` only logs: no heartbeat (33 of 112 tables had one in
    staging, 2026-11-02) and no mart refresh. So whoever owns the row keeps it.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT resource_identity, version FROM public.raw_table_versions "
                "WHERE schema_name = 'raw' AND table_name = :tn"
            ),
            {"tn": table_name},
        ).first()
        conn.rollback()
    if row:
        return str(row.resource_identity), int(row.version)
    return f"indec::{_sanitize_table_name(suffix).removeprefix('cache_indec_')}", 1


def _ingest_sheet(engine, ds_info: dict, sheet_name: str, df: pd.DataFrame) -> None:
    """Write one sheet to its table and register it."""
    suffix = ds_info["id"] if sheet_name == "main" else f"{ds_info['id']}_{sheet_name}"
    table_name = _sanitize_table_name(suffix)

    if len(df) > MAX_ROWS:
        df = df.head(MAX_ROWS)

    replace_table_keeping_dependents(engine, df, table_name)

    sheet_info = {
        **ds_info,
        "name": (ds_info["name"] if sheet_name == "main" else f"{ds_info['name']} — {sheet_name}"),
    }
    dataset_id = _register_dataset(engine, sheet_info, table_name, df)
    if dataset_id:
        from app.infrastructure.celery.tasks.scraper_tasks import (
            index_dataset_embedding,
        )

        index_dataset_embedding.delay(dataset_id)

    # Register in `raw_table_versions` so marts find this table via the
    # `live_table('indec::<source_id>')` macro, decoupling them from the
    # physical `cache_*` name (DEBT-019-006).
    resource_identity, version = _registry_key(engine, table_name, suffix)
    register_via_b_table(
        engine,
        resource_identity=resource_identity,
        table_name=table_name,
        schema_name="raw",
        version=version,
        row_count=len(df),
    )

    logger.info(
        "INDEC %s [%s]: %d rows cached → %s",
        ds_info["id"],
        sheet_name,
        len(df),
        table_name,
    )


def _refresh_indec_marts() -> None:
    """One last refresh of the portal's marts, once every sheet is in.

    `register_via_b_table` dispatches one per sheet, but debounced: the sheets
    that arrive within two minutes of a refresh get none. The matviews already
    hold the new rows (`replace_table_keeping_dependents`); what is left behind
    is `mart_definitions`, whose row count and date coverage are what the planner
    reads.
    """
    from app.infrastructure.celery.tasks.mart_tasks import (
        find_marts_for_portal,
        refresh_mart,
    )

    for mart_id in find_marts_for_portal("indec"):
        refresh_mart.apply_async(args=[mart_id])


def _report_failures(engine, failures: list[tuple[str, str]]) -> None:
    """Tell a person. Never raises.

    A logged warning is how the IPC went months without loading while the task
    reported success.
    """
    if not failures:
        return
    try:
        from app.application.quality.alerting import Alert, notify

        notify(
            engine,
            [
                Alert(kind="indec_load_failed", key=what, title=what[:80], detail=why[:300])
                for what, why in failures
            ],
            heading="OpenArg · INDEC no se pudo recargar",
        )
    except Exception:
        logger.warning("could not report INDEC failures", exc_info=True)


@celery_app.task(
    name="openarg.ingest_indec",
    bind=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=720,
)
def ingest_indec(self):
    """Download and cache INDEC high-value datasets.

    Each XLS file may contain multiple sheets.  Every sheet with parseable
    data is stored as a separate ``cache_indec_<id>_<sheet>`` table.
    """
    engine = get_sync_engine()
    results = {"ingested": 0, "skipped": 0, "errors": 0}
    failures: list[tuple[str, str]] = []

    try:
        for ds_info in INDEC_DATASETS:
            try:
                # The resolved edition becomes the dataset's URL, so the
                # catalogue and the download link name the file actually read.
                ds_info = {**ds_info, "url": resolve_dated_url(ds_info)}
                sheets = _download_and_parse(ds_info["url"])
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                results["errors"] += 1
                failures.append((f"{ds_info['id']}: descarga", str(exc)))
                logger.exception("Failed to download INDEC %s", ds_info["id"])
                continue

            if not sheets:
                results["skipped"] += 1
                logger.warning("INDEC %s: no data or unavailable", ds_info["id"])
                continue

            for sheet_name, df in sheets.items():
                if df.empty:
                    continue
                # Per sheet: a table that cannot load must not cost the others
                # in its file. The first IPC sheet failing kept `Ponderaciones`,
                # which nothing depends on, frozen along with it.
                try:
                    _ingest_sheet(engine, ds_info, sheet_name, df)
                    results["ingested"] += 1
                except SoftTimeLimitExceeded:
                    raise
                except Exception as exc:
                    results["errors"] += 1
                    failures.append((f"{ds_info['id']} [{sheet_name}]", str(exc)))
                    logger.exception("Failed to load INDEC %s [%s]", ds_info["id"], sheet_name)

        _report_failures(engine, failures)
        if results["ingested"]:
            _refresh_indec_marts()

        return results

    except SoftTimeLimitExceeded:
        logger.error("INDEC ingestion timed out")
        raise
    except Exception as exc:
        logger.exception("INDEC ingestion failed")
        raise self.retry(exc=exc, countdown=120)
    finally:
        engine.dispose()
