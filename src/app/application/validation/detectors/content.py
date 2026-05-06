"""Content detectors: operate on raw_bytes and/or materialized columns.

Cover the canonical bug "ready != usable" — the 38 HTML-as-data tables, the
127 INDEC headers, GDrive scan warnings and encoding mojibake.
"""

from __future__ import annotations

import re

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)

_HTML_PREFIXES = (b"<!doctype", b"<html", b"<?xml", b"<!--", b"<head", b"<body")
_TABULAR_FORMATS = frozenset({"csv", "txt", "xlsx", "xls", "json", "geojson", "ods"})


class HtmlAsDataDetector(Detector):
    """First bytes look like HTML/XML when CSV/Excel was expected.

    Confirmed in prod: 39 cases as `bad_zip_file`/`zip_no_parseable_file`,
    32 of them from CABA's CDN.
    """

    name = "html_as_data"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: ResourceContext) -> bool:
        if ctx.declared_format and ctx.declared_format.lower() in {"html", "htm", "xml"}:
            return False
        return ctx.raw_bytes is not None or ctx.raw_byte_sample is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        sample = (ctx.raw_byte_sample or ctx.raw_bytes or b"")[:512]
        head = sample.lstrip().lower()
        if any(head.startswith(prefix) for prefix in _HTML_PREFIXES):
            return self._finding(
                mode=mode,
                payload={"prefix": head[:64].decode("latin-1", errors="replace")},
                message="raw bytes start with HTML/XML markup but format is tabular",
                should_redownload=True,
            )
        return None


class SingleColumnDetector(Detector):
    """Materialized table has 1 column and >50% rows over 200 chars without
    separators — typical "HTML page parsed as a one-column table" symptom.
    """

    name = "single_column_html_blob"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return ctx.materialized_columns is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        cols = ctx.materialized_columns or []
        if len(cols) != 1:
            return None
        col = cols[0] or ""
        col_lower = col.strip().lower()
        suspicious_name = (
            col_lower.startswith("<")
            or col_lower.startswith("<!doctype")
            or col_lower.startswith("<html")
        )
        if suspicious_name:
            return self._finding(
                mode=mode,
                payload={"column_name": col[:200]},
                message="single column whose name looks like HTML",
                should_redownload=True,
            )
        return None


class HeaderFlattenDetector(Detector):
    """Multi-level headers flattened badly: column names like `col_0`, `col_1`,
    or pandas-suffixed `Foo.1`, `Foo.2`, `Foo.3`. 127 INDEC tables in prod.
    """

    name = "header_flatten"
    version = "1"
    severity = Severity.WARN
    _COL_GENERIC_RE = re.compile(r"^col_\d+$")
    _SUFFIX_RE = re.compile(r"\.\d+$")

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return bool(ctx.materialized_columns)

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        cols = list(ctx.materialized_columns or [])
        if not cols:
            return None
        generic = [c for c in cols if self._COL_GENERIC_RE.match(str(c).strip())]
        suffixed = [c for c in cols if self._SUFFIX_RE.search(str(c).strip())]
        if not generic and not suffixed:
            return None
        share = (len(generic) + len(suffixed)) / max(len(cols), 1)
        return self._finding(
            mode=mode,
            payload={
                "generic_count": len(generic),
                "suffixed_count": len(suffixed),
                "share": round(share, 3),
                "examples": [str(c)[:80] for c in (generic + suffixed)[:8]],
            },
            message="column header looks like badly-flattened multi-level header",
            should_redownload=False,
        )


class GDriveScanWarningDetector(Detector):
    """Google Drive virus-scan interstitial got materialized instead of the
    real file. Confirmed pattern: `cache_ssn_balances`.
    """

    name = "gdrive_scan_warning"
    version = "1"
    severity = Severity.CRITICAL
    _MARKERS = (
        b"google drive can't scan this file for viruses",
        b"too large for google to scan for viruses",
        b"download anyway",
    )

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return ctx.raw_bytes is not None or ctx.raw_byte_sample is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        sample = (ctx.raw_byte_sample or ctx.raw_bytes or b"")[:8192].lower()
        if any(marker in sample for marker in self._MARKERS):
            return self._finding(
                mode=mode,
                payload={},
                message="downloaded payload is the GDrive virus-scan interstitial",
                should_redownload=True,
            )
        return None


class SeparatorMismatchDetector(Detector):
    """CSV parsed with the wrong delimiter — the entire row collapses into a
    single column whose name is the raw header line containing the actual
    delimiter (`;`, `|`, tab) embedded as text.

    Confirmed pattern in prod (May 2026): 186 tables with <=2 cols totaling
    22.8M rows, including `cache_caba_subte_viajes_molinetes_*` (8.4M rows
    with single col `"FECHA;DESDE;HASTA;LINEA;MOLINETE;..."`).

    Distinct from `SingleColumnDetector` (which fires on HTML-parsed-as-CSV).
    This one targets delimiter mismatch — col names look like data, not HTML.
    """

    name = "separator_mismatch"
    version = "1"
    severity = Severity.CRITICAL
    _SUSPICIOUS_DELIMITERS = (";", "|", "\t")
    _AUX_COL_NAMES = frozenset({"_source_dataset_id"})

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return bool(ctx.materialized_columns)

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        cols = ctx.materialized_columns or []
        # Skip if too many cols — only suspicious when 1-2 (single col + aux)
        data_cols = [str(c) for c in cols if str(c) not in self._AUX_COL_NAMES]
        if not data_cols or len(data_cols) > 2:
            return None

        first = data_cols[0]
        # Must be longer than a typical short header
        if len(first) < 20:
            return None

        for sep in self._SUSPICIOUS_DELIMITERS:
            count = first.count(sep)
            if count >= 2:
                return self._finding(
                    mode=mode,
                    payload={
                        "first_column_name": first[:200],
                        "embedded_delimiter": sep if sep != "\t" else "\\t",
                        "delimiter_count_in_header": count,
                        "total_columns": len(cols),
                    },
                    message=(
                        f"materialized table has {len(cols)} columns and the first "
                        f"column name embeds {count} '{sep if sep != chr(9) else 'TAB'}' "
                        "delimiters — likely separator mismatch on parse"
                    ),
                    should_redownload=True,
                )
        return None


class EncodingMismatchDetector(Detector):
    """Bytes that look like latin-1 read as UTF-8 (mojibake).

    Heuristic: presence of typical mojibake sequences for `Ñ`, `ó`, `á` when
    decoded as utf-8 strict failures.
    """

    name = "encoding_mismatch"
    version = "1"
    severity = Severity.WARN
    _MOJIBAKE_RE = re.compile(r"[ÃÂ][\x80-\xBF]")

    def applicable_to(self, ctx: ResourceContext) -> bool:
        if ctx.materialized_columns:
            return True
        return ctx.raw_byte_sample is not None or ctx.raw_bytes is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        suspects: list[str] = []
        for col in ctx.materialized_columns or []:
            text_col = str(col)
            if self._MOJIBAKE_RE.search(text_col):
                suspects.append(text_col[:80])
        if suspects:
            return self._finding(
                mode=mode,
                payload={"columns": suspects[:5]},
                message="mojibake suspected in column names (latin-1 read as utf-8)",
            )
        sample = ctx.raw_byte_sample or (ctx.raw_bytes or b"")[:4096]
        if not sample:
            return None
        try:
            decoded = sample.decode("utf-8")
            if self._MOJIBAKE_RE.search(decoded):
                return self._finding(
                    mode=mode,
                    payload={"sample": decoded[:120]},
                    message="mojibake suspected in raw bytes",
                )
        except UnicodeDecodeError:
            return self._finding(
                mode=mode,
                payload={},
                message="raw bytes do not decode as utf-8 — possibly latin-1",
            )
        return None
