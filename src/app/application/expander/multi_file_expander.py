"""WS5 multi-file expander — turn ZIP containers into N logical resources.

Replaces the legacy "ZIP descomprimido total ≤ 500MB" rule with the new
contract from the plan:
  ✅ each *inner* file ≤ 500MB
  ✅ raw download ≤ 5GB
  ✅ no hard cap on total decompressed (just metric)

Per entry, classify into 3 paths:
  - tabular_known (CSV/XLSX/JSON/geojson)  → expand as zip_member resource
  - non_tabular  (PDF/image/video)         → either ignore or document_bundle
  - unknown                                → finding(warn), do not materialize

Format detection prefers magic bytes over file extension.
"""

from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass, field
from enum import StrEnum

logger = logging.getLogger(__name__)


PER_FILE_LIMIT_BYTES = 500 * 1024 * 1024


_TABULAR_SUFFIXES = frozenset(
    {".csv", ".txt", ".tsv", ".xlsx", ".xls", ".json", ".geojson", ".ods"}
)
_DOC_SUFFIXES = frozenset(
    {
        ".pdf",
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".odt",
        ".odp",
        ".rtf",
    }
)
_IMAGE_SUFFIXES = frozenset(
    {".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff", ".bmp"}
)
_VIDEO_SUFFIXES = frozenset({".mp4", ".mov", ".avi", ".mkv"})
_GEO_SUFFIXES = frozenset({".shp", ".dbf", ".shx", ".prj", ".kml", ".kmz"})


_MAGIC_TABLE: list[tuple[bytes, str]] = [
    (b"PK\x03\x04", "zip"),
    (b"%PDF", "pdf"),
    (b"\xff\xd8\xff", "jpeg"),
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"GIF8", "gif"),
    (b"\x1f\x8b", "gzip"),
    (b"Rar!\x1a\x07\x00", "rar"),
    (b"7z\xbc\xaf\x27\x1c", "7z"),
    (b"\xd0\xcf\x11\xe0", "ole"),  # legacy xls/.doc
    (b"<?xml", "xml"),
    (b"{", "json_or_text"),
    (b"[", "json_or_text"),
]


class EntryDecision(StrEnum):
    EXPAND_TABULAR = "expand_tabular"
    DOCUMENT_BUNDLE = "document_bundle"
    SKIP_NON_TABULAR = "skip_non_tabular"
    UNKNOWN = "unknown"
    OVERSIZED = "oversized"


@dataclass
class EntryClassification:
    name: str
    size_bytes: int
    suffix: str
    detected_format: str  # csv/xlsx/json/geojson/pdf/image/...
    decision: EntryDecision


@dataclass
class ExpandedEntry:
    name: str  # original entry name in the archive
    sub_path: str  # what to use as catalog_resources.sub_path
    size_bytes: int
    detected_format: str
    decision: EntryDecision


@dataclass
class MultiFileExpansion:
    entries: list[EntryClassification]
    expanded: list[ExpandedEntry]
    document_bundle: bool = False
    skipped_oversized: list[str] = field(default_factory=list)
    skipped_non_tabular: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)


def _suffix(name: str) -> str:
    name = name.lower()
    idx = name.rfind(".")
    return name[idx:] if idx >= 0 else ""


def _detect_format_from_magic(sample: bytes, suffix: str) -> str:
    if not sample:
        return suffix.lstrip(".")
    for magic, label in _MAGIC_TABLE:
        if sample.startswith(magic):
            if label == "json_or_text":
                return "json"
            if label == "ole":
                return "xls"
            return label
    # Heuristic: comma + newline → csv
    head = sample[:512]
    if b"," in head and b"\n" in head:
        return "csv"
    if suffix:
        return suffix.lstrip(".")
    return "unknown"


def _classify(name: str, size: int, sample: bytes) -> EntryClassification:
    suffix = _suffix(name)
    detected = _detect_format_from_magic(sample, suffix)
    if size > PER_FILE_LIMIT_BYTES:
        return EntryClassification(
            name=name,
            size_bytes=size,
            suffix=suffix,
            detected_format=detected,
            decision=EntryDecision.OVERSIZED,
        )
    # Magic bytes win over extension when they conflict. The 4 ACUMAR cases
    # in prod (`.zip` that's actually `.rar`) and HTML-pretending-to-be-CSV
    # are exactly this pattern.
    non_tabular_magic = {"pdf", "jpeg", "png", "gif", "rar", "7z"}
    if detected in non_tabular_magic:
        return EntryClassification(
            name=name,
            size_bytes=size,
            suffix=suffix,
            detected_format=detected,
            decision=EntryDecision.SKIP_NON_TABULAR,
        )
    tabular_magic = {"csv", "xlsx", "xls", "json", "geojson", "ods"}
    if detected in tabular_magic or suffix in _TABULAR_SUFFIXES:
        return EntryClassification(
            name=name,
            size_bytes=size,
            suffix=suffix,
            detected_format=detected,
            decision=EntryDecision.EXPAND_TABULAR,
        )
    if (
        suffix in _DOC_SUFFIXES
        or suffix in _IMAGE_SUFFIXES
        or suffix in _VIDEO_SUFFIXES
    ):
        return EntryClassification(
            name=name,
            size_bytes=size,
            suffix=suffix,
            detected_format=detected,
            decision=EntryDecision.SKIP_NON_TABULAR,
        )
    if suffix in _GEO_SUFFIXES:
        # Treat shapefile siblings as a single tabular bundle (caller may
        # choose to merge them; we expose them so the caller decides).
        return EntryClassification(
            name=name,
            size_bytes=size,
            suffix=suffix,
            detected_format="shapefile",
            decision=EntryDecision.EXPAND_TABULAR,
        )
    return EntryClassification(
        name=name,
        size_bytes=size,
        suffix=suffix,
        detected_format=detected,
        decision=EntryDecision.UNKNOWN,
    )


class MultiFileExpander:
    """Inspect a ZIP without fully decompressing it."""

    def __init__(self, *, magic_sample_bytes: int = 16) -> None:
        self._magic_sample_bytes = magic_sample_bytes

    def expand(self, zip_path: str) -> MultiFileExpansion:
        try:
            zf = zipfile.ZipFile(zip_path)
        except zipfile.BadZipFile:
            logger.warning("Not a valid ZIP: %s", zip_path)
            return MultiFileExpansion(entries=[], expanded=[], document_bundle=False)

        entries: list[EntryClassification] = []
        expanded: list[ExpandedEntry] = []
        oversized: list[str] = []
        non_tabular: list[str] = []
        unknown: list[str] = []

        try:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                # Read just a few bytes for magic detection — never decompress
                # the full entry just to classify.
                sample = b""
                try:
                    with zf.open(info) as stream:
                        sample = stream.read(self._magic_sample_bytes)
                except Exception:
                    logger.debug(
                        "Could not read magic bytes from %s in %s",
                        info.filename,
                        zip_path,
                        exc_info=True,
                    )
                cls = _classify(info.filename, info.file_size, sample)
                entries.append(cls)
                if cls.decision == EntryDecision.EXPAND_TABULAR:
                    expanded.append(
                        ExpandedEntry(
                            name=info.filename,
                            sub_path=info.filename,
                            size_bytes=info.file_size,
                            detected_format=cls.detected_format,
                            decision=cls.decision,
                        )
                    )
                elif cls.decision == EntryDecision.OVERSIZED:
                    oversized.append(info.filename)
                elif cls.decision == EntryDecision.SKIP_NON_TABULAR:
                    non_tabular.append(info.filename)
                elif cls.decision == EntryDecision.UNKNOWN:
                    unknown.append(info.filename)
        finally:
            zf.close()

        document_bundle = bool(entries) and not expanded and bool(non_tabular)
        return MultiFileExpansion(
            entries=entries,
            expanded=expanded,
            document_bundle=document_bundle,
            skipped_oversized=oversized,
            skipped_non_tabular=non_tabular,
            unknown=unknown,
        )


_default_expander = MultiFileExpander()


def expand_zip(zip_path: str) -> MultiFileExpansion:
    """Convenience function using the default expander."""
    return _default_expander.expand(zip_path)
