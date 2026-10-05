"""Standard detector suite for ingestion validation (WS0).

Importing this module gives access to all 15 detectors. `build_default_detectors()`
returns them in the order they should run inside `IngestionValidator`.
"""

import os

from app.application.validation.detector import Detector, Severity
from app.application.validation.detectors.content import (
    EncodingMismatchDetector,
    GDriveScanWarningDetector,
    HeaderFlattenDetector,
    HtmlAsDataDetector,
    SeparatorMismatchDetector,
    SingleColumnDetector,
)
from app.application.validation.detectors.headers import HeaderFromDataDetector
from app.application.validation.detectors.metadata import (
    MetadataIntegrityDetector,
    MissingKeyColumnDetector,
    RowCountDetector,
)
from app.application.validation.detectors.naming import (
    NonTabularZipDetector,
    TableNameCollisionDetector,
    UnsupportedArchiveDetector,
)
from app.application.validation.detectors.preingest import (
    FileTooLargeDetector,
    HttpErrorDetector,
    MissingDownloadUrlDetector,
)


def _header_from_data_severity() -> Severity:
    """CRITICAL por defecto; `OPENARG_HEADER_FROM_DATA_SEVERITY=warn` lo baja.

    Un hallazgo crítico abierto hace que el sandbox se niegue a consultar la
    tabla. Con `warn` el detector sigue registrando y no oculta nada: la palanca
    para decidir, sin deploy, si las tablas rotas se ocultan antes del backfill.
    """
    raw = os.getenv("OPENARG_HEADER_FROM_DATA_SEVERITY", "").strip().lower()
    return Severity.WARN if raw in {"warn", "warning"} else Severity.CRITICAL


def build_default_detectors() -> list[Detector]:
    """Return all detectors in the order they should run."""
    return [
        # Pre-ingest gate (cheap, network/metadata-only)
        MissingDownloadUrlDetector(),
        HttpErrorDetector(),
        FileTooLargeDetector(),
        # Naming / archive structure (run before parse)
        UnsupportedArchiveDetector(),
        NonTabularZipDetector(),
        TableNameCollisionDetector(),
        # Content (need raw_bytes)
        HtmlAsDataDetector(),
        GDriveScanWarningDetector(),
        EncodingMismatchDetector(),
        # Post-parse (need materialized state)
        SingleColumnDetector(),
        SeparatorMismatchDetector(),
        HeaderFlattenDetector(),
        HeaderFromDataDetector(severity=_header_from_data_severity()),
        RowCountDetector(),
        MetadataIntegrityDetector(),
        MissingKeyColumnDetector(),
    ]


__all__ = [
    "EncodingMismatchDetector",
    "FileTooLargeDetector",
    "GDriveScanWarningDetector",
    "HeaderFlattenDetector",
    "HeaderFromDataDetector",
    "HtmlAsDataDetector",
    "HttpErrorDetector",
    "MetadataIntegrityDetector",
    "MissingDownloadUrlDetector",
    "MissingKeyColumnDetector",
    "NonTabularZipDetector",
    "RowCountDetector",
    "SeparatorMismatchDetector",
    "SingleColumnDetector",
    "TableNameCollisionDetector",
    "UnsupportedArchiveDetector",
    "build_default_detectors",
]
