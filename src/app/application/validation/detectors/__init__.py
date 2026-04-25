"""Standard detector suite for ingestion validation (WS0).

Importing this module gives access to all 14 detectors. `build_default_detectors()`
returns them in the order they should run inside `IngestionValidator`.
"""

from app.application.validation.detector import Detector
from app.application.validation.detectors.content import (
    EncodingMismatchDetector,
    GDriveScanWarningDetector,
    HeaderFlattenDetector,
    HtmlAsDataDetector,
    SingleColumnDetector,
)
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
        HeaderFlattenDetector(),
        RowCountDetector(),
        MetadataIntegrityDetector(),
        MissingKeyColumnDetector(),
    ]


__all__ = [
    "EncodingMismatchDetector",
    "FileTooLargeDetector",
    "GDriveScanWarningDetector",
    "HeaderFlattenDetector",
    "HtmlAsDataDetector",
    "HttpErrorDetector",
    "MetadataIntegrityDetector",
    "MissingDownloadUrlDetector",
    "MissingKeyColumnDetector",
    "NonTabularZipDetector",
    "RowCountDetector",
    "SingleColumnDetector",
    "TableNameCollisionDetector",
    "UnsupportedArchiveDetector",
    "build_default_detectors",
]
