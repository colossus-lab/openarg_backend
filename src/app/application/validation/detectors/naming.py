"""Naming + storage detectors.

Cover the 10 `pg_type_typname_nsp_index` collisions, the ACUMAR `.rar`
disguised as `.zip`, and the `document_bundle` ZIPs (PDFs/images).
"""

from __future__ import annotations

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)

_PG_TYPNAME_LIMIT = 63
_TABULAR_SUFFIXES = frozenset(
    {".csv", ".txt", ".xlsx", ".xls", ".json", ".geojson", ".ods", ".xml"}
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
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".tif",
        ".tiff",
        ".webp",
        ".mp4",
        ".mp3",
        ".wav",
    }
)


def _suffix(name: str) -> str:
    name = name.lower()
    idx = name.rfind(".")
    return name[idx:] if idx >= 0 else ""


class TableNameCollisionDetector(Detector):
    """The `materialized_table_name` truncated to 63 chars collides with an
    existing table associated with another resource. 10 cases in prod.

    Runs **before** CREATE TABLE — the engine and a candidate name come from
    the caller. If `metadata['existing_table_resource']` is provided and
    differs from `ctx.resource_id`, that's the collision.
    """

    name = "table_name_collision"
    version = "1"
    severity = Severity.CRITICAL

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return ctx.table_name is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        candidate = (ctx.table_name or "").strip()
        if not candidate:
            return None
        truncated = candidate[:_PG_TYPNAME_LIMIT]
        existing_owner = ctx.metadata.get("existing_table_resource")
        if existing_owner and existing_owner != ctx.resource_id:
            return self._finding(
                mode=mode,
                payload={
                    "candidate": candidate,
                    "truncated": truncated,
                    "owner_resource_id": existing_owner,
                },
                message="materialized_table_name collides with another resource",
            )
        if len(candidate) > _PG_TYPNAME_LIMIT:
            return self._finding(
                mode=mode,
                payload={"candidate": candidate, "truncated": truncated},
                message="materialized_table_name exceeds 63 chars; needs discriminator",
                severity=Severity.WARN,
            )
        return None


class NonTabularZipDetector(Detector):
    """ZIP contains only PDFs/images/documents — not corruption, classification.

    Resource gets `materialization_status='non_tabular'` and stays in catalog
    as `document_bundle`. 2 cases in prod (ACUMAR Actas).
    """

    name = "non_tabular_zip"
    version = "1"
    severity = Severity.INFO

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return bool(ctx.zip_member_names)

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        members = ctx.zip_member_names or []
        if not members:
            return None
        suffixes = [_suffix(m) for m in members if not m.endswith("/")]
        if not suffixes:
            return None
        tabular = [s for s in suffixes if s in _TABULAR_SUFFIXES]
        documents = [s for s in suffixes if s in _DOC_SUFFIXES]
        if not tabular and documents:
            return self._finding(
                mode=mode,
                payload={
                    "doc_count": len(documents),
                    "member_count": len(members),
                    "examples": members[:5],
                },
                message="ZIP contains only documents/images — classify as document_bundle",
            )
        return None


class UnsupportedArchiveDetector(Detector):
    """File declared as .zip but the magic bytes look like .rar/.7z/.tar.gz.

    4 cases in prod (ACUMAR Actas 2025).
    """

    name = "unsupported_archive"
    version = "1"
    severity = Severity.WARN

    _RAR_MAGIC = (b"Rar!\x1a\x07\x00", b"Rar!\x1a\x07\x01\x00")
    _7Z_MAGIC = (b"7z\xbc\xaf\x27\x1c",)
    _GZ_MAGIC = (b"\x1f\x8b",)
    _ZIP_MAGIC = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")

    def applicable_to(self, ctx: ResourceContext) -> bool:
        if not ctx.declared_format:
            return False
        if ctx.declared_format.lower() not in {"zip"}:
            return False
        return ctx.raw_byte_sample is not None or ctx.raw_bytes is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        sample = (ctx.raw_byte_sample or ctx.raw_bytes or b"")[:16]
        if not sample:
            return None
        if any(sample.startswith(m) for m in self._ZIP_MAGIC):
            return None
        if any(sample.startswith(m) for m in self._RAR_MAGIC):
            return self._finding(
                mode=mode,
                payload={"detected": "rar"},
                message="file declared zip but magic bytes are .rar",
            )
        if any(sample.startswith(m) for m in self._7Z_MAGIC):
            return self._finding(
                mode=mode,
                payload={"detected": "7z"},
                message="file declared zip but magic bytes are .7z",
            )
        if any(sample.startswith(m) for m in self._GZ_MAGIC):
            return self._finding(
                mode=mode,
                payload={"detected": "gzip"},
                message="file declared zip but magic bytes are gzip",
            )
        return self._finding(
            mode=mode,
            payload={"first_bytes": sample.hex()},
            message="file declared zip but does not match any known archive magic",
        )
