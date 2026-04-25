"""Pre-ingest detectors: operate before any download or after a HEAD request.

These are cheap and short-circuit the pipeline before paying for a download.
"""

from __future__ import annotations

from urllib.parse import urlparse

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)


class MissingDownloadUrlDetector(Detector):
    """`datasets.download_url` empty or invalid. 8 cases in prod (`no_download_url`).

    Severity warn — bug de catalogación, no bloquea pipeline (de hecho, sin
    URL no hay nada que bloquear).
    """

    name = "missing_download_url"
    version = "1"
    severity = Severity.WARN

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return True

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        url = (ctx.download_url or "").strip()
        if not url:
            return self._finding(
                mode=mode,
                payload={"dataset_id": ctx.dataset_id},
                message="dataset has no download_url",
            )
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return self._finding(
                mode=mode,
                payload={"download_url": url[:200]},
                message="download_url is not a well-formed URL",
            )
        if parsed.scheme not in {"http", "https"}:
            return self._finding(
                mode=mode,
                payload={"download_url": url[:200], "scheme": parsed.scheme},
                message="download_url uses non-HTTP scheme",
            )
        return None


class HttpErrorDetector(Detector):
    """The download URL responded with a 4xx/5xx, redirect-loop or DNS-dead.

    65+ cases in prod between DNS failures, redirect loops, 410/503.
    """

    name = "http_error"
    version = "1"
    severity = Severity.CRITICAL

    DEAD_PORTAL_HOSTS: frozenset[str] = frozenset(
        {
            "datos.santafe.gob.ar",
            "datos.modernizacion.gob.ar",
            "datos.ambiente.gob.ar",
            "datos.rionegro.gov.ar",
            "datos.jujuy.gob.ar",
            "datos.salta.gob.ar",
            "datos.laplata.gob.ar",
            "datos.cordoba.gob.ar",
            "datos.cultura.gob.ar",
            "datos.cordoba.gov.ar",
        }
    )

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return bool(ctx.download_url) or ctx.http_status is not None

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        # Status-code path
        status = ctx.http_status
        if status is not None and status >= 400:
            return self._finding(
                mode=mode,
                payload={"http_status": status, "url": (ctx.download_url or "")[:200]},
                message=f"download URL returned HTTP {status}",
                severity=Severity.CRITICAL if status >= 500 or status in {404, 410} else Severity.WARN,
                should_redownload=False,
            )
        # Hostname path: known-dead portals
        if ctx.download_url:
            host = (urlparse(ctx.download_url).hostname or "").lower()
            if host in self.DEAD_PORTAL_HOSTS:
                return self._finding(
                    mode=mode,
                    payload={"host": host, "url": ctx.download_url[:200]},
                    message=f"download URL host {host} is in known-dead portal list",
                )
        return None


class FileTooLargeDetector(Detector):
    """Raw download or individual ZIP entry exceeds the per-file budget.

    Per WS5 the limit applies *per inner file*, not to the ZIP decompressed
    total. Reject large individual entries; ZIPs that simply expand to many
    small files are fine.
    """

    name = "file_too_large"
    version = "2"  # bumped: now per-file, not per-archive
    severity = Severity.WARN

    PER_FILE_LIMIT_BYTES = 500 * 1024 * 1024  # 500 MB
    RAW_DOWNLOAD_LIMIT_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return (
            ctx.declared_size_bytes is not None
            or ctx.zip_member_sizes is not None
        )

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        # Per-file ZIP entry check (preferred over total)
        if ctx.zip_member_sizes:
            offenders = {
                name: size
                for name, size in ctx.zip_member_sizes.items()
                if size > self.PER_FILE_LIMIT_BYTES
            }
            if offenders:
                return self._finding(
                    mode=mode,
                    payload={
                        "limit_bytes": self.PER_FILE_LIMIT_BYTES,
                        "offenders": dict(list(offenders.items())[:5]),
                    },
                    message=f"{len(offenders)} ZIP entries exceed per-file limit",
                )
        # Raw single-file download check
        size = ctx.declared_size_bytes or 0
        if size > self.RAW_DOWNLOAD_LIMIT_BYTES:
            return self._finding(
                mode=mode,
                payload={"size_bytes": size, "limit_bytes": self.RAW_DOWNLOAD_LIMIT_BYTES},
                message="raw download exceeds 5 GB cap",
                severity=Severity.CRITICAL,
            )
        # If a non-ZIP single file is larger than the per-file limit and we
        # didn't yet receive zip metadata, also flag it.
        if not ctx.zip_member_sizes and size > self.PER_FILE_LIMIT_BYTES:
            return self._finding(
                mode=mode,
                payload={"size_bytes": size, "limit_bytes": self.PER_FILE_LIMIT_BYTES},
                message="single file exceeds 500 MB per-file limit",
            )
        return None
