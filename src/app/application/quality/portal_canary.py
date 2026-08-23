"""One resource per portal, re-read on purpose, to notice when a portal changes.

Ten portals were found dead on 2026-03 — `santa_fe`, `rio_negro`, `salta`,
`la_plata` and others — by someone reading a list of failures by hand. Nothing
was watching, and there was nothing to watch: the collector only ever touched a
portal when it had work there, so a portal that quietly stopped serving looked
identical to one nobody had asked anything of.

That is what a canary is for. It asks one small question of every portal on a
schedule, whether or not there is work to do, so the answer to "is this portal
still serving data?" exists before a person needs it.

**It reads the first bytes, not the file.** A canary that downloads is a second
collector, and it would cost what collecting costs. The opening kilobytes are
enough to tell a CSV from a login page, which is the failure that actually
happens: 85 % of this catalogue's `html_as_data` cases are a Microsoft auth
redirect served with a 200, and to anything that only checks status codes that
portal looks perfectly healthy.

Three verdicts, and the middle one is why this exists:

- `ok` — answered, and what came back still looks like data.
- `serving_html` — answered 200 and returned a page. This is a portal that is
  *up* and no longer usable, and no status-code check will ever say so.
- `unreachable` — did not answer, or answered with an error.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

# Enough to see a doctype or a header row; far less than a file.
_PROBE_BYTES = 4096
_TIMEOUT = 20.0

_HTML_MARKERS = (b"<!doctype html", b"<html", b"<head", b"<script")

# Formats whose opening bytes are legitimately binary and would be mistaken for
# nothing in particular. A canary that flagged these would cry wolf on every run.
_BINARY_FORMATS = {"zip", "xlsx", "xls", "pdf", "shp", "7z", "gz", "rar", "parquet"}


@dataclass(frozen=True)
class CanaryResult:
    portal: str
    verdict: str  # ok | serving_html | unreachable
    detail: str
    url: str = ""


def probe(url: str, *, fmt: str | None = None, timeout: float = _TIMEOUT) -> CanaryResult:
    """Read the opening bytes of one URL and judge what came back."""
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True, max_redirects=10) as client:
            with client.stream("GET", url) as resp:
                status = resp.status_code
                ctype = (resp.headers.get("content-type") or "").lower()
                head = b""
                for chunk in resp.iter_bytes(chunk_size=_PROBE_BYTES):
                    head = chunk
                    break
    except Exception as exc:
        return CanaryResult(
            portal="", verdict="unreachable", detail=f"{type(exc).__name__}: {str(exc)[:120]}",
            url=url,
        )

    if status >= 400:
        return CanaryResult(portal="", verdict="unreachable", detail=f"HTTP {status}", url=url)

    lowered = head[:512].lower()
    looks_html = any(m in lowered for m in _HTML_MARKERS) or "text/html" in ctype
    if looks_html and (fmt or "").lower() not in {"html", "htm"}:
        # The failure that matters: a 200 with a page. An auth redirect, a
        # maintenance notice, a "resource moved" — all of them healthy to a
        # status check and useless as data.
        return CanaryResult(
            portal="", verdict="serving_html",
            detail=f"HTTP {status} pero devolvió HTML (content-type: {ctype[:40] or 'sin declarar'})",
            url=url,
        )

    if not head and (fmt or "").lower() not in _BINARY_FORMATS:
        return CanaryResult(portal="", verdict="unreachable", detail="respuesta vacía", url=url)

    return CanaryResult(portal="", verdict="ok", detail=f"HTTP {status} · {len(head)} bytes", url=url)
