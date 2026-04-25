"""CI validator for `config/curated_sources.json`.

Two passes:
  1. Static — reuses WS0 detectors (MissingDownloadUrlDetector,
     FileTooLargeDetector). No network.
  2. Live HEAD — checks every URL returns HTTP 200 with a content-type
     consistent with the declared format. Skipped when --no-network is set
     (so the CI step can still run in air-gapped environments).

Exit code: 0 = OK, 1 = at least one validation failed.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable

import httpx

from app.infrastructure.adapters.connectors.curated_loader import (
    CuratedSource,
    load_curated_sources,
    validate_curated_sources,
)

_FORMAT_CT = {
    "csv": ("text/csv", "text/plain", "application/csv"),
    "json": ("application/json", "text/json"),
    "geojson": ("application/json", "application/geo+json"),
    "xlsx": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/octet-stream",  # some servers send this for xlsx
    ),
    "xls": ("application/vnd.ms-excel", "application/octet-stream"),
    "zip": ("application/zip", "application/x-zip-compressed", "application/octet-stream"),
}


def _ct_matches(declared_format: str, ct_header: str) -> bool:
    if not declared_format:
        return True
    expected = _FORMAT_CT.get(declared_format.lower())
    if not expected:
        return True  # unknown format, trust the JSON
    ct = (ct_header or "").lower().split(";")[0].strip()
    return any(ct.startswith(e) for e in expected)


def _live_check(sources: Iterable[CuratedSource], timeout: float) -> list[str]:
    errors: list[str] = []
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for src in sources:
            try:
                resp = client.head(src.url)
                # Many servers don't support HEAD — fall back to a streamed GET
                # that we close immediately.
                if resp.status_code in (405, 501):
                    with client.stream("GET", src.url) as gresp:
                        resp = gresp
                        ct = gresp.headers.get("content-type", "")
                        status = gresp.status_code
                else:
                    ct = resp.headers.get("content-type", "")
                    status = resp.status_code
            except Exception as exc:
                errors.append(f"{src.id}: HTTP error: {exc}")
                continue
            if status >= 400:
                errors.append(f"{src.id}: HTTP {status}")
                continue
            if not _ct_matches(src.format, ct):
                errors.append(
                    f"{src.id}: content-type '{ct}' does not match declared format '{src.format}'"
                )
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate curated_sources.json")
    parser.add_argument("--no-network", action="store_true", help="Skip live HEAD")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    sources = load_curated_sources()
    if not sources:
        print("No curated sources to validate", file=sys.stderr)
        return 0
    errors = validate_curated_sources(sources)
    if not args.no_network:
        errors.extend(_live_check(sources, timeout=args.timeout))
    if errors:
        print("\n".join(f"  - {e}" for e in errors), file=sys.stderr)
        print(f"\n{len(errors)} validation error(s)", file=sys.stderr)
        return 1
    print(f"OK — {len(sources)} curated source(s) validated", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
