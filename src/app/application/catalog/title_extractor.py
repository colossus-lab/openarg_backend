"""Deterministic canonical title extraction (WS4).

No LLMs in the path. Sources, in priority order:
    1. Explicit index entry (`Índice` row)
    2. Caratula / first-rows title block
    3. Combined `dataset_title + sheet_name + cuadro_numero + provincia`
    4. Technical fallback (filename + sub_path stem)

Each result carries a `title_source` and a 0.0–1.0 `title_confidence`.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

_WHITESPACE_RE = re.compile(r"\s+")
_DUPLICATE_PIPE_RE = re.compile(r"(\s*[—\-|]\s*){2,}")
_LOWER_TRAIL_RE = re.compile(r"[\s\-—_,;:.\(\)]+$")
_LOWER_LEAD_RE = re.compile(r"^[\s\-—_,;:.\(\)]+")


class TitleSource(StrEnum):
    INDEX = "index"
    CARATULA = "caratula"
    METADATA = "metadata"
    FALLBACK = "fallback"
    MANUAL = "manual"


@dataclass
class TitleExtraction:
    canonical_title: str
    display_name: str
    title_source: TitleSource
    title_confidence: float  # 0.0 - 1.0


def _normalize(s: str | None) -> str:
    """Trim, collapse whitespace, drop duplicate dash separators."""
    if not s:
        return ""
    text = unicodedata.normalize("NFC", str(s))
    text = _WHITESPACE_RE.sub(" ", text).strip()
    text = _DUPLICATE_PIPE_RE.sub(" — ", text)
    text = _LOWER_TRAIL_RE.sub("", text)
    text = _LOWER_LEAD_RE.sub("", text)
    return text


def _looks_like_title(s: str | None) -> bool:
    if not s:
        return False
    text = _normalize(s)
    if not text:
        return False
    if len(text) < 3 or len(text) > 400:
        return False
    # Must have at least one letter (avoid pure numbers like "2024")
    if not re.search(r"[A-Za-zÀ-ÿ]", text):
        return False
    return True


class TitleExtractor:
    """Deterministic extraction strategy. Cheap to construct, no I/O."""

    def __init__(self, *, portal_format: dict[str, str] | None = None) -> None:
        # Optional per-portal display formats. Defaults to the INDEC/Censo
        # patterns from the plan.
        self._portal_format = portal_format or {
            "indec": "INDEC — {provincia?} — Cuadro {cuadro_numero?} — {raw}",
            "censo_2022": "Censo 2022 — {provincia?} — Cuadro {cuadro_numero?} — {raw}",
        }

    def extract(self, ctx: dict[str, Any]) -> TitleExtraction:
        # 1. Explicit index entry — highest confidence.
        index_title = _normalize(ctx.get("index_title"))
        if _looks_like_title(index_title):
            return self._build(
                ctx, raw=index_title, source=TitleSource.INDEX, confidence=0.95
            )

        # 2. Caratula / first-rows title block.
        caratula_title = _normalize(ctx.get("caratula_title"))
        if _looks_like_title(caratula_title):
            return self._build(
                ctx, raw=caratula_title, source=TitleSource.CARATULA, confidence=0.85
            )

        # 3. Metadata combination.
        dataset_title = _normalize(ctx.get("dataset_title"))
        sheet = _normalize(ctx.get("sheet_name"))
        cuadro = _normalize(ctx.get("cuadro_numero"))
        provincia = _normalize(ctx.get("provincia"))
        parts = [p for p in (dataset_title, sheet, f"Cuadro {cuadro}" if cuadro else "", provincia) if p]
        if dataset_title:
            return self._build(
                ctx,
                raw=" — ".join(parts),
                source=TitleSource.METADATA,
                confidence=0.7 if (sheet or cuadro or provincia) else 0.55,
            )

        # 4. Fallback to filename / sub_path stem.
        filename = _normalize(ctx.get("filename"))
        sub_path = _normalize(ctx.get("sub_path"))
        stem = filename or sub_path or _normalize(ctx.get("source_id")) or "untitled"
        # strip extension if present
        stem = re.sub(r"\.[A-Za-z0-9]{1,5}$", "", stem)
        return self._build(
            ctx, raw=_normalize(stem), source=TitleSource.FALLBACK, confidence=0.2
        )

    def _build(
        self,
        ctx: dict[str, Any],
        *,
        raw: str,
        source: TitleSource,
        confidence: float,
    ) -> TitleExtraction:
        canonical = _normalize(raw)
        portal = (ctx.get("portal") or "").strip().lower()
        template = self._portal_format.get(portal)
        if template:
            display = self._apply_template(template, ctx, canonical)
        else:
            # Default display: prepend org/portal qualifier when present
            organization = _normalize(ctx.get("organization"))
            display = f"{organization} — {canonical}" if organization else canonical
        display = _normalize(display)
        return TitleExtraction(
            canonical_title=canonical,
            display_name=display or canonical,
            title_source=source,
            title_confidence=round(confidence, 2),
        )

    @staticmethod
    def _apply_template(template: str, ctx: dict[str, Any], raw: str) -> str:
        # Placeholders ending with `?` are optional — drop them silently if empty.
        out_parts: list[str] = []
        for token in re.split(r"(\{[^}]+\})", template):
            if not token.startswith("{"):
                out_parts.append(token)
                continue
            name = token.strip("{}")
            optional = name.endswith("?")
            key = name.rstrip("?")
            if key == "raw":
                value = raw
            else:
                value = _normalize(ctx.get(key))
            if not value and optional:
                # Trim a trailing separator that the placeholder would have had
                if out_parts and out_parts[-1].strip().endswith(("—", "-")):
                    out_parts[-1] = re.sub(r"\s*[—\-]\s*$", "", out_parts[-1])
                continue
            if not value:
                value = ""
            out_parts.append(value)
        return "".join(out_parts)


_default_extractor = TitleExtractor()


def extract_title(ctx: dict[str, Any]) -> TitleExtraction:
    """Convenience wrapper using the default extractor."""
    return _default_extractor.extract(ctx)
