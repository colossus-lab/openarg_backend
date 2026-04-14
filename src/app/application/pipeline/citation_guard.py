"""Ground citations and validate numeric claims against connector results."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from app.domain.entities.connectors.data_result import DataResult

_NUM_RE = re.compile(r"(?<![\w/])[-+]?\d[\d.,]*(?:\s*%|(?:\s+(?:millones|millón|miles|mil|billones|billón)))?", re.IGNORECASE)
_PATH_SKIP = {"_type"}
_MAX_GROUNDED_ITEMS = 5


@dataclass(frozen=True)
class NumericEvidence:
    source_name: str
    portal: str
    url: str
    accessed_at: str
    path: str
    raw_value: Any
    normalized: float


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def _normalize_numeric_token(token: str) -> float | None:
    text = token.strip().lower().replace(" ", "")
    multiplier = 1.0
    for suffix, factor in (
        ("billones", 1_000_000_000_000.0),
        ("billón", 1_000_000_000_000.0),
        ("millones", 1_000_000.0),
        ("millón", 1_000_000.0),
        ("miles", 1_000.0),
        ("mil", 1_000.0),
    ):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
            multiplier = factor
            break
    text = text.rstrip("%")
    if not text:
        return None

    # Heuristic for ES/EN formatted numbers.
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        integer, _, decimal = text.partition(",")
        if len(decimal) == 3 and integer and integer[-1].isdigit():
            text = integer + decimal
        else:
            text = integer + "." + decimal
    else:
        if text.count(".") > 1:
            text = text.replace(".", "")

    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _iter_evidence_values(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _PATH_SKIP:
                continue
            path = f"{prefix}.{key}" if prefix else key
            rows.extend(_iter_evidence_values(item, path))
        return rows
    if isinstance(value, list):
        for idx, item in enumerate(value):
            path = f"{prefix}[{idx}]"
            rows.extend(_iter_evidence_values(item, path))
        return rows
    rows.append((prefix, value))
    return rows


def collect_numeric_evidence(results: list[DataResult]) -> list[NumericEvidence]:
    evidence: list[NumericEvidence] = []
    for result in results:
        accessed_at = _safe_str(result.metadata.get("fetched_at", ""))
        for idx, record in enumerate(result.records):
            for path, raw in _iter_evidence_values(record, f"records[{idx}]"):
                normalized = _normalize_numeric_token(_safe_str(raw))
                if normalized is None:
                    continue
                evidence.append(
                    NumericEvidence(
                        source_name=result.dataset_title,
                        portal=result.portal_name,
                        url=result.portal_url,
                        accessed_at=accessed_at,
                        path=path,
                        raw_value=raw,
                        normalized=normalized,
                    )
                )
        for path, raw in _iter_evidence_values(result.metadata, "metadata"):
            normalized = _normalize_numeric_token(_safe_str(raw))
            if normalized is None:
                continue
            evidence.append(
                NumericEvidence(
                    source_name=result.dataset_title,
                    portal=result.portal_name,
                    url=result.portal_url,
                    accessed_at=accessed_at,
                    path=path,
                    raw_value=raw,
                    normalized=normalized,
                )
            )
    return evidence


def _match_source_evidence(source_hint: str, evidence: list[NumericEvidence]) -> list[NumericEvidence]:
    hint = source_hint.strip().lower()
    if not hint:
        return evidence
    matched = [
        item
        for item in evidence
        if hint in item.source_name.lower() or hint in item.portal.lower()
    ]
    return matched if matched else evidence


def _numbers_in_text(text: str) -> list[float]:
    values: list[float] = []
    for match in _NUM_RE.findall(text):
        normalized = _normalize_numeric_token(match)
        if normalized is not None:
            values.append(normalized)
    return values


def _matches_any(target: float, evidence: list[NumericEvidence]) -> list[NumericEvidence]:
    hits: list[NumericEvidence] = []
    for item in evidence:
        if math.isclose(item.normalized, target, rel_tol=1e-6, abs_tol=1e-6):
            hits.append(item)
    return hits


def ground_citations(
    answer: str,
    citations: list[dict[str, Any]],
    results: list[DataResult],
    confidence: float,
) -> tuple[list[dict[str, Any]], list[str], float]:
    evidence = collect_numeric_evidence(results)
    grounded: list[dict[str, Any]] = []
    warnings: list[str] = []
    adjusted_confidence = confidence

    if _numbers_in_text(answer) and not citations:
        warnings.append(
            "La respuesta contiene números pero no incluye citas estructuradas; verificación parcial."
        )
        adjusted_confidence = min(adjusted_confidence, 0.6)

    for citation in citations:
        claim = _safe_str(citation.get("claim", ""))
        source = _safe_str(citation.get("source", ""))
        candidate_evidence = _match_source_evidence(source, evidence)
        claim_numbers = _numbers_in_text(claim)
        matched: list[NumericEvidence] = []
        unsupported: list[float] = []

        for number in claim_numbers:
            hits = _matches_any(number, candidate_evidence)
            if hits:
                matched.extend(hits[:_MAX_GROUNDED_ITEMS])
            else:
                unsupported.append(number)

        verified = bool(candidate_evidence) and not unsupported
        if claim_numbers and unsupported:
            warnings.append(
                f"Cita sin grounding numérico completo para '{claim[:80]}'"
            )
            adjusted_confidence = min(adjusted_confidence, 0.45)
        elif not claim_numbers and not candidate_evidence:
            warnings.append(f"Cita sin fuente enlazable para '{claim[:80]}'")
            adjusted_confidence = min(adjusted_confidence, 0.7)

        grounded.append(
            {
                **citation,
                "verified": verified,
                "grounding": [
                    {
                        "source_name": item.source_name,
                        "portal": item.portal,
                        "url": item.url,
                        "accessed_at": item.accessed_at,
                        "path": item.path,
                        "value": item.raw_value,
                    }
                    for item in matched[:_MAX_GROUNDED_ITEMS]
                ],
                "unsupported_numbers": unsupported,
            }
        )

    return grounded, warnings, adjusted_confidence
