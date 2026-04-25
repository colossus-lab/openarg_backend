"""Ground citations and validate numeric claims against connector results."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

from app.domain.entities.connectors.data_result import DataResult

_NUM_RE = re.compile(r"(?<![\w/])[-+]?\d[\d.,]*(?:\s*%|(?:\s+(?:millones|millón|miles|mil|billones|billón)))?", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_DATE_RE = re.compile(r"\b(19\d{2}|20\d{2})([-/])(\d{1,2})(?:\2(\d{1,2}))?\b")
_DATE_DMY_RE = re.compile(r"\b(\d{1,2})([-/])(\d{1,2})\2(19\d{2}|20\d{2})\b")
_DATE_TEXT_RE = re.compile(
    r"\b\d{1,2}\s+de\s+(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)(?:(?:\s+de)?\s+(?:19\d{2}|20\d{2}))?\b",
    re.IGNORECASE,
)
_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b")
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


@dataclass(frozen=True)
class CitationAssessment:
    claim: str
    source: str
    candidate_evidence: list[NumericEvidence]
    matched: list[NumericEvidence]
    claim_numbers: list[float]
    unsupported: list[float]
    verified: bool
    partially_grounded: bool
    derived_comparison: bool

    @property
    def has_source_match(self) -> bool:
        return bool(self.candidate_evidence)

    @property
    def is_soft_warning(self) -> bool:
        return bool(self.unsupported) and (
            self.partially_grounded or (self.derived_comparison and self.has_source_match)
        )

    @property
    def is_hard_failure(self) -> bool:
        return bool(self.unsupported) and not self.is_soft_warning

    @property
    def is_source_warning(self) -> bool:
        return not self.claim_numbers and not self.has_source_match


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
        elif "." in text:
            integer, _, decimal = text.partition(".")
            if len(decimal) == 3 and integer and integer[-1].isdigit():
                text = integer + decimal

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


def _iter_temporal_evidence_values(value: Any, prefix: str) -> list[tuple[str, Any, float]]:
    if not isinstance(value, str):
        return []
    text = value.strip()
    if not text or text.startswith("http://") or text.startswith("https://"):
        return []

    rows: list[tuple[str, Any, float]] = []
    date_matches = list(_DATE_RE.finditer(text))
    if date_matches:
        for match in date_matches:
            year = int(match.group(1))
            month = int(match.group(3))
            rows.append((f"{prefix}.year", year, float(year)))
            rows.append((f"{prefix}.month", month, float(month)))
            if match.group(4):
                day = int(match.group(4))
                rows.append((f"{prefix}.day", day, float(day)))
        return rows

    for match in _YEAR_RE.finditer(text):
        year = int(match.group(1))
        rows.append((f"{prefix}.year", year, float(year)))
    return rows


def _unit_scale_factor(units: str) -> float | None:
    text = units.strip().lower()
    if not text:
        return None
    if "millones" in text or "millón" in text:
        return 1_000_000.0
    if "miles" in text or re.search(r"\bmil\b", text):
        return 1_000.0
    return None


def collect_numeric_evidence(results: list[DataResult]) -> list[NumericEvidence]:
    evidence: list[NumericEvidence] = []
    for result in results:
        accessed_at = _safe_str(result.metadata.get("fetched_at", ""))
        unit_scale = _unit_scale_factor(_safe_str(result.metadata.get("units", "")))
        for path, raw, normalized in _iter_temporal_evidence_values(result.dataset_title, "dataset_title"):
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
        record_count = len(result.records)
        evidence.append(
            NumericEvidence(
                source_name=result.dataset_title,
                portal=result.portal_name,
                url=result.portal_url,
                accessed_at=accessed_at,
                path="summary.record_count",
                raw_value=record_count,
                normalized=float(record_count),
            )
        )
        for idx, record in enumerate(result.records):
            for path, raw in _iter_evidence_values(record, f"records[{idx}]"):
                normalized = _normalize_numeric_token(_safe_str(raw))
                if normalized is None:
                    for temporal_path, temporal_raw, temporal_normalized in _iter_temporal_evidence_values(raw, path):
                        evidence.append(
                            NumericEvidence(
                                source_name=result.dataset_title,
                                portal=result.portal_name,
                                url=result.portal_url,
                                accessed_at=accessed_at,
                                path=temporal_path,
                                raw_value=temporal_raw,
                                normalized=temporal_normalized,
                            )
                        )
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
                if unit_scale is not None:
                    evidence.append(
                        NumericEvidence(
                            source_name=result.dataset_title,
                            portal=result.portal_name,
                            url=result.portal_url,
                            accessed_at=accessed_at,
                            path=f"{path}.scaled_units",
                            raw_value=raw,
                            normalized=normalized * unit_scale,
                        )
                    )
        for path, raw in _iter_evidence_values(result.metadata, "metadata"):
            normalized = _normalize_numeric_token(_safe_str(raw))
            if normalized is None:
                for temporal_path, temporal_raw, temporal_normalized in _iter_temporal_evidence_values(raw, path):
                    evidence.append(
                        NumericEvidence(
                            source_name=result.dataset_title,
                            portal=result.portal_name,
                            url=result.portal_url,
                            accessed_at=accessed_at,
                            path=temporal_path,
                            raw_value=temporal_raw,
                            normalized=temporal_normalized,
                        )
                    )
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
            if unit_scale is not None:
                evidence.append(
                    NumericEvidence(
                        source_name=result.dataset_title,
                        portal=result.portal_name,
                        url=result.portal_url,
                        accessed_at=accessed_at,
                        path=f"{path}.scaled_units",
                        raw_value=raw,
                        normalized=normalized * unit_scale,
                    )
                )
    evidence.append(
        NumericEvidence(
            source_name="Resumen de resultados",
            portal="",
            url="",
            accessed_at="",
            path="summary.results_count",
            raw_value=len(results),
            normalized=float(len(results)),
        )
    )
    return evidence


def _match_source_evidence(source_hint: str, evidence: list[NumericEvidence]) -> list[NumericEvidence]:
    hint = source_hint.strip().lower()
    summary = [item for item in evidence if item.path.startswith("summary.")]
    if not hint:
        return evidence
    matched = [
        item
        for item in evidence
        if hint in item.source_name.lower() or hint in item.portal.lower()
    ]
    if matched:
        return matched + [item for item in summary if item not in matched]
    return evidence


def _numbers_in_text(text: str) -> list[float]:
    values: list[float] = []
    stripped = _TIME_RE.sub(" ", _DATE_TEXT_RE.sub(" ", _DATE_DMY_RE.sub(" ", _DATE_RE.sub(" ", text))))
    for match in _NUM_RE.findall(stripped):
        normalized = _normalize_numeric_token(match)
        if normalized is not None:
            values.append(normalized)
    return values


def _is_rounded_quote_match(target: float, item: NumericEvidence) -> bool:
    if item.path.endswith(".compra") or item.path.endswith(".venta"):
        if float(target).is_integer() and not float(item.normalized).is_integer():
            return math.isclose(item.normalized, target, rel_tol=0.0, abs_tol=0.5)
    return False


def _matches_any(target: float, evidence: list[NumericEvidence]) -> list[NumericEvidence]:
    hits: list[NumericEvidence] = []
    for item in evidence:
        if math.isclose(item.normalized, target, rel_tol=1e-6, abs_tol=1e-6) or _is_rounded_quote_match(
            target, item
        ):
            hits.append(item)
    return hits


def _has_partial_grounding_support(
    claim_numbers: list[float],
    unsupported: list[float],
    matched: list[NumericEvidence],
) -> bool:
    if not claim_numbers or not unsupported or not matched:
        return False
    non_temporal_paths = {
        item.path
        for item in matched
        if not item.path.endswith(".year")
        and not item.path.endswith(".month")
        and not item.path.endswith(".day")
    }
    if not non_temporal_paths:
        return False
    supported_count = len(claim_numbers) - len(unsupported)
    if supported_count < 2:
        return False
    return len(non_temporal_paths) >= 2


def _is_derived_comparison_claim(claim: str) -> bool:
    text = claim.lower()
    markers = (
        "salto",
        "aumento",
        "incremento",
        "recuperación",
        "recuperacion",
        "rebote",
        "repunte",
        "crecimiento",
        "caída",
        "descenso",
        "suba",
        "subió",
        "subieron",
        "bajó",
        "creció",
        "crecieron",
        "revirtió",
        "revertio",
        "variación",
        "cambio",
        "comparación",
        "comparado",
        "respecto",
    )
    return any(marker in text for marker in markers)


def _assess_citation(
    citation: dict[str, Any],
    evidence: list[NumericEvidence],
) -> CitationAssessment:
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

    return CitationAssessment(
        claim=claim,
        source=source,
        candidate_evidence=candidate_evidence,
        matched=matched,
        claim_numbers=claim_numbers,
        unsupported=unsupported,
        verified=bool(candidate_evidence) and not unsupported,
        partially_grounded=_has_partial_grounding_support(claim_numbers, unsupported, matched),
        derived_comparison=_is_derived_comparison_claim(claim),
    )


def _derive_confidence(
    answer: str,
    assessments: list[CitationAssessment],
    results: list[DataResult],
    llm_confidence: float,
) -> float:
    answer_has_numbers = bool(_numbers_in_text(answer))
    if not results:
        return min(llm_confidence, 0.45)

    if not assessments:
        return 0.6 if answer_has_numbers else min(max(llm_confidence, 0.7), 0.8)

    hard_failures = sum(1 for item in assessments if item.is_hard_failure)
    soft_warnings = sum(1 for item in assessments if item.is_soft_warning)
    source_warnings = sum(1 for item in assessments if item.is_source_warning)
    verified = sum(1 for item in assessments if item.verified)
    sourced = sum(1 for item in assessments if item.has_source_match)

    if hard_failures:
        return 0.45
    if verified and soft_warnings == 0 and source_warnings == 0:
        return 0.9
    if verified and soft_warnings:
        return 0.85
    if verified and source_warnings:
        return 0.8
    if soft_warnings:
        return 0.7
    if sourced:
        return 0.8
    if source_warnings:
        return 0.7
    return min(llm_confidence, 0.6)


def ground_citations(
    answer: str,
    citations: list[dict[str, Any]],
    results: list[DataResult],
    confidence: float,
) -> tuple[list[dict[str, Any]], list[str], float]:
    evidence = collect_numeric_evidence(results)
    grounded: list[dict[str, Any]] = []
    warnings: list[str] = []
    assessments: list[CitationAssessment] = []

    if _numbers_in_text(answer) and not citations:
        warnings.append(
            "La respuesta contiene números pero no incluye citas estructuradas; verificación parcial."
        )

    for citation in citations:
        assessment = _assess_citation(citation, evidence)
        assessments.append(assessment)

        if assessment.claim_numbers and assessment.unsupported:
            warnings.append(
                f"Cita sin grounding numérico completo para '{assessment.claim[:80]}'"
            )
        elif assessment.is_source_warning:
            warnings.append(f"Cita sin fuente enlazable para '{assessment.claim[:80]}'")

        grounded.append(
            {
                **citation,
                "verified": assessment.verified,
                "grounding": [
                    {
                        "source_name": item.source_name,
                        "portal": item.portal,
                        "url": item.url,
                        "accessed_at": item.accessed_at,
                        "path": item.path,
                        "value": item.raw_value,
                    }
                    for item in assessment.matched[:_MAX_GROUNDED_ITEMS]
                ],
                "unsupported_numbers": assessment.unsupported,
            }
        )

    adjusted_confidence = _derive_confidence(answer, assessments, results, confidence)
    return grounded, warnings, adjusted_confidence
