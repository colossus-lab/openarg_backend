"""Shared E2E test helpers — validation functions used across all test files.

Validates not just that the system responded, but that the response
is actually useful from a user perspective.
"""

from __future__ import annotations

import os
import re

_ERROR_PHRASES = ["ocurrió un error", "error al analizar", "probá reformulando"]

_NEGATIVE_PHRASES = [
    "no encontré",
    "no tengo",
    "no cuento con",
    "no dispongo",
    "no pude obtener",
    "no pude acceder",
    "fuera de mi alcance",
    "no tenemos datos",
    "no hay datos disponibles",
]

_INTERNAL_LEAKS = [
    "datos truncados",
    "truncated",
    "pendiente -",
    "null",
    "undefined",
    "traceback",
    "exception",
    "stack trace",
    "select * from",
    "cache_",
    "dataset_chunks",
    "pgvector",
]

# Reasonable numeric ranges by topic (min, max)
NUMERIC_RANGES: dict[str, tuple[float, float]] = {
    "inflacion_mensual": (0, 30),
    "inflacion_anual": (0, 300),
    "reservas_usd_millones": (5_000, 200_000),
    "presupuesto_millones": (1, 999_999_999),
    "tasa_mortalidad": (0, 200),
    "porcentaje": (0, 100),
    "cantidad_personas": (1, 50_000_000),
}


def headers() -> dict[str, str]:
    """Include API key header if BACKEND_API_KEY is set."""
    api_key = os.getenv("BACKEND_API_KEY", "")
    if api_key:
        return {"X-API-Key": api_key}
    return {}


def smart_payload(question: str) -> dict:
    return {"question": question, "user_email": "e2e-test@openarg.org"}


def extract_numbers(text: str) -> list[float]:
    """Extract all numbers (including decimals) from text."""
    raw = re.findall(r"[\d]+(?:[.,]\d+)*", text)
    numbers = []
    for r in raw:
        try:
            cleaned = r.replace(".", "").replace(",", ".")
            numbers.append(float(cleaned))
        except ValueError:
            continue
    return numbers


def extract_years(text: str) -> list[int]:
    return [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2})\b", text)]


async def ask(client, question: str) -> dict:
    """Send question and validate basic response quality.

    Checks:
    - HTTP 200
    - Non-empty answer
    - No error phrases
    - No internal leaks (datos truncados, cache_, null, etc.)
    """
    resp = await client.post(
        "/api/v1/query/smart",
        json=smart_payload(question),
        headers=headers(),
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    data = resp.json()
    assert data.get("answer"), f"Empty answer for: {question}"
    answer_lower = data["answer"].lower()
    for phrase in _ERROR_PHRASES:
        assert phrase not in answer_lower, (
            f"Got error response for '{question}': {data['answer'][:200]}"
        )
    for leak in _INTERNAL_LEAKS:
        assert leak not in answer_lower, (
            f"Response leaks internal detail '{leak}' for '{question}': {data['answer'][:200]}"
        )
    return data


def answer_contains(
    data: dict,
    keywords: list[str],
    label: str,
    *,
    require_numbers: bool = False,
) -> None:
    """Assert the answer mentions relevant keywords AND actually contains data.

    Args:
        data: Response dict with 'answer' key
        keywords: At least one must appear in the answer
        label: Description for error messages
        require_numbers: If True, answer must contain at least one number
    """
    answer_lower = data["answer"].lower()

    # Check keywords are present
    assert any(kw in answer_lower for kw in keywords), (
        f"'{label}' should mention {keywords}: {data['answer'][:300]}"
    )

    # Check it's not a "no data" response disguised with keywords
    for neg in _NEGATIVE_PHRASES:
        if neg in answer_lower:
            # The response says "no tengo" — check if it's about the topic
            for kw in keywords:
                if kw in answer_lower:
                    # "no tengo datos de inflación" → has "inflaci" but is negative
                    # Only fail if the negation is near the keyword (same sentence)
                    # Simple heuristic: check if negation and keyword are within 100 chars
                    neg_pos = answer_lower.find(neg)
                    kw_pos = answer_lower.find(kw)
                    if abs(neg_pos - kw_pos) < 100:
                        raise AssertionError(
                            f"'{label}': response says '{neg}' about the topic. "
                            f"User got no real data: {data['answer'][:200]}"
                        )

    # Check numbers are present for quantitative queries
    if require_numbers:
        numbers = extract_numbers(data["answer"])
        assert numbers, (
            f"'{label}': quantitative query should return numbers, "
            f"not just text: {data['answer'][:200]}"
        )


def assert_substantive(data: dict, question: str, *, min_length: int = 100) -> None:
    """Assert the response is substantive (not just a short filler).

    Replaces bare `len(answer) > 50` checks.
    """
    answer = data["answer"]
    assert len(answer) >= min_length, (
        f"Response too short ({len(answer)} chars) for '{question}': {answer}"
    )
    # Must contain at least some concrete content (number, $, %, or a data indicator)
    has_substance = bool(re.search(r"[\d$%]", answer))
    if not has_substance:
        # Allow if it explains data sources or gives guidance (meta responses)
        meta_indicators = ["portal", "fuente", "dataset", "datos abiertos", "openarg", "consulta"]
        has_meta = any(m in answer.lower() for m in meta_indicators)
        assert has_meta, f"Response has no data and no guidance for '{question}': {answer[:200]}"


def validate_numbers_in_range(
    data: dict,
    range_key: str,
    label: str,
) -> None:
    """Validate that extracted numbers fall within reasonable ranges.

    Used to catch hallucinated/absurd numbers.
    """
    if range_key not in NUMERIC_RANGES:
        return
    min_val, max_val = NUMERIC_RANGES[range_key]
    numbers = extract_numbers(data["answer"])
    # Filter out years and very small numbers (likely not the main data)
    years = set(extract_years(data["answer"]))
    data_numbers = [n for n in numbers if n not in years and n > 0.01]
    if not data_numbers:
        return
    # Check if any number is wildly outside range
    absurd = [n for n in data_numbers if (n > max_val * 10) and n not in years]
    if absurd:
        raise AssertionError(
            f"'{label}': numbers outside reasonable range "
            f"({min_val}-{max_val}): {absurd[:5]}. "
            f"Full answer: {data['answer'][:200]}"
        )


def validate_scope(
    data: dict,
    expected_scope: str,
    wrong_scopes: list[str],
    label: str,
) -> None:
    """Validate geographic scope — e.g., user asked 'nacional' but got 'CABA'.

    Only fails if the response mentions a wrong scope WITHOUT also mentioning
    the expected scope (it's OK to compare nacional vs CABA).
    """
    answer_lower = data["answer"].lower()
    if expected_scope.lower() not in answer_lower:
        found_wrong = [s for s in wrong_scopes if s.lower() in answer_lower]
        if found_wrong:
            raise AssertionError(
                f"'{label}': expected scope '{expected_scope}' but got "
                f"{found_wrong} without mentioning '{expected_scope}': "
                f"{data['answer'][:200]}"
            )
