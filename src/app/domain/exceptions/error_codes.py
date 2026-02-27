from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class ErrorDefinition:
    code: str
    i18n_key: str
    default_message: str
    http_status: int

    def to_dict(
        self,
        details: dict | None = None,
        field: str | None = None,
    ) -> dict:
        result: dict = {
            "code": self.code,
            "message": self.default_message,
        }
        if details:
            result["details"] = details
        if field:
            result["field"] = field
        return result


class ErrorCode(Enum):
    # DATASET errors (DS_001 - DS_099)
    DS_NOT_FOUND = ErrorDefinition(
        code="DS_001",
        i18n_key="errors.dataset.not_found",
        default_message="Dataset not found",
        http_status=404,
    )
    DS_DOWNLOAD_FAILED = ErrorDefinition(
        code="DS_002",
        i18n_key="errors.dataset.download_failed",
        default_message="Failed to download dataset",
        http_status=502,
    )
    DS_PARSE_ERROR = ErrorDefinition(
        code="DS_003",
        i18n_key="errors.dataset.parse_error",
        default_message="Failed to parse dataset",
        http_status=422,
    )

    # QUERY errors (QR_001 - QR_099)
    QR_EMPTY = ErrorDefinition(
        code="QR_001",
        i18n_key="errors.query.empty",
        default_message="Query cannot be empty",
        http_status=400,
    )
    QR_TOO_LONG = ErrorDefinition(
        code="QR_002",
        i18n_key="errors.query.too_long",
        default_message="Query exceeds maximum length",
        http_status=400,
    )
    QR_PROCESSING_FAILED = ErrorDefinition(
        code="QR_003",
        i18n_key="errors.query.processing_failed",
        default_message="Query processing failed",
        http_status=500,
    )

    # AGENT errors (AG_001 - AG_099)
    AG_TIMEOUT = ErrorDefinition(
        code="AG_001",
        i18n_key="errors.agent.timeout",
        default_message="Agent execution timed out",
        http_status=504,
    )
    AG_LLM_ERROR = ErrorDefinition(
        code="AG_002",
        i18n_key="errors.agent.llm_error",
        default_message="LLM provider error",
        http_status=502,
    )
    AG_NO_DATASETS_FOUND = ErrorDefinition(
        code="AG_003",
        i18n_key="errors.agent.no_datasets",
        default_message="No relevant datasets found for this query",
        http_status=404,
    )

    # SCRAPER errors (SC_001 - SC_099)
    SC_PORTAL_UNREACHABLE = ErrorDefinition(
        code="SC_001",
        i18n_key="errors.scraper.portal_unreachable",
        default_message="Data portal is unreachable",
        http_status=502,
    )
    SC_RATE_LIMITED = ErrorDefinition(
        code="SC_002",
        i18n_key="errors.scraper.rate_limited",
        default_message="Rate limited by data portal",
        http_status=429,
    )

    # SEARCH errors (SR_001 - SR_099)
    SR_EMBEDDING_FAILED = ErrorDefinition(
        code="SR_001",
        i18n_key="errors.search.embedding_failed",
        default_message="Failed to generate embedding for search",
        http_status=500,
    )
