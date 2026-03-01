"""Connector and infrastructure error types."""
from __future__ import annotations

from app.domain.exceptions.base import InfrastructureError
from app.domain.exceptions.error_codes import ErrorCode


class ConnectorError(InfrastructureError):
    """Raised when a data connector (MCP, HTTP, local) fails."""

    pass


class LLMProviderError(InfrastructureError):
    """Raised when an LLM provider (Gemini, Anthropic, OpenAI) fails."""

    def __init__(self, message: str = "", **kwargs: object) -> None:
        super().__init__(error_code=ErrorCode.AG_LLM_ERROR, details={"reason": message})


class EmbeddingError(InfrastructureError):
    """Raised when embedding generation fails."""

    def __init__(self, message: str = "", **kwargs: object) -> None:
        super().__init__(error_code=ErrorCode.SR_EMBEDDING_FAILED, details={"reason": message})
