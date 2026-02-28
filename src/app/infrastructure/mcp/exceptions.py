"""MCP Server exception hierarchy."""
from __future__ import annotations


class MCPServerError(Exception):
    retryable = True

    def __init__(self, message: str, server_name: str) -> None:
        self.message = message
        self.server_name = server_name
        super().__init__(message)


class MCPRateLimitError(MCPServerError):
    retryable = True


class MCPTimeoutError(MCPServerError):
    retryable = True


class MCPServiceUnavailableError(MCPServerError):
    retryable = True


class MCPNetworkError(MCPServerError):
    retryable = True


class MCPInvalidRequestError(MCPServerError):
    retryable = False


class MCPNotFoundError(MCPServerError):
    retryable = False


class MCPServerInternalError(MCPServerError):
    retryable = True


HTTP_STATUS_TO_EXCEPTION: dict[int, type[MCPServerError]] = {
    429: MCPRateLimitError,
    400: MCPInvalidRequestError,
    404: MCPNotFoundError,
    500: MCPServerInternalError,
    502: MCPServiceUnavailableError,
    503: MCPServiceUnavailableError,
    504: MCPServiceUnavailableError,
}


def classify_http_error(status_code: int, message: str, server_name: str) -> MCPServerError:
    exc_cls = HTTP_STATUS_TO_EXCEPTION.get(status_code, MCPServerError)
    return exc_cls(message, server_name)
