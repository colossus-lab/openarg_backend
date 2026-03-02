"""Rate limit key function — identifies clients by API key or IP."""
from __future__ import annotations

from slowapi.util import get_remote_address
from starlette.requests import Request


def get_rate_limit_identifier(request: Request) -> str:
    """Return the API key ID if authenticated, otherwise fall back to IP address."""
    api_key_id: str | None = getattr(request.state, "api_key_id", None)
    if api_key_id:
        return f"key:{api_key_id}"
    return get_remote_address(request)
