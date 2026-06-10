"""H6 + M21 (round v46): rate-limit key prioritizes user over key over IP.

Pre-fix every SPA user shared a single bucket because the frontend
proxies traffic with a single BACKEND_API_KEY. The middleware now
prefers `request.state.user_email` (set by GoogleJwtAuthMiddleware)
so each authenticated user gets their own bucket.

Server-to-server callers (data service token, public /ask Bearer)
keep the per-key bucket because each developer holds their own key.
"""

from __future__ import annotations

from types import SimpleNamespace

from app.presentation.http.middleware.rate_limit_key import (
    get_rate_limit_identifier,
)


def _req(state_attrs: dict, *, client_host: str = "10.0.0.1"):
    """Build a tiny Starlette-shaped Request stub for the key func."""
    state = SimpleNamespace(**state_attrs)
    client = SimpleNamespace(host=client_host)
    # slowapi.util.get_remote_address reads request.client.host
    return SimpleNamespace(state=state, client=client, headers={}, scope={"client": (client_host, 0)})


def test_prefers_user_email_when_jwt_middleware_set_it():
    req = _req({"user_email": "alice@example.com", "api_key_id": "abc123"})
    assert get_rate_limit_identifier(req) == "user:alice@example.com"


def test_user_email_normalized_to_lowercase():
    """Two visits from the same user with different capitalization
    must share a bucket — otherwise the cap is per-rendering, not
    per-user."""
    req = _req({"user_email": "Alice@Example.COM"})
    assert get_rate_limit_identifier(req) == "user:alice@example.com"


def test_falls_through_to_api_key_when_no_user_email():
    """The data-service path and /ask with personal Bearer don't
    carry a Google JWT — those still bucket per key."""
    req = _req({"user_email": None, "api_key_id": "key-12345"})
    assert get_rate_limit_identifier(req) == "key:key-12345"


def test_falls_through_to_ip_when_unauthenticated():
    req = _req({"user_email": None, "api_key_id": None})
    # IP-based identifier returned verbatim (slowapi format).
    assert get_rate_limit_identifier(req) == "10.0.0.1"


def test_h6_two_users_get_different_buckets_under_shared_api_key():
    """The exploit being closed: pre-fix Alice and Bob both proxying
    through the same BACKEND_API_KEY shared one bucket because
    api_key_id was the key. Post-fix their identifiers differ."""
    alice = _req({"user_email": "alice@example.com", "api_key_id": "shared"})
    bob = _req({"user_email": "bob@example.com", "api_key_id": "shared"})
    assert get_rate_limit_identifier(alice) != get_rate_limit_identifier(bob)
