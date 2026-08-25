"""Integration tests for the smart query endpoint (mocked connectors)."""

from __future__ import annotations

from uuid import uuid4


class TestSmartQueryEndpoint:
    async def test_returns_422_on_empty_body(self, client):
        response = await client.post("/api/v1/query/smart", json={})
        assert response.status_code == 422
        data = response.json()
        assert "error" in data

    async def test_returns_422_on_missing_question(self, client):
        response = await client.post(
            "/api/v1/query/smart",
            json={"user_email": "test@test.com"},
        )
        assert response.status_code == 422

    async def test_smart_query_route_is_post_only(self, client):
        """A GET on the chat endpoint must be refused as a wrong method.

        This used to assert on `app.routes`, looking the route up with
        `next()` and reading `.methods`. Two problems: `next()` without a
        default raises `StopIteration`, which inside a coroutine surfaces as
        `RuntimeError: coroutine raised StopIteration` and explains nothing;
        and the lookup assumed `include_router` flattens every route into
        `app.routes` with a full `.path`, which does not hold across FastAPI
        versions — in CI the same endpoint answers requests correctly while
        `[r.path for r in app.routes]` has nothing under `/api/v1/query`.

        Asserting through the interface says the same thing and cannot drift
        with the framework's internals: 405 is the router refusing the method,
        and it is decided before any dependency runs, so no API key is needed.
        """
        response = await client.get("/api/v1/query/smart")
        assert response.status_code == 405, (
            f"expected 405 Method Not Allowed for GET, got {response.status_code}"
        )

    # CONTRACT-02 (round v46): extra='forbid' means any unrecognised
    # field surfaces as a 422 instead of being silently dropped. Pre-fix
    # the BFF posted `history` and Pydantic ignored it; the planner ran
    # without context and nobody noticed.
    async def test_extra_fields_are_rejected_with_422(self, client):
        response = await client.post(
            "/api/v1/query/smart",
            json={
                "question": "test",
                "unexpected_field": "should be rejected",
            },
        )
        assert response.status_code == 422

    async def test_history_field_is_accepted_for_bff_compatibility(self, client):
        """Backward compat: the BFF still posts `history` as an advisory
        hint. The model accepts it (so deploys can interleave); the
        handler ignores the value and loads history from the DB.
        """
        # This will fall through to the H3 AUTH_REQUIRED branch (no JWT
        # in test transport) once conversation_id is provided, or 200
        # if no conversation_id is set. We're only asserting it doesn't
        # 422 because of `history` being declared.
        response = await client.post(
            "/api/v1/query/smart",
            json={
                "question": "test",
                "history": [{"role": "user", "content": "antes"}],
            },
        )
        assert response.status_code != 422


# ── H3 fix: conversation_id ownership (round v46) ───────────────────


class TestSmartQueryConversationOwnership:
    """End-to-end tests of the H3 fix at the controller layer.

    Pre-fix: any holder of the shared BACKEND_API_KEY could pass an
    arbitrary `conversation_id` + `user_email` and the planner loaded
    that conversation's history into its LLM prompt. Post-fix: the
    endpoint refuses to use a `conversation_id` unless the caller is
    authenticated AND owns the conversation.
    """

    async def test_rejects_conversation_id_without_authenticated_user(self, client):
        """No Google JWT (test transport bypasses the middleware), so the
        body-supplied user_email is the only identity hint. The endpoint
        must refuse to honor `conversation_id` in that state.
        """
        response = await client.post(
            "/api/v1/query/smart",
            json={
                "question": "what's in conversation 42",
                "user_email": "attacker@example.com",
                "conversation_id": str(uuid4()),
            },
        )
        assert response.status_code == 403
        body = response.json()
        assert body["error"]["code"] == "AUTH_REQUIRED"

    async def test_rejects_malformed_conversation_id(self, client, app):
        """The endpoint validates UUID shape before reaching the repo."""
        # Synthesize an authenticated request by setting request.state on
        # the next call via a middleware-style fixture: easiest path is to
        # bypass the public no-JWT-no-conv guard by also omitting
        # conversation_id altogether. So we test the UUID shape branch
        # via the WS path? No — easier: monkeypatch
        # `get_request_user_email` to return a value.
        from app.presentation.http.controllers.query import smart_query_v2_router

        original = smart_query_v2_router.get_request_user_email
        smart_query_v2_router.get_request_user_email = lambda req: "user@example.com"
        try:
            response = await client.post(
                "/api/v1/query/smart",
                json={
                    "question": "test",
                    "user_email": "user@example.com",
                    "conversation_id": "not-a-uuid",
                },
            )
        finally:
            smart_query_v2_router.get_request_user_email = original
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "BAD_CONVERSATION_ID"

    async def test_rejects_user_email_mismatch_with_jwt(self, client):
        """A body.user_email that disagrees with the authenticated email
        is rejected to stop caller-side identity spoofing.
        """
        from app.presentation.http.controllers.query import smart_query_v2_router

        original = smart_query_v2_router.get_request_user_email
        smart_query_v2_router.get_request_user_email = lambda req: "real@example.com"
        try:
            response = await client.post(
                "/api/v1/query/smart",
                json={
                    "question": "test",
                    "user_email": "spoofed@example.com",
                },
            )
        finally:
            smart_query_v2_router.get_request_user_email = original
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "AUTH_SPOOF"

    async def test_rejects_when_authed_user_not_synced(self, client):
        """Authenticated but user_repo cannot resolve the email → 403.

        This branch covers the case where the JWT validates but the user
        row doesn't exist yet. In the mocked container, get_by_email
        returns None, so this is exactly what happens.
        """
        from app.presentation.http.controllers.query import smart_query_v2_router

        original = smart_query_v2_router.get_request_user_email
        smart_query_v2_router.get_request_user_email = lambda req: "real@example.com"
        try:
            response = await client.post(
                "/api/v1/query/smart",
                json={
                    "question": "what did I ask last time",
                    "user_email": "real@example.com",
                    "conversation_id": str(uuid4()),
                },
            )
        finally:
            smart_query_v2_router.get_request_user_email = original
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "NO_OWNERSHIP"
