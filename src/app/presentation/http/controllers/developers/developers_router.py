"""Developer API key management endpoints.

These endpoints are called from the frontend (authenticated with the
shared BACKEND_API_KEY + X-User-Email header), NOT with user API keys.
"""

from __future__ import annotations

import logging
from uuid import UUID

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.application.api_key_service import PLAN_LIMITS, generate_api_key
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.user.user_repository import IUserRepository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/developers", tags=["developers"])


class CreateKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)


class KeyResponse(BaseModel):
    id: str
    name: str
    key_prefix: str
    plan: str
    is_active: bool
    last_used_at: str | None
    created_at: str


@router.post("/keys")
@inject
async def create_api_key_endpoint(
    request: Request,
    body: CreateKeyRequest,
    api_key_repo: FromDishka[IApiKeyRepository],
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Create a new API key. Returns the full key ONCE — save it immediately."""
    user_email = request.headers.get("X-User-Email", "")
    if not user_email:
        raise HTTPException(status_code=401, detail="X-User-Email header required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Limit: 1 active key per user
    existing = await api_key_repo.list_by_user(user.id)
    active_count = sum(1 for k in existing if k.is_active)
    if active_count >= 1:
        raise HTTPException(status_code=400, detail="You already have an active API key. Revoke it first to create a new one.")

    raw_key, key_hash = generate_api_key()

    api_key = ApiKey(
        user_id=user.id,
        key_hash=key_hash,
        key_prefix=raw_key[:16],
        name=body.name,
        plan="free",
    )
    await api_key_repo.create(api_key)

    logger.info("API key created for user %s: %s", user_email, api_key.key_prefix)

    return {
        "key": raw_key,  # Shown ONCE, never again
        "id": str(api_key.id),
        "name": api_key.name,
        "key_prefix": api_key.key_prefix,
        "plan": api_key.plan,
        "limits": PLAN_LIMITS[api_key.plan],
        "warning": "Save this key now. You will not be able to see it again.",
    }


@router.get("/keys")
@inject
async def list_api_keys(
    request: Request,
    api_key_repo: FromDishka[IApiKeyRepository],
    user_repo: FromDishka[IUserRepository],
) -> list[dict]:
    """List all API keys for the authenticated user (keys are masked)."""
    user_email = request.headers.get("X-User-Email", "")
    if not user_email:
        raise HTTPException(status_code=401, detail="X-User-Email header required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    keys = await api_key_repo.list_by_user(user.id)

    return [
        {
            "id": str(k.id),
            "name": k.name,
            "key_prefix": k.key_prefix,
            "plan": k.plan,
            "is_active": k.is_active,
            "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
            "created_at": k.created_at.isoformat() if k.created_at else None,
        }
        for k in keys
    ]


@router.delete("/keys/{key_id}")
@inject
async def revoke_api_key(
    request: Request,
    key_id: UUID,
    api_key_repo: FromDishka[IApiKeyRepository],
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Revoke (deactivate) an API key."""
    user_email = request.headers.get("X-User-Email", "")
    if not user_email:
        raise HTTPException(status_code=401, detail="X-User-Email header required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    success = await api_key_repo.deactivate(key_id, user.id)
    if not success:
        raise HTTPException(status_code=404, detail="API key not found")

    logger.info("API key revoked: %s by user %s", key_id, user_email)

    return {"revoked": True, "key_id": str(key_id)}


@router.get("/usage")
@inject
async def get_usage(
    request: Request,
    api_key_repo: FromDishka[IApiKeyRepository],
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Get usage summary for all API keys owned by the authenticated user."""
    user_email = request.headers.get("X-User-Email", "")
    if not user_email:
        raise HTTPException(status_code=401, detail="X-User-Email header required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return await api_key_repo.get_usage_summary(user.id)
