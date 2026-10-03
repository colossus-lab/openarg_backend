"""Developer API key management endpoints.

Called from the frontend with the shared ``BACKEND_API_KEY`` service
token plus the per-user Google OAuth ID token (see FIX-005). Not called
with user-owned API keys.
"""

from __future__ import annotations

import logging
from uuid import UUID

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.application.api_key_service import PLAN_LIMITS, generate_api_key
from app.application.public_quota import (
    first_of_next_month_utc,
    monthly_counter_key,
    resolve_tier,
)
from app.application.web_quota import web_quota
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.entities.credits.credits import CreditType
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.domain.ports.user.user_repository import IUserRepository
from app.presentation.http.middleware.google_jwt_middleware import get_request_user_email
from app.setup.app_factory import limiter

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
# Cada alta revoca la clave anterior; sin límite, regenerar en bucle es una
# forma gratis de resetear contadores atados a la clave. El bucket es por
# usuario (`request.state.user_email`, ver rate_limit_key.py).
@limiter.limit("5/hour")  # type: ignore[untyped-decorator]
@inject
async def create_api_key_endpoint(
    request: Request,
    body: CreateKeyRequest,
    api_key_repo: FromDishka[IApiKeyRepository],
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Create a new API key. Returns the full key ONCE — save it immediately."""
    user_email = get_request_user_email(request)
    if not user_email:
        raise HTTPException(status_code=401, detail="User identity required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Revoke any existing active key (1 key per user)
    existing = await api_key_repo.list_by_user(user.id)
    for k in existing:
        if k.is_active:
            await api_key_repo.deactivate(k.id, user.id)

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
    user_email = get_request_user_email(request)
    if not user_email:
        raise HTTPException(status_code=401, detail="User identity required")

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
    user_email = get_request_user_email(request)
    if not user_email:
        raise HTTPException(status_code=401, detail="User identity required")

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
    cache: FromDishka[ICacheService],
    credits: FromDishka[ICreditRepository],
) -> dict:
    """Usage of the authenticated user: this month's allowance, credits and tier."""
    user_email = get_request_user_email(request)
    if not user_email:
        raise HTTPException(status_code=401, detail="User identity required")

    user = await user_repo.get_by_email(user_email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    summary = await api_key_repo.get_usage_summary(user.id)
    # Los límites salen de acá, no del frontend, para que no se desfasen.
    tier = await resolve_tier(user.id, credits)
    try:
        balance = await credits.balance(user.id)
    except Exception:
        logger.warning("Credit balance lookup failed for %s", user.id, exc_info=True)
        balance = {"preguntas": 0, "datos": 0}

    async def used(tipo: CreditType) -> int:
        # El mismo contador que aplica el cupo (Redis). Cuenta intentos, así
        # que puede pasar el límite: se muestra como mucho el límite.
        try:
            value = await cache.get(monthly_counter_key(user.id, tipo))
            return int(value or 0)
        except Exception:
            return 0

    preguntas_usadas = min(await used("preguntas"), tier.preguntas)
    datos_usados = min(await used("datos"), tier.datos)
    # El chat web tiene su propio contador (30 por mes) y comparte los créditos.
    web = (await web_quota(user.id, cache, credits)).to_wire()
    return {
        **summary,
        "preguntas": {"usadas": preguntas_usadas, "limite": tier.preguntas},
        "datos": {"usadas": datos_usados, "limite": tier.datos},
        "web": web,
        "renueva": first_of_next_month_utc().isoformat(),
        "creditos": balance,
        "fundador": (
            {"hasta": tier.fundador_hasta.isoformat() if tier.fundador_hasta else None}
            if tier.nombre == "fundador"
            else None
        ),
        # Campos viejos, para el frontend anterior mientras se despliega el nuevo.
        "requests_today": preguntas_usadas,
        "limit_day": tier.preguntas,
        "limit_minute": PLAN_LIMITS["free"]["per_min"],
    }
