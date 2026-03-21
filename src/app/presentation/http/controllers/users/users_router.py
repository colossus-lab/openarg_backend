from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr

from app.domain.entities.user.user import User
from app.domain.ports.user.user_repository import IUserRepository

router = APIRouter(prefix="/users", tags=["users"])


class UserSyncRequest(BaseModel):
    email: EmailStr
    name: str = ""
    image: str = ""


class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    image_url: str
    created_at: str


@router.post("/sync", response_model=UserResponse)
@inject
async def sync_user(
    body: UserSyncRequest,
    user_repo: FromDishka[IUserRepository],
) -> UserResponse:
    """Upsert user from Google OAuth (NextAuth frontend)."""
    if not body.email:
        raise HTTPException(status_code=400, detail="email is required")

    user = User(
        email=body.email,
        name=body.name,
        image_url=body.image,
    )
    saved = await user_repo.upsert_by_email(user)

    return UserResponse(
        id=str(saved.id),
        email=saved.email,
        name=saved.name,
        image_url=saved.image_url or "",
        created_at=saved.created_at.isoformat(),
    )


@router.get("/me/data")
@inject
async def export_my_data(
    request: Request,
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Export all user data (Ley 25.326 ARCO right of access / data portability)."""
    email = request.headers.get("X-User-Email", "")
    if not email:
        raise HTTPException(status_code=401, detail="User email required")

    user = await user_repo.get_by_email(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    data = await user_repo.export_user_data(user.id)
    return data


@router.get("/me", response_model=UserResponse)
@inject
async def get_current_user(
    email: str,
    user_repo: FromDishka[IUserRepository],
) -> UserResponse:
    """Get user by email."""
    user = await user_repo.get_by_email(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return UserResponse(
        id=str(user.id),
        email=user.email,
        name=user.name,
        image_url=user.image_url or "",
        created_at=user.created_at.isoformat(),
    )


@router.delete("/me")
@inject
async def delete_my_account(
    request: Request,
    user_repo: FromDishka[IUserRepository],
) -> dict:
    """Delete current user and all associated data (Ley 25.326 ARCO right to erasure)."""
    email = request.headers.get("X-User-Email", "")
    if not email:
        raise HTTPException(status_code=401, detail="User email required")

    user = await user_repo.get_by_email(email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await user_repo.delete_user_and_data(user.id)
    return {"status": "deleted", "email": email}
