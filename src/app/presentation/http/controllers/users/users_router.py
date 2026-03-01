from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, HTTPException
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
