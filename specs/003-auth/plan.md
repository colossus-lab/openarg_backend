# Plan: Auth & User Management (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Entity | `User` | `domain/entities/user/user.py` |
| Domain Port | `IUserRepository` | `domain/ports/user/user_repository.py` |
| Infrastructure Adapter | `UserRepositorySQLA` | `infrastructure/adapters/user/user_repository_sqla.py` |
| Presentation | `users_router.py` | `presentation/http/controllers/users/users_router.py` |
| Middleware | Header extraction | `presentation/http/middleware/` |

## 2. Entity & Port

```python
@dataclass
class User:
    id: UUID
    email: str
    name: str
    image_url: str | None
    created_at: datetime
    privacy_accepted_at: datetime | None
    save_history: bool

class IUserRepository(ABC):
    async def upsert_by_email(self, user: User) -> User: ...
    async def get_by_email(self, email: str) -> User | None: ...
    async def export_user_data(self, user_id: UUID) -> dict: ...
    async def delete_user_and_data(self, user_id: UUID) -> None: ...
```

## 3. Endpoints

All authenticated endpoints below require `Authorization: Bearer <google_id_token>` — validated by `GoogleJwtAuthMiddleware` against Google's JWKS (FIX-005, enforced 2026-04-11) — **plus** the shared service token in `X-API-Key`. The user identity is read from the verified Google JWT `email` claim inside each handler via `get_request_user_email(request)`.

| Method | Path | Auth | Rate Limit | Behavior |
|---|---|---|---|---|
| POST | `/users/sync` | Bearer + X-API-Key (first sync after login) | — | Upsert user from NextAuth Google OAuth; returns user dict |
| GET | `/users/me` | Bearer + X-API-Key | — | Return current user profile |
| PATCH | `/users/me/settings` | Bearer + X-API-Key | — | Update `save_history` toggle; cascade-delete conversations if disabled |
| GET | `/users/me/data` | Bearer + X-API-Key | — | Export all user data as JSON (data portability) |
| DELETE | `/users/me` | Bearer + X-API-Key | — | Delete user and all data (ARCO erasure) |

## 4. Persistence

### `users` table
| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `email` | TEXT UNIQUE NOT NULL | |
| `name` | TEXT | |
| `image_url` | TEXT | |
| `created_at` | TIMESTAMPTZ | Default `now()` |
| `privacy_accepted_at` | TIMESTAMPTZ | |
| `save_history` | BOOLEAN | Default `true` |

### Cascade on delete
Foreign keys with `ON DELETE CASCADE` from:
- `conversations.user_id → users.id`
- `messages.conversation_id → conversations.id`
- `user_queries.user_id → users.id`

## 5. External Dependencies

- **NextAuth (frontend)**: handles the OAuth flow with Google; sends the user email to the backend
- **PostgreSQL**: store
- **Dishka DI**: `IUserRepository` injected at request scope

## 6. Source Files

| File | Role |
|---|---|
| `domain/entities/user/` | User entity |
| `domain/ports/user/` | IUserRepository |
| `infrastructure/adapters/user/` | SQLA impl |
| `presentation/http/controllers/users/users_router.py` | Endpoints |

## 7. Deviations from Constitution

- ~~**Principle XII (Security)**: the `X-User-Email` header-trust model is acceptable but suboptimal — it does not strictly meet "auth via JWT" as stated in the constitution.~~ **CLOSED 2026-04-11 via FIX-005**: the backend now validates the Google OAuth ID token itself at the API layer via `GoogleJwtAuthMiddleware` + JWKS, so authenticated endpoints are fully JWT-verified. Admin-key endpoints remain exempt per FR-007a because they carry their own `X-Admin-Key` authentication.

---

**End of plan.md**
