# Plan: Developers Portal (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Entities | `ApiKey`, `ApiUsage` | `domain/entities/` |
| Domain Port | `IApiKeyRepository` | `domain/ports/` |
| Application | `api_key_service.py` | `application/services/` |
| Infrastructure | `ApiKeyRepositorySQLA` | `infrastructure/adapters/` |
| Presentation | `developers_router.py` | `presentation/http/controllers/developers/` |

## 2. Endpoints

| Method | Path | Auth | Behavior |
|---|---|---|---|
| POST | `/developers/keys` | `Authorization: Bearer <google_id_token>` + `X-API-Key: <BACKEND_API_KEY>` | Create key (revoke existing first); return plaintext once |
| GET | `/developers/keys` | `Authorization: Bearer <google_id_token>` + `X-API-Key: <BACKEND_API_KEY>` | List user's keys (masked) |
| DELETE | `/developers/keys/{id}` | `Authorization: Bearer <google_id_token>` + `X-API-Key: <BACKEND_API_KEY>` | Deactivate key |
| GET | `/developers/usage` | `Authorization: Bearer <google_id_token>` + `X-API-Key: <BACKEND_API_KEY>` | Usage summary |

The user identity is read from the verified Google JWT `email` claim via `get_request_user_email(request)` inside each handler — see FIX-005 and `specs/003-auth/spec.md` FR-007. Admin endpoints (not listed here) are exempt from Google JWT under FR-007a.

## 3. Key Generation

```python
def generate_api_key() -> tuple[str, str]:
    raw = "oarg_sk_" + secrets.token_urlsafe(32)
    key_hash = sha256(raw.encode()).hexdigest()
    return raw, key_hash
```

## 4. Port Interface

```python
class IApiKeyRepository(ABC):
    async def create(self, api_key: ApiKey) -> ApiKey: ...
    async def get_by_key_hash(self, key_hash: str) -> ApiKey | None: ...
    async def list_by_user(self, user_id: UUID) -> list[ApiKey]: ...
    async def deactivate(self, api_key_id: UUID) -> None: ...
    async def record_usage(self, usage: ApiUsage) -> None: ...
    async def get_usage_summary(self, user_id: UUID) -> dict: ...
```

## 5. Persistence

See `007-public-api/plan.md` section 5 — same tables (`api_keys`, `api_usage`).

## 6. Source Files

- `presentation/http/controllers/developers/developers_router.py`
- `domain/ports/` + `infrastructure/adapters/` for the repo
- `application/services/api_key_service.py` (or similar)

## 7. Deviations from Constitution

- Complies with security and DI principles.

---

**End of plan.md**
