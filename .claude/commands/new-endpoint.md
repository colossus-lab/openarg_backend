# Create new FastAPI endpoint: $ARGUMENTS

Add a new endpoint to the API. Follow these steps:

## 1. Create the router

Create `src/app/presentation/http/controllers/{module}/{module}_router.py`:

```python
from fastapi import APIRouter
from dishka.integrations.fastapi import FromDishka, inject
from pydantic import BaseModel

router = APIRouter(prefix="/{module}", tags=["{module}"])

class {Module}Request(BaseModel):
    field: str

class {Module}Response(BaseModel):
    result: str

@router.post("/", response_model={Module}Response)
@inject
async def create_{module}(
    body: {Module}Request,
    service: FromDishka[IService],
) -> {Module}Response:
    result = await service.execute(body.field)
    return {Module}Response(result=result)
```

Reference: `src/app/presentation/http/controllers/query/query_router.py` (full example with DI, rate limiting, WebSocket), `src/app/presentation/http/controllers/datasets/datasets_router.py` (simple CRUD).

## 2. Add rate limiting (if needed)

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@router.post("/")
@limiter.limit("10/minute")
async def endpoint(request: Request, ...):
    ...
```

## 3. Define Pydantic schemas

Request/response models go in the same router file or a separate `schemas.py` if complex.

Conventions:
- Request: `{Name}Request`
- Response: `{Name}Response`
- Use `Field()` with descriptions for OpenAPI documentation

## 4. Register in the root router

Edit `src/app/presentation/http/controllers/root_router.py`:

```python
from .{module}.{module}_router import router as {module}_router

api_router.include_router({module}_router)
```

This mounts it under `/api/v1/{module}/`.

## 5. Inject dependencies

If the endpoint needs services/repos, make sure they're registered in Dishka (`src/app/setup/ioc/provider_registry.py`) and use `FromDishka[InterfaceType]` + `@inject` decorator.

## Checklist
- [ ] Router created with endpoint(s)
- [ ] Pydantic schemas defined
- [ ] Rate limiting configured if public-facing
- [ ] Registered in `root_router.py`
- [ ] Dependencies available in Dishka
- [ ] Tested: endpoint responds correctly
