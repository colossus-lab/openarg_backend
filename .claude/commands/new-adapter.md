# Create new infrastructure adapter: $ARGUMENTS

Implement a new adapter that fulfills a domain port (ABC interface). Follow these steps:

## 1. Identify or create the port

Check if a port already exists in `src/app/domain/ports/`. If not, create one:

```python
# src/app/domain/ports/{category}/{name}_port.py
from abc import ABC, abstractmethod

class I{Name}(ABC):
    @abstractmethod
    async def method(self, param: type) -> result:
        ...
```

Reference: `src/app/domain/ports/source/data_source.py` (IDataSource), `src/app/domain/ports/llm/llm_provider.py` (ILLMProvider).

## 2. Create the adapter

Create the file in `src/app/infrastructure/adapters/{category}/`:

```python
# src/app/infrastructure/adapters/{category}/{name}_adapter.py
from src.app.domain.ports.{category}.{port} import I{Name}

class {Name}Adapter(I{Name}):
    def __init__(self, ...dependencies):
        self._dep = ...dependencies

    async def method(self, param: type) -> result:
        # implementation
```

Reference: `src/app/infrastructure/adapters/source/datos_gob_ar_adapter.py`, `src/app/infrastructure/adapters/cache/redis_cache_adapter.py`.

Conventions:
- Async by default for I/O
- HTTPX for HTTP requests
- structlog for logging
- Handle exceptions and map to domain exceptions when appropriate

## 3. Register in Dishka IoC

Edit `src/app/setup/ioc/provider_registry.py`:

1. Create a new Provider or add to an existing one:
```python
class {Name}Provider(Provider):
    @provide(scope=Scope.APP)  # or Scope.REQUEST as needed
    async def provide_{name}(self, ...deps) -> I{Name}:
        return {Name}Adapter(...deps)
```

2. Add the provider to the tuple in `create_async_ioc_container()` or where the container is composed

Reference: look for `DataSourceProvider`, `CacheProvider`, `LLMProvider` as examples.

## 4. Use in presentation layer or workers

- In FastAPI routers: use `FromDishka[I{Name}]` as dependency + `@inject`
- In Celery tasks: resolve from the container manually

## Checklist
- [ ] Port ABC defined in `domain/ports/`
- [ ] Adapter implements all abstract methods
- [ ] Registered in Dishka (`provider_registry.py`)
- [ ] Correct scope (APP for singletons, REQUEST for per-request)
- [ ] Unit tests with mocked port
