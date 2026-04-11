# Plan: Connector Georef (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `IGeorefConnector` | `domain/ports/connectors/georef.py:8` |
| Application | `execute_georef_step` | `application/pipeline/connectors/georef.py:13` |
| Infrastructure Adapter | `GeorefAdapter` | `infrastructure/adapters/connectors/georef_adapter.py:19` |
| Infrastructure Task | `snapshot_georef` | `celery/tasks/georef_tasks.py` |

## 2. Ports & Contracts

```python
class IGeorefConnector(ABC):
    async def get_provincias(self, nombre: str | None = None) -> list[dict]: ...
    async def get_departamentos(self, provincia: str | None = None, nombre: str | None = None) -> list[dict]: ...
    async def get_municipios(self, provincia: str | None = None, nombre: str | None = None) -> list[dict]: ...
    async def get_localidades(self, provincia: str | None = None, nombre: str | None = None) -> list[dict]: ...
    async def normalize_location(self, query: str) -> DataResult | None: ...
```

## 3. External Dependencies

| Attribute | Value |
|---|---|
| **Base URL** | `https://apis.datos.gob.ar/georef/api` |
| **Endpoints** | `/provincias`, `/departamentos`, `/municipios`, `/localidades` |
| **Auth** | None |
| **HTTP Timeout** | 10s |
| **Retry** | `@with_retry(max_retries=2, base_delay=1.0, service_name="georef")` |
| **User-Agent** | Configurable in client |

## 4. Normalization Logic

`normalize_location(query)`:

```python
results = await asyncio.gather(
    get_provincias(nombre=query),
    get_departamentos(nombre=query),
    get_municipios(nombre=query),
    get_localidades(nombre=query),
    return_exceptions=True,
)
# Priority resolution
for entity_type in ["provincia", "departamento", "municipio", "localidad"]:
    if results[entity_type]:
        return DataResult(source="georef", format="geo", records=[...])
return None
```

## 5. Pipeline Step Logic

`execute_georef_step(step, adapter)`:
- Params: `query`
- Calls `adapter.normalize_location(query)`
- Returns `[result]` if not `None`, otherwise `[]`

## 6. Scheduled Work

`snapshot_georef` task:
- Schedule: Daily 04:15 ART (inferred from beat schedule)
- Queue: `ingest`
- Action: Calls the API for all entity types, registers them as cached datasets for local search

## 7. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/georef.py` | Port ABC |
| `application/pipeline/connectors/georef.py` | Pipeline step |
| `infrastructure/adapters/connectors/georef_adapter.py` | HTTP adapter |
| `infrastructure/celery/tasks/georef_tasks.py` | Snapshot task |

## 8. Deviations from Constitution

- **Principle II (Pinned stack)**: hardcoded base URL, not in config.
- **Principle VI (Circuit breaker)**: not applied.

---

**End of plan.md**
