# Plan: Connector Series de Tiempo (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `ISeriesTiempoConnector` | `domain/ports/connectors/series_tiempo.py:8` |
| Application | `execute_series_step` | `application/pipeline/connectors/series.py:55` |
| Infrastructure Adapter | `SeriesTiempoAdapter` | `infrastructure/adapters/connectors/series_tiempo_adapter.py:216` |
| Infrastructure Task | `snapshot_series_tiempo` | `infrastructure/celery/tasks/series_tiempo_tasks.py` |

## 2. Ports & Contracts

```python
class ISeriesTiempoConnector(ABC):
    async def search(self, query: str, limit: int = 10) -> list[dict]: ...
    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
    ) -> DataResult | None: ...
```

## 3. External Dependencies

| Attribute | Value |
|---|---|
| **Base URL** | `https://apis.datos.gob.ar/series/api` |
| **Endpoint 1** | `GET /search?q={query}&limit={limit}` |
| **Endpoint 2** | `GET /series?ids={csv}&start_date=...&end_date=...&collapse=...&representation=...&limit=...` |
| **Auth** | No auth |
| **Retry** | `@with_retry` not visible in the adapter — possible [DEBT] |
| **Rate limits** | Unknown |

## 4. Curated Catalog (hardcoded in the adapter)

15 entries with structure `{id, title, keywords: [...], default_collapse, default_representation, ...}`. Series covered:

- presupuesto, inflacion, tipo_cambio, ipc_regional
- reservas, base_monetaria, leliq_pases
- emae, desempleo, salarios
- canasta_basica, canasta_alimentaria
- exportaciones, importaciones, balanza_comercial
- actividad_industrial

## 5. Pipeline Step Logic

`execute_series_step(step, adapter)`:
1. Extracts `seriesIds` (or `series_ids`), `collapse`, `representation`, `query`, `startDate`, `endDate` from `step.params`
2. If there are no `seriesIds`, performs keyword matching in the curated catalog (`find_catalog_match`)
3. If it matches, auto-completes missing params with catalog defaults
4. **Regional upgrade**: if the query contains geographic keywords (provincia|region|gba|noa|nea|cuyo|pampeana|patagonia), upgrades `desempleo 45.2_ECTDT_0_T_33` to the 7-region set
5. If there are no matches, falls back to dynamic `adapter.search(query)`
6. Auto-completes `endDate` = today if missing
7. Calls `adapter.fetch(...)` and returns `DataResult` or raises `ConnectorError(CN_SERIES_UNAVAILABLE)` if there is no data

## 6. Persistence (Snapshot)

Task `snapshot_series_tiempo`:
- Downloads 12 hardcoded series (inflacion_ipc, tipo_cambio, emae, desempleo, gasto_publico, reservas_internacionales, base_monetaria, salarios, canasta_basica, exportaciones, importaciones, actividad_industrial)
- Persists each one in `cache_series_<slug>` via pandas
- Registers in `datasets` + `cached_datasets`
- Dispatches `index_dataset_embedding.delay(dataset_id)` for each one

## 7. Scheduled Work

| Task | Schedule | Queue |
|---|---|---|
| `snapshot_series_tiempo` | Daily 04:00 ART (per beat schedule) | `ingest` |

## 8. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/series_tiempo.py` | ABC port |
| `application/pipeline/connectors/series.py` | Pipeline step `execute_series_step` |
| `infrastructure/adapters/connectors/series_tiempo_adapter.py` | Adapter + curated catalog |
| `infrastructure/celery/tasks/series_tiempo_tasks.py` | Snapshot task |

## 9. Deviations from Constitution

- **Principle III (DI via Dishka)**: task instantiates the adapter inline. **[DEBT-007]**
- **Principle IV (Async-first)**: use of `asyncio.run()` in sync task. Accepted pattern.
- **Principle VI (Circuit breaker)**: not explicitly applied.

---

**End of plan.md**
