# Plan: Connector CKAN Search (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `ICKANSearchConnector` | `domain/ports/connectors/ckan_search.py:8` |
| Application | `execute_ckan_step` | `application/pipeline/connectors/ckan.py:143` |
| Infrastructure Adapter | `CKANSearchAdapter` | `infrastructure/adapters/connectors/ckan_search_adapter.py:321` |
| Infrastructure Tasks | `scrape_catalog` (per portal) | `celery/tasks/scraper_tasks.py` |

## 2. Ports & Contracts

```python
class ICKANSearchConnector(ABC):
    async def search_datasets(
        self,
        query: str,
        portal_id: str | None = None,
        rows: int = 10,
    ) -> list[DataResult]: ...

    async def query_datastore(
        self,
        portal_id: str,
        resource_id: str,
        q: str | None = None,
        limit: int = 100,
    ) -> list[dict]: ...
```

## 3. Portal Registry (hardcoded, 30 portals)

Defined as `PORTALS: list[dict]` in `ckan_search_adapter.py:21-229`:

```python
PORTALS = [
    {"id": "nacional", "base_url": "https://datos.gob.ar", "api_path": "/api/3/action"},
    {"id": "diputados", "base_url": "...", ...},
    # ... 28 more
]
```

### Active portals (20)
datos_gob_ar, diputados, justicia, energia, transporte, salud, produccion, magyp, arsat, acumar, mininterior, pami, desarrollo_social, turismo, ssn, caba, legislatura_caba, pba, cordoba_estadistica, mendoza, entrerios, neuquen_legislatura, tucuman, misiones, chaco, ciudad_mendoza, corrientes.

### Dead portals (10, documented)
- `santafe` — maintenance
- `modernizacion` — dissolved
- `ambiente` — redirects
- `agroindustria` — DNS dead
- `rio_negro`, `jujuy`, `salta`, `la_plata` — no response
- `cordoba_muni` — wrong URL (404)
- `rosario` — migrated to DKAN
- `bahia_blanca` — URL moved
- `csjn` — no response (Mar 2026)
- `cultura` — circuit-breaker skipped
- `cordoba_prov` — down (Mar 2026)

## 4. External Dependencies (per portal)

| Endpoint | Method | Parameters |
|---|---|---|
| `/api/3/action/package_search` | GET | `q`, `rows`, `fq` |
| `/api/3/action/datastore_search` | GET | `resource_id`, `q`, `limit` |
| `/api/3/action/resource_show` | GET | `id` |
| Direct CSV download | GET | — |

No auth. Timeout 30s. No retry visible at the adapter level.

## 5. Search Strategy (Cascade)

```
search_datasets(query, portal_id, rows)
    ↓
asyncio.gather over portals (in parallel)
    ↓
Per portal:
    1. package_search → list of datasets
    2. For each dataset:
        a. Try datastore_search on suitable CSV/JSON resources
        b. If it fails → download CSV (max 2MB, max 500 rows)
        c. If it fails → return metadata only
    3. Normalize to DataResult
    ↓
Aggregate results from all portals
```

## 6. Pipeline Step Logic

`execute_ckan_step(step, adapter)` in `application/pipeline/connectors/ckan.py:143`:

1. **Sanitize query**: remove stopwords (`buscar`, `datasets`, `datos`, `portal`, `nacional`, etc.) → extract meaningful keywords (`application/pipeline/connectors/ckan.py:23-75`)
2. **Search in local cache first**: query the sandbox for `cache_*` tables matching keywords, prefer consolidated ones, prioritize by `table_priority()` (line 175)
3. **If there is no local cache**: fall back to live `adapter.search_datasets()`
4. **If `resourceId` + `portal_id` in params**: direct query to `adapter.query_datastore()`
5. Returns `list[DataResult]`

## 7. Scheduled Work

Staggered daily scrape — one per portal every 10 minutes between 03:00-05:50 ART.

Examples from the beat schedule (`celery/app.py:181-189`):
- `scrape-datos-gob-ar`: 03:00
- `scrape-caba`: 03:10
- `scrape-pba`: 03:20
- ... (up to cordoba_estadistica 05:50)
- Queue: `scraper`

Task: `scrape_catalog(portal)` — calls the adapter, indexes new datasets in the `datasets` table, dispatches embedding per new/modified dataset.

## 8. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/ckan_search.py` | ABC port |
| `application/pipeline/connectors/ckan.py` | Pipeline step + query sanitization |
| `infrastructure/adapters/connectors/ckan_search_adapter.py` | Adapter + portal registry + dead portal list |
| `infrastructure/celery/tasks/scraper_tasks.py` | Task `scrape_catalog` |

## 9. Deviations from Constitution

- **Principle VI (Retry + circuit breaker)**: not visibly applied; silent exception swallowing per portal (line 423).
- **Principle VII (Observability)**: upstream errors do not emit metrics.
- **Principle VIII (Pinned config)**: portal list and dead portals hardcoded in code, not in config.

---

**End of plan.md**
