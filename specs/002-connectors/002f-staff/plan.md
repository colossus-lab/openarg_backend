# Plan: Connector Staff (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `IStaffConnector` | `domain/ports/connectors/staff.py:8` |
| Domain Entity | `Staff` | `domain/entities/staff/staff.py` |
| Application | `execute_staff_step` | `application/pipeline/connectors/staff.py:16` |
| Infrastructure Adapter | `StaffAdapter` | `infrastructure/adapters/connectors/staff_adapter.py:22` |
| Infrastructure Task (HCDN) | `snapshot_staff` | `celery/tasks/staff_tasks.py` |
| Infrastructure Task (Senado) | `snapshot_senado_staff` | `celery/tasks/senado_staff_tasks.py` |

## 2. Ports & Contracts

```python
class IStaffConnector(ABC):
    async def get_by_legislator(self, name: str, limit: int = 50) -> DataResult: ...
    async def count_by_legislator(self, name: str) -> DataResult: ...
    async def get_changes(self, name: str | None = None, limit: int = 20) -> DataResult: ...
    async def search(self, query: str, limit: int = 20) -> DataResult: ...
    async def stats(self) -> DataResult: ...
```

## 3. Persistence

### `staff_snapshots` (HCDN)
- `snapshot_date`, `legajo`, `apellido`, `nombre`, `escalafon`, `area_desempeno`, `convenio`
- UPSERT on `(snapshot_date, legajo)`

### `staff_changes`
- `legajo`, `apellido`, `nombre`, `area_desempeno`, `tipo` ("alta" | "baja"), `detected_at`
- INSERT-only log

### `senado_staff`
- `employee_name`, `categoria`, `senator_name`, `bloque`, `provincia`, `senator_id`
- Populated by scraping senador profiles

## 4. Smart Name Matching Cascade

```python
def _match_name(name: str) -> list[str]:
    # 1. Full name LIKE
    patterns = [f"%{name}%"]
    # 2. Individual words ≥3 chars, sorted by length descending
    words = [w for w in name.split() if len(w) >= 3]
    words.sort(key=len, reverse=True)
    for w in words:
        patterns.append(f"%{w}%")
    return patterns
```

The SQL tries each pattern sequentially until one returns `COUNT(*) > 0`.

## 5. Method-by-Method Logic

### `get_by_legislator(name, limit)`
1. Search `senado_staff` by `senator_name ILIKE` (cascade patterns)
2. If no match → `staff_snapshots` by `area_desempeno ILIKE` (cascade)
3. If no match → suggest `areas_similares`
4. Return `DataResult(records=[...], metadata={total, areas_similares?})`

### `count_by_legislator(name)`
Same logic, but returns a single record `{legislador, cantidad_asesores, areas_similares?}`.

### `get_changes(name, limit)`
```sql
SELECT * FROM staff_changes
WHERE (:name IS NULL OR area_desempeno ILIKE :name_pattern)
ORDER BY detected_at DESC
LIMIT :limit
```

### `search(query, limit)`
```sql
SELECT * FROM staff_snapshots_latest
WHERE apellido ILIKE :q OR nombre ILIKE :q OR area_desempeno ILIKE :q
LIMIT :limit
```

### `stats()`
```sql
SELECT COUNT(*), COUNT(DISTINCT area_desempeno), COUNT(DISTINCT escalafon)
FROM staff_snapshots
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM staff_snapshots)
```

## 6. Pipeline Step Routing

`execute_staff_step(step, adapter)`:
- Params: `action` (`get_by_legislator` | `count` | `changes` | `stats` | `search`), `name`, `query`
- Routing:
  - `action=get_by_legislator` or `count` + `name` → `get_by_legislator(name)` or `count_by_legislator(name)`
  - `action=changes` → `get_changes(name)`
  - `action=stats` → `stats()`
  - default → `search(query)`

## 7. Scheduled Work

| Task | Schedule | Queue | Action |
|---|---|---|---|
| `snapshot_staff` | Monday 02:30 ART | `scraper` | Scrape HCDN datastore, diff, upsert snapshots, detect changes |
| `snapshot_senado_staff` | Monday 01:30 ART | `ingest` | Scrape senador profiles, parse HTML, upsert `senado_staff` |

## 8. External Dependencies

- **CKAN datastore** (HCDN): `https://datos.hcdn.gob.ar/api/3/action/datastore_search?resource_id=6e49506e-6757-44cd-94e9-0e75f3bd8c38`
- **Senado web** (HTML scraping)
- **PostgreSQL**

## 9. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/staff.py` | Port ABC |
| `domain/entities/staff/staff.py` | Staff entity |
| `application/pipeline/connectors/staff.py` | Pipeline step routing |
| `infrastructure/adapters/connectors/staff_adapter.py` | Adapter (SQL-based) |
| `infrastructure/celery/tasks/staff_tasks.py` | HCDN snapshot |
| `infrastructure/celery/tasks/senado_staff_tasks.py` | Senado scrape |

## 10. Deviations from Constitution

- **Principle VII (Observability)**: no per-method metrics.
- **Principle VIII (No schema drift)**: suggested indexes (GIN on `area_desempeno`) not implemented — performance debt.

---

**End of plan.md**
