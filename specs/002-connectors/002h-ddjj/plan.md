# Plan: Connector DDJJ (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | **Absent** | `domain/ports/connectors/ddjj.py` (MISSING) **[DEBT-001]** |
| Application | `execute_ddjj_step` | `application/pipeline/connectors/ddjj.py:50` |
| Infrastructure Adapter | `DDJJAdapter` | `infrastructure/adapters/connectors/ddjj_adapter.py:41` |
| Data File | `ddjj_dataset.json` | `infrastructure/data/ddjj_dataset.json` |

## 2. Methods (de facto interface)

```python
class DDJJAdapter:
    async def search(self, query: str, limit: int = 10) -> DataResult: ...
    async def ranking(self, sort_by: str, top: int, order: str) -> DataResult: ...
    async def get_by_name(self, name: str) -> DataResult: ...
    async def stats(self) -> DataResult: ...
```

## 3. Data Loading Strategy

```python
_dataset: list[dict] | None = None
_last_error_at: float | None = None
_backoff_seconds: int = 60  # base
_max_backoff: int = 3600     # cap

def _ensure_loaded(self):
    if self._dataset is not None:
        return
    # Check backoff
    if self._last_error_at and (now - self._last_error_at) < self._backoff_seconds:
        raise DatasetNotAvailable
    try:
        with open("ddjj_dataset.json") as f:
            self._dataset = json.load(f)
    except Exception:
        self._last_error_at = now
        self._backoff_seconds = min(self._backoff_seconds * 2, self._max_backoff)
        raise
```

## 4. Record Schema (inferred from code)

```python
{
    "cuit": "20-12345678-9",
    "nombre": "Apellido Nombre",
    "sexo": "M|F",
    "fecha_nacimiento": "YYYY-MM-DD",
    "estado_civil": "soltero|casado|...",
    "cargo": "Diputado Nacional",
    "organismo": "HCDN",
    "anio_declaracion": 2024,
    "tipo_declaracion": "inicial|anual|...",
    "patrimonioInicio": float,
    "patrimonioCierre": float,
    "bienesInicio": float,
    "bienesCierre": float,
    "deudasInicio": float,
    "deudasCierre": float,
    "ingresosTrabajoNeto": float,
    "gastos_personales": float,
    "cantidad_bienes": int,
    "bienes": [
        {"tipo": "INMUEBLE URBANO EN EL PAÍS", "importe": float},
        ...
    ],
}
```

## 5. Pipeline Step Routing

`execute_ddjj_step(step, adapter)`:
- Params: `action` (`ranking` | `stats` | `detail` | `search`), `nombre`, `query`, `sortBy`, `top`, `order`, `position`
- Routing:
  - `action=ranking` → `adapter.ranking(sort_by, top, order)` + optional filter by `position`
  - `action=stats` → `adapter.stats()`
  - `action=detail` or has `nombre` param → `adapter.get_by_name(nombre)`
  - default → `adapter.search(query)`
- **Fail-safe**: if there is no match, returns an informative `DataResult` explaining that the dataset only contains 195 national diputados (no senadores/executive/judges).

## 6. Health Check

`HealthCheckService` uses `DDJJAdapter.record_count` as an indicator — if the adapter fails to load, the health check reports "degraded".

## 7. Scheduled Work

**No visible task**. The dataset is updated externally (manual).

## 8. Source Files

| File | Role |
|---|---|
| `application/pipeline/connectors/ddjj.py` | Pipeline step + fail-safe NOT FOUND |
| `infrastructure/adapters/connectors/ddjj_adapter.py` | Adapter + lazy loader |
| `infrastructure/data/ddjj_dataset.json` | Static dataset |

## 9. Deviations from Constitution

- **Principle I (Hexagonal ports)**: **no port** — DDJJ breaks the pattern just like BCRA. **[DEBT-001]**
- **Principle VIII (Migrations via Alembic)**: not applicable — data is JSON, not a table.

---

**End of plan.md**
