# Plan: Connector ArgentinaDatos (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `IArgentinaDatosConnector` | `domain/ports/connectors/argentina_datos.py:8` |
| Application | `execute_argentina_datos_step` | `application/pipeline/connectors/argentina_datos.py:13` |
| Infrastructure Adapter | `ArgentinaDatosAdapter` | `infrastructure/adapters/connectors/argentina_datos_adapter.py:32` |

## 2. Ports & Contracts

```python
class IArgentinaDatosConnector(ABC):
    async def fetch_dolar(self, casa: str | None = None) -> DataResult | None: ...
    async def fetch_riesgo_pais(self, ultimo: bool = False) -> DataResult | None: ...
    async def fetch_inflacion(self) -> DataResult | None: ...
```

## 3. Allowlist

`CASAS_ALLOWLIST = frozenset({"oficial", "blue", "bolsa", "ccl", "cripto", "mayorista", "solidario", "tarjeta"})`

## 4. Endpoints

| Endpoint | Method | Returns |
|---|---|---|
| `/v1/cotizaciones/dolares` | GET | List of all casas |
| `/v1/cotizaciones/dolares/{casa}` | GET | Series of a specific casa (last 60) |
| `/v1/finanzas/indices/riesgo-pais` | GET | Country risk series |
| `/v1/finanzas/indices/riesgo-pais/ultimo` | GET | Last value |
| `/v1/finanzas/indices/inflacion` | GET | Monthly inflation |

Base URL: `https://api.argentinadatos.com`

No auth. No retry visible at the adapter level.

## 5. Pipeline Step Logic

`execute_argentina_datos_step(step, adapter)`:
1. Reads `step.params.type` (`dolar` | `riesgo_pais` | `inflacion`)
2. Routing:
   - `dolar` → `adapter.fetch_dolar(casa=params.get("casa"))`
   - `riesgo_pais` → `adapter.fetch_riesgo_pais(ultimo=params.get("ultimo", False))`
   - `inflacion` → `adapter.fetch_inflacion()`
3. Returns `[result]` if not `None`, otherwise `[]`

## 6. Persistence

**No scheduled snapshot** visible. Data is always fetched on the fly.

## 7. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/argentina_datos.py` | ABC port |
| `application/pipeline/connectors/argentina_datos.py` | Pipeline step |
| `infrastructure/adapters/connectors/argentina_datos_adapter.py` | HTTP adapter |

## 8. Deviations from Constitution

- **Principle VI (Retry)**: `@with_retry` not visible in this adapter. Isolated violation.
- **Principle VII (Observability)**: no metrics, no circuit breaker.

---

**End of plan.md**
