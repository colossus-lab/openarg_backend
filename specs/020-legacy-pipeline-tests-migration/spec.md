# Spec 020 — Migrar tests del SmartQueryService al pipeline LangGraph

**Estado**: Draft / pendiente de ejecución
**Fecha de creación**: 2026-05-05
**Origen**: cleanup tras borrar `SmartQueryService` y endpoints legacy `/api/v1/query/*`

## Contexto

Hasta 2026-05-05, el chat productivo del frontend pegaba a `/api/v1/query/smart`,
servido por `smart_query_v2_router` que delega en el pipeline **LangGraph**
(`src/app/application/pipeline/`). En paralelo coexistía
`SmartQueryService` (`src/app/application/smart_query_service.py`), una
implementación monolítica de la misma lógica (plan → dispatch → analyze)
que **ya no atendía ningún endpoint** pero servía como reference
implementation para la test suite.

`SmartQueryService` fue **eliminado** junto con los endpoints legacy
`query_router.py` (`POST /`, `GET /{id}`, `POST /quick`,
`DELETE /cache/{hash}`, `WS /ws/stream`) y el archivo nunca-incluido
`smart_query_router.py`.

## Tests afectados (skipped)

| Archivo | Líneas | Qué probaba |
|---|---|---|
| `tests/unit/test_smart_query_service.py` | 853 | full unit suite de `SmartQueryService` (plan, dispatch, analyze, memory) |
| `tests/integration/test_smart_pipeline.py` | 173 | flow end-to-end con mocks de LLM/connectors |
| `tests/integration/test_query_pipeline.py` | 380 | id, otro slice |
| `tests/integration/conftest.py` | 299 | fixture central que instancia `SmartQueryService` |
| `tests/unit/test_error_scenarios.py` | 437 | manejo de errores en distintos pasos |
| `tests/unit/test_casual_detection.py` | 124 | detección de mensajes casuales/meta antes del plan |
| `tests/unit/test_data_context.py` | 150 | construcción del contexto de datos para el LLM |
| `tests/unit/test_cache_key.py` | 21 | key derivation para semantic cache |
| `tests/evaluation/run_eval.py` | 189 | eval harness con preguntas-de-oro |

**Total**: 2,626 líneas.

Todos marcados con
`pytest.skip("legacy SmartQueryService removed; pipeline tests TODO — spec 020")`
a nivel de módulo (skip del collect entero).

## Por qué se decidió skipear en lugar de migrar inline

Cada test usa patches del estilo
`patch("app.application.smart_query_service.generate_plan")`. Esos paths
apuntan a funciones que vivían reexportadas en el módulo monolítico.
Tras el borrado:
- `generate_plan` vive en `app.application.pipeline.nodes.planner`
- `load_memory` vive en `app.application.pipeline.memory.*`
- `discover_catalog_hints_for_planner` vive en `app.application.discovery.*`

Migrar 11 patches × 9 archivos × validar que el comportamiento testeado
se mantiene identical en el pipeline LangGraph (que organizan la lógica
en N nodos, no 1:1 con el monolito) es un trabajo de 6-10 h de cuidado
quirúrgico. Hacerlo apurado introduciría tests que pasan pero no
testean lo que los originales testeaban.

Los tests **no se borran** — quedan como punto de partida para la
migración eventual, una vez que haya tiempo dedicado para hacerla bien.

## Plan de migración (cuando se ejecute)

### Fase 0 — Inventario (1 h)
Mapear cada test del archivo monolítico al nodo equivalente del pipeline:

| Test name pattern | Nodo del pipeline equivalente |
|---|---|
| `test_plan_*` | `pipeline/nodes/planner.py` |
| `test_dispatch_*` | `pipeline/nodes/dispatcher.py` |
| `test_analyze_*` | `pipeline/nodes/analyst.py` |
| `test_memory_*` | `pipeline/memory/*` |
| `test_cache_*` | `pipeline/nodes/cache_lookup.py` |
| `test_casual_*` | `pipeline/nodes/casual_router.py` |

### Fase 1 — Reescritura por nodo (~4 h)
Por cada nodo:
1. Crear `tests/unit/pipeline/test_<node>.py` nuevo
2. Mover los tests relevantes del archivo monolítico
3. Reescribir la setup: en lugar de
   `service = SmartQueryService(**deps); await service.execute(...)`,
   hacer `state = build_state(...); await node(state)` aislando un nodo
4. Reescribir patches a sus paths reales en `pipeline/nodes/*`
5. Validar que el test sigue testeando el mismo concepto

### Fase 2 — Tests de integración (~2 h)
- `test_smart_pipeline.py` y `test_query_pipeline.py` ya tienen un
  pipeline equivalente en `tests/integration/test_pipeline_p1_tasks.py`
  (que SÍ corre). Identificar gaps y completar ahí.

### Fase 3 — Eval harness (~1 h)
- `tests/evaluation/run_eval.py` requiere el reference impl para
  generar baseline. Migrar a usar el pipeline directly o eliminar
  si el eval se moverá a tooling externo.

### Fase 4 — Cleanup (~30 min)
- Eliminar archivos skipped si la migración cubrió equivalentes
- Actualizar este spec con `Estado: completado`

## Decisión que NO se toma en este spec

- Si los tests del monolito siguen siendo el único reference de la
  lógica, **borrar la lógica monolítica fue prematuro**. Pero como el
  pipeline LangGraph atiende 100% del tráfico productivo y ha estado
  estable, la decisión fue razonable. Si aparecen regresiones en el
  pipeline, este spec acelera la migración.

## Definition of done

- 9 archivos de tests migrados o eliminados (no skipped)
- Coverage no baja en módulos del pipeline (verificar con `coverage report`)
- CI verde
- Este spec marcado `completado`
