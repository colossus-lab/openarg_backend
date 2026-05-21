# Plan — Marts Overhaul (reparar, crecer, migrar)

**Type**: Forward-engineering plan
**Status**: Draft 2026-05-06
**Sister specs**: [spec.md](spec.md), [plan_macro_robustness.md](plan_macro_robustness.md), [017-raw-layer](../017-raw-layer/spec.md)

---

## 1. Estado actual y problema

### 1.1 — Diagnóstico cuantitativo

| Capa | Estado |
|---|---|
| `raw.*` resource_identities live | **6,197** |
| `cached_datasets ready` | ~9,300 |
| Marts existentes | **6** (4 built/refreshed, 2 build_failed) |
| Cobertura raw→mart | **<0.4%** del catálogo raw |

### 1.2 — Marts existentes

| Mart | Status | Filas | Fuentes actuales |
|---|---|---|---|
| `legislatura_actividad` | ✅ built | 328 | `raw.diputados__bloques_*` + `public.cache_senado_listado_senadores` |
| `presupuesto_consolidado` | ✅ built | 4,959 | `public.cache_presupuesto_credito_*` (legacy) |
| `series_economicas` | ✅ built | 39 | `public.cache_bcra_cotizaciones` (legacy) |
| `staff_estado` | ✅ refreshed | 4,536 | `public.staff_snapshots` + `public.senado_staff` (legacy) |
| `escuelas_argentina` | ❌ build_failed | 0 | 4 UUIDs hardcoded missing (CKAN regenera) |
| `demo_energia_pozos` | ❌ build_failed | 0 | 1 UUID hardcoded missing |

### 1.3 — Lo que está mal

1. **2 marts inservibles** por UUIDs hardcoded volátiles
2. **3 de 4 marts OK usan schema legacy `public.cache_*`** en lugar del medallion `raw.*`
3. **No hay marts** para los 9 portales más grandes (caba 1.2K tablas, datos_gob_ar 800, mendoza 600, etc.)
4. **Macros actuales fallan** cuando un pattern no matchea o devuelve schemas heterogéneos

---

## 2. Visión

**6 meses**: 20+ marts cubriendo los dominios principales (educación, salud, justicia, energía, transporte, comercio, geo, finanzas), todos sobre `raw.*`, robustos a re-scrapes upstream, con tests de regresión que prevengan rotura silenciosa.

---

## 3. Plan en 4 fases

### Fase 0 — Refactor macros (PREREQUISITO)
**Plan ya escrito en [plan_macro_robustness.md](plan_macro_robustness.md). Resumen:**
- `live_table('id', expected_columns=[...])` con fallback typed cuando 0 matches
- `live_tables_by_table_pattern('pat')` con schema-intersection para N matches heterogéneos
- Migración 0048 con columna `mart_definitions.macro_diagnostics` JSONB
- **Esto debe completarse ANTES de tocar los YAML**

**Costo**: 8h. **Bloquea** Fase 1 y 2.

---

### Fase 1 — Reparar los 2 marts broken (post-Fase 0)

#### 1.A — `escuelas_argentina` v0.4.0

**Problema actual**: 4 `live_table('portal::UUID')` hardcoded, todos missing porque CKAN regenera UUIDs.

**Solución**: migrar a `live_tables_by_table_pattern('<portal>__establecimientos_educativos__*')` con `expected_columns` por bloque.

**SQL nuevo (esquema)**:
```yaml
# bloque BA Prov
SELECT cue::text AS cue, ..., latitud::numeric AS lat, longitud::numeric AS lng
FROM {{ live_tables_by_table_pattern(
    'buenos_aires_prov__establecimientos_educativos__*',
    expected_columns=['cue', 'nro_escuela', 'depend_func', 'latitud', 'longitud']
) }} ba_prov
WHERE cue IS NOT NULL
```

Misma migración para CABA, Mendoza, Corrientes (cada uno con su `expected_columns` propio).

**Tests obligatorios**:
- `test_escuelas_argentina_with_all_portals_present` — 4 portales, row_count > 25k
- `test_escuelas_argentina_with_one_portal_missing` — 3 portales, row_count > 18k (sigue funcionando)
- `test_escuelas_argentina_with_all_portals_missing` — 0 portales, mart construye con 0 rows (no error)

**Recovery datos**:
- Re-scrape de BA Prov + Corrientes (UUIDs nuevos llegan al raw)
- `refresh_mart escuelas_argentina` reconstruye con datos disponibles

**Costo**: 1.5h código + 1h tests + 30min validation. Total: **3h**.

#### 1.B — `demo_energia_pozos` v0.2.0

**Problema actual**: `live_table('energia::333fd72a-...')` — UUID missing.

**Solución**: migrar a `live_tables_by_table_pattern('energia__produccion_de_petroleo_y_gas_por_pozo*')` con `expected_columns`.

**Nota**: schema upstream es estable (mismas columnas anio/mes/empresa/sigla/formprod/prod_pet/prod_gas para todas las versions). Pattern matching seguro.

**Tests**:
- `test_demo_energia_pozos_with_data` — row_count > 800
- `test_demo_energia_pozos_no_data_yet` — mart construye con 0 rows

**Costo**: 1h código + 30min tests + 30min validation. Total: **2h**.

#### Fase 1 total: **5h**

---

### Fase 2 — Migrar marts legacy a `raw.*` (post-Fase 0)

#### 2.A — `presupuesto_consolidado` v0.3.0

**Hoy**: `FROM public.cache_presupuesto_credito_2016..2026` (11 tablas legacy hardcoded).

**Después**:
```yaml
FROM {{ live_tables_by_table_pattern(
    'presupuesto_abierto__credito_*',
    expected_columns=['ejercicio_presupuestario', 'jurisdiccion_desc',
                      'programa_desc', 'credito_vigente', 'credito_devengado']
) }} c
```

Pattern matchea automáticamente cuando se agrega el año 2027, etc. Sin hardcode anual.

**Tests**: row_count > 4500, todos los años cubiertos.

**Costo**: 1.5h.

#### 2.B — `series_economicas` v0.2.0

**Hoy**: `FROM public.cache_bcra_cotizaciones` (legacy single).

**Después**:
```yaml
FROM {{ live_table('bcra::cotizaciones',
    expected_columns=['codigoMoneda', 'descripcion', 'tipoCotizacion']
) }}
```

`bcra::cotizaciones` ES estable (es un slug interno de nuestro connector, no UUID upstream).

**Costo**: 30min.

#### 2.C — `staff_estado` v0.2.0

**Hoy**: `public.staff_snapshots` + `public.senado_staff` (legacy).

**Después**:
```yaml
FROM {{ live_table('staff_hcdn::snapshots', expected_columns=[...]) }}
FROM {{ live_table('senado::staff', expected_columns=[...]) }}
```

Ambos slug-stable (connectors propios), no UUIDs upstream.

**Costo**: 1h.

#### Fase 2 total: **3h**

---

### Fase 3 — Marts nuevos prioritarios (post-Fase 1+2)

#### 3.A — `salud_indicadores` (NEW)

**Dominio**: salud nacional, agregado por jurisdicción + indicador + período.

**Fuentes**: 332 raw tables del portal `salud`.

**Schema canónico**:
```yaml
canonical_columns:
  - {name: indicador, type: text, description: "Tipo de indicador (mortalidad, vacunación, EFR, etc)"}
  - {name: jurisdiccion, type: text, description: "Provincia/CABA/Total país"}
  - {name: periodo, type: text, description: "Año o año-mes"}
  - {name: valor, type: numeric, description: "Valor del indicador"}
  - {name: unidad, type: text, description: "%, casos, tasa por 1000 hab, etc"}
  - {name: fuente, type: text, description: "Programa/sistema fuente (SISA, SNVS, etc)"}
```

**Estrategia**: empezar con 5-10 datasets de salud más relevantes (vacunación COVID, mortalidad infantil, indicadores básicos). UNION ALL con casts canónicos.

**Costo**: 4h.

#### 3.B — `justicia_causas` (NEW)

**Dominio**: causas judiciales federales agregadas.

**Fuentes**: 524 raw tables del portal `justicia`.

**Schema canónico**:
```yaml
canonical_columns:
  - {name: jurisdiccion, type: text}
  - {name: tipo_causa, type: text}
  - {name: estado, type: text, description: "iniciada, en trámite, sentencia, archivada"}
  - {name: fecha_inicio, type: date}
  - {name: fuero, type: text, description: "civil, penal, comercial, laboral, etc"}
```

**Estrategia**: agregar por (fuero, jurisdicción, tipo_causa, mes).

**Costo**: 4h.

#### 3.C — `energia_produccion` (NEW)

**Dominio**: producción de hidrocarburos + electricidad.

**Fuentes**: 474 raw tables del portal `energia`.

**Schema canónico**:
```yaml
canonical_columns:
  - {name: anio, type: int}
  - {name: mes, type: int}
  - {name: provincia, type: text}
  - {name: empresa, type: text}
  - {name: producto, type: text, description: "petroleo|gas|electricidad"}
  - {name: cantidad, type: numeric}
  - {name: unidad, type: text, description: "m3, dam3, MWh"}
```

**Estrategia**: UNION ALL de producción petróleo/gas (por pozo agregada) + balances eléctricos.

**Costo**: 5h (la lógica upstream tiene quirks que ya conocemos).

#### 3.D — `caba_geo` (NEW)

**Dominio**: datasets georreferenciados de CABA (mapa de la ciudad).

**Fuentes**: ~50 raw tables de CABA con geometry o lat/lng.

**Schema canónico**:
```yaml
canonical_columns:
  - {name: dataset_origen, type: text, description: "Nombre del dataset fuente"}
  - {name: nombre, type: text}
  - {name: categoria, type: text}
  - {name: lat, type: numeric}
  - {name: lng, type: numeric}
  - {name: barrio, type: text}
  - {name: comuna, type: text}
```

**Estrategia**: pattern por table_name LIKE `caba__*geo*` o que tengan columnas geometry.

**Costo**: 4h.

#### 3.E — `magyp_agro` (NEW)

**Dominio**: producción agropecuaria nacional.

**Fuentes**: 343 raw tables del portal `magyp`.

**Schema canónico**:
```yaml
canonical_columns:
  - {name: cultivo, type: text}
  - {name: campania, type: text, description: "Año-año (ej 2023-2024)"}
  - {name: provincia, type: text}
  - {name: departamento, type: text}
  - {name: superficie_sembrada_ha, type: numeric}
  - {name: superficie_cosechada_ha, type: numeric}
  - {name: produccion_ton, type: numeric}
  - {name: rendimiento_kg_ha, type: numeric}
```

**Costo**: 4h.

#### Fase 3 total: **5 marts × 4h promedio = 20-21h**

---

### Fase 4 — Tests + Observabilidad + Documentación

#### 4.A — Tests de invariantes por mart (`tests/integration/test_mart_invariants.py`)

Para CADA mart:

```python
def test_<mart>_built():
    """Mart debe tener last_refresh_status in (built, refreshed)."""

def test_<mart>_min_rows():
    """Mart debe tener row_count > {threshold}."""

def test_<mart>_canonical_columns():
    """Cada column de canonical_columns existe en el matview."""

def test_<mart>_no_nulls_in_keys():
    """Columnas de unique_index_columns no son NULL."""

def test_<mart>_resilient_to_missing_source():
    """Si UNA fuente desaparece, mart sigue construyendo (no error)."""
```

#### 4.B — Métricas operacionales

- `mart_refresh_failures_total{mart_id}` — counter Prometheus / log
- `mart_row_count{mart_id}` — gauge actual
- `mart_macro_diagnostics{mart_id}` — JSONB para análisis post-mortem

Beat task `openarg.publish_mart_metrics` cada hora.

#### 4.C — Documentación

- `config/marts/README.md` — guía con:
  - Cuándo usar `live_table` vs `live_tables_by_table_pattern`
  - Cuándo necesitas `expected_columns`
  - Patrón canónico de SELECT con casts
  - Cómo agregar un mart nuevo (template)

- `specs/019-marts/spec.md` — actualizar con `expected_columns` y schema-intersection.

- `specs/constitution.md` §X — regla "marts NO pueden hardcodear UUIDs upstream sin `expected_columns` fallback".

#### Fase 4 total: **6h**

---

## 4. Cronograma propuesto

### Sprint 1 (1.5 días, 12h)
- **Fase 0**: Refactor macros (8h)
- **Fase 1**: Reparar 2 marts broken (5h)
- **Total**: 13h

### Sprint 2 (1 día, 8h)
- **Fase 2**: Migrar 3 marts legacy a raw (3h)
- **Fase 3.A**: `salud_indicadores` (4h)
- **Fase 4 (parcial)**: tests para los 5 marts existentes (2h)

### Sprint 3 (1 día, 8h)
- **Fase 3.B**: `justicia_causas` (4h)
- **Fase 3.C**: `energia_produccion` (4h)

### Sprint 4 (1 día, 8h)
- **Fase 3.D**: `caba_geo` (4h)
- **Fase 3.E**: `magyp_agro` (4h)

### Sprint 5 (medio día, 4h)
- **Fase 4**: documentación + métricas operacionales completas

**Total: 5 sprints, ~6 días dev efectivos.**

---

## 5. Métricas de éxito

### Por mart (cada uno)
- ✅ `last_refresh_status in ('built', 'refreshed')`
- ✅ `last_row_count > {threshold mínimo del dominio}`
- ✅ Tests integración pasan: row_count, canonical_columns, no_nulls_in_keys, resilient_to_missing_source

### Globales
- ✅ Marts: 4 OK → **10 OK** (los 6 + 4 nuevos en Sprint 1-3, +1 en Sprint 4)
- ✅ Cobertura raw→mart: 0.4% → **~5%** (~12-13× más raw tables alimentando marts)
- ✅ Filas en `mart.*`: ~10K → **~500K-2M**
- ✅ Re-scrape simulado (UUIDs cambiados upstream) NO rompe ningún mart
- ✅ Documentación + tests de regresión completos

---

## 6. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Refactor macros rompe los 4 marts OK actuales | Backward-compat: sin `expected_columns` se comporta como hoy. Tests integración antes de deploy. |
| Schemas heterogéneos en pattern → UNION ALL falla | `live_tables_by_table_pattern` calcula intersección, descarta cols dispares. Diagnostics reporta drops. |
| Mart nuevo con datos sucios (geo wrong, dups, etc) | Test de invariantes obligatorio antes de promote a CI. Validación manual de row_count + sample. |
| Beat refresh quema RDS IO | Schedule staggered (diferentes minutos), max 1 mart refresh por minuto, build incremental cuando posible. |
| Datos perdidos por re-scrape upstream | Pattern matching es resilient — si un dataset desaparece, mart construye con los demás. Diagnostics avisa. |

---

## 7. Out of scope

- **Column-as-metadata** (long-term plan separado, otro spec)
- **Refactor de raw_table_versions** para soportar resource identity migration cuando portal cambia UUID (sprint distinto)
- **Marts incremental refresh** (hoy todos son full rebuild — OK porque RDS aguanta)
- **Mart-level access control** (todos los marts son públicos hoy)

---

## 8. Decisiones a tomar antes de arrancar

1. **¿Sprint 1 ahora o esperar?** — Refactor macros es prerequisito. Sin él los 2 broken siguen rotos y los nuevos nacen frágiles.

2. **¿Priorizar dominios distintos?** — Propuse salud, justicia, energía, caba_geo, magyp como Sprint 3. Si el negocio prioriza otros (transporte, ambiente, comercio), reordenar.

3. **¿Tests integración obligatorios?** — Recomendado SÍ. Cuesta 30min por mart pero previene la rotura silenciosa que ya nos pasó dos veces.

4. **¿Migrar marts legacy o dejarlos?** — Recomiendo migrar (Fase 2). 3h de trabajo, pero los `public.cache_*` tienen su propio cycle de re-scrape que puede divergir de `raw.*` y romperlos eventualmente.

---

## 9. Resultado esperado tras los 5 sprints

```
ANTES (hoy):
  4 marts OK (10K filas)
  2 marts broken
  6,197 raw resources, ~20 referenciados por marts (0.4% cobertura)

DESPUÉS (5 sprints):
  10 marts OK (500K-2M filas estimadas)
  0 marts broken
  ~150-300 raw resources alimentando marts (3-5% cobertura)
  Macros robustas a UUIDs volátiles + schema heterogéneo
  Tests de regresión por mart
  Documentación + métricas operacionales
```

**Cobertura del producto a usuarios finales**:
- **Hoy**: AI puede contestar queries sobre 4 dominios (educación parcial, presupuesto, BCRA, staff)
- **Después**: AI contesta queries sobre 10+ dominios (+salud, justicia, energía, geo CABA, agro)

Eso es el ROI real del sprint.
