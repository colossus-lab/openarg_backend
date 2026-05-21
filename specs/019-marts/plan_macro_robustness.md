# Plan — Macro Robustness para Marts Inmunes a UUIDs Inestables

**Type**: Forward-engineering plan (sprint dedicado)
**Status**: Draft 2026-05-06
**Sister spec**: [spec.md](spec.md), [017-raw-layer](../017-raw-layer/spec.md)
**Trigger incident**: 2026-05-06 — `cleanup_raw_orphans` dropeó tablas referenciadas por `escuelas_argentina` mart. Análisis posterior reveló que el portal CKAN BA Prov regenera UUIDs en cada re-scrape, rompiendo cualquier mart con `live_table('portal::UUID')` hardcoded.

---

## 1. Problema

Hoy las macros del resolver tienen dos comportamientos quebradizos:

### 1.1 — `live_table('portal::UUID')` con UUID inestable

Cuando el portal upstream regenera el `source_id` (UUID nuevo) en re-scrape, la `resource_identity` queda obsoleta. La macro emite:

```sql
(SELECT NULL::text AS dummy WHERE FALSE)
```

Si el SQL del mart referencia columnas explícitas (`SELECT cue::text FROM …`), Postgres rompe con `column "cue" does not exist` y el mart entero queda `build_failed`.

**Impacto medido (2026-05-06)**:
- `escuelas_argentina`: 4 UUIDs hardcoded, 2 missing (BA Prov + Corrientes), mart roto.
- `demo_energia_pozos`: 1 UUID missing (energia), mart roto.
- `legislatura_actividad`: 1 UUID frágil (diputados, hoy presente, sujeto a romperse).

### 1.2 — `live_tables_by_table_pattern` con schemas heterogéneos

Cuando el pattern matchea N≥2 tablas con schemas distintos (ej: versions del mismo dataset con cols agregadas/removidas upstream), el `UNION ALL SELECT *` interno explota:

```
(psycopg.errors.SyntaxError) each UNION query must have the same number of columns
```

**Impacto**: cualquier mart que use pattern + portales que evolucionan schemas (la mayoría de CKAN gov) está sujeto a este crash silencioso al primer scrape.

---

## 2. Diseño técnico

### 2.1 — Macro fallback con schema explícito

Cada macro `live_table*` acepta un kwarg opcional `expected_columns` que el resolver usa cuando 0 matches:

```yaml
sql: |
  SELECT cue::text AS cue, …
  FROM {{ live_table(
    'buenos_aires_prov::70f9ae07-…',
    expected_columns=['cue', 'nro_escuela', 'depend_func', 'latitud', 'longitud']
  ) }}
```

Cuando no hay match, en lugar de `SELECT NULL AS dummy`, el resolver emite:

```sql
(SELECT
   NULL::text AS cue,
   NULL::text AS nro_escuela,
   NULL::text AS depend_func,
   NULL::text AS latitud,
   NULL::text AS longitud
 WHERE FALSE)
```

El mart compila y emite 0 rows en ese bloque, sin romper. El UNION ALL del mart sigue funcionando con los bloques que SÍ matchean.

### 2.2 — Macro con schema-aware UNION

Para `live_tables_by_table_pattern` y `live_tables_by_pattern`: cuando matchea N tablas con schemas distintos, la macro debe **proyectar solo la intersección de columnas** (o aceptar `expected_columns` para forzar proyección común).

Algoritmo:
1. Para cada tabla matched, leer schema desde `information_schema.columns`.
2. Calcular `intersect = set(cols_t1) ∩ set(cols_t2) ∩ … ∩ set(cols_tN)`.
3. Si `expected_columns` está dado, usar `intersect ∩ expected`.
4. Emitir `UNION ALL SELECT col1, col2, … FROM tableN` con la lista común.
5. Si `intersect == ∅`, emitir el fallback de §2.1 con `expected_columns`.

### 2.3 — Validación al build_mart

Cada `build_mart` registra en `mart_definitions.macro_diagnostics` (campo JSONB nuevo) un report:

```json
{
  "macros_resolved": 4,
  "macros_with_zero_matches": 1,
  "schema_intersection_warnings": [
    {"pattern": "diputados__bloques__*", "matched": 7, "columns_dropped": 3}
  ]
}
```

Operadores ven inmediatamente cuando un mart construye sin uno de sus orígenes.

---

## 3. Cambios concretos

### 3.1 — Código (estimado: 4-5h)

| Archivo | Cambio |
|---|---|
| `app/application/marts/sql_macros.py` | Aceptar `expected_columns` en parser de args; lookup schemas via SQL `information_schema.columns`; intersección con SELECT explícito por tabla |
| `app/application/marts/builder.py` | Pasar resultado de macros + diagnostics a `mart_definitions` |
| `tests/unit/test_sql_macros.py` | Casos: 0 match con expected_cols, N matches schemas iguales, N matches schemas distintos, intersección vacía |
| `migrations/0048_mart_diagnostics.py` | `ALTER TABLE mart_definitions ADD COLUMN macro_diagnostics JSONB` |

### 3.2 — Marts YAML (estimado: 1h)

Agregar `expected_columns` en los 3 marts críticos:

- `escuelas_argentina.yaml`: 4 bloques × 5 cols
- `demo_energia_pozos.yaml`: 1 bloque × ~15 cols
- `legislatura_actividad.yaml`: 1 bloque (diputados) × 5 cols + migrar a pattern

Resto de marts: mantener `live_table('portal::slug')` cuando el slug es estable (gestión interna). Solo migrar los que dependen de UUIDs CKAN.

### 3.3 — Documentación (1h)

- README en `config/marts/`: regla "para portales con UUIDs inestables, usar pattern + expected_columns".
- `specs/constitution.md` §X: "Marts no deben hardcodear `source_id` upstream sin fallback".

---

## 4. Tests obligatorios

### 4.1 — Unit tests `test_sql_macros.py`

```python
def test_live_table_with_expected_cols_emits_typed_fallback():
    """0 matches + expected_columns → SELECT NULL::text AS col1, … WHERE FALSE"""

def test_pattern_with_heterogeneous_schemas_intersects():
    """N matches con cols distintos → SELECT col1, col2 FROM each
    (cols comunes solamente)"""

def test_pattern_with_empty_intersection_falls_back():
    """N matches sin cols comunes + expected_columns → fallback typed"""

def test_schema_diagnostics_reported():
    """Macro logs warnings cuando se descartan cols por intersection"""
```

### 4.2 — Integration tests `test_mart_invariants.py`

- Trigger build_mart `escuelas_argentina` con UUIDs missing → mart `built`, `last_row_count > 0` (datos solo de portales que matchearon).
- Trigger build_mart con TODOS UUIDs missing → mart `built`, `last_row_count = 0`, sin error de columna.

---

## 5. Rollout

1. **Fase 1**: deploy macro nueva con backward-compat (sin `expected_columns` se comporta como hoy).
2. **Fase 2**: actualizar 3 marts YAML con `expected_columns`.
3. **Fase 3**: trigger refresh manual + validar `last_row_count > 0`.
4. **Fase 4**: dejar que el beat schedule normal mantenga refresh.

---

## 6. Métricas de éxito

- ✅ Los 6 marts existentes vuelven a `built` o `refreshed`.
- ✅ `escuelas_argentina` con `last_row_count > 3000` (CABA + Mendoza al menos).
- ✅ Re-scrape simulado con UUIDs nuevos NO rompe ningún mart.
- ✅ 0 incidentes "column does not exist" en logs.

---

## 7. Riesgos

| Riesgo | Mitigación |
|---|---|
| Schema intersection silenciosamente descarta cols críticas | `macro_diagnostics` reporta cada drop; tests integración verifican shape esperado |
| Backward-compat: marts existentes sin `expected_columns` no obtienen el fallback typed | Mantener comportamiento actual cuando no hay arg; documentar como "legacy mode" |
| Performance: lookup de schemas en `information_schema` por cada build | Cache por proceso vivo; refresh cada N min vía `_load_schema_cache` |

---

## 8. Estimación total

| Item | Horas |
|---|---|
| Refactor `sql_macros.py` | 2.5 |
| Migración Alembic 0048 | 0.5 |
| Tests unit + integration | 2 |
| Migración 3 marts YAML | 1 |
| Documentación | 1 |
| Deploy + validación staging | 1 |
| **Total** | **8h** |

---

## 9. Out of scope

- Refactor del esquema "column-as-metadata" (long-term plan separado).
- Migración masiva de tablas `public.cache_*` legacy a `raw.*` (sprint de cleanup).
- Cambio del comportamiento de `cleanup_raw_orphans` (ya tiene safety net mart-protection desde 2026-05-06).
