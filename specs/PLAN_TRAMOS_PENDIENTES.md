# Plan de acción — tramos pendientes del Plan 2026

**Fecha**: 2026-08-22
**Base**: `plan2026.md` de Dante (19-ago), contrastado contra el código y la base
de **producción** medida hoy. Cada número de este documento sale de una consulta,
no de una estimación.

---

## 0. Qué cambió respecto de la foto del 19-ago

| Punto del plan | Estado hoy | Evidencia |
|---|---|---|
| `raw.cached_datasets` reconstruida (T1b) | **hecho** | 26.781 filas, backfill de dos pasadas, colecta funcionando |
| `cleanup_raw_orphans` no borra de más (T1a, parcial) | **hecho** | excluye vistas, no toca vivas con datos, `LIKE` escapado |
| PR #48 a prod (T2) | **hecho** | radio real medido: 77 tablas, no 3.394 |
| Diff de esquema entre versiones → perfil y drift (T4) | **hecho** | specs 023/024, probado end-to-end contra Postgres |
| Consumidor de la métrica (T4, §5.5) | **parcial** | `/admin/data-health`; falta alerta y aviso al usuario |

Y tres cosas que el plan no anticipaba y que la medición de hoy encontró:

- **`cleanup_raw_orphans` estuvo tres meses sin dropear nada** porque
  `information_schema.tables` lista vistas y `DROP TABLE` falla sobre una. Cada
  corrida elegía las mismas 10 vistas, fallaba en las 10 y devolvía éxito.
- **El `search_path` de prod es no determinístico** por PGBouncer: 1 de 12
  conexiones resuelve `public` primero. Costó 23.445 snapshots sin identidad.
- **Un refresco puede aceptar una re-lectura peor** y destruir la tabla buena.
  Pasó: ~47 recursos degradados, recuperación en curso.

---

## Tramo 1 — Cortar el sangrado (lo que falta)

### 1.1 Fallar cerrado en los sweeps de borrado — **prioridad máxima**

**Medido**: `cleanup_raw_orphans` no verifica que `raw.cached_datasets` exista.
No hay ningún `to_regclass` ni chequeo previo en `ops_fixes.py`.

Es el punto exacto que causó el incidente del 3-ago: sin esa tabla, el
`NOT EXISTS (SELECT 1 FROM cached_datasets ...)` es verdadero para todo y **el
catálogo entero se vuelve huérfano**. Hoy la tabla existe, así que no hay
síntoma — y por eso es el momento de arreglarlo.

Alcance: un chequeo de existencia y de plausibilidad (si la tabla tiene menos
filas de las esperadas, abortar) al principio de cada sweep destructivo:
`cleanup_raw_orphans`, `cleanup_invariants`, `cleanup_empty_raw_tables`,
`retain_raw_versions`.

Cierra cuando: borrar `cached_datasets` en un entorno de prueba hace que los
cuatro sweeps aborten sin dropear nada.

### 1.2 `now()` fuera de la firma de embedding

**Medido**: `scraper_tasks._embedding_signature` sigue tomando `last_updated`.
No se tocó.

Costo de no hacerlo: re-embeddings masivos por cambios de metadata que no
alteran el contenido. El plan lo lista como causa del re-embed masivo de §2.4.

### 1.3 Snapshots de RDS, alarmas y retención

Es de Dante y sigue sin hacerse. Después del incidente de hoy —donde la
recuperación dependió de que S3 tuviera los crudos— vale subirlo de prioridad:
**no hubo backup de la base al que volver.**

---

## Tramo 2 — Cerrar la promoción

### 2.1 Decidir `OPENARG_USE_RAW_LAYER` — **la decisión que más cuesta postergar**

**Medido hoy**:

```
STAGING  OPENARG_USE_RAW_LAYER=1     4.386 tablas legacy · 22.722 versionadas
PROD     (no seteada → legacy)       6.232 tablas legacy · 20.466 versionadas
```

Prod corre el camino legacy: cada colecta nueva crea `cache_<portal>_<slug>__<hash>`
en vez de `<portal>__<titulo>__<hash>__vN`. **La divergencia crece todos los
días** — prod ya tiene 1.846 tablas legacy más que staging.

Consecuencia concreta para el trabajo de deriva: sin capa raw, un recurso no
tiene versiones, y sin versiones **no hay pares que comparar**. La detección de
deriva en prod está estructuralmente limitada mientras el flag esté apagado.

Es tuya la decisión. Las dos salidas son prenderlo (y planificar el cutover de
lo legacy) o declarar legacy como definitivo y adaptar el trabajo de deriva a
comparar snapshots de la misma tabla en el tiempo.

### 2.2 Marts caídos — **3 son deriva real, no deuda**

**Medido**: 69 marts, 47 `built`, 18 `refreshed`, **4 `build_failed`**.

| Mart | Error | Lectura |
|---|---|---|
| `legislatura_actividad` | column "BLOQUE" does not exist | deriva |
| `legisladores_argentina` | column "APELLIDO" does not exist | deriva |
| `decretos_presidenciales` | column "DECRETO" does not exist | deriva |
| `staff_estado` | relation `raw.staff_snapshots` does not exist | schema |

Los tres primeros fallaron a las **06:00 del 22-ago**, antes de cualquier
reparación mía de hoy (20:04 y 21:00). Verificado: de las 116 reparaciones que
alguna vez tocaron esas columnas, **114 son de mayo** y ninguna pasó mayúsculas
a minúsculas. **No las rompimos nosotros.**

Las columnas siguen existiendo en otras tablas (`BLOQUE` en 18, `APELLIDO` en
20, `DECRETO` en 6), así que el mart está apuntando a una tabla que dejó de
tenerlas. Es exactamente el caso que la spec 024 existe para explicar, y es el
primer ejemplo de deriva afectando producción de forma visible.

`staff_estado` es otra cosa: busca `raw.staff_snapshots` y esa tabla vive en
`public` por diseño. Es el mismo problema de calificación de schema.

`presupuesto_consolidado` refrescó hoy con **0 filas** — revisar si es correcto
o si su fuente desapareció.

### 2.3 Lo demás del tramo

Redespliegue de beat: **hecho**. El e2e por UI en staging y prod: pendiente.

---

## Tramo 3 — Ingesta CKAN 2.11

**Medido en prod hoy**:

```
datasets                       32.566
con `columns` vacías           32.081   (98,5 %)
URLs con múltiples source_id    5.660
columna original_identifier    NO EXISTE
formatos: csv 14.770 · zip 8.039 · geojson 4.134 · xlsx 2.782 · xls 1.712
```

Tres cosas que el plan supone y hoy no están:

1. **`original_identifier` no existe como columna.** La reconciliación que el
   plan propone necesita un lugar donde guardarla; hoy no hay ninguno.
2. **`columns` está vacía en el 98,5 %**, no sólo en datos.gob.ar. Es
   transversal a todos los portales.
3. **5.660 URLs con más de un `source_id`** — la regeneración de identificadores
   de CKAN 2.11, medida.

Y una corrección al plan basada en lo que construimos: el refresco que existe
hoy usa `last_updated_at` del portal, que es **metadata, no contenido**. Lo
medimos: 68 re-colectas y cero archivos efectivamente distintos. El plan pedía
"detección de cambios por contenido" y tenía razón — el camino es
`source_file_hash`, que ya se guarda en `raw_table_versions`.

**Formato**: `geojson` (4.134) no aparece en el plan y es el tercer formato del
catálogo. `DTA`/`SAV` que el plan menciona no figuran entre los siete primeros.

---

## Tramo 4 — Calidad de datos (lo que falta)

Lo grande está hecho. Falta el consumidor, que es justo lo que §5.5 declara no
negociable: *una métrica sin consumidor es decoración*.

**Existe**: `/api/v1/admin/data-health` — frescura, calidad de parseo,
reparaciones con sus rechazos, observabilidad de deriva. Todo desde consultas.

**Falta**:
- Una alerta humana por cada `UNEXPLAINED` nuevo. Hoy el reporte es sombra y
  eso es correcto — pero la razón para salir de sombra ya no es la precisión
  (0 de 480 pares son accionables), es que **no hay a quién avisarle**.
- El aviso en el chat con la edad del dato. Un usuario no puede saber que su
  respuesta se apoya en datos de mayo. El 80 % de los recursos servidos tienen
  más de 90 días.
- Expectativas manuales en marts. Los 3 marts caídos habrían sido una alerta
  en vez de un descubrimiento casual.

---

## Tramo 5 — Deuda

Sin medir en esta pasada. El plan lista: pentest de SSRF, allowlist del backend,
`/api/v1/data/*` en Caddy, `BACKEND_API_KEY` en el access log, Redis
`noeviction`, `APP_ENV` honesto.

Vale una medición propia antes de planificarlo; no la hice y no voy a inventar
su estado.

---

## Orden propuesto, y por qué

1. **1.1 fallar cerrado** — cinco líneas, y es lo único que separa a producción
   de un segundo 3 de agosto. Nada más debería ir antes.
2. **2.2 los tres marts caídos** — están caídos ahora, son deriva real, y
   arreglarlos valida que el trabajo de la spec 024 sirve para algo concreto.
3. **2.1 decidir `USE_RAW_LAYER`** — es tuya, y la divergencia crece mientras
   tanto. También determina si la detección de deriva en prod puede funcionar.
4. **4.x el consumidor** — una alerta y la edad del dato en el chat. Barato,
   y convierte todo lo construido en algo que alguien ve.
5. **1.2 la firma de embedding** — acotado, ahorro directo.
6. **3.x CKAN** — el más grande, y el que más se beneficia de decidir 2.1 antes.
7. **5 deuda** — después de medirlo.

**Regla del plan que conviene retomar**: *cada tramo cierra con una medición en
uso, no con un merge*. Hoy tenemos las mediciones; lo que falta es que alguien
las mire sin que se las pidan.
