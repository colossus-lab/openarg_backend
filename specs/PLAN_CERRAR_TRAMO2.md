# Plan — cerrar el Tramo 2

**Fecha**: 2026-08-23 · **Base**: `plan2026.md` de Dante, contrastado contra
producción medida hoy. Cada número sale de una consulta, no de una estimación.

---

## Por qué éste y no el Tramo 3

La regla transversal del plan de Dante es **congelar features hasta cerrar el
tramo 2**. El Tramo 2 no está cerrado: su gate pide *registro >90 % en prod* y
hoy está en **86,4 %**. Empezar CKAN antes de cerrarlo sería saltearse la única
regla de secuencia que el plan tiene.

---

## Qué falta exactamente

| Gate del Tramo 2 | Estado | Evidencia |
|---|---|---|
| prod = staging en código | ✅ | misma imagen, deriva de parches 0 en 4 contenedores |
| marts de presupuesto sirviendo | ✅ | los 6 + `inflacion_argentina` + `mediaciones_prejudiciales` |
| flag `USE_RAW_LAYER` decidido | ✅ | prendido 23-ago, verificado end-to-end |
| beat redesplegado | ✅ | |
| **registro >90 % en prod** | ❌ | **86,4 %** (25.465 de 29.484) |
| **chat verificado tras reconexión** | ❌ | requiere navegador |

---

## Punto 1 — el registro, de 86,4 % a ~100 %

**El hueco es una sola cosa, medida hoy**: 4.019 recursos `ready` cuya tabla
existe pero nunca se registró.

```
no registrados            4.019
  todas en schema         public      (4.019 de 4.019)
  todas con nombre        cache_*     (4.019 de 4.019)
  todas existen físicamente           (4.019 de 4.019)
  todas con filas > 0                 (4.019 de 4.019)
  todas identificables (portal::source_id)  (4.019 de 4.019)
  chocan con una identidad ya registrada        36
por portal: datos_gob_ar 3.469 · neuquen 302 · caba 61 · justicia 34 · resto 128
```

No es un problema de colecta: es sedimento de la era legacy que el backfill
masivo de mayo no alcanzó.

### Cómo

Un componente, no un script: `backfill_legacy_registry` en el módulo de
reconciliación que ya existe (`registry_reconcile.py`), porque es la misma
pregunta —*hacer que el registro diga la verdad sobre lo que hay*— vista desde
el otro lado. Hoy ese módulo arregla filas que mienten; esto agrega las que
faltan.

Reglas que el componente no puede saltear:

1. **`schema_name='public'`, porque ahí están.** Registrarlas como `raw` sería
   reintroducir exactamente el defecto que tuvo tres marts caídos y que
   `reconcile_locations` acaba de limpiar.
2. **Sólo tablas que existen y tienen filas.** Un registro que apunta a una
   tabla vacía es peor que un hueco: `live_table()` la serviría.
3. **Las 36 con identidad ya tomada no se tocan en esta pasada.** Insertarlas
   requiere decidir si son una versión nueva o un duplicado, y esa decisión
   necesita mirar las dos tablas. Se reportan, no se adivinan.
4. **Hereda el piso de fail-closed** de los otros barridos.
5. **`dry_run=True` por defecto**, aplicar explícito.

**Procedencia**: se registran con `parser_version = 'legacy:unknown'`, que
`is_real_provenance` rechaza. Es correcto y deliberado: no sabemos con qué
parser se leyeron, y escribir una huella inventada las haría elegibles para la
cascada de deriva con evidencia falsa. Suben el registro sin ensuciar la deriva.

**Cierra cuando**: la consulta del gate da >90 % y `reconcile_locations` sigue
devolviendo 0 desubicadas.

**Riesgo**: bajo. Es un INSERT sobre tablas que ya existen; no mueve datos, no
borra nada, no toca usuarios ni conversaciones. Reversible borrando las filas
insertadas por su `run_id`.

---

## Punto 2 — el chat tras reconexión

Este gate **necesita un navegador y no lo puedo automatizar**: pide verificar
que una conversación sobrevive a la pérdida de `checkpoint_blobs`.

Y hay algo que medí hoy y que lo hace más urgente de lo que el plan suponía:
**las tres tablas de checkpoint están duplicadas entre `public` y `raw` y
divergen**.

```
checkpoints        public=18.013   raw=20.765
checkpoint_writes  public=70.504   raw=84.256
checkpoint_blobs   public=18.003   raw=19.994
```

De 1.268 hilos, **1 solo existe en los dos schemas**: cada conversación cae
consistentemente de un lado, así que no se parten a la mitad. El riesgo es que
una conversación retomada aterrice del otro lado y aparezca vacía. Ya pasó una
vez.

**No se toca sin tu decisión.** Calificar el checkpointer a un schema deja sin
historial a 493 conversaciones o a 774, según cuál se elija.

**Lo que sí puedo hacer sin decidir nada**: instrumentar. Un chequeo que
reporte, por cada hilo activo, si su historial está donde el checkpointer lo va
a buscar — convierte "puede pasar" en "pasó N veces esta semana", que es lo que
hace falta para decidir bien.

**Lo tuyo, 5 minutos**: abrir el chat en prod, hacer dos preguntas seguidas y
confirmar que la segunda recuerda la primera.

---

## Lo que la medición de hoy corrigió sobre el Tramo 5

Lo dije sin medir y era más pesimista que la realidad. Medido:

| Ítem del plan | Estado real |
|---|---|
| `APP_ENV` honesto | ✅ `prod` |
| `/api/v1/data/*` protegido | ✅ gateado por service token, **falla cerrado** (503) |
| `BACKEND_API_KEY` en access log | ✅ 0 ocurrencias; Caddy no escribe access log |
| Redis `noeviction` | ⚠️ `allkeys-lru`, 512 MB — **0 desalojos jamás**, usa 25 MB |
| Pentest SSRF | ❌ sin hacer |

Tres de cinco ya están bien. El de Redis es real pero **latente**: al 5 % de su
techo y sin un solo desalojo en su historia. Es un cambio de una línea que
conviene hacer cuando haya otro despliegue, no uno que justifique uno.

---

## Y entonces, ¿falta poco?

Del Tramo 2, sí: un backfill y una verificación por navegador.

Del plan completo, no. **El Tramo 3 está entero sin empezar**, y es el más
grande de los seis:

```
datasets                          32.566
con `columns` vacías              32.081   (98,5 %)
URLs con más de un source_id       5.660
columna original_identifier       NO EXISTE
```

La reconciliación que el Tramo 3 propone necesita un lugar donde guardar
`original_identifier` y hoy no hay ninguno. Eso no es un ajuste: es el tramo que
Dante marcó como *"diseño primero"*, y con razón.

Del Tramo 4 falta la mitad barata (expectativas en marts, 0 tablas) y del 1
falta lo de Dante (snapshots de RDS, alarmas, retención) — que después del
incidente del 22-ago, donde la recuperación dependió de que S3 tuviera los
crudos porque **no hubo backup de la base al que volver**, merece subir.

---

## Orden propuesto

1. **Backfill del registro** — cierra el gate numérico del Tramo 2. Acotado,
   reversible, medible.
2. **Instrumentar los checkpoints** — convierte una decisión sobre datos de
   usuarios en una decisión con evidencia.
3. **Tu verificación por navegador** — 5 minutos, y cierra el Tramo 2.
4. **Expectativas en marts** — la mitad barata del Tramo 4.
5. **Tramo 3, diseño primero** — el grande.

**Regla que este documento se ganó a pulso**: una consecuencia afirmada sin
medir vale menos que no afirmar nada, porque decide prioridades igual. El Tramo
5 de hoy es el ejemplo: lo di por peor de lo que estaba, y esa suposición lo
habría puesto antes que cosas que sí importan.
