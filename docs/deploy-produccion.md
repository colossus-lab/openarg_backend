# Desplegar a producción

Este procedimiento no estaba escrito en ningún lado. Lo que sigue es lo que
se ejecutó y verificó el 2026-09-28 promoviendo 37 commits.

`docs/deployment.md` describe el entorno **local**; esto es el otro.

## Lo que hay que saber antes

**Prod consume el tag `:latest`, y `:latest` sólo lo publica `main`.**
`build.yml` lo agrega únicamente cuando `github.ref` es `refs/heads/main`;
`staging` publica `:staging`. No existe ningún tag `:prod`. Un merge a
`staging` **no** cambia nada en producción.

**El build tiene filtro de `paths`.** Un merge a `main` que toque sólo
`scripts/`, `dbt/` o `docs/` no genera imagen nueva, y `:latest` queda
apuntando al build anterior **sin que nada falle a la vista**.

**`make docker.prod` no sirve para esto**: no hace `pull`, y con tags
móviles no baja nada si la imagen local ya existe.

**El frontend se construye en otro repo** (`openarg_frontend`), con el mismo
esquema de tags. Si el cambio lo incluye, hay que promoverlo también.

**El único punto de retorno real es el tag `:sha-<7>`**, que se publica en
cada build. Los demás (`:latest`, `:main`, `:staging`) son móviles.

## 1. Congelar el punto de retorno

Antes de tocar nada, en el servidor. Crea alias locales: no descarga ni
borra nada, y hace que el rollback no dependa del registry.

```bash
T=rollback-$(date +%Y%m%d)
B=ghcr.io/colossus-lab/openarg
for c in $(docker ps --format '{{.Names}}' | grep -E 'openarg_(backend|beat|worker|frontend)'); do
  img=$(docker inspect -f '{{.Image}}' "$c")
  nombre=$(docker inspect -f '{{.Config.Image}}' "$c" | sed 's|.*/||; s|:.*||')
  docker tag "$img" "$B/$nombre:$T"
done
cp /opt/docker/openarg/docker-compose.yaml /opt/docker/openarg/docker-compose.yaml.bak-$(date +%Y%m%d)
```

## 2. Promover

PR de `staging` a `main`, en los repos que correspondan. El del backend y el
del frontend son independientes.

## 3. Esperar las imágenes

Verificar que **Build & Push Docker Images** termine en verde y publique las
**10 imágenes** del backend (`api`, `beat`, los 7 `worker-*` y `openarg-mcp`), más la del
frontend en su repo. Este paso es explícito por el filtro de `paths`: un
build que no corrió no se nota hasta que el deploy no cambia nada.

## 4. Desplegar

Todos los servicios en una pasada. **El beat es el que más importa**: buena
parte de los cambios de comportamiento viven en `beat_schedule`, así que con
los workers nuevos y el beat viejo las tareas se siguen despachando con la
configuración anterior.

```bash
cd /opt/docker/openarg
SVC="backend beat frontend mcp worker-scraper worker-collector \
     worker-collector-heavy worker-collector-heavy-retry worker-ingest \
     worker-embedding worker-analyst worker-transparency worker-s3"
docker compose pull $SVC
docker compose up -d --no-deps $SVC
```

Evitar las 3:30 ART (limpieza nocturna de catálogo) y las 6:00 UTC (barridos
y refresh de marts).

### Desplegar sin que algunas tareas corran solas

Si el código nuevo cambia lo que hace una tarea agendada y se la quiere correr
a mano la primera vez (p. ej. la ingesta de series, que reescribe tablas de
`raw`), sacarla de la agenda antes del `up -d` con `OPENARG_BEAT_DESACTIVADAS`
en el `.env` de `/opt/docker/openarg` (lo leen el beat y los workers):

```bash
OPENARG_BEAT_DESACTIVADAS=ingest-series-tiempo,check-series-freshness,snapshot-bcra
```

Van los nombres de las **entradas** de `beat_schedule`, no los de las tareas.
Verificar que el beat las sacó (y que no hay un nombre mal escrito, que sale
como `ERROR` y deja la tarea corriendo):

```bash
docker logs openarg_beat 2>&1 | grep OPENARG_BEAT_DESACTIVADAS
docker exec openarg_beat python -c "
from app.infrastructure.celery.app import celery_app as a
print(sorted(k for k in a.conf.beat_schedule if 'series' in k or 'bcra' in k))"
```

Para volver a agendarlas, sacar la línea del `.env` y recrear `beat` y los
workers (`docker compose up -d --no-deps beat worker-ingest …`).

## 5. Verificar

**El gate**, que existe por el deploy a medias del 2026-09-02 —API y frontend
nuevos, 9 workers y beat con imágenes de una semana antes, y el chat
contestando 512 diputados donde hay 256 durante 14 horas:

```bash
./scripts/verify_deploy.sh    # sale != 0 si hay deriva
```

El archivo tiene finales de línea CRLF en Windows. Si se copia desde ahí,
pasarlo por `tr -d '\r'` o falla con errores de sintaxis **que se leen como
éxito**.

Después, que el código nuevo llegó de verdad:

```bash
docker exec openarg_beat python -c "
from app.infrastructure.celery.app import celery_app as a
print(a.conf.task_routes['openarg.recover_stuck_tasks'])
print(sum(1 for v in a.conf.task_routes.values()
          if isinstance(v, dict) and v.get('queue') == 'default'))"
```

Y en uso:

| Qué | Criterio |
|---|---|
| Sitio y API | `openarg.org` HTTP 200, `api.openarg.org/health` → `healthy` |
| Colas | Ninguna crece sin drenar |
| Tareas periódicas | `recover_stuck_tasks` aparece en los logs de `worker_ingest` |
| Chat | Preguntas reales, no sólo el health check |

## Rollback

Sin migraciones pendientes es sólo de imágenes:

```bash
T=rollback-<fecha>
for s in api beat worker-collector worker-ingest worker-embedding \
         worker-analyst worker-scraper worker-transparency worker-s3 openarg-frontend; do
  docker tag ghcr.io/colossus-lab/openarg/$s:$T ghcr.io/colossus-lab/openarg/$s:latest
done
docker compose up -d --no-deps $SVC
./scripts/verify_deploy.sh
```

**Si el cambio incluye migraciones de Alembic, esto no alcanza**: las corre
el contenedor `backend` al arrancar (`alembic upgrade head` en su `command:`)
y volver la imagen no las revierte. Verificar antes con
`git diff --stat main..staging -- src/app/infrastructure/persistence_sqla/alembic/`.

## El compose de los servidores

`docker-compose.prod.yml` de este repo describe lo que corre. Staging y prod
se diferencian sólo en dos variables:

```bash
# staging
OPENARG_STACK=staging OPENARG_IMAGE_TAG=staging docker compose -f docker-compose.prod.yml up -d
```

Hasta el 2026-09-28 cada servidor tenía su propia copia sin versionar, y el
archivo del repo se había quedado atrás en límites de memoria, concurrencia
del colector y la política de desalojo de Redis. Los servidores todavía usan
su copia local: alinearlos con este archivo es un paso aparte, que conviene
hacer con una ventana y el rollback a mano.
