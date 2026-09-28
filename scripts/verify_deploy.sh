#!/usr/bin/env bash
# Verifica que un deploy haya llegado a TODOS los contenedores, no sólo a los
# que uno recreó.
#
# El 2026-09-02 el deploy quedó a medias: la API y el frontend corrían las
# imágenes nuevas y los 9 workers + beat seguían con imágenes de una semana
# antes. Reiniciar un contenedor no cambia su imagen, así que `docker ps` los
# mostraba "Up" y todo parecía bien. El barrido de las 06:00 UTC reconstruyó un
# mart con la definición vieja y revirtió el dato — el chat contestó 512
# diputados donde hay 256 durante 14 horas.
#
# Compara dos cosas, porque una sola no alcanza:
#   1. El image ID que cada contenedor está corriendo contra el ID al que apunta
#      hoy su tag. Detecta el contenedor que nunca se recreó.
#   2. La huella de un archivo de código en todos los contenedores de la app.
#      Detecta el caso contrario — imágenes distintas construidas de fuentes
#      distintas, que el ID no puede comparar entre sí porque cada worker tiene
#      su propio Dockerfile.
#
# Uso:  ./verify_deploy.sh [archivo-testigo]
# Sale con código != 0 si encuentra deriva, así que sirve de gate.

set -uo pipefail

TESTIGO="${1:-/app/src/app/application/marts/sql_macros.py}"
fallas=0

echo "── imagen corriendo vs tag ──"
for c in $(docker ps --format '{{.Names}}' | grep '^openarg_' | sort); do
    tag=$(docker inspect -f '{{.Config.Image}}' "$c")
    corriendo=$(docker inspect -f '{{.Image}}' "$c")
    apunta=$(docker inspect -f '{{.Id}}' "$tag" 2>/dev/null || echo "")

    if [ -z "$apunta" ]; then
        printf '  %-6s %-38s (tag %s no existe local)\n' '?' "$c" "$tag"
    elif [ "$corriendo" = "$apunta" ]; then
        printf '  %-6s %-38s %s\n' 'ok' "$c" "${corriendo:7:12}"
    else
        printf '  %-6s %-38s corriendo=%s  tag=%s\n' \
            'DERIVA' "$c" "${corriendo:7:12}" "${apunta:7:12}"
        fallas=$((fallas + 1))
    fi
done

echo
echo "── huella de $TESTIGO en los contenedores de la app ──"
# Sólo los que llevan el código de la app. caddy/redis/pgbouncer no lo tienen.
declare -A huellas
for c in $(docker ps --format '{{.Names}}' \
           | grep -E '^openarg_(backend|worker_|beat)' | sort); do
    h=$(docker exec "$c" md5sum "$TESTIGO" 2>/dev/null | cut -c1-12)
    if [ -z "$h" ]; then
        printf '  %-6s %-38s (sin el archivo)\n' '?' "$c"
        continue
    fi
    printf '  %-6s %-38s %s\n' '' "$c" "$h"
    huellas["$h"]=1
done

distintas=${#huellas[@]}
if [ "$distintas" -gt 1 ]; then
    echo
    echo "  DERIVA: $distintas huellas distintas — no todos corren el mismo código."
    fallas=$((fallas + 1))
fi

echo
if [ "$fallas" -eq 0 ]; then
    echo "Sin deriva."
else
    echo "$fallas problema(s). Un contenedor con código viejo puede revertir datos"
    echo "en el próximo barrido agendado, no sólo servir respuestas viejas."
fi
exit "$fallas"
