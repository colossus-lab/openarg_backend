#!/usr/bin/env bash
# Deja en el servidor sólo los últimos juegos de imágenes de rollback.
#
# Cada deploy congela las imágenes que estaban corriendo con un alias
# `rollback-<fecha>[sufijo]` (docs/deploy-produccion.md §1): unos 9 GB por
# juego. Los timers que ya había (`docker system prune -af --filter
# until=72h`) borran por antigüedad de la imagen, y con varios deploys por día
# todos los juegos son "recientes": el 10-oct-2026 staging llegó al 98 % de
# disco con 13 juegos en tres días y el deploy falló con "no space left on
# device". Esto limita por cantidad.
#
# - Ordena los alias por nombre, que codifica el momento del deploy
#   (20261009p < 20261009q < 20261010p; 20261010s < 20261010s2).
# - Borra los alias, no las imágenes: `docker rmi` sin `-f` se niega si la
#   imagen la usa un contenedor y le queda sólo ese alias, así que nunca toca lo
#   que está corriendo. Después borra las capas que quedaron sueltas.
# - Todas las versiones siguen en el registry como `:sha-<7>`.
#
# Uso:  limpiar_rollbacks.sh            (deja 2)
#       CONSERVAR=3 limpiar_rollbacks.sh
#       DRY_RUN=1 limpiar_rollbacks.sh  (sólo dice qué borraría)

set -uo pipefail

CONSERVAR="${CONSERVAR:-2}"
DRY_RUN="${DRY_RUN:-0}"
REPO="ghcr.io/colossus-lab/openarg/"

alias_todos=$(docker images --format '{{.Repository}} {{.Tag}}' \
    | awk -v r="$REPO" 'index($1, r) == 1 && $2 ~ /^rollback-/ {print $2}' | sort -u)
cantidad=$(printf '%s\n' "$alias_todos" | grep -c . || true)

if [ "$cantidad" -le "$CONSERVAR" ]; then
    echo "hay $cantidad juego(s) de rollback, se conservan hasta $CONSERVAR: nada que borrar"
    exit 0
fi

viejos=$(printf '%s\n' "$alias_todos" | head -n "$((cantidad - CONSERVAR))")
echo "se conservan: $(printf '%s\n' "$alias_todos" | tail -n "$CONSERVAR" | tr '\n' ' ')"
echo "se borran:    $(printf '%s\n' "$viejos" | tr '\n' ' ')"
echo "disco antes:  $(df -h / | awk 'NR==2 {print $5 " usado, " $4 " libres"}')"

[ "$DRY_RUN" = "1" ] && exit 0

for t in $viejos; do
    for img in $(docker images --format '{{.Repository}}:{{.Tag}}' | awk -v t=":$t" 'substr($0, length($0) - length(t) + 1) == t'); do
        docker rmi "$img" > /dev/null || echo "no se borró $img (en uso)"
    done
done
docker image prune -f > /dev/null
echo "disco después: $(df -h / | awk 'NR==2 {print $5 " usado, " $4 " libres"}')"
