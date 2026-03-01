#!/usr/bin/env bash
# OpenArg Backup Script — PostgreSQL + Redis
# Usage: ./scripts/backup.sh [BACKUP_DIR]
set -euo pipefail

# ── Configuration (overridable via env vars) ────────────────
BACKUP_DIR="${1:-${BACKUP_DIR:-/var/backups/openarg}}"
PG_CONTAINER="${PG_CONTAINER:-openarg_postgres}"
REDIS_CONTAINER="${REDIS_CONTAINER:-openarg_redis}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "╔══════════════════════════════════════════╗"
echo "║        OpenArg Backup — $TIMESTAMP       ║"
echo "╚══════════════════════════════════════════╝"

mkdir -p "$BACKUP_DIR"

# ── PostgreSQL backup ───────────────────────────────────────
PG_BACKUP="$BACKUP_DIR/pg_openarg_${TIMESTAMP}.sql.gz"

echo "[1/3] Backing up PostgreSQL..."
docker exec "$PG_CONTAINER" pg_dump \
    -U "${POSTGRES_USER:-openarg}" \
    -d "${POSTGRES_DB:-openarg_db}" \
    --format=custom \
    --compress=6 \
    --verbose 2>/dev/null \
    | gzip > "$PG_BACKUP"

PG_SIZE=$(du -h "$PG_BACKUP" | cut -f1)
echo "  ✓ PostgreSQL backup: $PG_BACKUP ($PG_SIZE)"

# ── Redis backup ───────────────────────────────────────────
REDIS_BACKUP="$BACKUP_DIR/redis_openarg_${TIMESTAMP}.rdb"

echo "[2/3] Backing up Redis..."
docker exec "$REDIS_CONTAINER" redis-cli \
    -a "${REDIS_PASSWORD:-}" --no-auth-warning BGSAVE >/dev/null 2>&1
# Wait for BGSAVE to complete
sleep 2
docker cp "$REDIS_CONTAINER":/data/dump.rdb "$REDIS_BACKUP" 2>/dev/null || {
    echo "  ⚠ Redis backup skipped (no dump.rdb found)"
    REDIS_BACKUP=""
}

if [ -n "$REDIS_BACKUP" ] && [ -f "$REDIS_BACKUP" ]; then
    REDIS_SIZE=$(du -h "$REDIS_BACKUP" | cut -f1)
    echo "  ✓ Redis backup: $REDIS_BACKUP ($REDIS_SIZE)"
fi

# ── Rotation: remove backups older than RETENTION_DAYS ──────
echo "[3/3] Rotating old backups (keeping last ${RETENTION_DAYS} days)..."
DELETED=$(find "$BACKUP_DIR" -name "pg_openarg_*.sql.gz" -mtime +"$RETENTION_DAYS" -delete -print | wc -l)
DELETED=$((DELETED + $(find "$BACKUP_DIR" -name "redis_openarg_*.rdb" -mtime +"$RETENTION_DAYS" -delete -print | wc -l)))
echo "  ✓ Deleted $DELETED old backup file(s)"

# ── Summary ─────────────────────────────────────────────────
echo ""
echo "Backup complete:"
echo "  PostgreSQL: $PG_BACKUP"
[ -n "$REDIS_BACKUP" ] && echo "  Redis:      $REDIS_BACKUP"
echo "  Retention:  ${RETENTION_DAYS} days"
echo ""
echo "Restore with: ./scripts/restore.sh $PG_BACKUP [$REDIS_BACKUP]"
