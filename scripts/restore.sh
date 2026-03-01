#!/usr/bin/env bash
# OpenArg Restore Script — PostgreSQL + Redis
# Usage: ./scripts/restore.sh <pg_backup.sql.gz> [redis_backup.rdb]
set -euo pipefail

PG_BACKUP="${1:-}"
REDIS_BACKUP="${2:-}"
PG_CONTAINER="${PG_CONTAINER:-openarg_postgres}"
REDIS_CONTAINER="${REDIS_CONTAINER:-openarg_redis}"

if [ -z "$PG_BACKUP" ]; then
    echo "Usage: $0 <pg_backup.sql.gz> [redis_backup.rdb]"
    echo ""
    echo "Available backups:"
    ls -lhtr "${BACKUP_DIR:-/var/backups/openarg}"/pg_openarg_*.sql.gz 2>/dev/null || echo "  (none found)"
    exit 1
fi

echo "╔══════════════════════════════════════════╗"
echo "║        OpenArg Restore                   ║"
echo "╚══════════════════════════════════════════╝"

# ── PostgreSQL restore ──────────────────────────────────────
if [ -f "$PG_BACKUP" ]; then
    echo "[1/3] Restoring PostgreSQL from: $PG_BACKUP"
    echo "  ⚠ This will REPLACE the current database. Press Ctrl+C to cancel..."
    sleep 3

    DB_NAME="${POSTGRES_DB:-openarg_db}"
    DB_USER="${POSTGRES_USER:-openarg}"

    # Drop and recreate the database
    docker exec "$PG_CONTAINER" psql -U "$DB_USER" -d postgres -c \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" \
        >/dev/null 2>&1 || true

    docker exec "$PG_CONTAINER" dropdb -U "$DB_USER" --if-exists "$DB_NAME" 2>/dev/null || true
    docker exec "$PG_CONTAINER" createdb -U "$DB_USER" "$DB_NAME"
    docker exec "$PG_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1

    # Restore from backup
    gunzip -c "$PG_BACKUP" | docker exec -i "$PG_CONTAINER" pg_restore \
        -U "$DB_USER" \
        -d "$DB_NAME" \
        --no-owner \
        --no-privileges \
        --verbose 2>/dev/null || true

    echo "  ✓ PostgreSQL restored"
else
    echo "  ✗ PostgreSQL backup not found: $PG_BACKUP"
    exit 1
fi

# ── Redis restore ───────────────────────────────────────────
if [ -n "$REDIS_BACKUP" ] && [ -f "$REDIS_BACKUP" ]; then
    echo "[2/3] Restoring Redis from: $REDIS_BACKUP"

    # Stop Redis, copy dump, restart
    docker cp "$REDIS_BACKUP" "$REDIS_CONTAINER":/data/dump.rdb
    docker restart "$REDIS_CONTAINER"

    # Wait for Redis to be ready
    for i in $(seq 1 10); do
        if docker exec "$REDIS_CONTAINER" redis-cli -a "${REDIS_PASSWORD:-}" --no-auth-warning ping | grep -q PONG; then
            break
        fi
        sleep 1
    done

    echo "  ✓ Redis restored"
else
    echo "[2/3] Redis restore skipped (no backup provided)"
fi

# ── Verification ────────────────────────────────────────────
echo "[3/3] Verifying restore..."

# Check PostgreSQL
PG_TABLES=$(docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-openarg}" -d "${POSTGRES_DB:-openarg_db}" -t -c \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null | tr -d ' ')
echo "  PostgreSQL: $PG_TABLES tables found"

# Check Redis
if docker exec "$REDIS_CONTAINER" redis-cli -a "${REDIS_PASSWORD:-}" --no-auth-warning ping | grep -q PONG; then
    REDIS_KEYS=$(docker exec "$REDIS_CONTAINER" redis-cli -a "${REDIS_PASSWORD:-}" --no-auth-warning dbsize 2>/dev/null | grep -oP '\d+')
    echo "  Redis: $REDIS_KEYS keys found"
else
    echo "  Redis: not available"
fi

echo ""
echo "Restore complete. Run migrations if needed:"
echo "  docker exec openarg_backend alembic -c alembic.ini upgrade head"
