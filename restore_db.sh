#!/bin/bash
set -euo pipefail

# ==== Config (edit paths/names if needed) ====
CONTAINER="mongo"  # name for the MongoDB container on THIS laptop
DATA_DIR="/home/kardinal/projects/cian_project_part2/db"  # fresh data dir on THIS laptop
BACKUP_ROOT="/home/kardinal/projects/cian_project_part2/loaded_backup"  # where dump_* folders live

# ==== Pick latest backup ====
LATEST_DUMP=$(ls -d "$BACKUP_ROOT"/dump_* 2>/dev/null | sort -V | tail -n1 || true)
if [[ -z "${LATEST_DUMP}" ]]; then
  echo "❌ No dump_* folder found in $BACKUP_ROOT"
  exit 1
fi
echo "🗂 Using backup: $LATEST_DUMP"

# ==== Sanity check before we rm -rf ====
if [[ -z "$DATA_DIR" || "$DATA_DIR" == "/" ]]; then
  echo "❌ Refusing to wipe dangerous DATA_DIR='$DATA_DIR'"
  exit 1
fi

# ==== Start fresh MongoDB ====
echo "🧹 Clearing old data dir via docker (handles permissions)..."
mkdir -p "$DATA_DIR"

docker run --rm \
  -v "$DATA_DIR:/data/db" \
  busybox sh -c 'rm -rf /data/db/* /data/db/.[!.]* /data/db/..?*'


# Remove any old container with same name
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "🧹 Removing old container $CONTAINER"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
fi

echo "🚀 Starting MongoDB container: $CONTAINER"
docker run -d \
  --name "$CONTAINER" \
  -p 27018:27017 \
  -v "$DATA_DIR:/data/db" \
  -v "${LATEST_DUMP}:/dump:ro" \
  mongo:latest

# ==== Wait for mongod to be ready ====
echo "⏳ Waiting for mongod (via mongosh ping)..."
READY=0
for i in {1..40}; do
  # Если контейнер умер — выходим и показываем логи
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "❌ Container $CONTAINER exited while starting mongod. Logs:"
    docker logs "$CONTAINER" || true
    exit 1
  fi

  # Пингуем через mongosh (mongo:8)
  if docker exec "$CONTAINER" mongosh --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; then
    READY=1
    echo "✅ mongod is ready"
    break
  fi

  sleep 0.5
done

if [[ "$READY" -ne 1 ]]; then
  echo "❌ mongod did not become ready in time (mongosh ping failed)"
  docker logs "$CONTAINER" || true
  exit 1
fi

# ==== Restore ====
echo "📥 Restoring from dump into fresh Mongo..."
docker exec "$CONTAINER" mongorestore --drop /dump

echo "✅ Restore complete. Mongo is running at mongodb://localhost:27018"
