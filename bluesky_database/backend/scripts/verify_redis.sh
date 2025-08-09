#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SECRETS_DIR="$BACKEND_DIR/secrets"

REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD_FILE="${REDIS_PASSWORD_FILE:-$SECRETS_DIR/redis_password.txt}"

if [[ ! -s "$REDIS_PASSWORD_FILE" ]]; then
  echo "[verify-redis] ERROR: Redis password file not found: $REDIS_PASSWORD_FILE" >&2
  exit 1
fi

REDIS_PASSWORD="$(cat "$REDIS_PASSWORD_FILE")"

echo "[verify-redis] Pinging Redis at $REDIS_HOST:$REDIS_PORT ..."
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" ping | cat

echo "[verify-redis] Checking a few key configs..."
docker compose -f "$BACKEND_DIR/docker-compose.monitoring.yml" exec -T redis sh -lc \
  'redis-cli -a "$(cat /run/secrets/redis_password)" CONFIG GET databases && \
   redis-cli -a "$(cat /run/secrets/redis_password)" CONFIG GET save && \
   redis-cli -a "$(cat /run/secrets/redis_password)" INFO memory' | cat

echo "[verify-redis] Done."


