#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$BACKEND_DIR"

echo "[start-stack] Preparing secrets..."
"$SCRIPT_DIR/prepare_secrets.sh"

echo "[start-stack] Starting monitoring stack via docker compose..."
docker compose -f docker-compose.monitoring.yml up -d

echo "[start-stack] Stack status:"
docker compose -f docker-compose.monitoring.yml ps | cat

echo "[start-stack] Waiting briefly for services to become healthy..."
sleep 5

echo "[start-stack] Done."


