#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "[orchestrator] Preparing secrets..."
"$SCRIPT_DIR/prepare_secrets.sh"

echo "[orchestrator] Starting stack..."
"$SCRIPT_DIR/start_stack.sh"

echo "[orchestrator] Verifying Redis and config..."
"$SCRIPT_DIR/verify_redis.sh"

echo "[orchestrator] Running configuration audit..."
"$SCRIPT_DIR/run_config_audit.sh"

echo "[orchestrator] All steps completed."




