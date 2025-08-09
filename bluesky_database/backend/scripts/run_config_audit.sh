#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CONDA_ENV_NAME="${CONDA_ENV_NAME:-bluesky-research}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD_FILE="${REDIS_PASSWORD_FILE:-$BACKEND_DIR/secrets/redis_password.txt}"

if [[ ! -s "$REDIS_PASSWORD_FILE" ]]; then
  echo "[run-config-audit] ERROR: Redis password file not found: $REDIS_PASSWORD_FILE" >&2
  exit 1
fi

export REDIS_HOST REDIS_PORT REDIS_PASSWORD="$(cat "$REDIS_PASSWORD_FILE")"

echo "[run-config-audit] Ensuring python-redis is available in conda env '$CONDA_ENV_NAME'..."
if ! conda run -n "$CONDA_ENV_NAME" python -c 'import redis, sys; print(redis.__version__)' >/dev/null 2>&1; then
  conda run -n "$CONDA_ENV_NAME" pip install -q 'redis>=5'
fi

echo "[run-config-audit] Running audit script..."
conda run -n "$CONDA_ENV_NAME" python "$BACKEND_DIR/redis_testing/01_configuration_audit.py"

echo "[run-config-audit] Done."


