#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

# Resolve directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SECRETS_DIR="$BACKEND_DIR/secrets"

mkdir -p "$SECRETS_DIR"

echo "[prepare-secrets] Using secrets directory: $SECRETS_DIR"

# 1) Redis password secret
if [[ ! -s "$SECRETS_DIR/redis_password.txt" ]]; then
  echo "[prepare-secrets] Creating redis_password.txt"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 32 > "$SECRETS_DIR/redis_password.txt"
  else
    # Fallback if openssl is not present
    head -c 32 /dev/urandom | base64 > "$SECRETS_DIR/redis_password.txt"
  fi
  chmod 600 "$SECRETS_DIR/redis_password.txt"
else
  echo "[prepare-secrets] redis_password.txt already exists; leaving as-is"
fi

# 2) Slack webhook URL for Alertmanager
if [[ ! -s "$SECRETS_DIR/slack_webhook_url" ]]; then
  if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
    echo "[prepare-secrets] Writing Slack webhook URL from environment"
    printf '%s' "$SLACK_WEBHOOK_URL" > "$SECRETS_DIR/slack_webhook_url"
  else
    echo "[prepare-secrets] WARNING: SLACK_WEBHOOK_URL not provided; writing placeholder. Update this file for real alerts."
    printf '%s' 'https://hooks.slack.com/services/PLACEHOLDER' > "$SECRETS_DIR/slack_webhook_url"
  fi
  chmod 600 "$SECRETS_DIR/slack_webhook_url"
else
  echo "[prepare-secrets] slack_webhook_url already exists; leaving as-is"
fi

# 3) Grafana admin password
if [[ ! -s "$SECRETS_DIR/grafana_admin_password" ]]; then
  echo "[prepare-secrets] Creating grafana_admin_password"
  if [[ -n "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
    printf '%s' "$GRAFANA_ADMIN_PASSWORD" > "$SECRETS_DIR/grafana_admin_password"
  else
    if command -v openssl >/dev/null 2>&1; then
      openssl rand -base64 24 > "$SECRETS_DIR/grafana_admin_password"
    else
      head -c 24 /dev/urandom | base64 > "$SECRETS_DIR/grafana_admin_password"
    fi
  fi
  chmod 600 "$SECRETS_DIR/grafana_admin_password"
else
  echo "[prepare-secrets] grafana_admin_password already exists; leaving as-is"
fi

echo "[prepare-secrets] Done."


