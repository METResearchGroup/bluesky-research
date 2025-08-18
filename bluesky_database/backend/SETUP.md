# Redis Monitoring Stack Setup

## Overview

This guide automates bringing up the Redis monitoring stack (Redis, Redis Exporter, Prometheus, Alertmanager, Grafana), verifies Redis connectivity and configuration, and runs the configuration audit test. It references the existing docs for deeper details and provides reusable scripts under `scripts/`.

## Prerequisites

- Docker and Docker Compose
- Conda environment `bluesky-research` available
- Open local ports: 6379, 9121, 9090, 9093, 3000

## One-Command Setup

From `bluesky_database/backend`:

```bash
./scripts/setup_and_verify.sh
```

This will:
- Prepare secrets (Redis password, Slack webhook, Grafana admin password)
- Start the monitoring stack via Docker Compose
- Verify Redis connectivity and key configs
- Run the configuration audit `01_configuration_audit.py`

## Script Reference

- `scripts/prepare_secrets.sh`:
  - Creates `secrets/redis_password.txt` if missing
  - Populates `secrets/slack_webhook_url` (from `$SLACK_WEBHOOK_URL` or placeholder)
  - Creates `secrets/grafana_admin_password` (from `$GRAFANA_ADMIN_PASSWORD` or random)

- `scripts/start_stack.sh`:
  - Starts services with `docker compose -f docker-compose.monitoring.yml up -d`
  - Shows container status

- `scripts/verify_redis.sh`:
  - Pings Redis using `redis-cli` and the password secret
  - Checks `CONFIG GET databases` and `CONFIG GET save`
  - Prints `INFO memory`

- `scripts/run_config_audit.sh`:
  - Ensures `redis` Python package in conda env
  - Runs `redis_testing/01_configuration_audit.py` with env `REDIS_HOST/PORT/PASSWORD`

- `scripts/setup_and_verify.sh`:
  - Orchestrates all the above in order

## Key Configuration Files

- `redis.conf`: Optimized for buffer use case; we adjusted:
  - `databases 1`
  - `save ""` (disable RDB; AOF-only)
  - `maxmemory 2gb`, `maxmemory-policy allkeys-lru`, `appendonly yes`, `appendfsync everysec`

- `docker-compose.monitoring.yml`: Orchestrates Redis, Exporter, Prometheus, Alertmanager, Grafana. Uses Docker secrets for credentials.

## Validation and Tests

- Configuration audit: `redis_testing/01_configuration_audit.py`
  - Run directly: `./scripts/run_config_audit.sh`
- Full test suite for monitoring: see `redis_testing/README_MONITORING.md`

## Slack Integration

- Follow `SLACK_SETUP.md` to configure the webhook secret and Alertmanager receiver.
- The orchestrator script will still run without a real webhook (placeholder), but no Slack messages will be delivered.

## Related Docs

- `REDIS_SETUP.md`: End-to-end Redis and monitoring stack setup details
- `redis_testing/README_MONITORING.md`: Monitoring architecture and validation guidance
- `SLACK_SETUP.md`: Consolidated Slack requirements and setup

## Common Commands

```bash
# Start stack
./scripts/start_stack.sh

# Verify Redis
./scripts/verify_redis.sh

# Run configuration audit
./scripts/run_config_audit.sh

# Stop stack
docker compose -f docker-compose.monitoring.yml down
```

## Notes

- Secrets are stored in `backend/secrets/` and should not be committed.
- For production hardening, see notes in `REDIS_SETUP.md` and `redis_testing/README_MONITORING.md`.
