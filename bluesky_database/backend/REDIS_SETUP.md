# Redis Setup (Secure, Local-First)

This guide provides end-to-end instructions to run Redis for the monitoring stack (Redis + Redis Exporter + Prometheus + Alertmanager + Grafana) with secure defaults: restricted host exposure, password authentication via Docker secrets, and no hardcoded credentials.

## Overview
- Redis runs in Docker with authentication enabled.
- The Redis password is sourced from a Docker secret file, not hardcoded.
- Host exposure is restricted by binding the published port to localhost. You may remove the port mapping entirely to keep Redis internal to the Docker network.

## Prerequisites
- Docker and Docker Compose (Compose V2 is recommended; commands below use `docker compose`)
- Open local ports as needed: 9121 (redis-exporter), 9090 (Prometheus), 9093 (Alertmanager), 3000 (Grafana). Redis (6379) will bind to localhost or can be left internal-only.

## Directory Layout
```
bluesky_database/backend/
  docker-compose.monitoring.yml
  redis.conf
  secrets/
    redis_password.txt   # you create this file
```

## 1) Create the Redis password secret
Create the `secrets` directory and a strong password file that will be mounted as a Docker secret.

```bash
mkdir -p secrets
openssl rand -base64 32 > secrets/redis_password.txt
chmod 600 secrets/redis_password.txt
```

Notes:
- Do not commit `secrets/redis_password.txt` to source control.
- Consider adding a `.gitignore` in `backend/secrets/` with `redis_password.txt` to prevent accidental commits.

## 2) Configuration details

### redis.conf
- Network binding is set to `127.0.0.1` to illustrate secure defaults:
  ```
  bind 127.0.0.1
  ```
- Authentication is not hardcoded in the file. Instead, it is supplied at runtime via Docker secrets. The config includes commented placeholders:
  ```
  # requirepass <password-from-docker-secret>
  # masterauth <password-from-docker-secret>
  ```

### docker-compose.monitoring.yml
- Redis host exposure is restricted to localhost by publishing the container port on the host as `127.0.0.1:6379:6379`. To make Redis internal-only, remove the `ports:` mapping entirely.
- Authentication is enabled by passing `--requirepass` from the mounted secret: `/run/secrets/redis_password`.
- The `redis-exporter` reads the same secret via `--redis.password-file=/run/secrets/redis_password`.
- For inter-container connectivity (exporter → redis) within Docker, the service starts Redis with `--bind 0.0.0.0`. Host-level exposure remains restricted via the localhost port binding.

Why `--bind 0.0.0.0` at runtime? Binding to `127.0.0.1` inside the container would prevent other containers (like `redis-exporter`) from reaching Redis over the Docker network. We restrict host exposure using the published ports mapping instead of limiting container-level interfaces. If you do not need `redis-exporter` or any other container to access Redis, you may remove the runtime override and keep the `bind 127.0.0.1` in effect.

## 3) Start the monitoring stack
From `bluesky_database/backend`:

```bash
docker compose -f docker-compose.monitoring.yml up -d
# or, for older Compose
# docker-compose -f docker-compose.monitoring.yml up -d
```

Check service status:
```bash
docker compose -f docker-compose.monitoring.yml ps
```

## 4) Access and validate
- Redis (host):
  - If port is published: `redis-cli -h 127.0.0.1 -p 6379 -a "$(cat secrets/redis_password.txt)" ping`
  - If the port mapping is removed, exec into the container instead:
    ```bash
    docker exec -it bluesky_redis sh -lc 'redis-cli -a "$(cat /run/secrets/redis_password)" ping'
    ```
- Redis Exporter: `http://localhost:9121/metrics`
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- Grafana: `http://localhost:3000`

## 5) Secure usage notes
- Use placeholders or environment variables for credentials in commands. Example for Grafana API calls:
  ```bash
  curl -u "$GRAFANA_USER:$GRAFANA_PASSWORD" http://localhost:3000/api/datasources
  ```
- Do not hardcode passwords in config files or docs.
- Prefer secrets files and least-privilege network exposure.

## 6) Troubleshooting
- Check logs:
  ```bash
  docker compose -f docker-compose.monitoring.yml logs redis
  docker compose -f docker-compose.monitoring.yml logs redis-exporter
  docker compose -f docker-compose.monitoring.yml logs prometheus
  docker compose -f docker-compose.monitoring.yml logs alertmanager
  docker compose -f docker-compose.monitoring.yml logs grafana
  ```
- Verify exporter can authenticate:
  ```bash
  curl -s http://localhost:9121/metrics | grep '^redis_up'
  ```
- Verify Prometheus targets:
  ```bash
  curl http://localhost:9090/api/v1/targets | jq .
  ```

## 7) Hardening options
- Remove the Redis `ports:` mapping to keep Redis accessible only to other containers on the Docker network.
- Change Grafana admin credentials via environment variables before first run.
- Add firewall rules to allow access only from trusted IPs if running outside localhost.
- Rotate the password in `secrets/redis_password.txt` periodically and restart services.

## 8) Cleanup
```bash
docker compose -f docker-compose.monitoring.yml down
```

This setup balances local development convenience with secure defaults (localhost host binding, password auth via secrets, and no hardcoded secrets). Adjust exposure and credentials as required for your environment.
