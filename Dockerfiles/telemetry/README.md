# Telemetry-related Dockerfiles

Related to spinning up Prometheus + Grafana.

Files:

- `docker-compose.yml`: spins up a local Prometheus and Grafana instance.
- `prometheus.yml`: Scrapes the Python services, exposing /metrics on port 8000.
- `grafana-dashboard.json`: Starter Grafana dashboard with basic metrics.
- `Makefile`: Makefile with steps for running and executing Docker.
