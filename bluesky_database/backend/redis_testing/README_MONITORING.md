# Redis Monitoring Stack - Prometheus + Grafana + Slack

## Overview

This directory contains the Prometheus + Grafana + Slack monitoring stack for the Redis optimization project. The monitoring stack provides real-time visibility into Redis performance, memory usage, and operational metrics, with proactive Slack alerting for critical events.

### Testing details

- **Tests**:
  - `07_monitoring_validation.py` — container health, Redis connectivity, exporter metrics, Prometheus targets, Alertmanager, Grafana dashboards.
  - `02_baseline_performance.py` — baseline ops/sec under monitoring.
  - `03_buffer_capacity_test.py` — memory usage and exporter metrics during load.
- **What’s covered**:
  - Service availability, metrics exposure, alert routing, dashboard provisioning.
  - Error cases: service down, missing metrics, auth failures (Grafana/Redis).

## Architecture

The monitoring stack consists of five main components:

1. **Redis** - The target database being monitored
2. **Redis Exporter** - Collects Redis metrics and exposes them in Prometheus format
3. **Prometheus** - Time-series database that scrapes and stores metrics
4. **Alertmanager** - Handles alert routing and Slack integration
5. **Grafana** - Visualization platform that displays metrics in dashboards

## Services

### Redis (bluesky_redis)

- **Port**: 6379
- **Configuration**: Optimized for buffer use case with 2GB memory limit
- **Persistence**: AOF enabled with appendfsync everysec
- **Eviction**: allkeys-lru policy

### Redis Exporter

- **Port**: 9121
- **Purpose**: Exposes Redis metrics in Prometheus format
- **Metrics**: Commands, memory, clients, keyspace, etc.

### Prometheus

- **Port**: 9090
- **Purpose**: Scrapes and stores time-series metrics
- **Retention**: 200 hours
- **Targets**: Redis Exporter, self-monitoring

### Alertmanager

- **Port**: 9093
- **Purpose**: Handles alert routing and Slack integration
- **Configuration**: Slack webhook integration for critical alerts
- **Features**: Alert grouping, routing, and escalation

### Grafana

- **Port**: 3000
- **Credentials**: admin/admin
- **Purpose**: Visualize metrics in dashboards
- **Datasource**: Prometheus (auto-configured)

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Ports 6379, 9121, 9090, 9093, and 3000 available
- Slack webhook URL configured (optional for basic setup)

### Start the Monitoring Stack

```bash
cd bluesky_database/backend

# Start all services
docker compose -f docker-compose.monitoring.yml up -d

# Check service status
docker compose -f docker-compose.monitoring.yml ps
```

### Access the Services

- **Redis**: `redis-cli -h 127.0.0.1 -p 6379 -a "$(cat ../secrets/redis_password.txt)"`
- **Redis Exporter**: <http://localhost:9121/metrics>
- **Prometheus**: <http://localhost:9090>
- **Alertmanager**: <http://localhost:9093>
- **Grafana**: <http://localhost:3000> (admin/admin)

#### Health Check (non-destructive)

```bash
# Expect "PONG" if Redis is healthy
redis-cli -h 127.0.0.1 -p 6379 -a "$(cat ../secrets/redis_password.txt)" PING
```

### Stop the Stack

```bash
cd bluesky_database/backend
docker compose -f docker-compose.monitoring.yml down
```

## Validation

Run the validation script to ensure all components are working correctly:

```bash
cd bluesky_database/backend/redis_testing
python 07_monitoring_validation.py
```

The validation script checks:

- Docker Compose startup
- Redis connectivity
- Redis Exporter metrics
- Prometheus targets and metrics
- Alertmanager connectivity and Slack integration
- Grafana connectivity and dashboard

## Configuration Files

### docker-compose.monitoring.yml

Main orchestration file defining all services, volumes, and networking.

### prometheus.yml

Prometheus configuration with scrape targets, intervals, and alerting rules.

### redis.conf

Optimized Redis configuration for the buffer use case.

### grafana/provisioning/

- **datasources/prometheus.yml**: Auto-configures Prometheus datasource
- **dashboards/dashboard.yml**: Dashboard provisioning configuration
- **dashboards/redis-dashboard.json**: Redis monitoring dashboard

### alertmanager/

- **alertmanager.yml**: Alertmanager configuration with Slack integration
- **templates/**: Custom message templates for Slack alerts

## Dashboard Metrics

The Redis dashboard includes the following key metrics:

### Performance Metrics

- **Commands per Second**: Rate of Redis commands processed
- **Connected Clients**: Number of active client connections
- **Total Keys**: Number of keys in the database

### Memory Metrics

- **Memory Usage**: Current memory consumption in bytes
- **Memory Limit**: Configured memory limit (2GB)
- **Memory Efficiency**: Memory usage patterns

### Operational Metrics

- **Redis Up**: Service availability
- **Command Latency**: Response times for operations
- **Error Rates**: Failed operations and exceptions

## Monitoring Alerts

The monitoring stack provides visibility and proactive alerting for:

### Performance Alerts (Slack Notifications)

- High command latency (> 10ms)
- Low throughput (< 1000 ops/sec)
- Memory pressure (> 90% utilization)
- Buffer overflow detection

### Operational Alerts (Slack Notifications)

- Redis service down
- High error rates
- Connection failures
- Alertmanager service down

## Integration with Existing Tests

The monitoring stack integrates with the existing Redis optimization test suite:

- **Phase 1-3 Tests**: Use the same Redis instance for validation
- **Performance Metrics**: Real-time visibility during load testing
- **Memory Monitoring**: Track memory usage during buffer capacity tests
- **Validation Scripts**: Automated testing of monitoring components

  - Validation scripts:
    - [01_configuration_audit.py](./01_configuration_audit.py)
    - [02_baseline_performance.py](./02_baseline_performance.py)
    - [03_buffer_capacity_test.py](./03_buffer_capacity_test.py)
    - [04_memory_pressure_test.py](./04_memory_pressure_test.py)
    - [05_persistence_recovery_test.py](./05_persistence_recovery_test.py)
    - [06_throughput_validation.py](./06_throughput_validation.py)
    - [07_monitoring_validation.py](./07_monitoring_validation.py)

## Troubleshooting

### Common Issues

1. **Port Conflicts**

   ```bash
   # Check for port conflicts
   ss -tulpn | grep -E ':(6379|9121|9090|9093|3000)' || \
   netstat -tulpn | grep -E ':(6379|9121|9090|9093|3000)'
   ```

2. **Container Startup Issues**

   ```bash
   # Check container logs
   cd bluesky_database/backend
   docker compose -f docker-compose.monitoring.yml logs [service_name]
   ```

3. **Metrics Not Appearing**

   ```bash
   # Check Redis Exporter metrics
   curl http://localhost:9121/metrics | grep redis_up

   # Check Prometheus targets
   curl http://localhost:9090/api/v1/targets
   ```

4. **Grafana Dashboard Issues**

   ```bash
   # Check datasource configuration
   curl -sSf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" http://localhost:3000/api/datasources
   ```

   Note: Avoid hardcoding credentials in commands. Prefer using environment variables or a credential manager, e.g.:

   ```bash
   curl -sSf -u "$GRAFANA_USER:$GRAFANA_PASSWORD" http://localhost:3000/api/datasources
   ```

### Log Locations

- **Redis**: `docker logs bluesky_redis`
- **Redis Exporter**: `docker logs redis_exporter`
- **Prometheus**: `docker logs prometheus`
- **Grafana**: `docker logs grafana`

## Performance Considerations

### Resource Usage

- **Redis**: 2GB memory limit (configurable)
- **Prometheus**: ~100MB RAM, grows with metrics retention
- **Alertmanager**: ~20MB RAM
- **Grafana**: ~50MB RAM
- **Redis Exporter**: ~10MB RAM

### Scaling Considerations

- **Metrics Retention**: 200 hours by default
- **Scrape Intervals**: 10s for Redis, 15s for Prometheus
- **Dashboard Refresh**: 5s default

## Security Notes

For secure defaults (localhost binding and auth via Docker secrets), see
[Security Posture (MET-27)](../REDIS_SETUP.md#security-posture-met-27).

### Default Credentials

- **Grafana**: admin/admin (change in production; set via `GF_SECURITY_ADMIN_PASSWORD` or Docker secrets, and disable signup)
- **Redis**: Authentication enabled via Docker secrets (change and rotate in production)
- **Alertmanager**: No authentication (configure for production)

### Network Access

- **Redis** binds to 127.0.0.1 (localhost-only). Other services (Prometheus, Alertmanager, Grafana, Redis Exporter) bind to 0.0.0.0 by default for local access.
- For production, restrict service exposure:
  - Place services behind a reverse proxy with auth (e.g., Nginx, Traefik)
  - Bind admin surfaces (Prometheus, Alertmanager, Grafana) to private interfaces
  - Enforce network policies/firewall rules; expose only required ports
  - Use long, rotated secrets for Redis and Grafana; disable default credentials
- Slack webhook requires outbound HTTPS access

## Production Deployment

For production deployment, consider:

1. **Security**
   - Change default passwords
   - Enable Redis authentication
   - Use reverse proxy for external access
   - Secure Slack webhook URLs

2. **Persistence**
   - Configure volume mounts for data persistence
   - Set up backup strategies

3. **Monitoring**
   - Configure alerting rules
   - Set up log aggregation
   - Monitor the monitoring stack itself
   - Configure Slack alert escalation policies

4. **Scaling**
   - Use Redis clustering for high availability
   - Configure Prometheus federation for multiple instances
   - Set up Grafana high availability
   - Configure Alertmanager clustering for high availability

## Related Documentation

- [Redis Optimization Plan](REDIS_OPTIMIZATION_PLAN.md)
- [Progress Notes](PROGRESS_NOTES.md)
- Validation scripts:
  - [01_configuration_audit.py](./01_configuration_audit.py)
  - [02_baseline_performance.py](./02_baseline_performance.py)
  - [03_buffer_capacity_test.py](./03_buffer_capacity_test.py)
  - [04_memory_pressure_test.py](./04_memory_pressure_test.py)
  - [05_persistence_recovery_test.py](./05_persistence_recovery_test.py)
  - [06_throughput_validation.py](./06_throughput_validation.py)
  - [07_monitoring_validation.py](./07_monitoring_validation.py)

## Support

For issues with the monitoring stack:

1. Check the troubleshooting section above
2. Review container logs for error messages
3. Run the validation script to identify specific issues
4. Check the progress notes for known issues and solutions
5. See validation scripts listed above for targeted checks

---

**Last Updated**: 2025-08-07
**Version**: 1.1
**Status**: MVP Complete (Slack Integration Pending)
