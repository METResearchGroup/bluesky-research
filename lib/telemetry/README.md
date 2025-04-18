# Telemetry Tools

This package provides telemetry and metrics collection tools for monitoring application performance and resource usage.

## Overview

The telemetry package includes:
- Base metrics collection framework using OpenTelemetry
- DuckDB-specific metrics collection
- Prometheus-based monitoring for real-time dashboards
- Grafana visualizations for backfill jobs
- CometML integration for ML experiment tracking
- Extensible design for adding new metrics collectors

## Components

### metrics.py
Base metrics collection functionality:
- Execution time
- Memory usage
- CPU utilization
- I/O operations
- Custom metric support

### duckdb_metrics.py
DuckDB-specific metrics:
- Data size
- Row count
- Query execution time
- Query profiling
- Memory usage during queries

### prometheus.py
Prometheus metrics for job monitoring:
- Request rates and response codes
- Queue sizes
- Processing times
- Rate limit hits
- Retry counts
- Batch sizes
- Backfill progress tracking

### grafana.py
Grafana dashboard utilities:
- API client for Grafana integration
- Dashboard provisioning
- Datasource configuration
- Dashboard template loading

### cometml.py
CometML integration for ML experiment tracking:
- Histogram plotting
- Label frequency visualization
- Metric logging
- Run metadata tracking

## Configuration

The telemetry tools can be configured through environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT`: OpenTelemetry collector endpoint
- `OTEL_SERVICE_NAME`: Service name for metrics
- `OTEL_RESOURCE_ATTRIBUTES`: Additional resource attributes
- `COMET_API_KEY`: API key for CometML integration

## Using with Docker

To use the monitoring with Docker-based deployments:

1. Make sure the monitoring infrastructure is running
2. Configure your application to expose metrics on port 8000
3. Update the Prometheus configuration to include your service's endpoint

For using Prometheus + Grafana, see `Dockerfiles/telemetry/`.

## Extension

To add new metrics collectors:

1. Inherit from `MetricsCollector`
2. Define new metrics using `self.meter.create_*`
3. Implement collection logic
4. Add appropriate context managers or methods

## Best Practices

1. Use meaningful context names and attributes
2. Group related operations in single measurement contexts
3. Add relevant attributes for filtering and grouping
4. Regular monitoring and alerting setup
5. Periodic review of collected metrics

## Dependencies

- OpenTelemetry
- prometheus-client
- requests (for Grafana API)
- psutil
- DuckDB (for DuckDB metrics)
- comet_ml (for ML experiment tracking)
- Docker and Docker Compose (for monitoring infrastructure)

## Installation

```bash
pip install -r requirements.txt
```

For using Prometheus + Grafana, see `Dockerfiles/telemetry/`.
