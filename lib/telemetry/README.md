# Telemetry Tools

This package provides telemetry and metrics collection tools for monitoring application performance and resource usage.

## Overview

The telemetry package includes:
- Base metrics collection framework using OpenTelemetry
- DuckDB-specific metrics collection
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

## Usage

### Basic Metrics Collection

To start collecting basic metrics, initialize the metrics collector and enable the necessary spans:

```python
from telemetry.metrics import MetricsCollector

collector = MetricsCollector()
collector.start()
```

## Additional Metrics to Consider

1. Query-specific metrics:
   - Query compilation time
   - Query optimization time
   - Cache hit/miss rates
   - Number of table scans

2. Resource metrics:
   - Disk I/O rates
   - Network bandwidth usage
   - Thread utilization
   - Memory fragmentation

3. Data quality metrics:
   - NULL value counts
   - Distinct value counts
   - Data type distribution
   - Value distribution statistics

## Configuration

The telemetry tools can be configured through environment variables:
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OpenTelemetry collector endpoint
- `OTEL_SERVICE_NAME`: Service name for metrics
- `OTEL_RESOURCE_ATTRIBUTES`: Additional resource attributes

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
- psutil
- DuckDB (for DuckDB metrics)

## Installation

```bash
pip install -r requirements.txt
```
