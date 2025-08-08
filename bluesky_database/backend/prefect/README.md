# Prefect Infrastructure Setup

This directory contains the Prefect infrastructure setup for the Bluesky data pipeline, including the Prefect server with SQLite backend and agent for job execution.

## Overview

The Prefect infrastructure is integrated with the existing Redis + Prometheus + Grafana monitoring stack to provide:

- **Prefect Server**: Web UI and API for flow management (http://localhost:4200)
- **Prefect Agent**: Job execution engine for running flows
- **SQLite Backend**: Lightweight database for flow metadata storage
- **Monitoring Integration**: Integration with existing Prometheus + Grafana stack

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Prefect UI    │    │  Prefect Agent  │    │  Prefect Server │
│  (Port 4200)    │◄──►│   (Container)   │◄──►│  (SQLite DB)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    Grafana      │    │    Prometheus   │    │      Redis      │
│   (Port 3000)   │    │   (Port 9090)   │    │   (Port 6379)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Services

### Prefect Server
- **Container**: `prefect_server`
- **Port**: 4200
- **URL**: http://localhost:4200
- **Backend**: SQLite database
- **Purpose**: Web UI and API for flow management

### Prefect Agent
- **Container**: `prefect_agent`
- **Work Queue**: `default`
- **Purpose**: Execute flows and tasks
- **Dependencies**: Prefect Server, Redis

### Integration Services
- **Redis**: Data buffer and message queue
- **Prometheus**: Metrics collection
- **Grafana**: Monitoring dashboards
- **Alertmanager**: Alert routing and Slack integration

## Quick Start

### 1. Start the Infrastructure

```bash
# Navigate to the backend directory
cd bluesky_database/backend

# Start all services including Prefect
docker compose -f docker-compose.prefect.yml up -d
```

### 2. Validate the Setup

```bash
# Navigate to the redis_testing directory
cd redis_testing

# Run the validation script
python 08_prefect_infrastructure_setup.py
```

### 3. Access the Services

- **Prefect UI**: http://localhost:4200
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Alertmanager**: http://localhost:9093

## Configuration

### Environment Variables

The Prefect setup uses the following environment variables:

- `PREFECT_SERVER_API_HOST`: Server API host (0.0.0.0)
- `PREFECT_SERVER_API_PORT`: Server API port (4200)
- `PREFECT_SERVER_DATABASE_CONNECTION_URL`: SQLite database URL
- `PREFECT_AGENT_WORK_QUEUES`: Work queues for the agent (default)
- `PREFECT_AGENT_MAX_CONCURRENT_FLOW_RUNS`: Maximum concurrent flow runs (5)

### Volumes

- `prefect_data`: Persistent storage for Prefect data
- `./prefect`: Mounted directory for flow code
- `./data`: Mounted directory for data output

## Testing

### Test Flow

A simple test flow is provided in `test_flow.py` to validate the infrastructure:

```bash
# Run the test flow
python prefect/test_flow.py
```

### Validation Script

The validation script (`08_prefect_infrastructure_setup.py`) performs comprehensive testing:

1. **Docker Compose Startup**: Verifies all containers start correctly
2. **Prefect Server Connectivity**: Tests API connectivity
3. **Prefect UI Accessibility**: Tests web UI access
4. **Agent Registration**: Verifies agent registration with server
5. **SQLite Backend Operations**: Tests database operations
6. **Monitoring Integration**: Verifies integration with monitoring stack
7. **Work Queue Operations**: Tests work queue functionality
8. **Container Health Checks**: Verifies container health status

**Note**: The validation script is compatible with Prefect v3.x API endpoints. It uses the correct v3 API patterns:
- Work pools: `POST /api/work_pools/filter` (instead of v2's `GET /api/work_pools`)
- Work queues: `POST /api/work_queues/filter` (instead of v2's `GET /api/work_queues`)

## Monitoring

### Prefect Metrics

Prefect provides metrics that can be integrated with Prometheus:

- Flow run counts and durations
- Task execution metrics
- Agent performance metrics
- Queue status and backlog

### Grafana Dashboards

The existing Grafana setup can be extended with Prefect dashboards to monitor:

- Flow execution status
- Task performance
- Agent health
- Queue metrics

## Troubleshooting

### Common Issues

1. **Container Startup Failures**
   ```bash
   # Check container logs
   docker logs prefect_server
   docker logs prefect_agent
   ```

2. **Database Connection Issues**
   ```bash
   # Check SQLite database
   docker exec -it prefect_server ls -la /app/.prefect/
   ```

3. **Agent Registration Issues**
   ```bash
   # Check agent status
   docker exec -it prefect_agent prefect agent status
   ```

4. **API Version Compatibility Issues**
   ```bash
   # Check Prefect version
   docker exec prefect_server prefect version
   
   # If you see 405 Method Not Allowed errors, ensure you're using v3 API endpoints:
   # - Use POST /api/work_pools/filter instead of GET /api/work_pools
   # - Use POST /api/work_queues/filter instead of GET /api/work_queues
   ```

### Logs

Container logs are available for debugging:

```bash
# Prefect server logs
docker logs -f prefect_server

# Prefect agent logs
docker logs -f prefect_agent
```

## Next Steps

After the infrastructure is validated, the next steps are:

1. **DataWriter Flow Implementation**: Create flows for processing Redis Streams
2. **Monitoring Integration**: Add Prefect metrics to Prometheus
3. **Scheduling Setup**: Configure 5-minute scheduled execution
4. **Production Deployment**: Deploy to production environment

## Files

- `docker-compose.prefect.yml`: Docker Compose configuration with Prefect
- `08_prefect_infrastructure_setup.py`: Validation script
- `test_flow.py`: Simple test flow for validation
- `requirements.txt`: Python dependencies
- `README.md`: This documentation
