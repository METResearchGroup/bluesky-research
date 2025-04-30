# Backfill Service Consolidation Proposal (Updated File Structure)

## 1. `api/backfill_router/` (API Layer)

```
api/backfill_router/
├── config/
│   └── backfill_configs/
│       ├── default.yaml
│       └── <other-configs>.yaml
├── models/
│   ├── config.py
│   └── request.py
├── routes.py
├── service.py
├── PROPOSAL.md
├── TICKETS.md
└── README.md
```

**Descriptions:**
- `config/backfill_configs/`: YAML configs for different backfill jobs.
- `models/`: Pydantic models for config and API requests.
- `routes.py`: FastAPI router for `/api/backfill`.
- `service.py`: API service logic (job management, config validation, etc).
- `PROPOSAL.md`, `TICKETS.md`, `README.md`: Documentation.

---

## 2. `services/backfill/` (Backfill Core Service)

```
services/backfill/
├── core/
│   ├── manager.py                # (from sync/backfill_manager.py, refactored)
│   ├── worker.py                 # (from sync/backfill_endpoint_worker.py, refactored)
│   ├── constants.py              # (from sync/constants.py, plus any global constants)
│   ├── atp_agent.py              # (from sync/atp_agent.py)
│   ├── models.py                 # (from sync/models.py)
│   ├── helper.py                 # (from sync/helper.py)
│   ├── session_metadata.py       # (from sync/session_metadata.py)
│   ├── backfill.py               # (from sync/backfill.py)
│   ├── __init__.py
│   └── tests/
│       ├── test_manager.py       # (from sync/tests/test_backfill.py, refactored)
│       ├── test_worker.py        # (from sync/tests/test_mainfile.py, refactored)
│       ├── test_helper.py        # (from sync/tests/test_helper.py)
│       ├── test_export_data.py   # (from sync/tests/test_export_data.py)
│       ├── test_session_metadata.py
│       └── __init__.py
├── storage/
│   ├── dynamodb.py               # (from sync/dynamodb_utils.py, refactored)
│   ├── sqlite_adapter.py         # (new: for Story 2.1)
│   ├── s3_adapter.py             # (new: for Story 2.2)
│   ├── write_queue_to_db.py      # (from sync/write_queue_to_db.py)
│   ├── export_data.py            # (from sync/export_data.py)
│   ├── load_data.py              # (from sync/load_data.py)
│   └── README.md
├── config/
│   ├── loader.py                 # (new: YAML config loader/validator)
│   └── schema.py                 # (new: config schema definition)
├── scripts/
│   ├── run_determine_which_dids_to_backfill.py  # (from pipelines/backfill_sync/)
│   ├── run_backfill.py           # (from pipelines/backfill_sync/)
│   ├── delete_invalid_sync_data.py # (from sync/)
│   ├── a.py                      # (from sync/, if still needed)
│   ├── steps.md                  # (from sync/, if still needed)
│   ├── experiments/
│   │   ├── experimental_backfill_endpoint_worker.py
│   │   ├── experiment_compare_serial_parallel.py
│   │   ├── parallelization_experiments.py
│   │   └── parallelization_requirements.txt
│   └── README.md
├── posts/
│   ├── main.py
│   ├── load_data.py
│   ├── constants.py
│   ├── helper.py
│   ├── README.md
│   └── tests/
│       ├── test_load_data.py
│       ├── test_mainfile.py
│       └── __init__.py
├── posts_used_in_feeds/
│   ├── main.py
│   ├── load_data.py
│   ├── constants.py
│   ├── helper.py
│   ├── README.md
│   └── tests/
│       ├── test_load_data.py
│       ├── test_mainfile.py
│       └── __init__.py
├── study_user_activity/
│   ├── main.py
│   ├── helper.py
│   └── README.md
├── README.md
└── main.py                      # (entrypoint, refactored)
```

---

## 3. Additional Notes & Migration/Refactor Plan

### **Files to Rename/Refactor**
- `backfill_manager.py` → `core/manager.py`
- `backfill_endpoint_worker.py` → `core/worker.py`
- `dynamodb_utils.py` → `storage/dynamodb.py`
- `write_queue_to_db.py` → `storage/write_queue_to_db.py`
- `export_data.py`/`load_data.py` → `storage/`
- `determine_dids_to_backfill.py` → `core/` or `scripts/` (if only used for setup)
- `run_determine_which_dids_to_backfill.py`/`run_backfill.py` → `scripts/`
- All test files should be under the appropriate `tests/` directory.

### **Files to Place in `/scripts` or `/experiments`**
- Any one-off scripts, data cleaning, or experiment code (e.g., `a.py`, `delete_invalid_sync_data.py`, all files in `sync/experiments/`).
- Any files in `pipelines/backfill_sync/` that are not core logic (e.g., `run_determine_which_dids_to_backfill.py`, `run_backfill.py`).

### **Functionality to Add/Modify**
- **YAML Config Loader/Validator:** New in `config/loader.py` and `config/schema.py`.
- **SQLite/S3 Source Adapters:** New in `storage/`.
- **API Models:** New in `api/backfill_router/models/`.
- **API Service Logic:** New in `api/backfill_router/service.py`.
- **Job Management:** New in `api/backfill_router/service.py` and `routes.py`.
- **Tests:** All logic should have corresponding tests in `tests/` subdirectories.
- **README.md:** Each major directory should have a README describing its purpose and usage.

### **Files to Archive or Remove**
- Any legacy or deprecated scripts not referenced in the new structure should be archived or removed after migration and validation.

---

## 4. Example: Directory Structure with File Mapping

```
services/backfill/
├── core/
│   ├── manager.py                # from sync/backfill_manager.py
│   ├── worker.py                 # from sync/backfill_endpoint_worker.py
│   ├── constants.py              # from sync/constants.py
│   ├── atp_agent.py              # from sync/atp_agent.py
│   ├── models.py                 # from sync/models.py
│   ├── helper.py                 # from sync/helper.py
│   ├── session_metadata.py       # from sync/session_metadata.py
│   ├── backfill.py               # from sync/backfill.py
│   └── tests/
│       ├── test_manager.py       # from sync/tests/test_backfill.py
│       ├── test_worker.py        # from sync/tests/test_mainfile.py
│       ├── ...
├── storage/
│   ├── dynamodb.py               # from sync/dynamodb_utils.py
│   ├── sqlite_adapter.py         # new
│   ├── s3_adapter.py             # new
│   ├── write_queue_to_db.py      # from sync/write_queue_to_db.py
│   ├── export_data.py            # from sync/export_data.py
│   ├── load_data.py              # from sync/load_data.py
│   └── README.md
├── config/
│   ├── loader.py                 # new
│   └── schema.py                 # new
├── scripts/
│   ├── run_determine_which_dids_to_backfill.py  # from pipelines/backfill_sync/
│   ├── run_backfill.py           # from pipelines/backfill_sync/
│   ├── delete_invalid_sync_data.py # from sync/
│   ├── a.py                      # from sync/
│   ├── steps.md                  # from sync/
│   ├── experiments/
│   │   ├── experimental_backfill_endpoint_worker.py
│   │   ├── experiment_compare_serial_parallel.py
│   │   ├── parallelization_experiments.py
│   │   └── parallelization_requirements.txt
│   └── README.md
├── posts/
│   ├── main.py
│   ├── load_data.py
│   ├── constants.py
│   ├── helper.py
│   ├── README.md
│   └── tests/
│       ├── test_load_data.py
│       ├── test_mainfile.py
│       └── __init__.py
├── posts_used_in_feeds/
│   ├── main.py
│   ├── load_data.py
│   ├── constants.py
│   ├── helper.py
│   ├── README.md
│   └── tests/
│       ├── test_load_data.py
│       ├── test_mainfile.py
│       └── __init__.py
├── study_user_activity/
│   ├── main.py
│   ├── helper.py
│   └── README.md
├── README.md
└── main.py                      # (entrypoint, refactored)
```

---

## 5. Summary

- All core logic is consolidated and modularized.
- Scripts, experiments, and one-off utilities are separated from production code.
- All tests are organized under `tests/` in the relevant module.
- New adapters and config loaders are introduced for extensibility.
- API and service layers are cleanly separated.

---

**This structure will ensure a maintainable, scalable, and testable backfill system, ready for future growth and easy onboarding.**

## Overview
This proposal outlines the consolidation of multiple backfill-related services into a unified, configurable backfill service that can be triggered via API endpoints. The goal is to improve maintainability, provide better configuration management, and enable more flexible backfill operations.

## Current Architecture
Currently, backfill functionality is spread across multiple directories:
- `/services/backfill`
- `/pipelines/backfill_records_coordination`
- `/pipelines/backfill_sync`

Core functionality is implemented in:
- `backfill_manager.py`: Main orchestration logic
- `backfill_endpoint_worker.py`: PDS endpoint-specific worker implementation

## Target Architecture

```
api/
├── integrations_router/
├── backfill_router/
│   ├── config/
│   │   ├── backfill_configs/
│   │   │   ├── default.yaml
│   │   │   └── custom_backfills/
│   │   ├── models/
│   │   │   ├── config.py
│   │   │   └── request.py
│   │   ├── routes.py
│   │   └── service.py
│   ├── main.py
│   └── README.md

services/
├── backfill/
│   ├── core/
│   │   ├── manager.py
│   │   ├── worker.py
│   │   └── constants.py
│   ├── storage/
│   │   ├── dynamodb.py
│   │   └── s3.py
│   ├── config/
│   │   └── loader.py
│   └── main.py
```

## YAML Configuration Schema

```yaml
backfill:
  name: "example_backfill"
  version: "1.0"
  
  # Source configuration for DIDs
  source:
    type: "sqlite" # or "s3"
    path: "/path/to/sqlite.db" # or "s3://bucket/path"
    query: "SELECT did FROM dids" # Optional SQL query for SQLite
    
  # Bluesky record types to process
  record_types:
    - "app.bsky.feed.post"
    - "app.bsky.feed.like"
    - "app.bsky.feed.repost"
    
  # Time range configuration  
  time_range:
    start_date: "2025-01-02"
    end_date: "2025-02-02" # Optional
    
  # Processing configuration
  processing:
    batch_size: 100
    max_concurrent_pds: 32
    requests_per_second: 10
    max_retries: 3
    
  # Storage configuration for results
  storage:
    type: "dynamodb"
    table_name: "backfill_results"
    region: "us-west-2"
    
  # Monitoring and alerting
  monitoring:
    prometheus_port: 8000
    log_level: "INFO"
    alert_on_failure: true
```

## API Endpoints

```
POST /api/backfill/start
- Start a new backfill job using provided YAML config
- Returns job ID

GET /api/backfill/status/{job_id}
- Get status of running backfill job

GET /api/backfill/configs
- List available backfill configurations

POST /api/backfill/configs
- Create new backfill configuration

DELETE /api/backfill/jobs/{job_id}
- Cancel running backfill job
```

## Implementation Epics

### Epic 1: Core Service Consolidation
**Stories:**
1. Create new unified service structure
2. Migrate core backfill manager functionality
3. Migrate worker functionality
4. Implement configuration loader
5. Add unit tests for core components

### Epic 2: Storage Layer Implementation
**Stories:**
1. Implement SQLite source adapter
2. Implement S3 source adapter
3. Implement DynamoDB result storage
4. Add storage layer tests
5. Implement storage migration utilities

### Epic 3: API Integration
**Stories:**
1. Create FastAPI router structure
2. Implement configuration management endpoints
3. Implement job management endpoints
4. Add API authentication/authorization
5. Add API documentation

### Epic 4: Monitoring & Observability
**Stories:**
1. Implement enhanced Prometheus metrics
2. Add structured logging
3. Create monitoring dashboards
4. Implement alerting system
5. Add performance tracking

## Technical Considerations

### 1. Backward Compatibility
- Maintain support for existing backfill formats
- Provide migration scripts for existing configurations
- Support graceful degradation for legacy features

### 2. Performance
- Implement efficient DID source loading
- Optimize PDS endpoint concurrency
- Use connection pooling for database operations
- Implement caching where appropriate

### 3. Scalability
- Design for horizontal scaling
- Implement job queuing system
- Support distributed processing
- Handle partial failures gracefully

### 4. Security
- Implement proper authentication/authorization
- Secure configuration storage
- Validate and sanitize inputs
- Implement rate limiting

### 5. Monitoring
- Comprehensive metrics collection
- Detailed error tracking
- Performance monitoring
- Resource usage tracking

### 6. Testing
- Unit tests for core components
- Integration tests for API
- Load testing for scalability
- Chaos testing for resilience

## Migration Strategy

### Phase 1: Core Service Migration
1. Create new service structure
2. Migrate core functionality
3. Add configuration support
4. Implement basic tests

### Phase 2: API Integration
1. Create API router
2. Implement basic endpoints
3. Add configuration management
4. Test API functionality

### Phase 3: Enhanced Features
1. Add advanced monitoring
2. Implement full observability
3. Add performance optimizations
4. Enhance error handling

### Phase 4: Legacy Migration
1. Migrate existing configurations
2. Update documentation
3. Deprecate old services
4. Remove legacy code

## Risk Mitigation

1. **Data Integrity**
   - Implement validation for all configurations
   - Add checksums for data transfers
   - Maintain audit logs

2. **Performance Impact**
   - Gradual rollout of changes
   - Performance testing in staging
   - Monitoring of resource usage

3. **Service Disruption**
   - Maintain backward compatibility
   - Implement feature flags
   - Plan for rollback scenarios

## Success Metrics

1. **Performance**
   - Improved throughput (requests/second)
   - Reduced latency
   - Better resource utilization

2. **Reliability**
   - Reduced error rates
   - Improved completion rates
   - Better error recovery

3. **Maintainability**
   - Reduced code complexity
   - Improved test coverage
   - Better documentation

## Implementation Timeline

### Week 1-2: Core Service Migration
- Set up new service structure
- Begin core functionality migration
- Initial configuration system

### Week 3-4: API Development
- Implement API router
- Basic endpoint functionality
- Configuration management

### Week 5-6: Enhanced Features
- Monitoring implementation
- Performance optimizations
- Testing infrastructure

### Week 7-8: Legacy Migration
- Migration scripts
- Documentation updates
- Legacy code removal

## Next Steps

1. Review and approve proposal
2. Create detailed technical design documents
3. Set up project tracking
4. Begin implementation of Phase 1
5. Schedule regular progress reviews

## Dependencies

1. **External Systems**
   - Bluesky API
   - AWS Services (S3, DynamoDB)
   - Monitoring systems

2. **Internal Systems**
   - Existing backfill services
   - Authentication system
   - Logging infrastructure

## Resource Requirements

1. **Engineering**
   - 2-3 Backend Engineers
   - 1 DevOps Engineer
   - 1 QA Engineer

2. **Infrastructure**
   - Development environment
   - Staging environment
   - CI/CD pipeline updates

3. **Support**
   - Documentation resources
   - Training materials
   - Monitoring setup

## Appendix

### A. Reference Documentation
- Bluesky API Documentation
- AWS Service Documentation
- Internal System Documentation

### B. Current System Analysis
- Performance metrics
- Error rates
- Resource usage

### C. Migration Scripts
- Configuration conversion
- Data migration
- Validation tools 