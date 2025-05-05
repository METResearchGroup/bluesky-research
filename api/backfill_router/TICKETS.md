# Backfill Service Consolidation: Epics & Stories

This document breaks down the PRD in PROPOSAL.md into actionable, well-scoped epics and stories, referencing the current state of `services/backfill` and `pipelines/backfill_sync`. Each story is clear, testable, and manageable for an engineer. Acceptance criteria are included for each story.

---

## Epic 1: Core Service Consolidation

### Story 1.1: Establish Unified Backfill Service Directory Structure
- **Description:** Create the new directory structure for the unified backfill service as described in the proposal.
- **Acceptance Criteria:**
  - All directories and placeholder files exist as per the target architecture.
  - README files are present in new directories.

### Story 1.2: Migrate Backfill Manager Logic
- **Description:** Move and refactor the orchestration logic from `services/backfill/sync/backfill_manager.py` into `services/backfill/core/manager.py`.
- **Acceptance Criteria:**
  - All orchestration logic is moved and imports are updated.
  - Legacy references are removed from the old location.
  - Unit tests pass for the migrated logic.

### Story 1.3: Migrate Endpoint Worker Logic
- **Description:** Move and refactor the worker logic from `services/backfill/sync/backfill_endpoint_worker.py` into `services/backfill/core/worker.py`.
- **Acceptance Criteria:**
  - All worker logic is moved and imports are updated.
  - Legacy references are removed from the old location.
  - Unit tests pass for the migrated logic.

### Story 1.4: Implement YAML Configuration Loader
- **Description:** Implement a loader that reads YAML config files and validates them against the schema in PROPOSAL.md.
- **Acceptance Criteria:**
  - Loader can parse and validate YAML configs.
  - Invalid configs are rejected with clear errors.
  - Unit tests cover valid and invalid cases.

### Story 1.5: Add Unit Tests for Core Components
- **Description:** Add or migrate unit tests for manager and worker logic.
- **Acceptance Criteria:**
  - Tests cover all major code paths.
  - Tests are located in a `tests/` directory under `services/backfill/core/`.

---

## Epic 2: Storage Layer Implementation

### Story 2.1: Implement SQLite Source Adapter
- **Description:** Implement a module to load DIDs from a SQLite database, supporting custom queries as per YAML config.
- **Acceptance Criteria:**
  - Adapter loads DIDs from SQLite given a path and query.
  - Handles errors gracefully.
  - Unit tests cover normal and error cases.

### Story 2.2: Implement S3 Source Adapter
- **Description:** Implement a module to load DIDs from S3, supporting CSV/Parquet formats.
- **Acceptance Criteria:**
  - Adapter loads DIDs from S3 given a bucket and path.
  - Handles errors and missing files gracefully.
  - Unit tests cover normal and error cases.

### Story 2.3: Implement DynamoDB Result Storage
- **Description:** Implement a module to write backfill results to DynamoDB as per YAML config.
- **Acceptance Criteria:**
  - Results are written to the correct table and region.
  - Handles retries and errors.
  - Unit tests cover normal and error cases.

### Story 2.4: Add Storage Layer Tests
- **Description:** Add comprehensive tests for all storage adapters.
- **Acceptance Criteria:**
  - All adapters have >90% test coverage.

### Story 2.5: Implement Storage Migration Utilities
- **Description:** Implement scripts/utilities to migrate existing data/configs to the new format.
- **Acceptance Criteria:**
  - Migration scripts are idempotent and well-documented.
  - Tests verify successful migration.

---

## Epic 3: API Integration

### Story 3.1: Create FastAPI Router Structure
- **Description:** Scaffold the FastAPI router for `/api/backfill` with placeholder endpoints.
- **Acceptance Criteria:**
  - Router is registered in `api/main.py`.
  - Placeholder endpoints return 200 OK.

### Story 3.2: Implement Configuration Management Endpoints
- **Description:** Implement endpoints to list, create, and validate backfill YAML configs.
- **Acceptance Criteria:**
  - Endpoints for GET/POST `/api/backfill/configs` work as described.
  - Configs are stored in the correct location.
  - Validation errors are returned with clear messages.

### Story 3.3: Implement Job Management Endpoints
- **Description:** Implement endpoints to start, monitor, and cancel backfill jobs.
- **Acceptance Criteria:**
  - POST `/api/backfill/start` launches a job and returns a job ID.
  - GET `/api/backfill/status/{job_id}` returns job status.
  - DELETE `/api/backfill/jobs/{job_id}` cancels a job.
  - Jobs are tracked in memory or persistent store.

### Story 3.4: Add API Authentication/Authorization
- **Description:** Add authentication and authorization to all endpoints.
- **Acceptance Criteria:**
  - Only authorized users can access endpoints.
  - Unauthorized requests are rejected.

### Story 3.5: Add API Documentation
- **Description:** Add OpenAPI/Swagger documentation for all endpoints.
- **Acceptance Criteria:**
  - All endpoints are documented and visible in `/docs`.

---

## Epic 4: Monitoring & Observability

### Story 4.1: Implement Enhanced Prometheus Metrics
- **Description:** Add detailed Prometheus metrics for job status, errors, and performance.
- **Acceptance Criteria:**
  - Metrics are exposed on the configured port.
  - Metrics include job counts, error rates, and latency.

### Story 4.2: Add Structured Logging
- **Description:** Implement structured logging throughout the backfill service.
- **Acceptance Criteria:**
  - Logs include job IDs, error details, and timestamps.
  - Logs are easily searchable.

### Story 4.3: Create Monitoring Dashboards
- **Description:** Create Grafana dashboards for key metrics.
- **Acceptance Criteria:**
  - Dashboards visualize job status, error rates, and throughput.

### Story 4.4: Implement Alerting System
- **Description:** Set up alerting for job failures and performance issues.
- **Acceptance Criteria:**
  - Alerts are triggered on job failure or high error rates.

### Story 4.5: Add Performance Tracking
- **Description:** Track and report on job throughput and latency.
- **Acceptance Criteria:**
  - Performance metrics are available via API or dashboard.

---

## Epic 5: Legacy Migration & Cleanup

### Story 5.1: Migrate Existing Configurations
- **Description:** Convert existing configs in `services/backfill` and `pipelines/backfill_sync` to the new YAML format.
- **Acceptance Criteria:**
  - All legacy configs are migrated and validated.
  - Old configs are archived.

### Story 5.2: Update Documentation
- **Description:** Update all READMEs and documentation to reflect the new architecture and usage.
- **Acceptance Criteria:**
  - All documentation is up to date and accurate.

### Story 5.3: Deprecate Old Services
- **Description:** Mark old backfill services as deprecated and remove from active deployment.
- **Acceptance Criteria:**
  - Old services are not started in CI/CD.
  - Deprecation is documented.

### Story 5.4: Remove Legacy Code
- **Description:** Remove legacy code after successful migration and validation.
- **Acceptance Criteria:**
  - No references to old code remain.
  - All tests pass.

---

# End of TICKETS.md 