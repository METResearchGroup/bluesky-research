# Backfill Core Service

## High-level Overview
This directory implements the core logic for the Bluesky backfill pipeline. It orchestrates the process of fetching, filtering, and persisting data from Bluesky PDS endpoints. The core service manages distributed work, rate limiting, error handling, and batching, and interacts with the `storage` module for queueing and persistence.

## Detailed Explanation of the Code

### Main Files

- **main.py**
  - Entrypoint for running the backfill core service. Sets up the event loop, configures the worker(s), and starts the backfill process.

- **worker.py**
  - Implements the `PDSEndpointWorker` class, which manages the lifecycle of backfilling for a single PDS endpoint. Handles:
    - Work queue initialization
    - Rate limiting (via token bucket)
    - Fetching and paginating records (via `atp_agent.py`)
    - Filtering and transformation (via `validate.py`, `transform.py`)
    - Writing results and deadletters to persistent queues (via `storage`)
    - Flushing and persisting results to permanent storage (e.g., Parquet)
    - Error handling, retries, and metrics

- **manager.py**
  - Orchestrates multiple `PDSEndpointWorker` instances, manages distributed execution, and coordinates the overall backfill process across endpoints.

- **atp_agent.py**
  - Provides the `AtpAgent` class, which wraps the Bluesky API and handles HTTP requests for listing records, with support for pagination and error handling.

- **backfill.py**
  - Contains core transformation logic for backfilled records, including normalization and enrichment.

- **transform.py**
  - Post-processing and additional transformation utilities for records after initial backfill.

- **validate.py**
  - Validation utilities for filtering records by type, time range, and other criteria.

- **constants.py**
  - Central location for service-wide constants, such as default endpoints, rate limits, and service names.

- **models.py**
  - Pydantic models for user and record metadata used throughout the backfill process.

- **query_plc_endpoint.py**
  - Utilities for querying the PLC directory for DID resolution and metadata enrichment.

- **requirements.txt / requirements.in**
  - Python dependencies for the core service.

- **tests/**
  - Unit and integration tests for all major components. See below for details.

## How Components Relate (with Storage)
- The `worker.py` uses the `Queue` class (from `lib/db/queue.py`) to manage intermediate work, results, and deadletter queues. These queues are persisted locally (SQLite) or could be swapped for a distributed queue (e.g., Kafka).
- The `storage` module (e.g., `storage/load_data.py`, `storage/write_queue_to_db.py`) is used to load previously processed DIDs, write results to permanent storage, and manage session metadata.
- The `manager.py` coordinates multiple workers, each of which interacts with its own set of queues and storage.

## Architecture Diagram

```
+-------------------+
|   main.py         |
+-------------------+
          |
          v
+-------------------+
|   manager.py      |<-------------------+
+-------------------+                    |
          |                              |
          v                              |
+-------------------+                    |
|   worker.py       |                    |
+-------------------+                    |
   |        |        |                   |
   v        v        v                   |
 atp_   validate   transform             |
agent.py   .py        .py                |
   |        |        |                   |
   +--------+--------+                   |
            |                            |
            v                            |
      +-------------------+              |
      |   storage/        |<-------------+
      |   (load_data,     |
      |    write_queue,   |
      |    session_meta)  |
      +-------------------+
```

- `main.py` starts the process, instantiates the manager.
- `manager.py` creates and manages multiple workers.
- Each `worker.py` instance fetches, filters, and transforms data using `atp_agent.py`, `validate.py`, and `transform.py`.
- Results and deadletters are written to queues and then persisted via the `storage` module.

## Testing Details

- All tests are located in the `tests/` directory within `core/`.
- Tests cover:
    - Worker queue initialization, record fetching, filtering, and error handling
    - Manager orchestration logic
    - ATP agent API interactions and pagination
    - Record transformation and validation logic
    - Integration with storage and queueing
- Example test files:
    - `tests/test_worker.py`: Unit tests for `worker.py` (queueing, fetching, filtering, error handling)
    - `tests/test_manager.py`: Tests for `manager.py` (worker orchestration)
    - `tests/test_atp_agent.py`: Tests for `atp_agent.py` (API, pagination)
    - `tests/test_transform.py`, `tests/test_validate.py`: Tests for transformation and validation utilities

For more details on storage and queueing, see the `../storage/` directory and its README.
