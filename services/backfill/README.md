# Backfill

Encompasses two types of backfill:

- **Backfilling the PDS**: this involves syncing with the PDS endpoints from Bluesky
and then getting the raw data for users. This is in the `pds_backfills/` directory.

- **Backfilling existing records**: this is the main backfill use case managed by `pipelines/backfill_records_coordination/app.py`. It involves taking existing records that are missing labels from ML inference integrations and backfilling them. This workflow is implemented in the `services/backfill/` directory.

## Main Backfill Workflow

The main backfill workflow processes existing posts through ML inference integrations to generate classification labels. It's orchestrated via the CLI app at `pipelines/backfill_records_coordination/app.py` and consists of three main phases:

### 1. Enqueueing Records (`EnqueueService`)

Loads posts from the database and adds them to integration input queues:

- **Record Types Supported**:
  - `posts`: All posts in the database
  - `posts_used_in_feeds`: Only posts that have been used in feeds

- **Filtering Logic**:
  - Filters by date range (start_date and end_date, inclusive)
  - Automatically excludes posts that have already been classified by the target integration
  - Only includes posts that have been preprocessed (have a `preprocessing_timestamp`)

- **Process**:
  1. `BackfillDataLoaderService` loads posts from the database based on record type and date range
  2. Posts are filtered to exclude those already labeled by the target integration
  3. `QueueManagerService` adds the filtered posts to the integration's input queue

### 2. Running Integrations (`IntegrationRunnerService`)

Executes ML inference services to process posts from input queues and write results to output queues:

- **Supported Integrations**:
  - `ml_inference_perspective_api`: Perspective API toxicity classification
  - `ml_inference_sociopolitical`: Sociopolitical classification
  - `ml_inference_ime`: IME (Identity-Motivated Engagement) classification
  - `ml_inference_valence_classifier`: Valence classification
  - `ml_inference_intergroup`: Intergroup classification

- **Configuration**:
  - `backfill_period`: Time period unit ("days" or "hours")
  - `backfill_duration`: Duration of the backfill period (optional)

- **Process**:
  1. Each integration service reads posts from its input queue
  2. Processes posts through the ML inference pipeline
  3. Writes classification results to the integration's output queue

### 3. Writing Cache Buffers (`CacheBufferWriterService`)

Persists results from output queues to permanent storage:

- **Process**:
  1. Loads records from the integration's output queue
  2. Writes records to persistent storage via `BackfillDataRepository`
  3. Optionally clears the output queue after writing (to free up memory)

- **Features**:
  - Decoupled write and clear operations (can write without clearing)
  - Efficient queue clearing that only loads IDs before deletion
  - Supports bypassing write operations (clear cache only)

## Key Components

- **`EnqueueService`**: Orchestrates loading and enqueuing posts to integration queues
- **`BackfillDataLoaderService`**: Loads posts from database and filters already-classified posts
- **`IntegrationRunnerService`**: Runs ML inference integrations with configurable backfill parameters
- **`CacheBufferWriterService`**: Writes cache buffers to storage and manages queue cleanup
- **`QueueManagerService`**: Manages all queue I/O operations (insert, load, delete)
- **`BackfillDataRepository`**: Data access layer for loading posts and writing results

## Usage

The backfill workflow is triggered via the CLI app:

```bash
python pipelines/backfill_records_coordination/app.py \
  --record-type posts \
  --add-to-queue \
  --run-integrations \
  --integrations ml_inference_perspective_api \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --write-cache-buffer-to-storage \
  --service-source-buffer ml_inference_perspective_api \
  --clear-queue
```

See `pipelines/backfill_records_coordination/app.py` for all available CLI options.
