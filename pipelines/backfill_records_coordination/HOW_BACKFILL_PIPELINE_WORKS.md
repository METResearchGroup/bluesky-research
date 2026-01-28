# How the Backfill Pipeline Works

This document explains how the backfill pipeline processes posts through the ML inference system, from initial data loading to permanent storage. We use the **intergroup classifier** as a case study example throughout.

## Overview

The backfill pipeline consists of four main stages:

1. **Loading Posts into Input Queues** - Extract unprocessed posts and queue them for classification
2. **Running Inference** - Process queued posts through ML inference services
3. **Output Queues** - Store classified results temporarily
4. **Writing to Permanent Storage** - Persist results to database/parquet files

## Stage 1: Loading Posts into Input Queues

### Flow Diagram

```
CLI (app.py) → Handler (handler.py) → Backfill Main → Load Data → Queue Writing
```

### Step-by-Step Process

**1. CLI Invocation** (`pipelines/backfill_records_coordination/app.py`)
- User runs: `python -m pipelines.backfill_records_coordination.app -r posts --add-to-queue -i g`
- CLI constructs payload with integration kwargs and calls `lambda_handler()`

**2. Handler** (`pipelines/backfill_records_coordination/handler.py`)
- Receives event payload
- Calls `services/backfill/main.py` → `backfill_records()`

**3. Backfill Routing** (`services/backfill/main.py`)
- Routes to `services/backfill/posts/main.py` → `backfill_posts()`

**4. Load Data** (`services/backfill/posts/load_data.py`)
- Calls `load_posts_to_backfill(integrations=["ml_inference_intergroup"], ...)`
- For intergroup specifically:
  - Loads all preprocessed posts from storage (within date range if specified)
  - Checks which posts have already been processed by intergroup classifier
  - Filters out already-processed posts
  - Returns `{"ml_inference_intergroup": [list of unprocessed post dicts]}`

**5. Queue Writing** (`services/backfill/posts/main.py`)
- Creates queue: `Queue(queue_name="input_ml_inference_intergroup")`
- Calls `queue.batch_add_items_to_queue(items=post_dicts)` to add posts to input queue
- Each post dict includes: `uri`, `text`, `preprocessing_timestamp`, `batch_id`, `batch_metadata`

### Example: Intergroup Queue Entry

Posts are now in the `input_ml_inference_intergroup` queue, waiting to be processed.

## Stage 2: Running Inference

### Flow Diagram

```
Pipeline Invocation → Registry → Handler → Orchestration → Classification → Export
```

### Step-by-Step Process

**1. Pipeline Invocation** (`services/backfill/posts/main.py`)
- Calls `invoke_pipeline_handler(service="ml_inference_intergroup", payload={...})`

**2. Registry Lookup** (`lib/pipeline_invocation/registry.py`)
- Looks up handler factory for `"ml_inference_intergroup"`
- Lazy-loads: `pipelines/classify_records/intergroup/handler.py` → `lambda_handler`

**3. Handler** (`pipelines/classify_records/intergroup/handler.py`)
- Calls `services/ml_inference/intergroup/intergroup.py` → `classify_latest_posts()`

**4. Orchestration Wrapper** (`services/ml_inference/intergroup/intergroup.py`)
- Uses `INTERGROUP_CONFIG` which specifies:
  - `inference_type="intergroup"`
  - `queue_inference_type="intergroup"`
  - `classification_func=run_batch_classification` (from `batch_classifier.py`)
- Calls `services/ml_inference/helper.py` → `orchestrate_classification(config=INTERGROUP_CONFIG)`

**5. Orchestration Logic** (`services/ml_inference/helper.py`)
- Calls `get_posts_to_classify(inference_type="intergroup")`:
  - Loads posts from `input_ml_inference_intergroup` queue
  - Filters invalid posts (empty text, missing timestamps, etc.)
  - Deduplicates by URI
- Calls `config.classification_func()` (i.e., `run_batch_classification` from `batch_classifier.py`)
- Converts metadata: if classification returns a Pydantic model (like `BatchClassificationMetadataModel`), converts to dict for `ClassificationSessionModel`

**6. Batch Classification** (`services/ml_inference/intergroup/batch_classifier.py`)
- `run_batch_classification()` processes posts in batches:
  - Converts queue dicts to `PostToLabelModel` instances
  - For each batch:
    - Calls `IntergroupClassifier.classify_batch()` to get labels
    - Splits labels into successful and failed
    - For successful labels: calls `write_posts_to_cache()` (writes to output queue, deletes from input queue)
    - For failed labels: calls `return_failed_labels_to_input_queue()` (re-queues for retry)
  - Returns `BatchClassificationMetadataModel` with counts

### Example: Intergroup Classification

Input: Posts from `input_ml_inference_intergroup` queue
Output: Classified labels written to `output_ml_inference_intergroup` queue

## Stage 3: Output Queues

### Flow Diagram

```
Classification Results → Export Functions → Output Queue
```

### Step-by-Step Process

**1. Successful Labels** (`services/ml_inference/export_data.py`)
- `write_posts_to_cache(inference_type="intergroup", posts=successful_dicts)`:
  - Creates `output_ml_inference_intergroup` queue
  - Writes successfully classified posts to output queue via `out_queue.batch_add_items_to_queue()`
  - Deletes processed batch IDs from input queue via `in_queue.batch_delete_items_by_ids()`

**2. Failed Labels** (`services/ml_inference/export_data.py`)
- `return_failed_labels_to_input_queue(inference_type="intergroup", failed_label_models=failed_dicts)`:
  - Re-inserts failed posts back into `input_ml_inference_intergroup` queue for retry

### Example: Intergroup Output Queue

Classified intergroup labels are now in the `output_ml_inference_intergroup` queue, waiting to be written to permanent storage.

## Stage 4: Writing to Permanent Storage

### Flow Diagram

```
CLI (--write-cache) → Write Cache Handler → Write Cache Main → Queue Reading → Parquet Export
```

### Step-by-Step Process

**1. CLI Invocation** (`pipelines/backfill_records_coordination/app.py`)
- User runs: `python -m pipelines.backfill_records_coordination.app --write-cache ml_inference_intergroup`
- Or: `python -m pipelines.backfill_records_coordination.app --write-cache all` (writes all services)

**2. Handler** (`pipelines/write_cache_buffers/handler.py`)
- Calls `services/write_cache_buffers_to_db/main.py` → `write_cache_buffers_to_db()`
- Validates service name is in `SERVICES_TO_WRITE` (intergroup is included)

**3. Write Cache Main** (`services/write_cache_buffers_to_db/main.py`)
- Calls `services/write_cache_buffers_to_db/helper.py` → `write_cache_buffer_queue_to_db(service="ml_inference_intergroup")`

**4. Queue Reading & Export** (`services/write_cache_buffers_to_db/helper.py`)
- Creates `Queue(queue_name="output_ml_inference_intergroup")`
- Loads all pending items from output queue
- Exports to local storage as Parquet files via `export_data_to_local_storage(service="ml_inference_intergroup", df=df, export_format="parquet")`
- Optionally clears the output queue after writing (if `--clear-queue` flag is used)

### Example: Intergroup Storage

Intergroup classification results are now permanently stored as Parquet files in local storage, accessible for analysis and downstream processing.

## Complete Flow: Intergroup Case Study

Here's the complete flow for the intergroup classifier:

```
1. CLI: python -m pipelines.backfill_records_coordination.app -r posts --add-to-queue -i g
   → Posts loaded into input_ml_inference_intergroup queue

2. CLI: python -m pipelines.backfill_records_coordination.app -i g --run-integrations
   → Posts processed through IntergroupClassifier
   → Results written to output_ml_inference_intergroup queue
   → Input queue cleaned up (processed posts removed)

3. CLI: python -m pipelines.backfill_records_coordination.app --write-cache ml_inference_intergroup
   → Results exported from output_ml_inference_intergroup queue
   → Written to Parquet files in permanent storage
   → Output queue optionally cleared

Or, all-in-one:
   python -m pipelines.backfill_records_coordination.app -r posts --add-to-queue -i g --run-integrations --write-cache ml_inference_intergroup
```

## Key Files and Functions

### Intergroup-Specific Files

- **Orchestration**: `services/ml_inference/intergroup/intergroup.py`
  - `INTERGROUP_CONFIG`: Configuration for intergroup inference
  - `classify_latest_posts()`: Entry point for classification

- **Batch Processing**: `services/ml_inference/intergroup/batch_classifier.py`
  - `run_batch_classification()`: Processes batches, handles I/O

- **Classification**: `services/ml_inference/intergroup/classifier.py`
  - `IntergroupClassifier.classify_batch()`: Pure classification logic

- **Pipeline Handler**: `pipelines/classify_records/intergroup/handler.py`
  - `lambda_handler()`: Lambda entry point

### Shared Infrastructure Files

- **Backfill Loading**: `services/backfill/posts/load_data.py`
  - `load_posts_to_backfill()`: Loads unprocessed posts
  - `INTEGRATIONS_LIST`: Includes `"ml_inference_intergroup"`

- **Orchestration**: `services/ml_inference/helper.py`
  - `orchestrate_classification()`: Shared orchestration logic
  - `get_posts_to_classify()`: Queue loading with filtering

- **Export**: `services/ml_inference/export_data.py`
  - `write_posts_to_cache()`: Writes to output queues
  - `return_failed_labels_to_input_queue()`: Re-queues failures

- **Write Cache**: `services/write_cache_buffers_to_db/helper.py`
  - `write_cache_buffer_queue_to_db()`: Exports to permanent storage
  - `SERVICES_TO_WRITE`: Includes `"ml_inference_intergroup"`

- **Queue Constants**: `lib/db/queue_constants.py`
  - `input_ml_inference_intergroup`: Input queue name
  - `output_ml_inference_intergroup`: Output queue name

- **Pipeline Registry**: `lib/pipeline_invocation/registry.py`
  - Registers intergroup handler for service discovery

## Queue Names

For intergroup (and other ML inference services), queue names follow this pattern:

- **Input Queue**: `input_ml_inference_intergroup`
  - Stores posts waiting to be classified
  - Posts are removed after successful classification

- **Output Queue**: `output_ml_inference_intergroup`
  - Stores successfully classified posts
  - Posts are removed after writing to permanent storage

## Data Models

### Input Model
- **Queue Format**: `dict` with fields: `uri`, `text`, `preprocessing_timestamp`, `batch_id`, `batch_metadata`
- **Classification Format**: `PostToLabelModel` (Pydantic model)

### Output Model
- **Classification Result**: `IntergroupLabelModel` (service-specific)
- **Metadata**: `BatchClassificationMetadataModel` (shared across services)
- **Session Summary**: `ClassificationSessionModel` (orchestration return type)

## Integration Checklist

To integrate a new ML inference service (like intergroup), ensure:

1. ✅ Service added to `INTEGRATIONS_LIST` in `services/backfill/posts/load_data.py`
2. ✅ Queue names added to `lib/db/queue_constants.py`
3. ✅ Service added to `INTEGRATION_MAP` and `DEFAULT_INTEGRATION_KWARGS` in `pipelines/backfill_records_coordination/app.py`
4. ✅ Service added to CLI `--integration` choices in `app.py`
5. ✅ Service added to `SERVICES_TO_WRITE` in `services/write_cache_buffers_to_db/helper.py`
6. ✅ Service added to CLI `--write-cache` choices in `app.py`
7. ✅ Pipeline handler created and registered in `lib/pipeline_invocation/registry.py`
8. ✅ Queue mappings added to `services/ml_inference/helper.py` and `export_data.py`
9. ✅ `InferenceConfig` type hints updated in `services/ml_inference/config.py`
10. ✅ Export functions support the service (Literal types updated)
