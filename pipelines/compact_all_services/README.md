# Compact All Services

Pipeline for compacting local partitioned datasets across configured services.

## Overview

**Scheduled production runs** ([`orchestration/compaction_pipeline.py`](../../orchestration/compaction_pipeline.py)) execute **local storage compaction only**: reload each service from disk, rewrite via [`export_data_to_local_storage`](../../lib/db/manage_local_data.py), then delete superseded files. Implementation lives in [`services/compact_all_services/local_compaction.py`](../../services/compact_all_services/local_compaction.py).

**Legacy / manual S3 compaction** (Athena query → JSONL under `compacted/` → delete raw S3 keys) remains in [`services/compact_all_services/s3_compaction.py`](../../services/compact_all_services/s3_compaction.py). It is **not** invoked by this pipeline’s handler.

**S3 → local migration** for ad hoc backfills is in [`services/compact_all_services/migration.py`](../../services/compact_all_services/migration.py).

## Local compaction behavior

- Loads all partitions for the service from local storage.
- Exports consolidated layout (special cases: `preprocessed_posts` and ML inference datasets split by `source`; `study_user_activity` filtered by-record type).
- Deletes filenames captured before export and prunes empty directories.

## Services handled (local list)

See `LOCAL_COMPACTION_SERVICE_NAMES` in [`local_compaction.py`](../../services/compact_all_services/local_compaction.py).

## Usage

- **HPC:** [`submit_job.sh`](submit_job.sh) runs [`handler.py`](handler.py).
- **Orchestration:** Prefect flow in [`orchestration/compaction_pipeline.py`](../../orchestration/compaction_pipeline.py); `snapshot_data` waits for this job.

## Related

- **Session log compaction** (different pipeline): [`pipelines/compact_user_session_logs/`](../compact_user_session_logs/).
