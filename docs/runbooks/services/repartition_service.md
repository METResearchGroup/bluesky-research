# Runbook: Repartition service

## Purpose (study context)

The repartition service migrates partitioned on-disk parquet for keyed services declared in [`MAP_SERVICE_TO_METADATA`](lib/db/service_constants.py). Per partition date it backs up the tree, verifies backup and temp exports, deletes the original only after both verify steps succeed, then re-exports via [`export_data_to_local_storage`](lib/db/manage_local_data.py) with metadata where `timestamp_field` is swapped to `new_service_partition_key`. Sequential and parallel modes share the same per-date semantics; parallel batches dates across worker processes (`use_parallel=true` payload / helper flag).

## Path layout (contract)

Paths come from [`get_service_paths`](services/repartition_service/helper.py):

- **Original**: `{local_prefix}/cache/partition_date=<YYYY-MM-DD>` where `local_prefix` is `MAP_SERVICE_TO_METADATA[service]["local_prefix"]` (basename used for sibling folders).
- **Backup**: `{root_local_data_directory}/old_{basename(local_prefix)}/cache/partition_date=<date>`
- **Temp**: `{root_local_data_directory}/tmp_{basename(local_prefix)}/cache/partition_date=<date>`

`root_local_data_directory` / data roots follow [`lib.constants`](lib/constants.py). Operators must verify these roots before running destructive work.

## How to run (local example)

From repository root:

```bash
PYTHONPATH=. uv run python -m services.repartition_service.main
```

Override behavior by editing the payload in `main`'s `if __name__ == "__main__"` block or by importing `repartition_service` from another runner. Typical payload keys: `service` (required), `start_date`, `end_date`, `new_service_partition_key`, `exclude_partition_dates`, `use_parallel`.

## Failure modes & recovery

- **Verification mismatch**: If backup vs source or temp vs source dataframe checks fail, the run fails that date without completing export; original may still exist. Inspect logs for `VerificationError` / FAILED `OperationResult`. Roll forward by fixing data/code and re-running the affected dates.
- **Interrupted run**: A partial backup may exist under `old_*`; temp may linger under `tmp_*`. Temporary paths are cleared in [`cleanup_temp_files`](services/repartition_service/helper.py) in the per-date `finally` when paths were computed; interrupted runs mid-stream may leave `tmp_*` or partial exports—inspect filesystem before retrying.
- **Parallel worker / chunk failures**: Chunk-level exceptions are logged; failed dates feed [`recover_failed_chunks`](services/repartition_service/parallel_processing.py) retries. Persisting failures retain last `OperationResult` per date—treat parallel output as authoritative per-date status.
- **Rollback anchor**: Recover from a known-good backup path `old_{basename(local_prefix)}` by copying or re-pointing ingestion back after operator validation (project-specific ingest rules apply).

## Escalation

If disk layout differs from documented `MAP_SERVICE_TO_METADATA.local_prefix`, stop until metadata is corrected. If parallelism causes suspected corruption, switch to sequential `use_parallel: false`, capture logs, then compare counts against backup before further writes.
