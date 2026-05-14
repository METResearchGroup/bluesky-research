# Runbook: compact_all_services

## Purpose

Periodically **rewrites local partitioned datasets** for configured study services so storage stays compact and consistent. Runs as part of the **Compaction** Prefect flow, before snapshotting.

## Operating model

1. Prefect [`compaction_pipeline.py`](../../orchestration/compaction_pipeline.py): task `compact_all_services` submits SLURM [`pipelines/compact_all_services/submit_job.sh`](../../pipelines/compact_all_services/submit_job.sh).
2. The job runs [`pipelines/compact_all_services/handler.py`](../../pipelines/compact_all_services/handler.py), which calls `compact_all_local_services()` in [`services/compact_all_services/local_compaction.py`](../../services/compact_all_services/local_compaction.py).
3. Task `snapshot_data` waits on successful completion of compaction.

Cron is defined where the flow is deployed (e.g. `compaction_pipeline.serve(...)` in the same module).

## Logs

SLURM stdout/stderr path is set in [`submit_job.sh`](../../pipelines/compact_all_services/submit_job.sh) (`#SBATCH --output=...`).

## Failure modes

- **Job non-zero exit:** Handler logs the exception; check SLURM log and application stderr.
- **Partial deletes:** Logic records filenames **before** export, then deletes those paths after export. An interrupted run after export but before delete can leave duplicate-era files; rerunning compaction should converge if exports are deterministic.
- **Empty service:** For `preprocessed_posts` and ML inference services, an empty frame logs a warning and returns without export/delete.

## Recovery

1. Inspect SLURM log and Prefect run for the failing task.
2. Fix underlying storage or dependency issue (disk, permissions, corrupt parquet).
3. Re-submit the compaction job manually if needed (`sbatch pipelines/compact_all_services/submit_job.sh` on the cluster), then confirm `snapshot_data` or downstream steps.

## Scope note

This runbook covers **local** compaction only. Athena/S3 compaction and S3→local migration live under [`s3_compaction.py`](../../services/compact_all_services/s3_compaction.py) and [`migration.py`](../../services/compact_all_services/migration.py) and are typically manual.

## Escalation

If compaction repeatedly fails for one service, narrow to that service in a dev clone (temporary change to `LOCAL_COMPACTION_SERVICE_NAMES` or a one-off script) and capture stack traces plus a sample of the local partition tree.
