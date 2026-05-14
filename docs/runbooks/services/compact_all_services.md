# Runbook: compact_all_services

## Purpose in the study

**Scheduled local compaction** rewrites partitioned study datasets on disk (via `export_data_to_local_storage`) so shards stay merged and old files are pruned. That keeps downstream analysis and snapshots working against a tidy local layout. It runs inside the **Compaction** Prefect flow before **`snapshot_data`** waits on completion.

Manual **S3/Athena compaction** and **S3→local migration** are separate operator workflows and are not executed by the scheduled handler.

For mermaid diagrams, key modules, pipeline paths, `LOCAL_COMPACTION_SERVICE_NAMES` behavior, tests, and how manual AWS scripts fit in, see [`services/compact_all_services/README.md`](../../services/compact_all_services/README.md).

## Dependencies

- **Prefect** — [`orchestration/compaction_pipeline.py`](../../orchestration/compaction_pipeline.py): `compact_all_services` task submits SLURM; `snapshot_data` depends on successful compaction (per README).
- **Northwestern Quest / SLURM** — [`pipelines/compact_all_services/submit_job.sh`](../../pipelines/compact_all_services/submit_job.sh), [`pipelines/compact_all_services/handler.py`](../../pipelines/compact_all_services/handler.py) → `compact_all_local_services()` and [`lib/db/manage_local_data`](../../lib/db/manage_local_data) load/export/delete paths.
- **Local filesystem** — study partition tree that compaction reads and rewrites (same layout as in README / `local_compaction.py`).
- **Optional manual track** — Athena, Glue, S3, DynamoDB (`compaction_sessions`) when running [`services/compact_all_services/s3_compaction.py`](../../services/compact_all_services/s3_compaction.py) or [`migration.py`](../../services/compact_all_services/migration.py); see README.

## Failure modes

| Symptom | Likely cause |
|---------|----------------|
| SLURM job non-zero exit | Uncaught exception in handler or OOM; see `#SBATCH --output` in `submit_job.sh` |
| Prefect task stuck / failed | `run_slurm_job` timeout or cluster issue; same logs as above |
| Duplicate or orphaned files after a crash | Run interrupted **after** export but **before** deletes; compaction is designed to delete pre-export filenames — see README flow |
| Warn-only “empty service” | `preprocessed_posts` or ML inference frame empty — logged warning, no export (per README) |

## Recovery

1. Read the SLURM log path configured in [`submit_job.sh`](../../pipelines/compact_all_services/submit_job.sh) and the Prefect run for `compact_all_services` / `snapshot_data`.
2. Fix the underlying issue (disk space, permissions, corrupt parquet, env/`PYTHONPATH`).
3. Re-run compaction on the cluster (e.g. `sbatch pipelines/compact_all_services/submit_job.sh`), then confirm `snapshot_data` or downstream steps.
4. If one service repeatedly fails, reproduce with a narrowed `LOCAL_COMPACTION_SERVICE_NAMES` (temporary dev change or one-off script) and capture stack trace plus a sample of that service’s partition tree.

For how this differs from **user session log** compaction, see [`services/compact_user_session_logs/README.md`](../../services/compact_user_session_logs/README.md).
