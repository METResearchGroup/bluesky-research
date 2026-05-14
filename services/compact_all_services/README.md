# compact_all_services

Storage hygiene for study datasets: rewrite local partitioned exports to consolidate files and prune old shards.

## Production entry

- **HPC:** [`pipelines/compact_all_services/handler.py`](../../pipelines/compact_all_services/handler.py) calls `compact_all_local_services()` from [`local_compaction.py`](local_compaction.py).
- **Orchestration:** [`orchestration/compaction_pipeline.py`](../../orchestration/compaction_pipeline.py) submits [`pipelines/compact_all_services/submit_job.sh`](../../pipelines/compact_all_services/submit_job.sh), then runs `snapshot_data` after success.

## Modules

| Module | Role |
| --- | --- |
| [`local_compaction.py`](local_compaction.py) | **Default path:** load from local → export → delete old files. |
| [`cleanup.py`](cleanup.py) | Remove empty directories under service prefixes (also used by scripts). |
| [`s3_compaction.py`](s3_compaction.py) | Manual Athena/S3 compaction (not used by the scheduled handler). |
| [`migration.py`](migration.py) | Athena → `export_data_to_local_storage` for backfills. |

## Environment

Match other HPC jobs: repo on `PYTHONPATH`, conda env `bluesky_research`, AWS credentials where Athena/S3 paths are used (migration / S3 compaction only).

## Tests

```bash
uv run pytest services/compact_all_services/tests -v --import-mode=importlib
```

## Related

- [`pipelines/compact_user_session_logs/`](../../pipelines/compact_user_session_logs/) — separate compaction for session logs before analytics aggregation.
