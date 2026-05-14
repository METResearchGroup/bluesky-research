# Runbook: compact_user_session_logs

## Purpose in the study

Session logs capture how participants use the study feed; this job **consolidates** S3 JSONL under `partition_date=…` to roughly **one compacted file per day**, and **backfills** from **CloudWatch** when study DIDs showed up in feed API logs but were missing from the normal logging path. **`aggregate_study_user_activities`** is only scheduled **after** this job completes in [`orchestration/analytics_pipeline.py`](../../orchestration/analytics_pipeline.py).

For architecture diagrams, key files, pipeline/handler paths, related services (`feed_api`, `compact_all_services`), tests, and where literals are defined, see [`services/compact_user_session_logs/README.md`](../../services/compact_user_session_logs/README.md).

## Dependencies

- **Prefect** — `analytics_pipeline`: `compact_user_session_logs` task then `aggregate_study_user_activities` (dependency as in README).
- **Northwestern Quest / SLURM** — [`pipelines/compact_user_session_logs/submit_job.sh`](../../pipelines/compact_user_session_logs/submit_job.sh) runs [`handler.py`](../../pipelines/compact_user_session_logs/handler.py).
- **AWS** — S3 prefix and layout, Glue crawler, Athena table `user_session_logs`, CloudWatch Logs Insights (group/stream + query), and DynamoDB via `get_all_users()` during CloudWatch backfill. **Concrete names and strings** live in [`services/compact_user_session_logs/constants.py`](../../services/compact_user_session_logs/constants.py) (see README).

## Failure modes

| Symptom | Likely cause | What to do |
|---------|----------------|------------|
| Job exits immediately | Python env, `PYTHONPATH`, or handler exception | SLURM log from `submit_job.sh`; reproduce locally with repo root on `PYTHONPATH` and credentials |
| Glue crawler errors or hangs | Permissions, catalog drift, overlapping crawler runs | AWS Glue → Crawler history; avoid concurrent jobs on the same crawler when possible |
| `ValueError` — S3 keys exist but Athena has no rows for that `partition_date` | Crawler lag or partition mismatch | Confirm crawler finished; Athena partitions vs `partition_date=` prefixes |
| Backfill writes nothing | No matching Insights rows in the lookback window | Often expected; confirm log group/stream in `constants.py` still match the deployed feed host (README operational note) |

## Recovery

1. Fix config or AWS issues (especially **`constants.py`** when the feed host or logging layout changes — see README).
2. Re-run the Prefect task or SLURM job; treat concurrent compaction on the same calendar partitions as risky.
3. If deletion after a successful compact fails mid-flight, inspect S3 for a new compacted object plus leftover shards; use bucket versioning / backup policy if you need to roll back.

This job is **not** the same as **local** `compact_all_services`; see [`services/compact_all_services/README.md`](../../services/compact_all_services/README.md).
