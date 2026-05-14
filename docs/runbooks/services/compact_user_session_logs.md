# Compact user session logs — runbook

## Purpose in the study

User session logs record how study participants interact with the custom feed (timestamps, cursor, feed slice metadata). Raw shards land in S3; this service **merges multiple objects per day** into a **single compacted JSONL** per `partition_date`, keeping Athena/Glue aligned. It also **backfills** rows from **CloudWatch** when the feed API logged eligible study DIDs that did not appear in the normal session log path.

Downstream **`aggregate_study_user_activities`** is scheduled to run **after** this job succeeds (see [`orchestration/analytics_pipeline.py`](../../orchestration/analytics_pipeline.py)).

## Operating model

- **Orchestration:** Prefect flow **`analytics_pipeline`** (`compact_user_session_logs` task → **`aggregate_study_user_activities`** with a dependency on the first).
- **When deployed via `serve`:** `cron="0 8 * * *"` in `analytics_pipeline.py` (UTC unless your Prefect deployment overrides time zone).
- **Compute:** Northwestern Quest — SLURM script [`pipelines/compact_user_session_logs/submit_job.sh`](../../pipelines/compact_user_session_logs/submit_job.sh) runs [`pipelines/compact_user_session_logs/handler.py`](../../pipelines/compact_user_session_logs/handler.py).

## Configuration

Operational literals (S3 root prefix, Glue crawler, Athena SQL, CloudWatch group/stream and Insights query, backfill column defaults) live in [`services/compact_user_session_logs/constants.py`](../../services/compact_user_session_logs/constants.py). Change that file when:

- The feed API moves to a new EC2 instance (**log stream** in CloudWatch changes).
- Glue crawler or Athena table names change (must stay consistent with the study catalog).

## AWS dependencies

| Resource | Role |
|----------|------|
| **S3** | Prefix `user_session_logs/` (and `partition_date=*` subfolders); read listing, write `compacted_*.jsonl` / `backfill_*.jsonl`, delete non-compacted sources after merge. |
| **Glue** | Crawler `user_session_logs_glue_crawler` — run before Athena read and after writes so `user_session_logs` is fresh. |
| **Athena** | Table `user_session_logs`; query `SELECT * FROM user_session_logs` for dedupe source of truth. |
| **CloudWatch Logs Insights** | Log group / stream in `constants.py`; query filters feed API lines used for backfill. |
| **DynamoDB** | Indirect: `get_all_users()` (participant data) resolves study DIDs during CloudWatch backfill **when `main()` runs**, not at import time. |

## Failure modes

| Symptom | Likely cause | What to do |
|---------|----------------|------------|
| SLURM job fails immediately | Python env / `PYTHONPATH` / handler exception | Check SLURM stderr log path in `submit_job.sh`; reproduce with `PYTHONPATH=<repo>` and `uv run python pipelines/compact_user_session_logs/handler.py` in a dev shell with credentials. |
| Glue crawler errors or long stall | Catalog drift, permissions, or concurrent crawler runs | AWS console → Glue → Crawlers; check last run. Avoid running multiple compaction jobs against the same crawler simultaneously. |
| `ValueError` for partition with S3 keys but empty Athena slice | Catalog lag or partition mismatch | Confirm crawler completed; confirm Athena partition values match S3 `partition_date=` folders. |
| CloudWatch backfill empty | No matching log lines in lookback window | Expected if no study DIDs matched the Insights filter; verify log group/stream in `constants.py` still match the running feed host. |

## Recovery

- **Re-run** the Prefect task or SLURM job after fixing config — compaction is intended as a **single concurrent** batch: avoid overlapping runs on the same day’s prefixes if possible.
- **Data:** If deletion after compact fails mid-way, treat as an incident: inspect S3 for partial compacted file + leftover shards; restore from versioning/backup if your bucket policy allows.

## Observability

- Application logs from `helper.py` / CloudWatch backfill module (logger name under `services.compact_user_session_logs`).
- SLURM stdout/stderr path: see `#SBATCH --output` in `submit_job.sh`.

## Tests

```bash
uv run pytest services/compact_user_session_logs/tests -v --import-mode=importlib
```

## Escalation

- **Feed API / log stream churn:** coordinate with whoever operates the EC2 feed host; update `constants.py` stream ID.
- **Athena/Glue ownership:** data platform / whoever maintains the `user_session_logs` table definition.
