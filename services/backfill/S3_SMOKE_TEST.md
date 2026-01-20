# S3 backfill smoke test (DuckDB + Parquet)

This repo supports reading study Parquet datasets directly from S3 via DuckDB.

## Prereqs

- **AWS credentials available** to the runtime (any of):
  - `AWS_PROFILE` pointing at a profile in `~/.aws/credentials`
  - `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN`)
  - IAM role (if running in AWS)
- Network access to download DuckDB extensions on first run (DuckDB `INSTALL httpfs`).

## Quick smoke test (single partition date)

Run from repo root:

```bash
PYTHONPATH=. python - <<'PY'
from lib.db.manage_s3_data import S3ParquetBackend, S3ParquetDatasetRef

backend = S3ParquetBackend()
df = backend.query_dataset_as_df(
    dataset=S3ParquetDatasetRef(dataset="preprocessed_posts"),
    storage_tiers=["cache"],
    partition_date="2024-11-13",
    query="SELECT uri, text, preprocessing_timestamp FROM preprocessed_posts WHERE text IS NOT NULL AND text != '' LIMIT 5",
    query_metadata={
        "tables": [
            {
                "name": "preprocessed_posts",
                "columns": ["uri", "text", "preprocessing_timestamp"],
            }
        ]
    },
)
print(df.head())
print("rows:", len(df))
PY
```

## Troubleshooting

- **`Failed to install httpfs`**: the runtime may not allow extension downloads; pre-bundle extensions or run where downloads are permitted.
- **Auth errors** (e.g., 403/AccessDenied): ensure DuckDB can see your AWS creds (try exporting `AWS_PROFILE`, or using env vars).
- **Region issues**: set `AWS_REGION` or `AWS_DEFAULT_REGION` if your environment doesn’t supply it.
