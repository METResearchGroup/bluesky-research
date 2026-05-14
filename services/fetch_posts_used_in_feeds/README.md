# Fetch posts used in feeds

## Purpose

For each feed generation day, derives the set of post URIs that appeared in study feeds and writes them as parquet under the `fetch_posts_used_in_feeds` dataset. Upstream feeds must already be in `generated_feeds` (serialized JSON in the `feed` column, partition by day). Downstream, this table constrains which posts matter for preprocessing joins, backfill coordination, and analytics that should only consider in-feed content.

## Key Files

| File | Description |
|------|-------------|
| `helper.py` | Loads `generated_feeds` for a `partition_date`, parses `feed` JSON (including double-encoded strings via `load_feed_from_json_str`), collects unique `post["item"]` URIs, exports `PostInFeedModel` rows to local storage. Also exposes `get_and_export_posts_used_in_feeds_for_partition_dates` for date-range batch runs. |
| `models.py` | `PostInFeedModel`: `uri`, `partition_date`. |
| `main.py` | CLI-style entry: reads optional payload (`start_date`, `end_date`, `exclude_partition_dates`) and calls the batch export helper. |
| `migrate_feeds_to_db.py` | One-off migration: reads consolidated feeds at `constants.root_feeds_path` via `pd.read_parquet`, partitions by `feed_generation_timestamp`, exports into `generated_feeds`. Run before the main workflow if feeds are not yet in that store. |
| `constants.py` | `root_feeds_path` for the legacy feeds parquet (under `scripts/analytics/feeds`). |

## How the key files relate

The package is both a small batch job (`main.py`) and a library: other code imports `load_feed_from_json_str` to parse feed blobs consistently.

### Migrate legacy feeds into `generated_feeds`

```mermaid
flowchart TB
  LEG["Legacy feeds<br/>root_feeds_path<br/>(scripts/analytics/feeds)"]
  MIG["migrate_feeds_to_db.main"]
  EXP1["export_data_to_local_storage"]
  GEN["generated_feeds<br/>(partition_date from feed_generation_timestamp)"]

  LEG --> MIG
  MIG --> EXP1
  EXP1 --> GEN
```

### Export post URIs per day

```mermaid
flowchart TB
  MAIN["main.fetch_posts_used_in_feeds<br/>(optional date payload)"]
  BATCH["helper.get_and_export_posts_used_in_feeds_for_partition_dates"]
  DAY["helper.get_and_export_posts_used_in_feeds_for_partition_date"]
  LOAD["load_feeds_from_local_storage<br/>(service=generated_feeds)"]
  PARSE["load_feed_from_json_str"]
  URI["get_posts_used_in_feeds<br/>(dedupe item URIs)"]
  EXP2["export_data_to_local_storage<br/>(service=fetch_posts_used_in_feeds)"]

  MAIN --> BATCH
  BATCH --> DAY
  DAY --> LOAD
  LOAD --> PARSE
  PARSE --> URI
  URI --> EXP2
```

### Downstream

```mermaid
flowchart TB
  OUT["fetch_posts_used_in_feeds<br/>(parquet: uri + partition_date)"]
  PRE["get_preprocessed_posts_used_in_feeds<br/>join with preprocessed_posts + lookback"]
  BF["backfill LocalStorageAdapter<br/>POSTS_USED_IN_FEEDS_TABLE_NAME"]
  ANA["calculate_analytics<br/>load_feed_from_json_str imports"]

  OUT --> PRE
  OUT --> BF
  HELPER["helper.load_feed_from_json_str"] -.-> ANA
```

Dataset metadata (local prefix, S3, Glue) is registered in [`lib/db/service_constants.py`](../../lib/db/service_constants.py). Historical archive layout may appear as `archive_fetch_posts_used_in_feeds` in Terraform/Glue for the Nature paper study bucket.

## Other Files

| File | Description |
|------|-------------|
| `submit_main.sh` | SLURM job that runs `main.py` on the Quest-style cluster path. |
| `submit_migrate_feeds_to_db.sh` | SLURM job that runs `migrate_feeds_to_db.py`. |

## Related

- [`services/get_preprocessed_posts_used_in_feeds/`](../get_preprocessed_posts_used_in_feeds/) — preprocessed rows limited to posts that appear in `fetch_posts_used_in_feeds`.
- [`pipelines/backfill_records_coordination/README.md`](../../pipelines/backfill_records_coordination/README.md) — coordinates backfills and queueing; operators often run it after this dataset is populated.
- [`services/backfill/README.md`](../backfill/README.md) — backfill adapters load URIs from `fetch_posts_used_in_feeds`.
