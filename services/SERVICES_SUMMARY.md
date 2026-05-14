# Services summary

Top-level folders under `services/` are summarized below. **Entryway** is the file most useful as a starting point when tracing how the service runs in production (`pipelines/` handlers and Lambda entrypoints) or how it is invoked locally (`helper.py`, CLIs, or standalone scripts).

| Service | Description | Entryway |
| --- | --- | --- |
| `aggregate_study_user_activities` | Loads study-participant activity across sources (for example via Athena) and builds consolidated analytics-oriented datasets partitioned by date. | `pipelines/aggregate_study_user_activities/handler.py` |
| `backfill` | Supports historical label generation: enqueueing posts missing ML labels, running integration runners against queues, writing cache buffers to storage, and separate PDS-oriented backfill helpers. | `pipelines/backfill_records_coordination/app.py` |
| `calculate_analytics` | Operational tooling (for example per-integration record counts over date ranges) plus study-centric and one-off research analyses under `analyses/` and `study_analytics/`. | `services/calculate_analytics/count_records_for_integration.py` |
| `calculate_superposters` | Computes users who post at unusually high volume so feeds can down-rank or otherwise treat superposters differently. | `pipelines/calculate_superposters/handler.py` |
| `compact_all_services` | Walks configured service datasets and compacts storage (for example merging partition files) across the stack as part of storage hygiene. | `pipelines/compact_all_services/handler.py` |
| `compact_dedupe_data` | **Deprecated.** Older compaction and deduplication path backed by DynamoDB session tracking and Athena/Glue. | `pipelines/deprecated/compact_dedupe_data/handler.py` |
| `compact_user_session_logs` | Compacts raw application session/interaction logs into forms suitable for downstream analytics. | `pipelines/compact_user_session_logs/handler.py` |
| `consolidate_enrichment_integrations` | Merges preprocessed posts with outputs from ML integrations (labels, optional similarity scores) into consolidated enriched post records. | `pipelines/consolidate_enrichment_integrations/handler.py` |
| `consolidate_post_records` | Normalizes firehose, feed, and related post shapes into one consolidated post schema for downstream use. | `services/consolidate_post_records/helper.py` |
| `deprecated` | Archived one-off pipelines and services (legacy feeds, engagement updates, training-data helpers, etc.) kept for reference, not current production. | `services/README.md` (Deprecated Services) |
| `fetch_posts_used_in_feeds` | Given serialized feeds (by day), resolves and persists the underlying posts that appeared in those feeds. | `services/fetch_posts_used_in_feeds/helper.py` |
| `generate_vector_embeddings` | Embeds preprocessed posts with a transformer model and derives similarity-style scores for research or enrichment workflows. | [`orchestration/vector_embeddings_pipeline.py`](../orchestration/vector_embeddings_pipeline.py) (Prefect DAG); `pipelines/generate_vector_embeddings/handler.py` |
| `get_author_to_average_toxicity_outrage` | Aggregates Perspective-style toxicity and outrage signals to per-author averages for a partition date. | `services/get_author_to_average_toxicity_outrage/helper.py` |
| `get_pipeline_analytics` | Planned home for pipeline telemetry (for example daily counts per data repo); currently documented intent only. | `services/get_pipeline_analytics/README.md` |
| `get_posts_liked_by_study_users` | Matches study users’ likes (from PDS backfill outputs) to stored posts over a lookback window and writes aligned post datasets. | `services/get_posts_liked_by_study_users/helper.py` |
| `get_preprocessed_posts_used_in_feeds` | Joins “posts used in feeds” with `preprocessed_posts` so you can analyze labels for content that actually surfaced in feeds. | `services/get_preprocessed_posts_used_in_feeds/helper.py` |
| `ml_inference` | Queue-driven classification integrations (Perspective, sociopolitical, IME, valence, intergroup, etc.) sharing common batching and session metadata patterns. | `services/ml_inference/helper.py` |
| `participant_data` | Reads and writes study participant profiles (for example DynamoDB `study_participants`) used across pipelines and research tooling. | `services/participant_data/helper.py` |
| `preprocess_raw_data` | Dequeues raw synced posts, runs filtering and enrichment steps (spam/NSFW/language and related), and feeds ML integration queues. | `pipelines/preprocess_raw_data/handler.py` |
| `rank_score_feeds` | End-to-end personalized feed generation: load enriched posts and context, score, rank/rerank, and export feeds via the orchestrator wiring. | `services/rank_score_feeds/orchestrator.py` |
| `repartition_service` | Safely migrates on-disk partition layouts between schemes with backups, staging, and verification steps. | `services/repartition_service/helper.py` |
| `snapshot_data` | Copies designated active datasets into cache/backup locations for point-in-time snapshots. | `pipelines/snapshot_data/handler.py` |
| `sync` | Ingests Bluesky data via the live firehose app (`stream`), optional Jetstream CLI ingestion, curated most-liked feeds, and search pagination helpers. | `pipelines/sync_post_records/firehose/handler.py` |
| `write_cache_buffers_to_db` | Drains integration/backfill cache queues and exports batches into persistent local/tabular storage (supplemented by backfill-specific writers in `services/backfill/`). | `services/write_cache_buffers_to_db/helper.py` |

## Related pipeline entrypoints (same logical service)

Some services are invoked from more than one place; the table picks one primary entryway. Other common triggers:

- **`sync`**: `pipelines/sync_post_records/most_liked/handler.py` (trending feeds); `services/sync/stream/app.py` (firehose Flask runtime); `services/sync/jetstream/jetstream_cli.py` (Jetstream CLI).
- **`ml_inference`**: `pipelines/classify_records/*/handler.py` (per-integration Lambda handlers); integration modules under `services/ml_inference/<integration>/`.
- **`rank_score_feeds`**: `pipelines/rank_score_feeds/handler.py` (Lambda handler; intended to call into this service).
- **`generate_vector_embeddings`**: [`orchestration/vector_embeddings_pipeline.py`](../orchestration/vector_embeddings_pipeline.py) (scheduled Prefect flow submitting `pipelines/generate_vector_embeddings/submit_job.sh`).
