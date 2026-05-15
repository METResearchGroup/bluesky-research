# Pipelines

This directory holds the pipelines for the project. The orchestration DAGs run the pipeline code, and each pipeline exposes a `handler.py` that is a thin indirection layer to the `services/` logic (this is a remnant of a deprecated architectural decision around deploying each service as a lambda; else we would just remove the `pipelines/` folder and have the DAGs trigger from `services/` directly).

| Category | What belongs here |
| --- | --- |
| Production pipeline | Job directories reached from Prefect flows under [`orchestration/`](../orchestration/). Flows call `run_slurm_job` on the `submit_job.sh` (or equivalent) for these paths. |
| Everything else | Backfill and coordination jobs, optional / research classifiers, and other handlers kept off the scheduled production DAGs. |

DAG relationships and task order are summarized in [`orchestration/README.md`](../orchestration/README.md).

---

## Production pipeline

| Pipeline | Related DAG | Related `services/` package |
| --- | --- | --- |
| [`sync_post_records/firehose/`](sync_post_records/firehose/) (ingest: `submit_job.sh`) | Sync pipeline — [`sync_pipeline.py`](../orchestration/sync_pipeline.py) (`sync_data_pipeline`), task `sync_firehose` | [`sync/stream/`](../services/sync/stream/README.md) |
| [`sync_post_records/firehose/`](sync_post_records/firehose/) (persistence: `submit_firehose_writes_job.sh`) | Sync pipeline — [`sync_pipeline.py`](../orchestration/sync_pipeline.py), task `write_firehose_data` | [`sync/jetstream/`](../services/sync/README.md) *(see [`sync/README.md`](../services/sync/README.md))* |
| [`sync_post_records/most_liked/`](sync_post_records/most_liked/) | Integrations Sync Pipeline — [`integrations_sync_pipeline.py`](../orchestration/integrations_sync_pipeline.py), task `sync_most_liked` | [`sync/most_liked_posts/`](../services/sync/README.md) *(see [`sync/README.md`](../services/sync/README.md))* |
| [`preprocess_raw_data/`](preprocess_raw_data/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `preprocess_raw_data` | [`preprocess_raw_data/`](../services/preprocess_raw_data/README.md) |
| [`calculate_superposters/`](calculate_superposters/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `calculate_superposters` | [`calculate_superposters/`](../services/calculate_superposters/README.md) |
| [`classify_records/perspective_api/`](classify_records/perspective_api/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `run_ml_inference_perspective_api` | [`ml_inference/perspective_api/`](../services/ml_inference/perspective_api/README.md) |
| [`classify_records/sociopolitical/`](classify_records/sociopolitical/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `run_ml_inference_sociopolitical` | [`ml_inference/sociopolitical/`](../services/ml_inference/sociopolitical/README.md) |
| [`classify_records/ime/`](classify_records/ime/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `run_ml_inference_ime` | [`ml_inference/ime/`](../services/ml_inference/ime/README.md) |
| [`consolidate_enrichment_integrations/`](consolidate_enrichment_integrations/) | Production data pipeline — [`data_pipeline.py`](../orchestration/data_pipeline.py), task `consolidate_enrichment_integrations` | [`consolidate_enrichment_integrations/`](../services/consolidate_enrichment_integrations/README.md) |
| [`generate_vector_embeddings/`](generate_vector_embeddings/) | Vector embeddings pipeline — [`vector_embeddings_pipeline.py`](../orchestration/vector_embeddings_pipeline.py), task `generate_vector_embeddings` | [`generate_vector_embeddings/`](../services/generate_vector_embeddings/README.md) |
| [`rank_score_feeds/`](rank_score_feeds/) | Recommendation pipeline — [`recommendation_pipeline.py`](../orchestration/recommendation_pipeline.py), task `rank_score_feeds` | [`rank_score_feeds/`](../services/rank_score_feeds/README.md) |
| [`compact_all_services/`](compact_all_services/) | Compaction pipeline — [`compaction_pipeline.py`](../orchestration/compaction_pipeline.py), task `compact_all_services` | [`compact_all_services/`](../services/compact_all_services/README.md) |
| [`snapshot_data/`](snapshot_data/) | Compaction pipeline — [`compaction_pipeline.py`](../orchestration/compaction_pipeline.py), task `snapshot_data` | [`snapshot_data/`](../services/snapshot_data/README.md) |
| [`compact_user_session_logs/`](compact_user_session_logs/) | Analytics pipeline — [`analytics_pipeline.py`](../orchestration/analytics_pipeline.py), task `compact_user_session_logs` | [`compact_user_session_logs/`](../services/compact_user_session_logs/README.md) |
| [`aggregate_study_user_activities/`](aggregate_study_user_activities/) | Analytics pipeline — [`analytics_pipeline.py`](../orchestration/analytics_pipeline.py), task `aggregate_study_user_activities` | [`aggregate_study_user_activities/`](../services/aggregate_study_user_activities/) *(no README)* |

---

## Everything else

| Pipeline | Related DAG | Related `services/` package |
| --- | --- | --- |
| [`backfill_records_coordination/`](backfill_records_coordination/) | Not part of a scheduled Prefect DAG; coordination / integration-specific backfill entrypoints | [`backfill/`](../services/backfill/README.md) |
| [`backfill_sync/`](backfill_sync/) | Not part of a scheduled Prefect DAG; invoked for historical record sync | [`backfill/`](../services/backfill/README.md) |
| [`classify_records/valence_classifier/`](classify_records/valence_classifier/) | Not on the production data pipeline; run via backfill / ad hoc jobs | [`ml_inference/valence_classifier/`](../services/ml_inference/valence_classifier/README.md) |
| [`classify_records/intergroup/`](classify_records/intergroup/) | Not on the production data pipeline; research / dedicated jobs | [`ml_inference/intergroup/`](../services/ml_inference/intergroup/README.md) |
