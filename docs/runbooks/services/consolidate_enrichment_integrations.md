# Runbook: consolidate_enrichment_integrations

## Purpose in the study

This job produces one enriched row per URI by merging preprocessed posts with Perspective API, sociopolitical classifier, and optional similarity-score annotations, exporting `consolidated_enriched_post_records` for downstream feed features and recording high-water timestamps in DynamoDB for incremental merges.

For architecture diagrams, file-level behavior, Athena table name, and pipeline paths, see [`services/consolidate_enrichment_integrations/README.md`](../../../services/consolidate_enrichment_integrations/README.md).

## Dependencies

### Prefect — Production data pipeline

[`orchestration/data_pipeline.py`](../../../orchestration/data_pipeline.py): `consolidate_enrichment_integrations` submits SLURM only after preprocessing and after `calculate_superposters`, `run_ml_inference_perspective_api`, `run_ml_inference_sociopolitical`, and `run_ml_inference_ime` complete (graph order in README).

### Northwestern Quest / SLURM

[`pipelines/consolidate_enrichment_integrations/submit_job.sh`](../../../pipelines/consolidate_enrichment_integrations/submit_job.sh) runs [`handler.py`](../../../pipelines/consolidate_enrichment_integrations/handler.py).

### AWS similarity scores

Athena table `post_cosine_similarity_scores` backs `vector_embeddings/similarity_scores/`, which may contain multiple Parquet objects per pipeline tick (legacy exact cosine batches plus `{timestamp}_ann_topk.parquet` from the offline ANN path). Consolidation builds `{uri: score}` in Python; which row wins for duplicate URIs depends on Athena row order, so overlapping legacy and ANN exports may yield ambiguous `similarity_score`—see [`services/generate_vector_embeddings/README.md`](../../../services/generate_vector_embeddings/README.md).

### AWS similarity scores — incremental watermark

When consolidating incrementally, rows are filtered with `insert_timestamp > timestamp` against Athena results (`loaders.py`); stale watermarks can hide newer similarity shards.

### AWS session bookkeeping

DynamoDB table `enrichment_consolidation_sessions` (written each run); used to derive `latest_timestamp` semantics for loads when not backfilling.

### Local study storage

`lib.db.manage_local_data` read/write paths for `preprocessed_posts`, `ml_inference_perspective_api`, `ml_inference_sociopolitical`, and `consolidated_enriched_post_records` — same broad layout as other study services (`compact_all_services` compacts unrelated local datasets separately).

### Optional Lambda deployment

Terraform `consolidate_enrichment_integrations_lambda` (ECR image in repo); operational behavior parallels the SLURM handler.

## Failure modes

| Symptom | Likely cause |
|---------|----------------|
| SLURM or Lambda exits non‑zero | Unhandled Python exception (`helper`/`loaders`), OOM, or missing env / `PYTHONPATH` |
| Athena query errors | Table or catalog mismatch, IAM, or malformed filter when `timestamp` string is interpolated |
| Empty or unexpectedly small `consolidated_enriched_post_records` export | All candidate URIs already in prior consolidated output (URI skip logic), stale `enrichment_consolidation_timestamp` in DynamoDB shrinking visible inputs, or empty upstream preprocess / ML shards |

## Recovery

1. Inspect SLURM `#SBATCH --output` in [`submit_job.sh`](../../../pipelines/consolidate_enrichment_integrations/submit_job.sh) or Lambda / CloudWatch logs for the traceback.
2. Confirm DynamoDB still has sane sessions and that the latest `enrichment_consolidation_timestamp` matches expectation (too new can starve Athena similarity rows filtered by `insert_timestamp > timestamp`).
3. Verify Athena `post_cosine_similarity_scores` is queryable from the runner’s credential path and reflects recent ingestion (including any `_ann_topk` objects). If `similarity_score` looks wrong for a URI, check for duplicate rows across Parquet files under the same prefix.
4. Re-run `handler` with `backfill_period` / `backfill_duration` (see README) only after confirming how far back watermark-based loads should rewind — backfill overrides the Dynamo watermark window for preprocess and ML inference local loads (`helper.py`).
5. Avoid overlapping concurrent runs sharing the same local `consolidated_enriched_post_records` layout if your environment does not tolerate parallel writers.

Downstream freshness for ranking depends on consolidated output correctness; `rank_score_feeds` loads via `load_enriched_posts`. Local shard hygiene is governed by `compact_all_services`, described in [`docs/runbooks/services/compact_all_services.md`](./compact_all_services.md).
