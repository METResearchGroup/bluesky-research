# Runbook: generate_vector_embeddings

## Purpose in the study

This job produces offline Transformer embeddings, FAISS ANN artifacts over cached post vectors, a query-vector JSON (global most-liked centroid), and Parquet similarity rows compatible with `PostSimilarityScoreModel` for Athena and downstream consolidation.

For architecture, module map, S3 prefixes, and diagrams, see [`services/generate_vector_embeddings/README.md`](../../../services/generate_vector_embeddings/README.md).

## Dependencies

### Prefect — Vector embeddings pipeline

[`orchestration/vector_embeddings_pipeline.py`](../../../orchestration/vector_embeddings_pipeline.py): task `generate_vector_embeddings` (GPU-oriented workers; model may fall back to CPU unless `VECTOR_EMBEDDINGS_REQUIRE_GPU` is set).

### Production data pipeline (upstream data)

Preprocessed posts consumed via `lib.db.manage_local_data.load_latest_data` (`service="generate_vector_embeddings"`); embeddings pipeline runs after preprocessing material exists (see [`services/README.md`](../../../services/README.md) DAG overview).

### Northwestern Quest / SLURM / Lambda

Entrypoint [`pipelines/generate_vector_embeddings/handler.py`](../../../pipelines/generate_vector_embeddings/handler.py) calls `run_vector_embedding_offline_pipeline()` (which wraps `do_vector_embeddings()` plus ANN/query/similarity stages).

### AWS S3

Writes under `vector_embeddings/` (versioned `post_embeddings/`, legacy `in_network_*` / `most_liked_*`, `similarity_scores/`, `average_most_liked_feed_embeddings/`, `ann_indices/`, `query_embeddings/`). See service README for the full layout.

### AWS DynamoDB

Table `vector_embedding_sessions` stores embedding batch metadata (`embedding_timestamp`, `s3_keys`, model/revision/schema fields).

### Python extras

`faiss-cpu` is declared under the `ml` optional dependency group in `pyproject.toml` (CI installs `--extra ml`).

### Downstream

Glue/Athena `post_cosine_similarity_scores` scans `vector_embeddings/similarity_scores/`; [`consolidate_enrichment_integrations`](./consolidate_enrichment_integrations.md) merges URIs with `insert_timestamp` filters.

## Failure modes

| Symptom | Likely cause |
| --- | --- |
| Job exits non‑zero | Uncaught exception in `helper`, missing `PYTHONPATH`, OOM on GPU/CPU, or S3/Dynamo permission failures mid-write |
| No embedding session returned | No posts to embed after incremental filters, or empty preprocess slice for this tick |
| ANN stages skipped | No Parquet objects under `post_embeddings/{schema}/`, or empty concatenated embedding matrix |
| Query JSON / ANN similarity skipped | Missing `average_most_liked_feed_embeddings` key (placeholder string when no most-liked batch), or unreadable centroid Parquet |
| FAISS / import errors | Environment missing `ml` extras (`faiss-cpu`, Torch stack) on the runner |

## Recovery

1. Inspect SLURM stdout/stderr or Lambda/CloudWatch logs for the traceback from [`pipelines/generate_vector_embeddings/handler.py`](../../../pipelines/generate_vector_embeddings/handler.py).
2. Confirm DynamoDB `vector_embedding_sessions` rows and that `s3_keys` match objects present in S3 for the same `embedding_timestamp`.
3. Verify S3 listings under `vector_embeddings/post_embeddings/` and `vector_embeddings/ann_indices/` for the schema version in use (default `v1` in code unless changed).
4. For consolidation freshness issues involving duplicate URIs across multiple similarity Parquet objects under `similarity_scores/`, see [`docs/runbooks/services/consolidate_enrichment_integrations.md`](./consolidate_enrichment_integrations.md) (ordering / merge semantics).
5. Re-run the pipeline after fixing upstream preprocess availability or AWS credential paths; avoid overlapping writers corrupting the same Dynamo session semantics if your deployment assumes single-writer batches.
