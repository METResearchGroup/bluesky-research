# Generate Vector Embeddings

This pipeline runs **offline batch work only**: Transformer embedding generation,
FAISS approximate nearest-neighbour index construction, export of a global
query vector (most-liked centroid), and optional large-scale ANN similarity
materialization into Parquet under `vector_embeddings/similarity_scores/`.

## Invariants

- **No request-time inference**: feed serving or APIs must not call into
  `services.generate_vector_embeddings.helper` embedding paths.
- **Artifacts**: versioned post embeddings live under
  `vector_embeddings/post_embeddings/{embedding_schema_version}/`. ANN indexes
  live under `vector_embeddings/ann_indices/{embedding_schema_version}/{timestamp}/`.
  Query vectors are JSON under `vector_embeddings/query_embeddings/...`.
- **Compatibility**: the legacy Athena prefixes for `in_network_post_embeddings`,
  `most_liked_post_embeddings`, `similarity_scores`, and
  `average_most_liked_feed_embeddings` are still written when new posts are embedded.

## Execution

The Lambda handler invokes `run_vector_embedding_offline_pipeline()` which:

1. Runs `do_vector_embeddings()` (lazy-loads Hugging Face weights on first use).
2. Rebuilds a corpus ANN index from all Parquet batches under the active schema prefix.
3. Exports a `QueryEmbeddingModel` JSON for the latest average most-liked centroid.
4. Writes `{timestamp}_ann_topk.parquet` beside legacy similarity exports using inner-product
   scores in [-1, 1] (cosine on unit vectors). Tune breadth with env `VECTOR_EMBEDDING_ANN_TOP_K`.

Local smoke (requires AWS credentials and data):

```bash
uv run python -m pipelines.generate_vector_embeddings.handler
```
