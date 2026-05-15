# Plan: Add Missing Vector Embedding Generation And ANN Retrieval

## Remember

- Exact file paths always
- Exact commands with expected output
- DRY, YAGNI, TDD, frequent commits
- Maximum safely delegable parallelism
- Delegated tasks must be impossible to misread

## Overview

We need to restore and productionize the vector-embedding portion of the feed-ranking system so it matches the manuscript-level claim that the engagement-based feed used engagement prediction with approximate nearest-neighbour search and feed ranking. The current repository has the start of this system in `services/generate_vector_embeddings/`, but it only computes BERT embeddings in batch and converts each in-network post into one cosine similarity score against the average embedding of "most-liked" posts. This plan keeps the existing downstream `similarity_score` contract while replacing the missing pieces: offline precomputation, cached embedding artifacts, ANN index construction, fast retrieval, and clean integration with feed scoring.

## Paper And Repo Baseline

The paper describes the engagement-based feed at a high level: it used a machine-learning model for engagement prediction, approximate nearest-neighbour search, and a feed-ranking recommendation algorithm. The repository currently implements only a simpler proxy:

- `services/generate_vector_embeddings/helper.py` generates post embeddings with `bert-base-uncased`.
- `services/generate_vector_embeddings/helper.py` computes cosine similarity between each in-network post and the average embedding of most-liked posts.
- `services/generate_vector_embeddings/models.py` defines `PostSimilarityScoreModel`.
- `services/consolidate_enrichment_integrations/loaders.py` loads `post_cosine_similarity_scores` from Athena.
- `services/consolidate_enrichment_integrations/models.py` stores `similarity_score` on consolidated enriched post records.
- `services/rank_score_feeds/services/scoring.py` uses `similarity_score` as a fallback estimate of likeability when `like_count` is missing.
- `pipelines/generate_vector_embeddings/README.md` says the original intent was naive vector similarity search for posts similar to those previously liked by a user, but that the pipeline is not currently run in production.

The two concrete gaps to fix are:

- The surviving implementation does not build or query an ANN index, so exact similarity scoring would remain O(n) over candidate posts if used as a retrieval step.
- Embeddings must not be generated during feed requests. Embedding generation, ANN index construction, and score materialization must happen offline and be cached.

## Happy Flow

1. New preprocessed posts are produced by the existing preprocessing pipeline and made available through the data-loading path used by `services/generate_vector_embeddings/helper.py`.
2. The scheduled vector embedding pipeline at `pipelines/generate_vector_embeddings/handler.py` calls `services/generate_vector_embeddings/helper.py` to embed only posts that have not already been embedded for the active model name, revision, and embedding schema version.
3. The embedding service writes versioned post embedding artifacts to S3 under `vector_embeddings/post_embeddings/` and records a DynamoDB session in `vector_embedding_sessions`.
4. A new ANN index builder loads the latest valid post embeddings, normalizes vectors for cosine similarity, builds an ANN index, writes the index artifact plus URI mapping to S3 under `vector_embeddings/ann_indices/`, and records index metadata in DynamoDB.
5. A profile-vector step creates query vectors. The first implementation preserves the current global "most-liked centroid" behavior; a later implementation adds per-user vectors from liked, clicked, followed, or otherwise engaged posts.
6. A similarity materialization step queries the cached ANN index, converts ANN distances/scores into the existing `PostSimilarityScoreModel` shape, and writes `post_cosine_similarity_scores`-compatible rows.
7. `services/consolidate_enrichment_integrations/` loads the materialized similarity rows into consolidated enriched posts without changing its external schema.
8. `services/rank_score_feeds/services/scoring.py` continues to use `similarity_score` as a likeability fallback while feed requests avoid Transformer inference and avoid O(n) scans.

## Target Architecture

The implementation should have these offline artifacts:

- Post embeddings: versioned Parquet rows keyed by `uri`, `embedding_model`, `embedding_model_revision`, `embedding_schema_version`, and `insert_timestamp`.
- ANN index artifact: a binary FAISS or hnswlib index in S3, plus a URI mapping file that maps ANN row positions back to post URIs.
- Query/profile vectors: a global most-liked centroid first, then optional per-user vectors keyed by user DID and profile timestamp.
- Similarity scores: materialized rows compatible with `PostSimilarityScoreModel`, so downstream consolidation and ranking do not need a broad contract change.

Feed serving should not call `transformers.AutoTokenizer`, `transformers.AutoModel`, `cosine_similarity` over all posts, or any index-building code.

## Serial Coordination Spine

1. Confirm the intended embedding model and retrieval library.
   - Default recommendation: use `sentence-transformers/all-MiniLM-L6-v2` or another sentence-embedding model rather than raw `bert-base-uncased` mean pooling.
   - Default ANN recommendation: use FAISS if deployable on the target runtime; use hnswlib if simpler wheels and persistence are more reliable on Quest/SLURM.
2. Freeze the first production contract.
   - Preserve `PostSimilarityScoreModel` fields for downstream compatibility.
   - Add embedding/index metadata models without changing `ConsolidatedEnrichedPostModel` unless required by analytics.
3. Implement and test offline embedding generation.
4. Implement and test ANN index building.
5. Implement and test score materialization from cached index queries.
6. Integrate the materialized scores with `services/consolidate_enrichment_integrations/`.
7. Run final scoring and ranking tests to verify the feed-ranking path still consumes `similarity_score` correctly.

## Interface Or Contract Freeze

Allowed to preserve as stable public/internal contracts:

- `services.generate_vector_embeddings.models.PostSimilarityScoreModel`
- Athena table shape for `post_cosine_similarity_scores`
- `ConsolidatedEnrichedPostModel.similarity_score`
- `services.rank_score_feeds.services.scoring.score_post_likeability`

Allowed to add:

- `PostEmbeddingModel` in `services/generate_vector_embeddings/models.py`
- `EmbeddingSessionModel` in `services/generate_vector_embeddings/models.py`
- `AnnIndexSessionModel` in `services/generate_vector_embeddings/models.py`
- `ProfileEmbeddingModel` or `QueryEmbeddingModel` in `services/generate_vector_embeddings/models.py`
- New helper modules under `services/generate_vector_embeddings/`, such as `embedding_generation.py`, `ann_index.py`, `profile_vectors.py`, and `similarity_materialization.py`
- Tests under `services/generate_vector_embeddings/tests/`

Do not change during the first pass unless the coordinator explicitly approves:

- `feed_api/**`
- `services/rank_score_feeds/services/scoring.py` behavior
- `services/consolidate_enrichment_integrations/models.py` field names
- Existing S3 prefixes consumed by Athena, unless a migration path is included

## Parallel Task Packets

### Task ID: EMB-1

Objective: Add explicit models and metadata for post embeddings, embedding sessions, ANN index sessions, and query/profile embeddings.

Why parallelizable: This task defines data contracts and does not require ANN implementation details beyond fields named in this plan.

Exact files to inspect:

- `services/generate_vector_embeddings/models.py`
- `services/generate_vector_embeddings/helper.py`
- `services/consolidate_enrichment_integrations/models.py`

Exact files allowed to change:

- `services/generate_vector_embeddings/models.py`
- `services/generate_vector_embeddings/tests/test_models.py`

Exact files forbidden to change:

- `services/rank_score_feeds/**`
- `services/consolidate_enrichment_integrations/**`
- `feed_api/**`

Preconditions:

- Coordinator confirms whether to keep `bert-base-uncased` for historical parity or move to a sentence-embedding model.

Dependency tasks:

- None.

Required contracts and invariants:

- Do not remove or rename `PostSimilarityScoreModel`.
- Every embedding row must include `uri`, `embedding`, `embedding_model`, `embedding_model_revision`, `embedding_schema_version`, and `insert_timestamp`.
- Every ANN index session must include the index S3 key, URI mapping S3 key, embedding source keys, model metadata, vector dimension, distance metric, and creation timestamp.

Step-by-step implementation instructions:

1. Add `PostEmbeddingModel`.
2. Add `EmbeddingSessionModel`.
3. Add `AnnIndexSessionModel`.
4. Add `QueryEmbeddingModel` with fields for `query_id`, `query_type`, `embedding`, model metadata, source URI list or source artifact key, and `insert_timestamp`.
5. Add model unit tests for required fields and simple validation.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_models.py
```

Expected outputs from verification:

- Pytest exits with code 0.
- All model tests pass.

Done-when checklist:

- [ ] New models exist in `services/generate_vector_embeddings/models.py`.
- [ ] `PostSimilarityScoreModel` remains backward-compatible.
- [ ] Unit tests cover required fields.

Coordinator review checklist:

- [ ] Metadata is sufficient to reproduce which model/index generated a score.
- [ ] No downstream ranking schema was changed.

### Task ID: EMB-2

Objective: Refactor embedding generation so it is offline, incremental, versioned, and safe to import without loading a Transformer model immediately.

Why parallelizable: This task owns embedding generation only and can use the models from EMB-1 once merged.

Exact files to inspect:

- `services/generate_vector_embeddings/helper.py`
- `pipelines/generate_vector_embeddings/handler.py`
- `services/preprocess_raw_data/models.py`
- `lib/db/manage_local_data.py`
- `lib/aws/s3.py`
- `lib/aws/dynamodb.py`

Exact files allowed to change:

- `services/generate_vector_embeddings/helper.py`
- `services/generate_vector_embeddings/embedding_generation.py`
- `services/generate_vector_embeddings/tests/test_embedding_generation.py`
- `pipelines/generate_vector_embeddings/handler.py`

Exact files forbidden to change:

- `services/rank_score_feeds/**`
- `services/consolidate_enrichment_integrations/**`
- `feed_api/**`

Preconditions:

- EMB-1 models are merged, or the task creates local minimal models consistent with EMB-1 and resolves during integration.

Dependency tasks:

- EMB-1 preferred.

Required contracts and invariants:

- Importing `services.generate_vector_embeddings.helper` must not instantiate `AutoTokenizer` or `AutoModel`.
- Embeddings are generated in batches in scheduled jobs, never during feed API requests.
- Embedding generation skips URIs already embedded for the same model name, model revision, and schema version.
- Embedding vectors are normalized if cosine similarity or inner-product ANN search is used.
- CPU fallback is allowed for local tests; production can still prefer CUDA/MPS when available.

Step-by-step implementation instructions:

1. Move tokenizer/model loading into an explicit loader function.
2. Add an `EmbeddingGenerator` class or pure functions that accept model name, revision, batch size, and device.
3. Change `get_device()` so CPU fallback does not raise by default in test/local mode.
4. Add an idempotency function that checks previously embedded `(uri, embedding_model, embedding_model_revision, embedding_schema_version)` combinations.
5. Write post embedding artifacts to a stable S3 prefix such as `vector_embeddings/post_embeddings/{embedding_schema_version}/{timestamp}.parquet`.
6. Record the session in `vector_embedding_sessions` with enough metadata to find the artifact later.
7. Keep a compatibility path for writing current `in_network_post_embeddings` if downstream Athena tables still depend on it.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_embedding_generation.py
```

Expected outputs from verification:

- Pytest exits with code 0.
- Tests prove importing the module does not load a Transformer model.
- Tests prove already embedded URIs are skipped.
- Tests prove output rows include model metadata.

Done-when checklist:

- [ ] Embedding code is import-safe.
- [ ] Embedding generation is incremental by URI and model version.
- [ ] Output artifacts include model metadata.
- [ ] No feed-serving path performs embedding generation.

Coordinator review checklist:

- [ ] Batch behavior is compatible with Quest/SLURM jobs.
- [ ] S3 prefixes are compatible with existing Athena usage or include a migration note.

### Task ID: EMB-3

Objective: Build and persist an ANN index over cached post embeddings.

Why parallelizable: This task consumes embedding artifacts and produces independent index artifacts without changing ranking behavior.

Exact files to inspect:

- `services/generate_vector_embeddings/helper.py`
- `services/generate_vector_embeddings/models.py`
- `lib/aws/s3.py`
- `pyproject.toml`

Exact files allowed to change:

- `services/generate_vector_embeddings/ann_index.py`
- `services/generate_vector_embeddings/tests/test_ann_index.py`
- `services/generate_vector_embeddings/models.py` if EMB-1 has not already added index metadata
- `pyproject.toml` only if adding the chosen ANN dependency is approved by the coordinator

Exact files forbidden to change:

- `services/rank_score_feeds/**`
- `services/consolidate_enrichment_integrations/**`
- `feed_api/**`

Preconditions:

- Coordinator chooses FAISS, hnswlib, or another ANN backend.
- EMB-1 metadata model is available.

Dependency tasks:

- EMB-1 required.
- EMB-2 preferred for real artifact input; tests may use synthetic embeddings.

Required contracts and invariants:

- ANN index uses normalized vectors and cosine-equivalent scoring.
- URI mapping order exactly matches ANN row order.
- Index artifact and URI mapping are written atomically enough that a session is not recorded until both exist.
- Index metadata records vector dimension, metric, backend library, embedding model metadata, source artifact keys, and creation timestamp.

Step-by-step implementation instructions:

1. Add a loader that reads post embedding Parquet artifacts and returns `uris: list[str]` plus `vectors: np.ndarray`.
2. Add a backend abstraction with a minimal interface: `build(vectors)`, `query(query_vectors, top_k)`, `save(path_or_bytes)`, and `load(path_or_bytes)`.
3. Implement the selected backend.
4. Add URI mapping serialization as Parquet or JSONL.
5. Add S3 write/read functions for the index artifact and mapping.
6. Add an index session writer that records only after artifact writes succeed.
7. Add synthetic-vector tests proving query results are stable and URI mapping is correct.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_ann_index.py
```

Expected outputs from verification:

- Pytest exits with code 0.
- Tests show an obvious nearest neighbor is returned first.
- Tests show saved and loaded index results match before-save results.

Done-when checklist:

- [ ] ANN index can be built from cached embeddings.
- [ ] ANN index can be saved and loaded.
- [ ] URI mapping is deterministic and tested.
- [ ] Session metadata is sufficient for reproducibility.

Coordinator review checklist:

- [ ] ANN dependency is acceptable for local, CI, and Quest/SLURM environments.
- [ ] Index artifact format is portable across build and runtime machines.

### Task ID: EMB-4

Objective: Materialize similarity scores from cached ANN retrieval while preserving `PostSimilarityScoreModel`.

Why parallelizable: This task can be tested with a fake ANN index and does not need to modify feed scoring.

Exact files to inspect:

- `services/generate_vector_embeddings/helper.py`
- `services/generate_vector_embeddings/models.py`
- `services/consolidate_enrichment_integrations/loaders.py`
- `services/rank_score_feeds/services/scoring.py`

Exact files allowed to change:

- `services/generate_vector_embeddings/similarity_materialization.py`
- `services/generate_vector_embeddings/tests/test_similarity_materialization.py`
- `services/generate_vector_embeddings/helper.py`

Exact files forbidden to change:

- `services/rank_score_feeds/**`
- `services/consolidate_enrichment_integrations/**`
- `feed_api/**`

Preconditions:

- EMB-1 models are available.
- EMB-3 provides an index query interface, or this task uses a fake interface in tests.

Dependency tasks:

- EMB-1 required.
- EMB-3 preferred.

Required contracts and invariants:

- Output rows remain valid `PostSimilarityScoreModel` instances.
- Similarity scores should be in a documented range that `score_post_likeability` can safely consume.
- Materialization must not generate embeddings on the fly unless explicitly running in an offline embedding job.
- Materialization should be idempotent by query/index/session timestamp.

Step-by-step implementation instructions:

1. Add a function that accepts query vectors, ANN index metadata, top-k, and URI mapping.
2. Convert ANN distances into similarity scores.
3. Emit rows with `uri`, `similarity_score`, `insert_timestamp`, and `most_liked_average_embedding_key` or a generalized source key.
4. If preserving the exact field name is awkward, store the ANN query/profile artifact key in `most_liked_average_embedding_key` for backward compatibility and document this in code.
5. Write rows to the existing similarity score S3/Athena-compatible prefix.
6. Add tests for score conversion, top-k materialization, duplicate URI handling, and empty result handling.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_similarity_materialization.py
```

Expected outputs from verification:

- Pytest exits with code 0.
- Tests prove rows validate as `PostSimilarityScoreModel`.
- Tests prove no embedding model is loaded during materialization.

Done-when checklist:

- [ ] Materialized rows match the current downstream schema.
- [ ] ANN distances are converted to documented similarity scores.
- [ ] Empty and duplicate retrieval results are handled deterministically.

Coordinator review checklist:

- [ ] `score_post_likeability` receives values in the expected range.
- [ ] The compatibility use of `most_liked_average_embedding_key` is acceptable or a migration is planned.

### Task ID: EMB-5

Objective: Add profile/query vector generation, starting with the current global most-liked centroid and leaving a clear path for per-user vectors.

Why parallelizable: Query vector construction is separate from index construction and can be tested with synthetic post embeddings.

Exact files to inspect:

- `services/generate_vector_embeddings/helper.py`
- `services/generate_vector_embeddings/models.py`
- `services/aggregate_study_user_activities/**`
- `services/rank_score_feeds/**`

Exact files allowed to change:

- `services/generate_vector_embeddings/profile_vectors.py`
- `services/generate_vector_embeddings/tests/test_profile_vectors.py`
- `services/generate_vector_embeddings/helper.py`

Exact files forbidden to change:

- `services/rank_score_feeds/**`
- `feed_api/**`

Preconditions:

- Coordinator confirms first implementation scope: global centroid only, or global centroid plus per-user vectors.

Dependency tasks:

- EMB-1 required.
- EMB-2 preferred for real embedding artifact input.

Required contracts and invariants:

- Global centroid behavior must be reproducible from a fixed set of source URIs.
- Per-user vector generation, if implemented, must not require feed request-time embedding.
- Query vectors must record their source data and timestamp.

Step-by-step implementation instructions:

1. Implement `build_global_most_liked_centroid(embeddings)` using normalized vectors.
2. Persist the centroid as a `QueryEmbeddingModel`.
3. Add a placeholder or implementation for `build_user_profile_vector(user_did, activity_rows, post_embeddings)`.
4. If per-user vectors are in scope, use recent liked/clicked/engaged post URIs and average their cached embeddings.
5. Add tests for centroid shape, normalization, empty inputs, and metadata.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_profile_vectors.py
```

Expected outputs from verification:

- Pytest exits with code 0.
- Tests prove query vectors are normalized and metadata includes source URIs or source artifact keys.

Done-when checklist:

- [ ] Global most-liked centroid can be rebuilt offline.
- [ ] Query/profile vector artifacts are versioned and reproducible.
- [ ] Per-user vector extension point is explicit.

Coordinator review checklist:

- [ ] First implementation remains faithful to the historical paper system where possible.
- [ ] Future personalized retrieval does not require schema churn.

### Task ID: EMB-6

Objective: Integrate the offline embedding, index, query-vector, and score-materialization steps into the existing pipeline handler.

Why parallelizable: This task should wait for the implementation packets and then connect them with minimal business logic.

Exact files to inspect:

- `pipelines/generate_vector_embeddings/handler.py`
- `pipelines/generate_vector_embeddings/README.md`
- `services/generate_vector_embeddings/helper.py`
- `orchestration/README.md`
- `pipelines/README.md`

Exact files allowed to change:

- `pipelines/generate_vector_embeddings/handler.py`
- `pipelines/generate_vector_embeddings/README.md`
- `services/generate_vector_embeddings/helper.py`
- `services/generate_vector_embeddings/tests/test_helper.py`

Exact files forbidden to change:

- `feed_api/**`
- `services/rank_score_feeds/**`

Preconditions:

- EMB-2, EMB-3, EMB-4, and EMB-5 are merged.

Dependency tasks:

- EMB-2 required.
- EMB-3 required.
- EMB-4 required.
- EMB-5 required.

Required contracts and invariants:

- Handler runs an offline batch workflow only.
- Handler logs each artifact key it creates.
- Handler fails before recording sessions if critical artifact writes fail.
- README must state that this pipeline is for offline embedding/index/score generation, not request-time feed serving.

Step-by-step implementation instructions:

1. Replace the single `do_vector_embeddings()` path with an orchestrated function that runs embedding generation, ANN index building, query-vector generation, and score materialization in order.
2. Keep `do_vector_embeddings()` as a compatibility wrapper only if existing orchestration imports it.
3. Add structured logging for S3 keys, session IDs, model metadata, vector dimensions, and row counts.
4. Update `pipelines/generate_vector_embeddings/README.md` to describe the restored production intent and the no-request-time-inference invariant.
5. Add tests with mocked S3/DynamoDB/model/index dependencies.

Exact verification commands:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_helper.py
uv run python -m pipelines.generate_vector_embeddings.handler
```

Expected outputs from verification:

- Pytest exits with code 0.
- The handler smoke command either completes with mocked/local dependencies or fails with an explicit credential/data dependency error documented in the plan implementation notes.

Done-when checklist:

- [ ] Pipeline handler invokes the full offline workflow.
- [ ] Pipeline README accurately describes the workflow.
- [ ] No feed-serving code imports Transformer-loading paths.

Coordinator review checklist:

- [ ] Operational logs are sufficient to debug missing embeddings, stale indices, and stale similarity scores.
- [ ] Handler behavior matches Prefect/SLURM expectations.

## Integration Order

1. Merge EMB-1 first to freeze metadata contracts.
2. Merge EMB-2 next so cached post embeddings exist as an artifact source.
3. Merge EMB-5 after EMB-2 so query/profile vectors can be built from cached embeddings.
4. Merge EMB-3 after EMB-2 so the ANN index can be built from cached embeddings.
5. Merge EMB-4 after EMB-3 and EMB-5 so similarity scores can be materialized from query vectors and ANN retrieval.
6. Merge EMB-6 last to wire the complete offline workflow into `pipelines/generate_vector_embeddings/handler.py`.
7. Run final verification and compare ranking behavior against the current baseline.

## Manual Verification

- [ ] Run model tests:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_models.py
```

Expected output: pytest exits with code 0.

- [ ] Run embedding-generation tests:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_embedding_generation.py
```

Expected output: pytest exits with code 0 and includes coverage for import-safe model loading.

- [ ] Run ANN index tests:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_ann_index.py
```

Expected output: pytest exits with code 0 and confirms save/load/query correctness.

- [ ] Run query/profile vector tests:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_profile_vectors.py
```

Expected output: pytest exits with code 0 and confirms normalized query vectors.

- [ ] Run similarity materialization tests:

```bash
uv run pytest services/generate_vector_embeddings/tests/test_similarity_materialization.py
```

Expected output: pytest exits with code 0 and confirms rows validate as `PostSimilarityScoreModel`.

- [ ] Run downstream consolidation loader tests:

```bash
uv run pytest services/consolidate_enrichment_integrations/tests/test_loaders.py
```

Expected output: pytest exits with code 0 and existing similarity-score loading behavior remains valid.

- [ ] Run ranking score tests:

```bash
uv run pytest services/rank_score_feeds/tests/services/test_scoring_functions.py
```

Expected output: pytest exits with code 0 and `score_post_likeability` still uses `similarity_score` as expected.

- [ ] Run full vector embedding service tests:

```bash
uv run pytest services/generate_vector_embeddings/tests
```

Expected output: pytest exits with code 0.

- [ ] Run lint on edited Python files:

```bash
uv run ruff check services/generate_vector_embeddings pipelines/generate_vector_embeddings
```

Expected output: Ruff exits with code 0.

- [ ] Manually inspect generated S3 keys in logs for a staging run:

```bash
uv run python -m pipelines.generate_vector_embeddings.handler
```

Expected output: command logs post embedding artifact keys, ANN index artifact key, URI mapping artifact key, query/profile vector artifact key, similarity score artifact key, and DynamoDB session metadata. If credentials or private data are unavailable locally, the command should fail early with a clear missing-credential or missing-data error.

## Final Verification

After all implementation tasks are integrated:

1. Confirm no request-time path imports or calls `transformers.AutoTokenizer.from_pretrained` or `transformers.AutoModel.from_pretrained`.
1. Confirm `services/rank_score_feeds/services/scoring.py` receives similarity scores through consolidated enriched post records, not by querying embeddings directly.
1. Confirm ANN recall on a sampled dataset by comparing ANN top-k against exact cosine top-k:

```bash
uv run python services/generate_vector_embeddings/scripts/evaluate_ann_recall.py --sample-size 1000 --top-k 100
```

Expected output: recall@100 meets the threshold chosen by the coordinator, for example `recall_at_100 >= 0.95`.

1. Confirm latency is dominated by cached lookup/ranking rather than embedding generation:

```bash
uv run python services/generate_vector_embeddings/scripts/benchmark_ann_query.py --queries 100 --top-k 500
```

Expected output: p95 query latency is below the threshold chosen by the coordinator for batch feed generation.

1. Confirm end-to-end feed scoring still works:

```bash
uv run pytest services/rank_score_feeds/tests
```

Expected output: pytest exits with code 0.

## Alternative Approaches

The fastest compatibility path is to keep the current global most-liked centroid and simply precompute exact cosine scores offline. That addresses request-time latency but not the paper's ANN claim and does not scale well if we later personalize retrieval. A managed vector database would also work, but it adds new infrastructure and operational dependencies outside the current S3/Athena/SLURM shape of the repo. The recommended first implementation is therefore local ANN artifacts, either FAISS or hnswlib, built offline on Quest/SLURM and stored in S3 with explicit metadata.

## Plan Asset Storage

Use this folder for any supporting notes, benchmark outputs, sample logs, or implementation artifacts created while executing this plan:

```text
docs/plans/2026-05-14_add_missing_generate_vector_embeddings_105361/
```

This is not a UI change, so no before/after screenshots are required.
