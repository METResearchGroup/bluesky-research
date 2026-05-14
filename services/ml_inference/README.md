# ML inference

## Purpose

This package runs batch ML classification jobs over preprocessed posts.

## Key files

| File | Description |
|------|-------------|
| `helper.py` | `get_posts_to_classify()` loads pending queue payloads into `PostToLabelModel`; `cap_max_records_for_run()` limits work by whole `batch_id` groups; `orchestrate_classification()` drives backfill timestamps, classification, and `ClassificationSessionModel` results. |
| `config.py` | `QueueInferenceType` literal and `InferenceConfig` (inference id, queue id, `classification_func`, optional `extract_classification_kwargs` from Lambda/event payloads). |
| `models.py` | Shared contracts: `PostToLabelModel`, `LabelWithBatchId`, per-service label models, `ClassificationSessionModel`, `BatchClassificationMetadataModel`. |
| `export_data.py` | Maps inference type → queue names; `write_posts_to_cache()` moves successes to output queue and deletes processed batch IDs from input; `return_failed_labels_to_input_queue()` re-enqueues failures; helpers to attach `batch_id` to label dicts. |
| `{ime,perspective_api,sociopolitical,valence_classifier}/…` | Thin `classify_latest_posts()` entrypoints wrapping `orchestrate_classification` + `InferenceConfig`; batch logic in `ml_tooling/`. |
| `intergroup/…` | Intergroup LLM classifier, batching adapter, and `classify_latest_posts()` entrypoint (implementation stays in this repo). |

## Inference modules

| Module | README |
|--------|--------|
| IME (intergroup / moral / emotion) | [`ime/README.md`](./ime/README.md) |
| Intergroup (LLM) | [`intergroup/README.md`](./intergroup/README.md) |
| Perspective API | [`perspective_api/README.md`](./perspective_api/README.md) |
| Sociopolitical (LLM) | [`sociopolitical/README.md`](./sociopolitical/README.md) |
| Valence (VADER) | [`valence_classifier/README.md`](./valence_classifier/README.md) |

## How the key files relate

### End-to-end (all inference types)

```mermaid
flowchart TB
  subgraph upstream ["Upstream"]
    PRE["Preprocessing / coordination<br/>enqueues posts"]
  end

  subgraph queues ["Per-type queues"]
    IN["input_ml_inference_*<br/>(pending batches + metadata)"]
    OUT["output_ml_inference_*<br/>(labeled rows)"]
  end

  subgraph runner ["services/ml_inference"]
    ORC["classify_latest_posts<br/>→ orchestrate_classification"]
    GET["get_posts_to_classify<br/>(filter + dedupe)"]
    BATCH["classification_func<br/>(batch classify + export)"]
    ORC --> GET
    GET --> BATCH
    BATCH --> OUT
    BATCH -.->|failures| IN
  end

  subgraph impl ["Model integration"]
    MT["ml_tooling/*<br/>(IME, Perspective, sociopolitical, valence)"]
    IG["intergroup/*<br/>(LLM classifier in-repo)"]
  end

  PRE --> IN
  IN --> GET
  BATCH --> MT
  BATCH --> IG
  OUT --> DS["Analytics / consolidation<br/>ml_inference_* datasets"]
```

### Shared orchestration flow

```mermaid
flowchart TB
  ENTRY["classify_latest_posts<br/>(per module)"]
  CFG["InferenceConfig<br/>(queue id + classification_func)"]
  TS["Optional backfill timestamp<br/>determine_backfill_latest_timestamp"]
  LOAD["get_posts_to_classify<br/>Queue.load_dict_items_from_queue<br/>status=pending"]
  CAP["cap_max_records_for_run<br/>(optional, whole batches only)"]
  KW["extract_classification_kwargs(event)<br/>(optional)"]
  RUN["classification_func(posts, **kwargs)"]
  SESS["ClassificationSessionModel<br/>inference_metadata"]

  ENTRY --> CFG
  CFG --> TS
  TS --> LOAD
  LOAD --> CAP
  CAP --> KW
  KW --> RUN
  RUN --> SESS
```

### Queue lifecycle inside `classification_func`

```mermaid
flowchart TB
  subgraph batch ["run_batch_classification (pattern)"]
    CHUNK["create_batches"]
    LABEL["model / API / LLM call"]
    SPLIT["split successful vs failed labels"]
    CHUNK --> LABEL
    LABEL --> SPLIT
  end

  subgraph io ["export_data"]
    OK["write_posts_to_cache<br/>output queue + delete input batch IDs"]
    BAD["return_failed_labels_to_input_queue"]
  end

  SPLIT --> OK
  SPLIT --> BAD
```
