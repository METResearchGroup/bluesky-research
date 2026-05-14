# Perspective API

## Purpose

Calls Google Perspective API (`commentanalyzer`) attributes (toxicity, insult, experimental construct scores, etc.) for each post.

## Key files

| File | Description |
|------|-------------|
| `perspective_api.py` | `PERSPECTIVE_API_CONFIG` + `classify_latest_posts()` → `orchestrate_classification` with `ml_tooling.perspective_api.model.run_batch_classification`. |
| `constants.py` | Cache directory (local vs Lambda `/tmp`); root S3 key name for the `ml_inference_perspective_api` dataset family. |

## Core logic

```mermaid
flowchart TB
  ENTRY["perspective_api.classify_latest_posts"]
  ORC["orchestrate_classification"]
  POSTS["get_posts_to_classify('perspective_api')"]

  ENTRY --> ORC
  ORC --> POSTS
  POSTS --> RUN["ml_tooling.perspective_api.model<br/>run_batch_classification"]

  subgraph client ["ml_tooling batch loop"]
    BATCH["create_batches + rate limit / delay"]
    API["Google API client<br/>analyzeComment per attribute"]
    MAP["Map responses → PerspectiveApiLabelsModel"]
    BATCH --> API
    API --> MAP
  end

  subgraph export ["export_data"]
    OK["write_posts_to_cache('perspective_api')"]
    BAD["return_failed_labels_to_input_queue('perspective_api')"]
  end

  RUN --> client
  MAP --> OK
  MAP --> BAD
```
