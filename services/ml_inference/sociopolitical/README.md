# Sociopolitical LLM classification

## Purpose

Uses an LLM to decide whether text is sociopolitical (politics or broad social issues) and, when yes, a coarse US political lean label (`left` / `right` / `moderate` / `unclear`). Implementation is in `services/ml_inference/sociopolitical/model.py` (prompting, JSON parsing, batching); `sociopolitical.py` is the orchestration entry.

## Key files

| File | Description |
|------|-------------|
| `sociopolitical.py` | `SOCIOPOLITICAL_CONFIG` + `classify_latest_posts()` → `orchestrate_classification` + `run_batch_classification`. |
| `model.py` | `generate_prompt`, `parse_llm_result`, minibatching via `ml_tooling.llm.llm_service`, `create_batches`, export through `write_posts_to_cache` / `return_failed_labels_to_input_queue`. |
| `constants.py` | Cache path and S3 root key for `ml_inference_sociopolitical`. |

## Core logic

```mermaid
flowchart TB
  ENTRY["sociopolitical.classify_latest_posts"]
  ORC["orchestrate_classification"]
  POSTS["get_posts_to_classify('sociopolitical')"]

  ENTRY --> ORC
  ORC --> POSTS
  POSTS --> RUN["model.run_batch_classification"]

  subgraph llm ["model.py"]
    MB["create_batches + minibatch size"]
    PROMPT["generate_prompt<br/>(enumerated texts)"]
    SVC["get_llm_service() completion"]
    PARSE["parse_llm_result → LLMSociopoliticalLabelModel"]
    ROWS["SociopoliticalLabelsModel + batch_id"]
    MB --> PROMPT
    PROMPT --> SVC
    SVC --> PARSE
    PARSE --> ROWS
  end

  subgraph export ["export_data"]
    OK["write_posts_to_cache('sociopolitical')"]
    BAD["return_failed_labels_to_input_queue('sociopolitical')"]
  end

  RUN --> llm
  ROWS --> OK
  ROWS --> BAD
```

