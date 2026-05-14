# Valence classifier (VADER)

## Purpose

Assigns positive / neutral / negative valence (and compound score) using the [VADER](https://github.com/cjhutto/vaderSentiment) rule-based sentiment model.

## Key files

| File | Description |
|------|-------------|
| `valence_classifier.py` | `VALENCE_CLASSIFIER_CONFIG` + `classify_latest_posts()` → `orchestrate_classification` with `ml_tooling.valence_classifier.model.run_batch_classification`. |

## Core logic

```mermaid
flowchart TB
  ENTRY["valence_classifier.classify_latest_posts"]
  ORC["orchestrate_classification"]
  POSTS["get_posts_to_classify('valence_classifier')"]

  ENTRY --> ORC
  ORC --> POSTS
  POSTS --> RUN["ml_tooling.valence_classifier.model<br/>run_batch_classification"]

  subgraph vader ["ml_tooling.valence_classifier"]
    BATCH["create_batches"]
    INF["run_vader_on_posts<br/>(compound → valence_label thresholds)"]
    LAB["create_labels → ValenceClassifierLabelModel dicts"]
    BATCH --> INF
    INF --> LAB
  end

  subgraph export ["export_data"]
    OK["write_posts_to_cache('valence_classifier')"]
    BAD["return_failed_labels_to_input_queue('valence_classifier')"]
  end

  RUN --> vader
  LAB --> OK
  LAB --> BAD
```
