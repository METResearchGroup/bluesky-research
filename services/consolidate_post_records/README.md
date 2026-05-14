# Consolidate post records

## Purpose

The posts from the firehose have a different format than the posts that come from the feeds (specifically, the posts that come from the feeds have more information given that they have to be fully hydrated with the info needed to display the post on the frontend).

We consolidate those two formats in this service and create a "ConsolidatedPostRecord" object.

## Key Files

| File | Description |
|------|-------------|
| `helper.py` | `consolidate_firehose_post` / `consolidate_feedview_post` map Bluesky-specific intermediate models into `ConsolidatedPostRecordModel`; `consolidate_post_record` / `consolidate_post_records` dispatch by type. Firehose rows set `source="firehose"` and leave engagement metrics and sparse author fields empty where unknown; feed rows set `source="most_liked"` and copy hydrated author + counts. |
| `models.py` | `ConsolidatedPostRecordModel` (canonical post schema), `ConsolidatedPostRecordMetadataModel`, and `ConsolidatedMetrics`. Used downstream for preprocessing, SQLite filtered-post metadata, and classifiers. |

## How the key files relate

This package is a library, not a standalone job: ingest paths import it so every consumer can assume one post shape.

### Firehose path

```mermaid
flowchart TB
  RAW["Raw firehose post dict"]
  TPOST["transform_post<br/>(post.py)"]
  PROC["process_firehose_post"]
  TRWA["TransformedRecordWithAuthorModel"]
  CF["consolidate_firehose_post"]
  CPR["ConsolidatedPostRecordModel"]
  DICT["dict + json.dumps(embed)"]
  ROUTE["Stream routing / export"]

  RAW --> TPOST
  TPOST --> PROC
  PROC --> TRWA
  TRWA --> CF
  CF --> CPR
  CPR --> DICT
  DICT --> ROUTE
```

### Feed / “most liked” path

```mermaid
flowchart TB
  API["Custom feed URL /<br/>FeedViewPost"]
  TX["transform_feedview_posts"]
  TFV["TransformedFeedViewPostModel"]
  FILT["filter_posts"]
  CFV["consolidate_feedview_post"]
  CPR["ConsolidatedPostRecordModel"]
  DICT["dict + json.dumps(embed)"]
  EXP["export_posts"]

  API --> TX
  TX --> TFV
  TFV --> FILT
  FILT --> CFV
  CFV --> CPR
  CPR --> DICT
  DICT --> EXP
```

### End-to-end

Unified view: the two sync jobs feed this library, which exposes a single post contract to preprocessing and later pipeline stages.

```mermaid
flowchart TB
  subgraph sync ["Sync pipeline inputs"]
    SF["sync_firehose_stream<br/>(stream record processors)"]
    SM["sync_most_liked_feed<br/>(most_liked_posts)"]
  end

  subgraph lib ["consolidate_post_records (this package)"]
    CFH["consolidate_firehose_post"]
    CFV["consolidate_feedview_post"]
    MOD["ConsolidatedPostRecordModel<br/>+ ConsolidatedPostRecordMetadataModel,<br/>ConsolidatedMetrics"]
    SER["Stored / exported batches<br/>(dict, embed as JSON string)"]

    CFH --> MOD
    CFV --> MOD
    MOD --> SER
  end

  subgraph consumers ["Downstream consumers"]
    PP["preprocess_raw_data<br/>(load_data → model round-trip)"]
    SQ["preprocessing_database SQLite<br/>(metadata + metrics on filtered rows)"]
    CL["Preprocess classifiers<br/>(language, NSFW, spam, ...)"]
    LATER["Later stages<br/>(e.g. consolidate_enrichment_integrations,<br/>rank_score_feeds)"]

    SER --> PP
    MOD -.-> SQ
    PP --> CL
    PP --> LATER
  end

  SF --> CFH
  SM --> CFV
```
