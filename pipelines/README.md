# Pipelines

This directory contains the individual pipeline components that are run in production. The pipelines are orchestrated in `orchestration/`. Each pipeline component is mapped to the orchestration pipeline that uses it.

## Pipeline Components and Their Orchestration Usage

### Used by Sync Pipeline (`orchestration/sync_pipeline.py`)
- **sync_post_records/firehose/** - Firehose data ingestion and writing
- **sync_post_records/most_liked/** - Used by Integrations Sync Pipeline (`orchestration/integrations_sync_pipeline.py`)

### Used by Integrations Sync Pipeline (`orchestration/integrations_sync_pipeline.py`)
- **sync_post_records/most_liked/** - Syncs popular posts from external feeds

### Used by Data Pipeline (`orchestration/data_pipeline.py`)
- **preprocess_raw_data/** - Preprocesses and filters raw sync data
- **calculate_superposters/** - Identifies users who post excessively
- **classify_records/perspective_api/** - Toxicity classification using Google's Perspective API
- **classify_records/sociopolitical/** - Political content classification (currently disabled)
- **classify_records/ime/** - Individualized Moral Equivalence scoring
- **consolidate_enrichment_integrations/** - Merges all ML inference results

### Used by Recommendation Pipeline (`orchestration/recommendation_pipeline.py`)
- **rank_score_feeds/** - Generates personalized feeds using ranking algorithms

### Used by Compaction Pipeline (`orchestration/compaction_pipeline.py`)
- **compact_all_services/** - Compacts data files across all services
- **snapshot_data/** - Creates point-in-time data snapshots

### Used by Analytics Pipeline (`orchestration/analytics_pipeline.py`)
- **compact_user_session_logs/** - Compacts user interaction logs
- **aggregate_study_user_activities/** - Aggregates study user activities for analytics

### Standalone/Utility Pipelines (Not Used by Main Orchestration)
- **add_users_to_study/** [STANDALONE] - Utilities for adding users to research studies
- **backfill_records_coordination/** [STANDALONE] - Coordinates backfill operations for historical data
- **backfill_sync/** [STANDALONE] - Syncs historical records for backfill operations
- **classify_records/valence_classifier/** [STANDALONE] - Valence classification (not integrated into main pipeline)
- **generate_vector_embeddings/** [STANDALONE] - Generates embeddings for posts (not in main pipeline)
- **get_existing_user_social_network/** [STANDALONE] - Retrieves user social network data
- **write_cache_buffers/** [STANDALONE] - Writes cache buffers (not in main orchestration)

### Deprecated Pipelines
- **deprecated/compact_dedupe_data/** [DEPRECATED] - Old data compaction logic
- **deprecated/consume_sqs_messages/** [DEPRECATED] - Legacy SQS message processing
- **deprecated/create_feeds.py** [DEPRECATED] - Old feed creation logic
- **deprecated/filter_posts.py** [DEPRECATED] - Legacy post filtering
- **deprecated/generate_features.py** [DEPRECATED] - Old feature generation
- **deprecated/update_muted_users/** [DEPRECATED] - Legacy muted users update
- **deprecated/update_network_connections/** [DEPRECATED] - Old network connection updates
- **deprecated/update_user_bluesky_engagement/** [DEPRECATED] - Legacy engagement updates
- **deprecated/write_latest_feeds_to_s3/** [DEPRECATED] - Old S3 feed writing logic

## Pipeline Architecture Summary

The main orchestration flow follows this pattern:
1. **Data Ingestion**: Sync Pipeline + Integrations Sync Pipeline
2. **Data Processing**: Data Pipeline (preprocessing → ML inference → consolidation)
3. **Feed Generation**: Recommendation Pipeline
4. **Data Management**: Compaction Pipeline (storage optimization)
5. **Analytics**: Analytics Pipeline (user activity analysis)

Standalone pipelines provide utility functions for research operations, backfilling, and one-off tasks that don't fit into the main production workflow.
