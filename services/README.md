# Services

This directory contains the microservices that make up our data processing pipeline. Each service provides specific functionality and is called by the corresponding pipeline components. The orchestration of pipelines is handled in the `orchestration/` directory.

## Services Used by Main Orchestration Pipelines

### Used by Sync Pipeline (`orchestration/sync_pipeline.py`)
- **sync/stream/** - Connects to and processes Bluesky's real-time firehose stream
- **sync/jetstream/** - Writes streamed firehose data to persistent storage
- **sync/most_liked_posts/** - Used by Integrations Sync Pipeline (`orchestration/integrations_sync_pipeline.py`)

### Used by Integrations Sync Pipeline (`orchestration/integrations_sync_pipeline.py`)
- **sync/most_liked_posts/** - Queries Bluesky API for trending/popular posts from external feeds

### Used by Data Pipeline (`orchestration/data_pipeline.py`)
- **preprocess_raw_data/** - Filters, cleans, and transforms raw sync data; includes bot/spam/NSFW classification
- **calculate_superposters/** - Identifies users who post excessively for feed ranking penalties
- **ml_inference/perspective_api/** - Toxicity and content moderation scoring using Google's Perspective API
- **ml_inference/sociopolitical/** - Political content classification (currently disabled in orchestration)
- **ml_inference/ime/** - Individualized Moral Equivalence scoring for moral reasoning content
- **consolidate_enrichment_integrations/** - Merges and consolidates all ML inference results

### Used by Recommendation Pipeline (`orchestration/recommendation_pipeline.py`)
- **rank_score_feeds/** - Implements ranking algorithms and generates personalized content feeds

### Used by Compaction Pipeline (`orchestration/compaction_pipeline.py`)
- **compact_all_services/** - Compacts data files across all services for storage optimization
- **snapshot_data/** - Creates point-in-time data snapshots for backup and analysis

### Used by Analytics Pipeline (`orchestration/analytics_pipeline.py`)
- **compact_user_session_logs/** - Compacts and processes user interaction logs from the application
- **aggregate_study_user_activities/** - Aggregates all study user activities into comprehensive analytics tables

## Standalone/Utility Services (Not Used by Main Orchestration)
- **backfill/** [STANDALONE] - Handles historical data backfill operations and coordination
- **calculate_analytics/** [STANDALONE] - Analytics calculations and reporting (separate from main analytics pipeline)
- **consolidate_post_records/** [STANDALONE] - Post record consolidation (separate from main consolidation)
- **fetch_posts_used_in_feeds/** [STANDALONE] - Fetches and manages posts used in feed generation
- **generate_vector_embeddings/** [STANDALONE] - Generates vector embeddings for posts (not integrated into main pipeline)
- **get_pipeline_analytics/** [STANDALONE] - Pipeline performance analytics and monitoring
- **get_posts_liked_by_study_users/** [STANDALONE] - Retrieves posts liked by study participants
- **get_preprocessed_posts_used_in_feeds/** [STANDALONE] - Gets preprocessed posts for feed operations
- **ml_inference/valence_classifier/** [STANDALONE] - Valence classification (not in main pipeline)
- **participant_data/** [STANDALONE] - Manages research study participant data and profiles
- **repartition_service/** [STANDALONE] - Data repartitioning utilities
- **sync/search/** [STANDALONE] - Bluesky search API integration (not in main sync pipeline)
- **write_cache_buffers_to_db/** [STANDALONE] - Cache buffer database operations

## Deprecated Services
- **compact_dedupe_data/** [DEPRECATED] - Legacy data compaction and deduplication logic
- **deprecated/add_context/** [DEPRECATED] - Old context addition functionality
- **deprecated/analytics_reporting/** [DEPRECATED] - Legacy analytics reporting
- **deprecated/classify_civic/** [DEPRECATED] - Old civic content classification
- **deprecated/consume_sqs_messages/** [DEPRECATED] - Legacy SQS message processing
- **deprecated/create_feeds/** [DEPRECATED] - Old feed creation logic
- **deprecated/delete_old_s3_objects/** [DEPRECATED] - Legacy S3 cleanup
- **deprecated/dump-db-to-parquet/** [DEPRECATED] - Old database export functionality
- **deprecated/generate_training_data/** [DEPRECATED] - Legacy training data generation
- **deprecated/update_network_connections/** [DEPRECATED] - Old network connection updates
- **deprecated/update_user_bluesky_engagement/** [DEPRECATED] - Legacy engagement tracking
- **deprecated/write_latest_feeds_to_s3/** [DEPRECATED] - Old S3 feed writing logic

## Service Architecture Summary

**Core Production Services** (used by main orchestration):
1. **Data Ingestion**: `sync/stream/`, `sync/jetstream/`, `sync/most_liked_posts/`
2. **Data Processing**: `preprocess_raw_data/`, `ml_inference/*`, `consolidate_enrichment_integrations/`
3. **Feed Generation**: `rank_score_feeds/`
4. **Data Management**: `compact_all_services/`, `snapshot_data/`
5. **Analytics**: `compact_user_session_logs/`, `aggregate_study_user_activities/`

**Utility Services** provide specialized functionality for research operations, backfilling, and administrative tasks that complement but don't directly integrate with the main production workflow.
