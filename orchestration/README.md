# Bluesky Research Orchestration Pipeline Documentation

Orchestration logic, defined using **Prefect** for workflow orchestration and **SLURM** for job scheduling on the HPC cluster. Each pipeline consists of:
- A main orchestration Python script (`orchestration/*.py`)
- A SLURM submission script (`orchestration/submit_*_job.sh`) 
- Component pipelines with their own handlers and bash scripts
- Associated services that perform the actual work

## Pipeline Overview

The system processes data from Bluesky's firehose through multiple stages: ingestion → preprocessing → enrichment → feed generation → analytics.

---

## 1. Sync Pipeline

**Purpose**: Ingests real-time data from Bluesky's firehose stream and syncs popular posts from external feeds.

**Orchestration File**: `orchestration/sync_pipeline.py`
**Trigger Script**: `orchestration/submit_sync_pipeline_job.sh`
**Schedule**: Long-running processes (not cron-based)

### Component Services:
1. **Firehose Sync** (`sync_firehose()`)
   - **Pipeline**: `pipelines/sync_post_records/firehose/`
   - **Handler**: `pipelines/sync_post_records/firehose/handler.py`
   - **Trigger**: `pipelines/sync_post_records/firehose/submit_job.sh`
   - **Service**: `services/sync/stream/` - Connects to Bluesky's real-time firehose stream
   - **Purpose**: Continuously ingests posts, likes, follows, and other activities from Bluesky's real-time stream

2. **Firehose Data Writer** (`write_firehose_data()`)
   - **Pipeline**: `pipelines/sync_post_records/firehose/`
   - **Trigger**: `pipelines/sync_post_records/firehose/submit_firehose_writes_job.sh`
   - **Service**: `services/sync/jetstream/` - Writes streamed data to persistent storage
   - **Purpose**: Persists the streamed firehose data to local storage as .parquet files

### Execution:
Both components run in parallel (`wait_for=False`) as long-running processes.

---

## 2. Integrations Sync Pipeline

**Purpose**: Syncs popular/trending posts from Bluesky's external feeds and APIs.

**Orchestration File**: `orchestration/integrations_sync_pipeline.py`
**Trigger Script**: `orchestration/submit_integrations_sync_pipeline_job.sh`
**Schedule**: Every 2 hours

### Component Services:
1. **Most Liked Posts Sync** (`sync_most_liked()`)
   - **Pipeline**: `pipelines/sync_post_records/most_liked/`
   - **Handler**: `pipelines/sync_post_records/most_liked/handler.py`
   - **Trigger**: `pipelines/sync_post_records/most_liked/submit_job.sh`
   - **Service**: `services/sync/most_liked_posts/` - Queries Bluesky API for trending content
   - **Purpose**: Fetches posts from popular feeds like "What's Hot" to capture trending content that might be missed by the firehose

### Execution:
Runs as a Prefect service with 2-hour intervals.

---

## 3. Data Pipeline

**Purpose**: Processes raw data through preprocessing, ML inference, and enrichment consolidation.

**Orchestration File**: `orchestration/data_pipeline.py`
**Trigger Script**: `orchestration/submit_data_pipeline_job.sh`
**Schedule**: Every 2 hours

### Component Services:

1. **Preprocess Raw Data** (`preprocess_raw_data()`)
   - **Pipeline**: `pipelines/preprocess_raw_data/`
   - **Handler**: `pipelines/preprocess_raw_data/handler.py`
   - **Trigger**: `pipelines/preprocess_raw_data/submit_job.sh`
   - **Service**: `services/preprocess_raw_data/` - Filters, cleans, and transforms raw sync data
   - **Purpose**: Filters raw posts, removes spam/bots, classifies content, and prepares data for downstream processing

2. **Calculate Superposters** (`calculate_superposters()`)
   - **Pipeline**: `pipelines/calculate_superposters/`
   - **Handler**: `pipelines/calculate_superposters/handler.py`
   - **Trigger**: `pipelines/calculate_superposters/submit_job.sh`
   - **Service**: `services/calculate_superposters/` - Identifies users who post excessively
   - **Purpose**: Creates daily lists of users who post too frequently (for feed ranking penalties)

3. **Perspective API Classification** (`run_ml_inference_perspective_api()`)
   - **Pipeline**: `pipelines/classify_records/perspective_api/`
   - **Handler**: `pipelines/classify_records/perspective_api/handler.py`
   - **Trigger**: `pipelines/classify_records/perspective_api/submit_job.sh`
   - **Service**: `services/ml_inference/perspective_api/` - Toxicity and content moderation scoring
   - **Purpose**: Scores posts for toxicity, harassment, and other harmful content using Google's Perspective API

4. **Sociopolitical Classification** (`run_ml_inference_sociopolitical()`) - *Currently disabled*
   - **Pipeline**: `pipelines/classify_records/sociopolitical/`
   - **Handler**: `pipelines/classify_records/sociopolitical/handler.py`
   - **Trigger**: `pipelines/classify_records/sociopolitical/submit_job.sh`
   - **Service**: `services/ml_inference/sociopolitical/` - Political content classification
   - **Purpose**: Classifies posts for political content and orientation

5. **IME Classification** (`run_ml_inference_ime()`) - *No consolidation dependency*
   - **Pipeline**: `pipelines/classify_records/ime/`
   - **Handler**: `pipelines/classify_records/ime/handler.py`
   - **Trigger**: `pipelines/classify_records/ime/submit_job.sh`
   - **Service**: `services/ml_inference/ime/` - Individualized Moral Equivalence scoring
   - **Purpose**: Scores posts for moral reasoning and ethical content

6. **Consolidate Enrichment Integrations** (`consolidate_enrichment_integrations()`)
   - **Pipeline**: `pipelines/consolidate_enrichment_integrations/`
   - **Handler**: `pipelines/consolidate_enrichment_integrations/handler.py`
   - **Trigger**: `pipelines/consolidate_enrichment_integrations/submit_job.sh`
   - **Service**: `services/consolidate_enrichment_integrations/` - Merges all ML inference results
   - **Purpose**: Combines results from all ML classifiers into consolidated enriched post records

### Execution Flow:
1. **Preprocessing** runs first
2. **Superposters + Perspective API** run in parallel after preprocessing completes
3. **Consolidation** runs after all parallel jobs complete (waits for superposters and Perspective API)

---

## 4. Recommendation Pipeline

**Purpose**: Generates personalized content feeds for users using ranking and scoring algorithms.

**Orchestration File**: `orchestration/recommendation_pipeline.py`
**Trigger Script**: `orchestration/submit_recommendation_pipeline_job.sh`
**Schedule**: Every 4 hours

### Component Services:
1. **Rank Score Feeds** (`rank_score_feeds()`)
   - **Pipeline**: `pipelines/rank_score_feeds/`
   - **Handler**: `pipelines/rank_score_feeds/handler.py`
   - **Trigger**: `pipelines/rank_score_feeds/submit_job.sh`
   - **Service**: `services/rank_score_feeds/` - Ranking algorithm and feed generation
   - **Purpose**: Takes posts from the last 3 days, applies scoring algorithms, ranks them, and generates personalized feeds exported as .jsonl files to S3

### Additional Components:
- **Feed TTL Management**: `pipelines/rank_score_feeds/submit_feed_ttl_job.sh` - Cleans up old feed files

### Execution:
Single task that handles the complete feed generation workflow.

---

## 5. Compaction Pipeline

**Purpose**: Optimizes data storage by compacting files and creating data snapshots.

**Orchestration File**: `orchestration/compaction_pipeline.py`
**Trigger Script**: `orchestration/submit_compaction_pipeline_job.sh`
**Schedule**: Twice daily at 7 AM and 7 PM (`cron="0 7,19 * * *"`)

### Component Services:
1. **Compact All Services** (`compact_all_services()`)
   - **Pipeline**: `pipelines/compact_all_services/`
   - **Handler**: `pipelines/compact_all_services/handler.py`
   - **Trigger**: `pipelines/compact_all_services/submit_job.sh`
   - **Service**: `services/compact_all_services/` - Data compaction across all services
   - **Purpose**: Consolidates data files from multiple services into efficient formats, handles deduplication, and moves data from "active" to "cache" directories

2. **Snapshot Data** (`snapshot_data()`)
   - **Pipeline**: `pipelines/snapshot_data/`
   - **Handler**: `pipelines/snapshot_data/handler.py`
   - **Trigger**: `pipelines/snapshot_data/submit_job.sh`
   - **Service**: `services/snapshot_data/` - Creates point-in-time data snapshots
   - **Purpose**: Creates snapshots of current data state for backup and analytical purposes

### Execution Flow:
1. **Compaction** runs first
2. **Snapshot** runs after compaction completes

---

## 6. Analytics Pipeline

**Purpose**: Processes user activity data and generates analytics reports.

**Orchestration File**: `orchestration/analytics_pipeline.py`
**Trigger Script**: `orchestration/submit_analytics_pipeline_job.sh`
**Schedule**: Daily at 8 AM (`cron="0 8 * * *"`)

### Component Services:
1. **Compact User Session Logs** (`compact_user_session_logs()`)
   - **Pipeline**: `pipelines/compact_user_session_logs/`
   - **Handler**: `pipelines/compact_user_session_logs/handler.py`
   - **Trigger**: `pipelines/compact_user_session_logs/submit_job.sh`
   - **Service**: `services/compact_user_session_logs/` - User activity log processing
   - **Purpose**: Compacts and processes user interaction logs from the application

2. **Aggregate Study User Activities** (`aggregate_study_user_activities()`)
   - **Pipeline**: `pipelines/aggregate_study_user_activities/`
   - **Handler**: `pipelines/aggregate_study_user_activities/handler.py`
   - **Trigger**: `pipelines/aggregate_study_user_activities/submit_job.sh`
   - **Service**: `services/aggregate_study_user_activities/` - User activity aggregation
   - **Purpose**: Aggregates all study user activities into comprehensive analytics tables

### Execution Flow:
1. **Session log compaction** runs first
2. **Activity aggregation** runs after compaction completes

---

## Infrastructure Notes

### SLURM Configuration
All jobs use the Quest HPC cluster with:
- Account: `p32375`
- Python environment: `bluesky_research` conda environment
- Logging: Structured logs in `/projects/p32375/bluesky-research/lib/log/`
- Error notifications: Email alerts on job failures

### Data Flow
1. **Raw Data**: Firehose → Local storage (parquet files)
2. **Processed Data**: Preprocessing → Enrichment → Consolidation
3. **Analytics**: Compaction → Aggregation → Reporting
4. **Feeds**: Ranking → S3 export → Application consumption

### Scheduling Summary
- **Sync Pipeline**: Continuous (long-running)
- **Integrations Sync**: Every 2 hours
- **Data Pipeline**: Every 2 hours
- **Recommendation Pipeline**: Every 4 hours
- **Compaction Pipeline**: Twice daily (7 AM, 7 PM)
- **Analytics Pipeline**: Daily (8 AM)
