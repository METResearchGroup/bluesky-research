# Rank Score Feeds Service

The Rank Score Feeds service generates personalized content feeds for study participants using ranking and scoring algorithms. It processes posts from multiple sources (firehose, most-liked), applies scoring algorithms, ranks posts, and generates personalized feeds based on user conditions.

## Overview

This service orchestrates the complete feed generation pipeline, from loading raw data to exporting personalized feeds. It supports three feed conditions:
- **reverse_chronological**: Chronologically ordered posts from the firehose
- **engagement**: Posts ranked by engagement score (likeability + freshness)
- **representative_diversification**: Posts ranked by treatment score (engagement adjusted for toxicity/constructiveness)

## Architecture

The service follows a layered architecture with dependency injection, organized around the `FeedGenerationOrchestrator` class which serves as the composition root.

### High-Level Flow

The `FeedGenerationOrchestrator.run()` method executes a 9-step pipeline:

1. **Load Data** - Loads study users, enriched posts, social networks, and previous feeds
2. **Deduplicate & Filter** - Removes duplicate posts and filters excluded authors
3. **Score Posts** - Calculates engagement and treatment scores (with caching)
4. **Generate Candidate Pools** - Creates three sorted candidate pools (reverse_chronological, engagement, treatment)
5. **Generate Feeds** - Creates personalized feeds for each user
6. **Calculate Analytics** - Computes session-level analytics
7. **Export Artifacts** - Saves feeds and analytics to storage
8. **TTL Old Feeds** - Moves old feeds from active to cache storage
9. **Insert Metadata** - Stores session metadata in DynamoDB

## Directory Structure

### Root Level Files

- **`orchestrator.py`** - Main orchestrator class that wires together all components and executes the pipeline
- **`config.py`** - Centralized configuration (`FeedConfig`) for all algorithm parameters
- **`models.py`** - Pydantic models and dataclasses for type-safe data structures
- **`scoring.py`** - Core scoring functions (likeability, freshness, treatment algorithm)
- **`metrics.py`** - Utility functions for calculating and plotting score metrics
- **`constants.py`** - Service-level constants

### `services/` - Business Logic Services

Contains service classes that implement specific business logic:

- **`data_loading.py`** - `DataLoadingService`: Loads feed input data from various sources (enriched posts, social networks, superposters, latest feeds)
- **`scoring.py`** - `ScoringService`: Orchestrates post scoring with caching support (loads cached scores, calculates new scores, saves to repository)
- **`candidate.py`** - `CandidateGenerationService`: Generates three candidate post pools sorted by different criteria
- **`context.py`** - `UserContextService`: Builds user-specific context by calculating in-network posts for personalization
- **`feed.py`** - `FeedGenerationService`: Orchestrates feed generation for individual users (ranking → reranking → statistics)
- **`ranking.py`** - `RankingService`: Creates initial ranked candidate feeds based on condition
- **`reranking.py`** - `RerankingService`: Applies business rules and constraints (freshness, length, jitter)
- **`feed_statistics.py`** - `FeedStatisticsService`: Calculates statistics for individual feeds
- **`feed_generation_session_analytics.py`** - `FeedGenerationSessionAnalyticsService`: Calculates session-level analytics across all feeds
- **`export.py`** - `DataExporterService`: Transforms and exports feeds and analytics to storage

### `repositories/` - Data Access Layer

Abstracts data access with repository pattern:

- **`scores_repo.py`** - `ScoresRepository`: Handles loading cached post scores and saving new scores to local storage
- **`feed_repo.py`** - `FeedStorageRepository`: Repository wrapper for feed storage operations using adapters

### `storage/` - Storage Abstraction

Provides adapter pattern for different storage backends:

- **`base.py`** - Abstract base classes:
  - `FeedStorageAdapter`: Interface for writing feeds and analytics
  - `FeedTTLAdapter`: Interface for moving old feeds to cache
  - `SessionMetadataAdapter`: Interface for storing session metadata
- **`adapters.py`** - Concrete implementations:
  - `S3FeedStorageAdapter`: S3 implementation for feeds and analytics
  - `LocalFeedStorageAdapter`: Local storage implementation
  - `S3FeedTTLAdapter`: S3 implementation for TTL management
  - `DynamoDBSessionMetadataAdapter`: DynamoDB implementation for session metadata
- **`exceptions.py`** - Storage-related exceptions (`StorageError`)

### `tests/` - Test Suite

Contains unit and integration tests for the service components.

### `experiments/` - Experimental Work

Contains notebooks and scripts for experimentation with scoring algorithms.

## Key Components

### FeedGenerationOrchestrator

The main orchestrator class that coordinates the entire feed generation process. It:

- Wires together all dependencies using dependency injection
- Executes the 9-step pipeline in sequence
- Handles error propagation and logging
- Manages test mode and user filtering

Key methods:
- `run()`: Main entry point that executes the pipeline
- `_load_data()`: Loads and transforms all input data
- `_score_posts()`: Delegates to ScoringService
- `_generate_feeds()`: Delegates to FeedGenerationService
- `_export_artifacts()`: Delegates to DataExporterService

### Scoring System

The scoring system calculates two types of scores for each post:

1. **Engagement Score**: Based on likeability (log of like count or similarity to popular posts) + freshness (decay based on post age)
2. **Treatment Score**: Engagement score multiplied by treatment algorithm score, which adjusts for:
   - Toxicity (probability of toxic content)
   - Constructiveness (probability of constructive/reasoning content)
   - Superposter penalty (reduces scores for high-volume authors)

Scoring uses caching: scores from previous runs (within lookback window) are loaded and reused, only calculating scores for new posts.

### Feed Generation

For each user, feeds are generated through:

1. **Context Building**: Calculate which posts are "in-network" (from accounts the user follows)
2. **Candidate Pool Selection**: Select the appropriate candidate pool based on user condition
3. **Ranking**: Create initial ranked feed prioritizing in-network posts
4. **Reranking**: Apply business rules:
   - Clip to preprocessing window
   - Enforce freshness (max % of old posts)
   - Truncate to max feed length
   - Apply jitter (random position shifts for experimentation)
5. **Statistics**: Calculate feed statistics (proportion in-network, total posts, etc.)

### Configuration

All algorithm parameters are centralized in `FeedConfig`:

- Feed length and filtering constraints
- Scoring coefficients (toxicity, constructiveness, superposter penalty)
- Freshness scoring parameters (decay functions, lookback periods)
- Business rule parameters (max proportion old posts, jitter amount)
- Storage parameters (TTL keep count)

This makes it easy to tune the algorithm without changing code.

## Data Models

Key data models (defined in `models.py`):

- **`FeedInputData`**: Input data (enriched posts, social networks, superposters)
- **`LoadedData`**: Loaded and transformed input data
- **`CandidatePostPools`**: Three sorted candidate pools
- **`FeedWithMetadata`**: Feed with user metadata and statistics
- **`FeedGenerationSessionAnalytics`**: Session-level analytics
- **`StoredFeedModel`**: Format for persisted feeds

## Usage

```python
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.orchestrator import FeedGenerationOrchestrator

# Create orchestrator with default config
config = FeedConfig()
orchestrator = FeedGenerationOrchestrator(feed_config=config)

# Run pipeline
orchestrator.run(
    users_to_create_feeds_for=None,  # All users if None
    export_new_scores=True,
    test_mode=False
)
```

## Testing

Run tests with:
```bash
pytest services/rank_score_feeds/tests/
```

The test suite includes:
- Unit tests for individual services
- Integration tests for the orchestrator
- Repository tests for data access layer

## Dependencies

Key external dependencies:
- **pandas**: Data manipulation and DataFrame operations
- **pydantic**: Data validation and models
- **lib.aws**: AWS service wrappers (S3, DynamoDB, Athena)
- **lib.db**: Local storage utilities
- Other service dependencies (participant_data, consolidate_enrichment_integrations, etc.)

## Notes

- The service uses dependency injection throughout, making it easy to test and modify individual components
- Scoring is cached to avoid recalculating scores for posts that haven't changed
- Feeds are generated with personalization based on user social networks (in-network vs out-of-network posts)
- The service supports three study conditions for A/B testing different feed algorithms
- All configuration is centralized in `FeedConfig` for easy tuning
