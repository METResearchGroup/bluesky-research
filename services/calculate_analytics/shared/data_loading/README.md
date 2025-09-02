# Data Loading Module

This module provides unified interfaces for loading various types of data used across the analytics system, eliminating code duplication and ensuring consistent data handling patterns.

## Overview

The data loading module is designed to centralize data access patterns for the Bluesky research analytics system. It provides standardized interfaces for loading user data, engagement data, feed data, and ML labels, with built-in filtering for study participants and proper data transformation.

## Modules

### `engagement.py` - User Engagement Data

Loads and processes content engagement data from study users, including:

- **Posts written by study users** (posts, replies, shares/retweets)
- **Likes** - posts liked by study users
- **Content engagement mapping** - maps URIs to engagement records

**Key Functions:**
- `get_content_engaged_with(record_type, valid_study_users_dids)` - Gets content engaged with by record type
- `get_engaged_content(valid_study_users_dids)` - Loads all engagement content in one operation
- `get_content_engaged_with_per_user(engaged_content)` - Maps engagements per user per date

**Data Structure:**
Returns a mapping of post URIs to engagement records, where each engagement contains:
- `did`: User DID who engaged
- `date`: Date of engagement
- `record_type`: Type of engagement (like, post, repost, reply)

### `feeds.py` - Feed Data Management

Handles loading and processing of generated feed data:

- **Feed loading** by partition date
- **User-to-posts mapping** for feeds
- **Feed content aggregation** per user per date

**Key Functions:**
- `get_feeds_for_partition_date(partition_date)` - Loads feeds for a specific date
- `map_users_to_posts_used_in_feeds(partition_date)` - Maps users to posts in their feeds
- `get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)` - Gets feed content per user per day
- `get_all_post_uris_used_in_feeds(user_to_content_in_feeds)` - Gets all unique post URIs from feeds

**Data Structure:**
Returns nested mappings of user DIDs to date-based feed content, with deduplicated post URIs.

### `users.py` - User Data and Demographics

Manages study participant data and demographic information:

- **User demographic loading** from DynamoDB
- **Study condition mapping** (user DID to condition)
- **Week assignment data** loading
- **Valid study user filtering**

**Key Functions:**
- `load_user_demographic_info()` - Loads user demographics (handle, DID, condition)
- `get_user_condition_mapping()` - Creates DID-to-condition mapping
- `load_user_date_to_week_df()` - Loads user date-to-week assignments
- `load_user_data()` - Comprehensive user data loading with transformations

**Data Structure:**
Returns DataFrames with user information, week assignments, and a set of valid study user DIDs for filtering.

### `labels.py` - ML Label Data

Loads and transforms various types of ML labels for content analysis:

- **Perspective API labels** (toxicity, constructive content, etc.)
- **Sociopolitical labels** (political ideology, sociopolitical content)
- **IME labels** (intergroup, moral, emotion, other)
- **Valence classifier labels** (positive/negative/neutral sentiment)

**Key Functions:**
- `get_perspective_api_labels(lookback_start_date, lookback_end_date)` - Loads Perspective API labels
- `get_labels_for_partition_date(integration, partition_date)` - Loads labels for specific date
- `transform_labels_dict(integration, labels_dict)` - Transforms raw labels to analysis-ready format
- `get_all_labels_for_posts(post_uris, partition_dates)` - Comprehensive label loading for posts

**Supported Integrations:**
- `perspective_api` - Google's Perspective API for content safety
- `sociopolitical` - Political content classification
- `ime` - Intergroup Moral Emotions classifier
- `valence_classifier` - Sentiment analysis

### `posts.py` - Post Data Utilities

*Note: This module is currently a placeholder with minimal implementation.*

## Common Patterns

### Data Filtering
All modules consistently filter for `valid_study_users_dids` to ensure only study participant data is included in analyses.

### Date Range Handling
Most functions support date-based filtering using `STUDY_START_DATE` and `STUDY_END_DATE` constants from the shared constants module.

### Memory Management
The engagement and labels modules include garbage collection (`gc.collect()`) to manage memory when processing large datasets.

### Error Handling
User and feed modules include comprehensive error handling with logging for debugging and monitoring.

## Usage Examples

### Loading User Data
```python
from services.calculate_analytics.shared.data_loading.users import load_user_data

user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
```

### Loading Engagement Data
```python
from services.calculate_analytics.shared.data_loading.engagement import get_engaged_content

engaged_content = get_engaged_content(valid_study_users_dids)
```

### Loading Feed Data
```python
from services.calculate_analytics.shared.data_loading.feeds import get_post_uris_used_in_feeds_per_user_per_day

feeds_per_user = get_post_uris_used_in_feeds_per_user_per_day(valid_study_users_dids)
```

### Loading ML Labels
```python
from services.calculate_analytics.shared.data_loading.labels import get_all_labels_for_posts

labels = get_all_labels_for_posts(post_uris, partition_dates)
```

## Dependencies

- **pandas** - Data manipulation and analysis
- **lib.db.manage_local_data** - Local data storage management
- **lib.log.logger** - Logging functionality
- **services.participant_data** - User data access
- **services.fetch_posts_used_in_feeds** - Feed data utilities

## Data Sources

- **Local storage** - Parquet files organized by service and partition date
- **DynamoDB** - User demographic and study data
- **CSV files** - Static user week assignments
- **ML inference services** - Various ML model outputs for content labeling

## Notes

- The module is designed for the Bluesky research study context with specific date ranges and user filtering
- Memory management is important for large datasets, especially in the labels module
- All functions return data in formats optimized for downstream analytics processing
- Error handling and logging are built-in for production monitoring
