# User Engagement Analysis 2025-06-16

This analysis examines the content that study users engaged with during the research period, providing insights into user behavior patterns and content preferences.

## Overview

This analysis processes engagement data from Bluesky users to understand:
- What types of content users engage with (posts, likes, reposts, replies)
- Content classification patterns (toxicity, political orientation, emotional content, etc.)
- Daily and weekly engagement trends per user

## Migration Note

This analysis migrated from `services/calculate_analytics/study_analytics/get_user_engagement/get_agg_labels_for_engagements.py` to provide a more modular and maintainable structure.

## Main Script: `main.py`

The main script performs the following operations:

### 1. Data Loading (`do_setup()`)
- **User Data**: Loads study participants and their metadata
- **Engagement Data**: Retrieves all content engaged with by study users (posts, likes, reposts, replies)
- **Content Labels**: Loads ML classification labels for engaged content including:
  - Perspective API: toxicity and constructiveness scores
  - Sociopolitical: political orientation (left, right, moderate, unclear)
  - IME: intergroup, moral, emotional content classification
  - Valence: positive, negative, neutral sentiment

### 2. Analysis and Aggregation (`do_aggregations_and_export_results()`)

#### Daily Analysis
- Calculates per-user, per-day content label proportions
- For each engagement type (post/like/repost/reply), computes the proportion of content with specific characteristics
- Exports results to CSV with columns like:
  - `prop_liked_posts_toxic`
  - `prop_posted_posts_political_left`
  - `prop_replied_posts_positive`
  - etc.

#### Weekly Analysis
- Aggregates daily metrics into weekly averages
- Handles missing data appropriately (users with no engagement in a week)
- Provides weekly trends for each user and content classification

### 3. Output Files
- **Daily**: `daily_content_label_proportions_per_user_{timestamp}.csv`
- **Weekly**: `weekly_content_label_proportions_per_user_{timestamp}.csv`

## Key Features

- **Comprehensive Coverage**: Analyzes all engagement types across all study users
- **Multiple Classifications**: Integrates 4 different ML classification systems
- **Temporal Analysis**: Provides both daily and weekly granularity
- **Robust Data Handling**: Properly handles missing data and edge cases
- **Modular Design**: Separated data loading, analysis, and export functions

## Usage

Run the analysis with:
```bash
python main.py
```

The script will automatically:
1. Load all required data
2. Perform daily and weekly aggregations
3. Export results to CSV files in the `results/` directory
