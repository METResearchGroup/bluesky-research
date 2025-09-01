# User Feed Analysis 2025-04-08

This analysis examines the content that appeared in study users' feeds during the research period, providing insights into what content users were exposed to and how it varied across different conditions and time periods.

## Overview

This analysis processes feed data from Bluesky users to understand:
- What content appeared in users' feeds on a daily basis
- Content classification patterns in feed content (toxicity, political orientation, emotional content, etc.)
- Daily and weekly trends in feed content characteristics per user
- Differences in feed content across experimental conditions

## Migration Note

This analysis represents a major refactoring and migration from two legacy scripts:
- `services/calculate_analytics/study_analytics/generate_reports/condition_aggregated.py`
- `services/calculate_analytics/study_analytics/calculate_analytics/feed_analytics.py`

The migration provides a more modular, maintainable structure with better separation of concerns and improved error handling.

## Main Script: `main.py`

The main script performs the following operations:

### 1. Data Loading (`do_setup()`)
- **User Data**: Loads study participants and their metadata
- **Feed Data**: Retrieves all content that appeared in users' feeds (posts, likes, reposts, replies)
- **Content Labels**: Loads ML classification labels for feed content including:
  - Perspective API: toxicity and constructiveness scores
  - Sociopolitical: political orientation (left, right, moderate, unclear)
  - IME: intergroup, moral, emotional content classification
  - Valence: positive, negative, neutral sentiment

### 2. Analysis and Aggregation (`do_aggregations_and_export_results()`)

#### Daily Analysis
- Calculates per-user, per-day feed content label metrics
- For each user and date, computes the characteristics of content shown in their feed
- Exports results to CSV with columns like:
  - `avg_prob_toxic`
  - `avg_is_political_left`
  - `prop_is_positive`
  - etc.

#### Weekly Analysis
- Aggregates daily metrics into weekly averages
- Handles missing data appropriately (users with no feed data in a week)
- Provides weekly trends for each user and content classification

### 3. Output Files
- **Daily**: `daily_feed_content_aggregated_results_per_user_{timestamp}.csv`
- **Weekly**: `weekly_feed_content_aggregated_results_per_user_{timestamp}.csv`

## Key Features

- **Comprehensive Coverage**: Analyzes all feed content across all study users
- **Multiple Classifications**: Integrates 4 different ML classification systems
- **Temporal Analysis**: Provides both daily and weekly granularity
- **Robust Data Handling**: Properly handles missing data and edge cases
- **Modular Design**: Separated data loading, analysis, and export functions
- **Error Handling**: Comprehensive error handling with detailed logging

## Migration Benefits

The refactoring from the legacy scripts provides several improvements:

1. **Better Structure**: Clear separation between data loading, analysis, and export
2. **Shared Components**: Reuses shared data loading and analysis functions
3. **Improved Error Handling**: More robust error handling with detailed logging
4. **Maintainability**: Easier to modify and extend individual components
5. **Consistency**: Uses the same patterns as other analyses in the codebase

## Usage

Run the analysis with:
```bash
python main.py
```

Or use the provided shell script:
```bash
./submit_user_feed_analysis.sh
```

The script will automatically:
1. Load all required data (users, feeds, labels)
2. Perform daily and weekly aggregations
3. Export results to CSV files in the `results/` directory

## Legacy Scripts

The original scripts that were migrated:

- **`condition_aggregated.py`**: Generated condition-aggregated CSV files with user demographics and feed averages
- **`feed_analytics.py`**: Calculated average feed content for each user with various ML classification metrics

These scripts have been replaced by the more modular approach in this analysis.
