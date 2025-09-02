# Proportion of In-Network Feed Content Analysis

This analysis calculates the proportion of in-network posts in user feeds for each study participant, aggregated at both daily and weekly levels.

## Overview

The analysis examines how much of the content shown to users in their personalized feeds comes from their social network (in-network posts) versus external sources (out-of-network posts). This metric is important for understanding the echo chamber effect and content diversity in personalized feeds.

## What It Does

1. **Loads Feed Data**: Retrieves all generated feeds for valid study users using `get_feeds_per_user()`
2. **Parses Feed Content**: Extracts the `is_in_network` flag from each post in each feed
3. **Calculates Proportions**: For each feed, computes the proportion of posts where `is_in_network = True`
4. **Daily Aggregation**: Averages proportions across all feeds for each user on each day
5. **Weekly Aggregation**: Averages daily proportions to get weekly metrics per user
6. **Exports Results**: Generates CSV files with daily and weekly in-network proportions

## Data Flow

```
Generated Feeds → Parse JSON → Extract is_in_network → Calculate Proportions → Daily Metrics → Weekly Metrics → CSV Export
```

## Output Files

### Data Files

The analysis generates two CSV files with timestamps:

- **Daily Results**: `daily_in_network_feed_content_proportions_{timestamp}.csv`
- **Weekly Results**: `weekly_in_network_feed_content_proportions_{timestamp}.csv`

### Visualization Files

The visualization script generates time series plots:

- **Daily Visualization**: `daily_in_network_proportions_by_condition.png`
- **Weekly Visualization**: `weekly_in_network_proportions_by_condition.png`

The visualizations show:
- Time series of average in-network proportions by condition
- Three distinct colored lines for each study condition:
  - **Red**: engagement condition
  - **Green**: representative_diversification condition  
  - **Black**: reverse_chronological condition
- Individual data points as translucent bars showing variability
- Reference line at 0.5 for context

### Output Schema

Both files contain the following columns:
- `handle`: User's Bluesky handle
- `condition`: Study condition (treatment/control)
- `date`/`week`: Time period
- `feed_average_prop_in_network_posts`: Average proportion of in-network posts (0.0 to 1.0)

## Key Metrics

- **Metric Name**: `feed_average_prop_in_network_posts`
- **Range**: 0.0 (all out-of-network) to 1.0 (all in-network)
- **Aggregation**: Mean across all feeds per user per time period
- **Rounding**: Values rounded to 3 decimal places

## Technical Implementation

### Dependencies

- Uses shared functions from `services.calculate_analytics.shared.analysis.content_analysis`
- Leverages `transform_daily_content_per_user_metrics()` and `transform_weekly_content_per_user_metrics()`
- Follows established patterns from other analytics in the system

### Visualization Features

- **Automatic File Discovery**: Finds the most recent CSV files with timestamps
- **Condition Color Mapping**: Maps each study condition to distinct colors for clear differentiation
- **Time Series Plotting**: Creates professional time series plots with proper styling
- **Variability Indication**: Shows individual data points as translucent bars
- **Timestamped Output**: Organizes visualization files in timestamped directories

### Data Structure

The analysis creates an intermediate structure:
```python
{
    "<user_did>": {
        "<date>": {
            "feed_average_prop_in_network_posts": 0.165
        }
    }
}
```

### Error Handling

Comprehensive try/except blocks around:
- Data loading operations
- Feed parsing and processing
- Metric calculations
- Data transformations
- CSV export operations

## Usage

### Running the Analysis

```bash
cd services/calculate_analytics/analyses/prop_in_network_feed_content_2025_09_02
python main.py
```

### Generating Visualizations

```bash
python visualize_results.py
```

The visualization script automatically finds the latest CSV files and generates time series plots showing in-network proportions by condition.

## Results Location

### Data Files
CSV files are saved to:
```
services/calculate_analytics/analyses/prop_in_network_feed_content_2025_09_02/results/
```

### Visualization Files
PNG files are saved to timestamped subdirectories:
```
services/calculate_analytics/analyses/prop_in_network_feed_content_2025_09_02/results/{timestamp}/
├── daily_in_network_proportions_by_condition.png
└── weekly_in_network_proportions_by_condition.png
```

## Related Analyses

This analysis complements other feed content analyses in the system:
- `user_feed_analysis_2025_04_08/`: General feed content analysis
- Other content labeling and classification analyses

## Notes

- Only processes feeds for valid study users
- Handles empty feeds gracefully with warnings
- Excludes partition dates where feed generation was broken
- Uses the same date range and user filtering as other analytics
