# Shared Processing Modules

This directory contains reusable processing functions extracted from monolithic analytics scripts to eliminate code duplication and ensure consistent processing patterns across the analytics system.

## Overview

The processing modules provide unified interfaces for:
- **Feature Calculation**: ML feature extraction and calculation from post data
- **Threshold Calculation**: Weekly threshold and date mapping logic for study analysis
- **Engagement Analysis**: User engagement metrics and aggregation
- **Common Utilities**: Shared processing functions and helpers

## Module Structure

```
shared/processing/
├── __init__.py          # Main module exports
├── features.py          # Feature calculation functions
├── thresholds.py        # Threshold calculation functions
├── engagement.py        # Engagement analysis functions
├── utils.py            # Common processing utilities
└── README.md           # This documentation
```

## Features Module (`features.py`)

### Core Functions

#### `calculate_feature_averages(posts_df, user)`
Calculate average feature values for a user's posts.

**Args:**
- `posts_df`: DataFrame containing posts with feature columns
- `user`: User identifier

**Returns:**
- Dictionary containing average values for all features

**Example:**
```python
from shared.processing.features import calculate_feature_averages

averages = calculate_feature_averages(posts_df, "user123")
print(f"Average toxicity: {averages['avg_prob_toxic']}")
```

#### `calculate_feature_proportions(posts_df, user, threshold=0.5)`
Calculate feature proportions for a user's posts using threshold.

**Args:**
- `posts_df`: DataFrame containing posts with feature columns
- `user`: User identifier
- `threshold`: Probability threshold for binary classification

**Returns:**
- Dictionary containing proportions for all features

**Example:**
```python
from shared.processing.features import calculate_feature_proportions

proportions = calculate_feature_proportions(posts_df, "user123", threshold=0.7)
print(f"Proportion of toxic posts: {proportions['prop_toxic_posts']}")
```

#### `calculate_political_averages(posts_df)`
Calculate political ideology averages from posts.

**Args:**
- `posts_df`: DataFrame containing posts with political columns

**Returns:**
- Dictionary containing political averages

#### `calculate_valence_averages(posts_df)`
Calculate valence averages from posts.

**Args:**
- `posts_df`: DataFrame containing posts with valence columns

**Returns:**
- Dictionary containing valence averages

## Thresholds Module (`thresholds.py`)

### Core Functions

#### `map_date_to_static_week(partition_date, wave)`
Map a partition date to a static week number based on the user's wave.

**Args:**
- `partition_date`: Date string in YYYY-MM-DD format
- `wave`: User's wave number (1 or 2)

**Returns:**
- Week number between 1 and 8

**Example:**
```python
from shared.processing.thresholds import map_date_to_static_week

week = map_date_to_static_week("2024-10-15", 1)
print(f"Week number: {week}")
```

#### `get_week_thresholds_per_user_static(user_handle_to_wave_df)`
Get static week thresholds for each user based on Monday->Monday schedule.

**Args:**
- `user_handle_to_wave_df`: DataFrame with user handle and wave information

**Returns:**
- DataFrame with static week assignments for each user and date

#### `get_week_thresholds_per_user_dynamic(qualtrics_logs, user_handle_to_wave_df)`
Get dynamic week thresholds for each user based on survey completion.

**Args:**
- `qualtrics_logs`: DataFrame with survey log data
- `user_handle_to_wave_df`: DataFrame with user handle and wave information

**Returns:**
- DataFrame with dynamic week assignments for each user and date

## Engagement Module (`engagement.py`)

### Core Functions

#### `get_num_records_per_user_per_day(record_type)`
Get all records of a specific type for study users.

**Args:**
- `record_type`: The type of record to get (like, post, follow, repost)

**Returns:**
- Dictionary mapping user IDs to date-based record counts

**Example:**
```python
from shared.processing.engagement import get_num_records_per_user_per_day

likes_per_day = get_num_records_per_user_per_day("like")
print(f"User123 likes on 2024-10-01: {likes_per_day['user123']['2024-10-01']}")
```

#### `aggregate_metrics_per_user_per_day(users, partition_dates)`
Get all engagement metrics for users per day.

**Args:**
- `users`: List of user dictionaries
- `partition_dates`: List of partition dates to process

**Returns:**
- Dictionary with comprehensive engagement metrics per user per day

#### `get_engagement_summary_per_user(users, partition_dates)`
Get engagement summary for each user across all partition dates.

**Args:**
- `users`: List of user dictionaries
- `partition_dates`: List of partition dates to process

**Returns:**
- DataFrame with engagement summary per user

## Utils Module (`utils.py`)

### Core Functions

#### `calculate_probability_threshold_proportions(probability_series, threshold=0.5)`
Calculate the proportion of values above a threshold in a probability series.

**Args:**
- `probability_series`: Series containing probability values
- `threshold`: Probability threshold for binary classification

**Returns:**
- Proportion of values above the threshold

**Example:**
```python
from shared.processing.utils import calculate_probability_threshold_proportions

proportion = calculate_probability_threshold_proportions(
    posts_df['prob_toxic'], threshold=0.7
)
print(f"Proportion above threshold: {proportion}")
```

#### `safe_mean(series, default=0.0)`
Safely calculate the mean of a series, handling empty/NaN cases.

**Args:**
- `series`: Series to calculate mean for
- `default`: Default value to return if calculation fails

**Returns:**
- Mean value or default if calculation fails

#### `validate_probability_series(series)`
Validate that a series contains valid probability values.

**Args:**
- `series`: Series to validate

**Returns:**
- True if series contains valid probabilities, False otherwise

## Configuration Integration

All processing functions integrate with the shared configuration system:

```python
from shared.config import get_config

config = get_config()
feature_config = config.features
study_config = config.study
week_config = config.weeks
```

## Error Handling

All functions include comprehensive error handling:
- Input validation with meaningful error messages
- Safe defaults for edge cases (empty data, NaN values)
- Logging of warnings and errors
- Graceful degradation when possible

## Performance Considerations

- Functions are designed to work with pandas DataFrames efficiently
- Vectorized operations where possible
- Minimal memory allocation
- Configurable batch processing for large datasets

## Testing

Each module includes comprehensive tests:
- Unit tests for individual functions
- Integration tests for module interactions
- Performance benchmarks
- Edge case validation

## Migration Guide

### From Monolithic Scripts

**Before (feed_analytics.py):**
```python
# Calculate averages manually
averages = {
    "user": user,
    "avg_prob_toxic": posts_df["prob_toxic"].dropna().mean(),
    # ... many more manual calculations
}
```

**After (using shared modules):**
```python
from shared.processing.features import calculate_feature_averages

averages = calculate_feature_averages(posts_df, user)
```

### From Weekly Thresholds Script

**Before (calculate_weekly_thresholds_per_user.py):**
```python
# Manual week mapping
def map_date_to_static_week(partition_date, wave):
    # ... complex logic duplicated across files
```

**After (using shared modules):**
```python
from shared.processing.thresholds import map_date_to_static_week

week = map_date_to_static_week(partition_date, wave)
```

## Best Practices

1. **Import from shared modules**: Always use `from shared.processing import ...`
2. **Configuration-driven**: Use configuration values instead of hardcoded constants
3. **Error handling**: Functions handle edge cases gracefully
4. **Type hints**: All public functions include complete type annotations
5. **Documentation**: Functions include detailed docstrings with examples

## Examples

### Complete Feature Analysis Pipeline

```python
from shared.processing.features import (
    calculate_feature_averages,
    calculate_feature_proportions
)
from shared.processing.thresholds import get_week_thresholds_per_user_static

# Load data
posts_df = load_posts_for_user("user123")
user_info = load_user_info("user123")

# Calculate features
averages = calculate_feature_averages(posts_df, "user123")
proportions = calculate_feature_proportions(posts_df, "user123", threshold=0.6)

# Get week thresholds
week_thresholds = get_week_thresholds_per_user_static(user_info)

# Combine results
results = {
    "user": "user123",
    "features": averages,
    "proportions": proportions,
    "week_assignments": week_thresholds
}
```

### Engagement Analysis

```python
from shared.processing.engagement import (
    get_engagement_summary_per_user,
    calculate_engagement_rates
)

# Get engagement summary
summary = get_engagement_summary_per_user(users, partition_dates)

# Calculate engagement rates
rates = calculate_engagement_rates(users, partition_dates)

# Analyze high-engagement users
high_engagement = rates[rates['engagement_rate'] > rates['engagement_rate'].mean()]
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're using absolute imports from the project root
2. **Configuration Errors**: Verify configuration files are properly loaded
3. **Data Type Errors**: Check that input DataFrames have expected column names
4. **Performance Issues**: Use vectorized operations and avoid loops where possible

### Debug Mode

Enable debug logging for detailed function execution:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

When adding new processing functions:

1. Follow the established naming conventions
2. Include comprehensive type hints
3. Add detailed docstrings with examples
4. Include error handling for edge cases
5. Add tests for new functionality
6. Update this README with new function documentation

## Related Documentation

- [Configuration Management](../config/README.md)
- [Data Loading](../data_loading/README.md)
- [Coding Standards](../../../ai-rules/agents/task_instructions/execution/CODING_RULES.md)
- [Project Planning](../../../../projects/analytics_system_refactor/plan_refactor.md)
