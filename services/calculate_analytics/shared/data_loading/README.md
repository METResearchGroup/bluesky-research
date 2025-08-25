# Shared Data Loading Modules

This directory contains shared data loading modules for the analytics system, providing unified interfaces for loading various types of data used across the analytics system.

## Overview

The shared data loading modules eliminate code duplication and ensure consistent data handling patterns across all analytics scripts. These modules extract the common data loading logic from existing monolithic scripts and provide clean, reusable interfaces.

## Module Structure

```
shared/data_loading/
├── __init__.py              # Main module exports
├── posts.py                 # Post data loading functionality
├── labels.py                # ML label loading functionality
├── feeds.py                 # Feed data loading functionality
├── users.py                 # User data loading functionality
├── example_usage.py         # Usage examples
├── test_basic_imports.py    # Import validation tests
└── README.md                # This file
```

## Key Benefits

1. **Eliminates Code Duplication**: Common data loading logic is now in one place
2. **Consistent Patterns**: All scripts use the same data loading interfaces
3. **Centralized Configuration**: Data loading behavior is configuration-driven
4. **Easy Maintenance**: Changes to data loading logic only need to be made once
5. **Better Error Handling**: Centralized error handling and logging
6. **Reusability**: Functions can be easily imported and reused across scripts

## Modules

### Posts Module (`posts.py`)

Provides functions for loading and filtering post data:

- `load_filtered_preprocessed_posts()`: Load posts with custom filters (e.g., exclude invalid authors)
- `get_hydrated_posts_for_partition_date()`: Load posts with all available labels and features
- `load_posts_with_labels()`: Merge posts with their ML labels

**Example Usage:**
```python
from services.calculate_analytics.study_analytics.shared.data_loading.posts import (
    load_filtered_preprocessed_posts
)

posts_df = load_filtered_preprocessed_posts(
    partition_date="2024-10-15",
    lookback_start_date="2024-10-08",
    lookback_end_date="2024-10-15"
)
```

### Labels Module (`labels.py`)

Provides functions for loading various types of ML labels:

- `get_perspective_api_labels_for_posts()`: Load Perspective API toxicity labels
- `get_sociopolitical_labels_for_posts()`: Load sociopolitical classification labels
- `get_ime_labels_for_posts()`: Load IME (Intergroup Moral Evaluation) labels
- `get_valence_labels_for_posts()`: Load valence classification labels
- `load_all_labels_for_posts()`: Load and merge all available labels for posts

**Example Usage:**
```python
from services.calculate_analytics.study_analytics.shared.data_loading.labels import (
    load_all_labels_for_posts
)

labels_df = load_all_labels_for_posts(
    posts=posts_df,
    partition_date="2024-10-15",
    lookback_start_date="2024-10-08",
    lookback_end_date="2024-10-15"
)
```

### Feeds Module (`feeds.py`)

Provides functions for loading feed data and creating user-to-posts mappings:

- `get_feeds_for_partition_date()`: Load feeds for a specific partition date
- `map_users_to_posts_used_in_feeds()`: Create mapping from users to posts in their feeds
- `get_feeds_with_post_mapping()`: Convenience function to get both feeds and mapping

**Example Usage:**
```python
from services.calculate_analytics.study_analytics.shared.data_loading.feeds import (
    map_users_to_posts_used_in_feeds
)

users_to_posts = map_users_to_posts_used_in_feeds("2024-10-15")
```

### Users Module (`users.py`)

Provides functions for loading study user data and demographic information:

- `load_study_users()`: Load all study users
- `load_user_demographic_info()`: Load user demographic information as DataFrame
- `get_study_user_manager()`: Get StudyUserManager singleton instance
- `get_study_user_dids()`: Get set of all study user DIDs
- `get_in_network_user_dids()`: Get set of in-network user DIDs
- `get_user_condition_mapping()`: Create mapping from user DID to study condition

**Example Usage:**
```python
from services.calculate_analytics.study_analytics.shared.data_loading.users import (
    load_user_demographic_info
)

user_demographics = load_user_demographic_info()
```

## Configuration

The data loading modules use the shared configuration system. Key configuration options include:

- **Default columns**: Columns to load for posts (`data_loading.default_columns`)
- **Invalid author sources**: Sources for filtering out invalid authors
- **Feature configurations**: Column names and thresholds for different ML features

## Error Handling

All modules include comprehensive error handling:

- **Graceful degradation**: If one label type fails to load, others continue
- **Detailed logging**: All operations are logged with appropriate detail levels
- **Exception propagation**: Errors are logged and re-raised for proper handling
- **Data validation**: Input data is validated before processing

## Testing

The modules include basic import tests to ensure they can be imported without errors:

```bash
cd services/calculate_analytics/study_analytics/shared/data_loading
python test_basic_imports.py
```

## Migration Guide

### From Old Scripts

**Before (old way):**
```python
# Direct imports from individual modules
from services.calculate_analytics.study_analytics.load_data.load_data import (
    load_filtered_preprocessed_posts
)
from services.calculate_analytics.study_analytics.load_data.load_labels import (
    get_perspective_api_labels_for_posts
)
```

**After (new way):**
```python
# Single import from shared module
from services.calculate_analytics.study_analytics.shared.data_loading import (
    load_filtered_preprocessed_posts,
    get_perspective_api_labels_for_posts
)
```

### Benefits of Migration

1. **Cleaner imports**: Single import statement instead of multiple
2. **Consistent interfaces**: All functions follow the same patterns
3. **Better error handling**: Centralized error handling and logging
4. **Configuration-driven**: Behavior can be modified via config files
5. **Easier maintenance**: Changes only need to be made in one place

## Future Enhancements

Planned improvements for the data loading modules:

1. **Caching**: Add intelligent caching for frequently accessed data
2. **Async support**: Add async versions of data loading functions
3. **Data validation**: Enhanced input/output validation
4. **Performance monitoring**: Add performance metrics and monitoring
5. **Batch processing**: Support for batch loading operations

## Contributing

When adding new data loading functionality:

1. **Follow existing patterns**: Use the same function signatures and error handling
2. **Add comprehensive docstrings**: Include Args, Returns, and Examples sections
3. **Update tests**: Add tests for new functionality
4. **Update documentation**: Update this README and example usage
5. **Use configuration**: Make behavior configurable where appropriate

## Support

For questions or issues with the data loading modules:

1. Check the example usage script (`example_usage.py`)
2. Review the configuration files
3. Check the logs for detailed error information
4. Refer to the original source scripts for context
