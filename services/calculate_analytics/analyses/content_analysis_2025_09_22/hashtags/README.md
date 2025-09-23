"""README for Hashtag Analysis Module

This module implements comprehensive hashtag analysis for feed content,
extracting and analyzing hashtag usage patterns across experimental 
conditions and time periods (pre/post election).

## Overview

The hashtag analysis module provides insights into how hashtag adoption 
and popularity vary across different feed conditions and temporal periods,
enabling understanding of viral content patterns and topic trends in 
political discourse.

## Features

- **Hashtag Extraction**: Regex-based extraction with case normalization
- **Stratified Analysis**: Analysis by condition and pre/post election periods
- **Frequency Filtering**: Configurable minimum frequency thresholds
- **Standardized Output**: CSV format compatible with visualization tools
- **Visualizations**: Multiple chart types for analysis results
- **Comprehensive Testing**: Full test suite for all functionality

## Module Structure

```
hashtags/
├── main.py                    # Main execution file
├── load_data.py              # Data loading utilities
├── hashtag_analysis.py       # Core analysis functions
├── visualization.py          # Visualization functions
├── tests/
│   └── test_hashtag_analysis.py  # Comprehensive test suite
└── README.md                 # This file
```

## Usage

### Basic Usage

```bash
python main.py
```

### Programmatic Usage

```python
from main import do_setup, do_analysis_and_export_results

# Setup data
setup_objs = do_setup()

# Run analysis
do_analysis_and_export_results(
    user_df=setup_objs["user_df"],
    user_to_content_in_feeds=setup_objs["user_to_content_in_feeds"],
    posts_data=setup_objs["posts_data"],
    partition_dates=setup_objs["partition_dates"],
)
```

## Output Format

### Main Results CSV
Standardized format: `condition | pre_post_election | hashtag | count | proportion`

### Pre-sliced CSV Files
- `hashtag_overall_<timestamp>.csv` - Overall hashtag analysis
- `hashtag_condition_<condition>_<timestamp>.csv` - By condition
- `hashtag_period_<period>_<timestamp>.csv` - By election period

### Visualizations
- `top_hashtags_by_condition_<timestamp>.png` - Top hashtags by condition
- `pre_post_election_comparison_<timestamp>.png` - Pre/post election comparison
- `hashtag_frequency_distribution_<timestamp>.png` - Frequency distribution
- `condition_election_heatmap_<timestamp>.png` - Condition-election period heatmap

### Metadata
- `hashtag_analysis_metadata_<timestamp>.json` - Analysis metadata and statistics

## Configuration

### Constants
- `ELECTION_DATE`: "2024-11-05" - Election date for pre/post analysis
- `MIN_HASHTAG_FREQUENCY`: 5 - Minimum frequency threshold for hashtag inclusion
- `HASHTAG_REGEX`: `r'#\w+'` - Regex pattern for hashtag extraction

### Customization
You can modify these constants in `hashtag_analysis.py` to adjust the analysis parameters.

## Dependencies

### Required Packages
- pandas
- matplotlib
- seaborn
- numpy

### Internal Dependencies
- `lib.helper` - Utility functions
- `lib.log.logger` - Logging
- `services.calculate_analytics.shared.*` - Shared data loading modules

## Testing

Run the comprehensive test suite:

```bash
python tests/test_hashtag_analysis.py
```

The test suite covers:
- Hashtag extraction functionality
- Analysis and processing functions
- DataFrame creation and validation
- Visualization generation
- Integration testing

## Integration

This module integrates with the existing analytics infrastructure:

- **Data Loading**: Uses shared data loading modules for consistency
- **User Data**: Leverages existing user condition data
- **Feed Data**: Works with existing feed content data
- **Output Format**: Follows established CSV schema patterns
- **Directory Structure**: Uses timestamped results directories

## Performance Considerations

- **Memory Usage**: Efficient data structures for large datasets
- **Regex Performance**: Optimized regex patterns for hashtag extraction
- **Batch Processing**: Processes data in manageable chunks
- **Filtering**: Early filtering of rare hashtags to reduce memory usage

## Error Handling

The module includes comprehensive error handling:

- **Data Validation**: Validates input data and analysis results
- **Graceful Degradation**: Continues processing when individual components fail
- **Detailed Logging**: Comprehensive logging for debugging and monitoring
- **Exception Handling**: Proper exception handling with informative error messages

## Future Enhancements

Potential future improvements:

- **Advanced Filtering**: More sophisticated hashtag filtering algorithms
- **Temporal Analysis**: More granular temporal analysis (weekly, monthly)
- **Network Analysis**: Hashtag co-occurrence and network analysis
- **Sentiment Analysis**: Integration with sentiment analysis for hashtag context
- **Real-time Processing**: Support for real-time hashtag analysis

## Troubleshooting

### Common Issues

1. **No Posts Data**: Ensure posts data is available for the specified partition dates
2. **Memory Issues**: Reduce `MIN_HASHTAG_FREQUENCY` or process data in smaller chunks
3. **Visualization Errors**: Check matplotlib/seaborn installation and dependencies
4. **Import Errors**: Ensure all required packages and modules are installed

### Debug Mode

Enable debug logging by modifying the logger level in `main.py`:

```python
logger.setLevel(logging.DEBUG)
```

## Contributing

When contributing to this module:

1. Follow existing code patterns and conventions
2. Add comprehensive tests for new functionality
3. Update documentation for any API changes
4. Ensure integration with existing shared modules
5. Maintain backward compatibility where possible

## License

This module is part of the Bluesky Research project and follows the same licensing terms.
