# Feed Correlation Analysis

This directory contains scripts for analyzing correlations between various feed metrics from the user feed analysis results.

## Scripts

### `feed_correlation_analysis.py`
Main analysis script that:
- Automatically finds the latest daily and weekly CSV files from the results directory
- Calculates correlations between feed metrics across study conditions
- Generates correlation matrix heatmaps (PNG files)
- Organizes all outputs in timestamped subdirectories

### `test_visualization.py`
Test script to verify the heatmap generation functionality works correctly.

### Shell Scripts
- `submit_feed_correlation_analysis.sh` - Submit the analysis job to Slurm

## Output Structure

Each analysis run creates a timestamped subdirectory in `results/` with the following structure:

```
results/
└── feed_correlation_analysis_YYYY-MM-DD_HH-MM-SS/
    ├── feed_correlation_analysis_YYYY-MM-DD_HH:MM:SS.json  # Combined results
    ├── daily_feed_correlations_YYYY-MM-DD_HH:MM:SS.json    # Daily analysis results
    ├── weekly_feed_correlations_YYYY-MM-DD_HH:MM:SS.json   # Weekly analysis results
    ├── daily_average_all_conditions_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── daily_average_condition_X_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── daily_proportion_all_conditions_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── daily_proportion_condition_X_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── weekly_average_all_conditions_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── weekly_average_condition_X_correlations_YYYY-MM-DD_HH:MM:SS.png
    ├── weekly_proportion_all_conditions_correlations_YYYY-MM-DD_HH:MM:SS.png
    └── weekly_proportion_condition_X_correlations_YYYY-MM-DD_HH:MM:SS.png
```

## Metrics Analyzed

### Average Metrics
- `feed_average_toxic`
- `feed_average_constructive`
- `feed_average_intergroup`
- `feed_average_moral`
- `feed_average_moral_outrage`
- `feed_average_is_sociopolitical`

### Proportion Metrics
- `feed_proportion_toxic`
- `feed_proportion_constructive`
- `feed_proportion_intergroup`
- `feed_proportion_moral`
- `feed_proportion_moral_outrage`
- `feed_proportion_is_sociopolitical`

## Usage

### Run Analysis
```bash
# Submit to Slurm
./submit_feed_correlation_analysis.sh

# Or run directly
python feed_correlation_analysis.py
```

### Test Visualization
```bash
python test_visualization.py
```

## Dependencies

The script requires:
- pandas
- numpy
- matplotlib
- seaborn
- Standard library modules (os, glob, json, datetime, pathlib, typing)

## Input Data

The script expects CSV files in:
`/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/`

Required columns:
- `user_id`
- `condition`
- All average and proportion metric columns listed above

## Output Files

### JSON Files
- **Combined results**: Complete analysis summary with all correlations
- **Daily/Weekly results**: Individual file analysis results
- Contains correlation matrices, pairwise correlations, and summary statistics

### PNG Files
- **Correlation heatmaps**: Visual representation of correlation matrices
- Lower triangular format (like the example image)
- Color-coded: red (negative), white (neutral), blue (positive)
- High resolution (300 DPI) for publication quality

## Analysis Features

1. **Automatic file detection**: Finds latest daily and weekly CSV files
2. **Condition-based analysis**: Analyzes correlations within each study condition
3. **Comprehensive metrics**: Both average and proportion-based correlations
4. **Visual output**: Publication-ready correlation heatmaps
5. **Organized output**: All assets grouped in timestamped directories
6. **Error handling**: Graceful handling of missing columns or data issues
7. **Detailed logging**: Comprehensive progress and result reporting