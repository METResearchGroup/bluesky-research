# Time Series Visualizations for Bluesky Study Analytics

This directory contains scripts to generate time series visualizations from the study analytics data.

## Available Scripts

### 1. `time_series.py`

A modular script that generates time series visualizations for all feature columns in the `condition_aggregated.csv` file. It creates separate plots for each feature, showing the average probability over time grouped by condition.

**Features:**
- Generates visualizations for all feature columns
- Excludes non-feature columns (bluesky_handle, user_did, condition, date)
- Shows raw data points with lower opacity in the background
- Creates smoothed trend lines for each condition
- Automatically formats column names for display
- Saves all plots to an output directory

**Usage:**
```bash
python time_series.py
```

### 2. `plot_toxicity.py`

A focused script that recreates specifically the toxicity time series chart shown in the example. This script is a simpler version that only plots the `avg_prob_toxic` column.

**Usage:**
```bash
python plot_toxicity.py
```

## Output

Both scripts create plots in the `output` directory within this folder. The plots are saved as PNG files.

## Data Source

The scripts read data from `services/calculate_analytics/study_analytics/generate_reports/condition_aggregated.csv`.

## Requirements

Required Python packages:
- pandas
- matplotlib
- seaborn
- numpy 