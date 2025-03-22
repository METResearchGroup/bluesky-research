# Time Series Visualizations for Bluesky Study Analytics

This directory contains scripts to generate time series visualizations from the study analytics data.

## Available Scripts

### 1. `time_series.py`

A modular script that generates time series visualizations for all feature columns in the `condition_aggregated.csv` file. It creates separate plots for each feature, showing the average probability over time grouped by condition.

**Features:**
- Generates visualizations for all feature columns
- Excludes non-feature columns (bluesky_handle, user_did, condition, date)
- Shows raw data points with lower opacity in the background
- Creates smoothed trend lines using LOESS smoothing
- Uses specific colors for each condition:
  - Reverse Chronological: black
  - Engagement-Based: red
  - Diversified Extremity: green
- Automatically formats column names for display
- Saves all plots to an output directory

**Usage:**
```bash
python time_series.py
```

### 2. `plot_toxicity.py`

A focused script that recreates specifically the toxicity chart. This script is a simpler version that only plots the `avg_prob_toxic` column.

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
- statsmodels (for LOESS smoothing)

## Run Script

The `run_visualizations.sh` script will:
1. Activate the conda environment
2. Check for required dependencies
3. Run both visualization scripts
4. Save all output to the `output` directory

To run all visualizations:
```bash
./run_visualizations.sh
``` 