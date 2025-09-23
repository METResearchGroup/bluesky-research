# Hashtag Analysis Demo

This folder contains demonstration scripts for the hashtag analysis module.

## Files

- `demo.py` - Comprehensive demo showing all functionality with mock data
- `visualization_demo.py` - Creates visualizations for analysis results
- `simple_test.py` - Minimal test script to verify core functionality works
- `README.md` - This file

## Usage

### Quick Test
Run the simple test to verify basic functionality:

```bash
cd services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/demo
python simple_test.py
```

### Full Demo
Run the comprehensive demo with mock data:

```bash
cd services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/demo
python demo.py
```

### Visualization Demo
Create visualizations for the latest analysis:

```bash
cd services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/demo
python visualization_demo.py
```

## Output Structure

The demo creates a proper directory structure:

```
results/
├── analysis/
│   └── <timestamp>/
│       ├── hashtag_analysis_<timestamp>.csv
│       ├── hashtag_overall_<timestamp>.csv
│       ├── hashtag_condition_<condition>_<timestamp>.csv
│       ├── hashtag_period_<period>_<timestamp>.csv
│       └── hashtag_analysis_metadata_<timestamp>.json
└── visualizations/
    └── <timestamp>/
        ├── metadata.json
        ├── condition/
        │   └── top_hashtags_<condition>_<timestamp>.png
        ├── election_date/
        │   └── pre_post_comparison_<timestamp>.png
        └── overall/
            ├── frequency_distribution_<timestamp>.png
            └── top_hashtags_overall_<timestamp>.png
```

## What the Demo Shows

### Simple Test (`simple_test.py`)
- ✅ Hashtag extraction from text
- ✅ Hashtag counting for posts
- ✅ Filtering rare hashtags
- ✅ Election period determination
- ✅ Edge case handling

### Full Demo (`demo.py`)
- 📊 Mock data creation
- 🔍 Hashtag extraction testing
- 📈 Analysis functionality testing
- 🎯 Stratified analysis demonstration
- 📊 DataFrame creation and validation
- 💾 CSV export functionality with proper directory structure

### Visualization Demo (`visualization_demo.py`)
- 📊 Condition-specific visualizations
- 📅 Pre/post election comparison charts
- 📈 Overall frequency distributions
- 📋 Metadata generation
- 🔄 Automatic latest analysis detection

## Requirements

- pandas
- matplotlib (for visualizations)
- seaborn (for visualizations)

The simple test only requires pandas and will work even if matplotlib/seaborn are not installed.
