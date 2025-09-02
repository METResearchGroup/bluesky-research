# Histogram Analysis for Feed and Engagement Data 2025-09-01

**Analysis Date**: September 1, 2025  
**Analysis Type**: Distribution Analysis of Content Label Metrics

## Purpose

This analysis generates comprehensive histograms for various content label metrics from both feed and engagement data. It provides visual distribution analysis to understand the spread, central tendencies, and patterns in content characteristics across different data sources and time aggregations.

## What This Analysis Does

### **Core Functionality**
The analysis creates distribution visualizations by:

1. **Multi-Dataset Analysis**: Processes data from both feed and engagement analyses
2. **Content Label Metrics**: Analyzes 5 key content classification metrics:
   - **Constructive**: Constructive vs. non-constructive content
   - **Intergroup**: Intergroup content classification
   - **Moral**: Moral content classification
   - **Moral Outrage**: Moral outrage content classification
   - **Sociopolitical**: Political content classification

3. **Dual Metric Types**: Analyzes both average and proportion metrics for each label
4. **Temporal Granularity**: Processes both daily and weekly aggregated data
5. **Statistical Analysis**: Includes mean, median, and standard deviation in visualizations

### **Data Sources**
The analysis processes data from previous analytics runs:

- **Feed Data**: From `user_feed_analysis_2025_04_08`
  - Daily: `daily_feed_content_aggregated_results_per_user.csv`
  - Weekly: `weekly_feed_content_aggregated_results_per_user.csv`

- **Engagement Data**: From `user_engagement_analysis_2025_06_16`
  - Daily: `daily_content_label_proportions_per_user.csv`
  - Weekly: `weekly_content_label_proportions_per_user.csv`

### **Data Flow**

```
Previous Analysis Results → Load CSV Files → Extract Metric Columns → 
Generate Histograms → Statistical Analysis → PNG Export
```

## Output Structure

### **Directory Organization**
The analysis creates a timestamped output directory with the following structure:

```
results/{timestamp}/
├── feed_daily_average/
│   ├── constructive.png
│   ├── intergroup.png
│   ├── moral.png
│   ├── moral_outrage.png
│   └── sociopolitical.png
├── feed_daily_proportion/
│   └── [same 5 PNG files]
├── feed_weekly_average/
│   └── [same 5 PNG files]
├── feed_weekly_proportion/
│   └── [same 5 PNG files]
├── engagement_daily_average/
│   └── [same 5 PNG files]
├── engagement_daily_proportion/
│   └── [same 5 PNG files]
├── engagement_weekly_average/
│   └── [same 5 PNG files]
└── engagement_weekly_proportion/
    └── [same 5 PNG files]
```

### **Total Output**
- **8 subdirectories** (4 datasets × 2 metric types)
- **40 histogram files** (8 subdirectories × 5 metrics)
- **High-resolution PNG files** (300 DPI) suitable for publication

## Histogram Features

### **Visual Elements**
Each histogram includes:
- **Distribution bars**: 30-bin histogram showing data distribution
- **Mean line**: Red dashed line indicating arithmetic mean
- **Median line**: Green dashed line indicating median value
- **Statistics**: Sample size (n) and standard deviation (σ) in title
- **Grid**: Light grid for easier value reading
- **Legend**: Clear identification of mean and median lines

### **Statistical Information**
- **Sample size**: Number of data points contributing to the histogram
- **Mean**: Arithmetic average of the metric values
- **Median**: Middle value of the distribution
- **Standard deviation**: Measure of data spread
- **Data cleaning**: Automatic removal of NaN values

## Key Metrics Analyzed

### **Content Classification Metrics**
1. **Constructive**: Measures of constructive vs. non-constructive content
2. **Intergroup**: Content related to intergroup dynamics and relationships
3. **Moral**: Content with moral implications and moral reasoning
4. **Moral Outrage**: Content expressing moral outrage or indignation
5. **Sociopolitical**: Content with political or social implications

### **Metric Types**
- **Average Metrics**: Mean values of continuous scores
- **Proportion Metrics**: Proportions of binary classifications

## Expected Insights

This analysis provides:

1. **Distribution Understanding**: Visual representation of how content metrics are distributed
2. **Central Tendency**: Clear view of mean and median values for each metric
3. **Data Spread**: Understanding of variability through standard deviation and visual spread
4. **Comparison Context**: Ability to compare distributions across different data sources
5. **Quality Assessment**: Identification of potential data quality issues or outliers
6. **Research Insights**: Foundation for understanding content characteristics in the dataset

## File Structure

```
histogram_analyses_2025_09_01/
├── histogram_analysis.py              # Main analysis script
├── README.md                          # This documentation
└── results/                           # Output directory
    └── {timestamp}/                   # Timestamped results
        ├── feed_daily_average/        # Feed daily average histograms
        ├── feed_daily_proportion/     # Feed daily proportion histograms
        ├── feed_weekly_average/       # Feed weekly average histograms
        ├── feed_weekly_proportion/    # Feed weekly proportion histograms
        ├── engagement_daily_average/  # Engagement daily average histograms
        ├── engagement_daily_proportion/ # Engagement daily proportion histograms
        ├── engagement_weekly_average/ # Engagement weekly average histograms
        └── engagement_weekly_proportion/ # Engagement weekly proportion histograms
```

## Dependencies

- pandas
- matplotlib
- Standard project libraries (lib.helper, lib.log)

## Running the Analysis

### **Prerequisites**
Before running this analysis, ensure that the following analyses have been completed:
- `user_feed_analysis_2025_04_08` (for feed data)
- `user_engagement_analysis_2025_06_16` (for engagement data)

### **Execution**
Run the analysis with:

```bash
cd services/calculate_analytics/analyses/histogram_analyses_2025_09_01
python histogram_analysis.py
```

## Technical Details

### **Column Detection**
The script automatically detects relevant columns using pattern matching:
- **Average columns**: `feed_average_*` or `engagement_average_*`
- **Proportion columns**: `feed_proportion_*` or `engagement_proportion_*`
- **Special handling**: Precise matching for `moral` vs `moral_outrage` and `sociopolitical`

### **Error Handling**
- **File validation**: Checks for file existence before loading
- **Data validation**: Handles missing data and empty datasets gracefully
- **Column detection**: Warns when expected columns are not found
- **Robust processing**: Continues analysis even if individual datasets fail

### **Memory Management**
- **Efficient processing**: Processes one dataset at a time
- **Plot cleanup**: Closes matplotlib figures to prevent memory leaks
- **Directory management**: Creates output directories as needed

## Integration with Other Analyses

This analysis complements other analytics scripts by:

1. **Visualizing Results**: Provides visual representation of metrics from other analyses
2. **Distribution Analysis**: Shows how metrics are distributed across users and time
3. **Quality Assessment**: Helps identify potential data quality issues
4. **Research Foundation**: Provides visual foundation for deeper statistical analysis

## Usage in Research

The generated histograms can be used for:

1. **Exploratory Data Analysis**: Understanding data distributions before formal analysis
2. **Publication Figures**: High-resolution plots suitable for research papers
3. **Quality Control**: Identifying outliers or data quality issues
4. **Method Validation**: Verifying that metrics behave as expected
5. **Comparative Analysis**: Comparing distributions across different data sources

## Notes

- The analysis requires completed runs of feed and engagement analyses
- Output files are high-resolution (300 DPI) suitable for publication
- The script automatically handles missing data and continues processing
- All histograms include statistical summaries for easy interpretation
- The timestamped output structure allows for multiple analysis runs without conflicts
