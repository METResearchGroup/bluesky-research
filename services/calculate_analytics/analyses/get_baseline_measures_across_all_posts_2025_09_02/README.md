# Baseline Measures Analysis Across All Posts 2025-09-02

**Analysis Date**: September 2, 2025  
**Analysis Type**: Baseline Content Label Measures Across All Labeled Posts

## Purpose

This analysis calculates baseline measures (averages and proportions) for each content label across ALL labeled posts in the dataset, providing a comprehensive baseline without any user-specific filtering. This serves as a reference point for comparing user-specific analyses (feed content, engagement patterns) against the overall content landscape.

## What This Analysis Does

### **Core Functionality**
The analysis provides baseline measures by:

1. **Complete Coverage**: Analyzes ALL labeled posts in the dataset, not just those used in feeds or engaged with by study users
2. **Content Label Analysis**: Calculates averages and proportions for multiple ML classifiers:
   - **Perspective API**: Toxic vs. constructive content
   - **Sociopolitical**: Political ideology (left, moderate, right, unclear) and political vs. non-political
   - **IME (Intergroup, Moral, Emotion)**: Content categorization by social dynamics
   - **Valence Classifier**: Emotional tone (positive, negative, neutral)

3. **Time Aggregation**: Provides both daily and weekly aggregated baseline metrics
4. **Memory Efficiency**: Processes data day-by-day to handle large datasets without memory issues

### **Data Flow**

```
All Labeled Posts → Day-by-Day Processing → Calculate Baseline Metrics → 
Daily/Weekly Aggregation → CSV Export
```

## Key Differences from Other Analyses

This analysis differs from other analytics scripts in important ways:

- **User Feed Analysis**: Analyzes only posts that appeared in users' feeds
- **User Engagement Analysis**: Analyzes only posts that users engaged with
- **Baseline Analysis**: Analyzes ALL labeled posts regardless of user interaction

This provides the complete picture of content characteristics across the entire dataset.

## Output Files

### **Data Files**

The analysis generates timestamped CSV files in the `results/` directory:

- **Daily Results**: `daily_baseline_content_label_metrics_{timestamp}.csv`
- **Weekly Results**: `weekly_baseline_content_label_metrics_{timestamp}.csv`

### **File Structure**

Each CSV contains:
- **Date/Week columns**: Temporal identifiers
- **Baseline user identifier**: "baseline" (not tied to specific users)
- **Content label metrics**: Averages and proportions for each ML classifier
- **Sample sizes**: Number of posts contributing to each metric

## Key Metrics Generated

### **Per-Day Baseline Metrics:**
- Average toxicity scores across all labeled posts
- Proportion of constructive content in the dataset
- Political orientation distributions
- Emotional valence patterns
- IME categorization distributions

### **Per-Week Baseline Metrics:**
- Weekly aggregated versions of all daily metrics
- Smoothed trends over time
- Baseline reference points for comparison with user-specific analyses

## Expected Insights

This analysis provides:

1. **Dataset Overview**: Complete picture of content characteristics across all labeled posts
2. **Baseline Reference**: Reference point for comparing user-specific content patterns
3. **Temporal Trends**: How overall content characteristics change over time
4. **Quality Assessment**: Understanding of the overall content landscape
5. **Comparison Context**: Baseline for evaluating whether user feeds/engagement differ from overall content

## File Structure

```
get_baseline_measures_across_all_posts_2025_09_02/
├── main.py                              # Main analysis script
├── submit_baseline_measures_analysis.sh # Slurm script for cluster execution
├── README.md                            # This documentation
└── results/                             # Output directory
    ├── daily_baseline_content_label_metrics_{timestamp}.csv
    └── weekly_baseline_content_label_metrics_{timestamp}.csv
```

## Dependencies

- pandas
- Standard project libraries (lib.helper, lib.log, services.calculate_analytics.shared.*)

## Running the Analysis

### **Using Slurm Script (Recommended)**

The analysis should be run using the provided Slurm script on the cluster:

```bash
cd services/calculate_analytics/analyses/get_baseline_measures_across_all_posts_2025_09_02
sbatch submit_baseline_measures_analysis.sh
```

### **Manual Execution (Alternative)**

If running locally without Slurm:

```bash
cd services/calculate_analytics/analyses/get_baseline_measures_across_all_posts_2025_09_02
python main.py
```

## Technical Details

### **Memory Management**
The script processes data day-by-day to manage memory usage:
- Loads labels for one partition date at a time
- Calculates baseline metrics for that day's data
- Cleans up memory before processing the next day
- Continues processing even if individual days fail

### **Error Handling**
- Comprehensive error handling with detailed logging
- Continues processing even if individual days fail
- Graceful handling of days with no labeled posts
- Detailed error messages for debugging

### **Data Processing**
- Uses `get_all_labels_for_posts(post_uris=None, load_all_labels=True)` to get ALL labeled posts
- Leverages shared analysis functions from the analytics framework
- Transforms results using existing transformation functions for consistency

## Usage in Downstream Analysis

The baseline measures generated by this analysis can be used to:

1. **Compare User Patterns**: Compare individual user feed/engagement patterns against baseline
2. **Identify Anomalies**: Find users whose content patterns significantly differ from baseline
3. **Temporal Analysis**: Track how user patterns change relative to baseline over time
4. **Condition Comparison**: Compare different experimental conditions against baseline
5. **Quality Assessment**: Evaluate whether user experiences differ from overall content landscape

## Integration with Other Analyses

This analysis complements other analytics scripts:

- **User Feed Analysis**: Compare feed content against baseline
- **User Engagement Analysis**: Compare engagement patterns against baseline  
- **Network Analysis**: Compare in-network vs out-of-network against baseline
- **Correlation Analysis**: Use baseline as reference point for correlation studies

## Notes

- The analysis processes ALL labeled posts, which may be a very large dataset
- Memory management is critical for successful execution
- Results provide the most comprehensive baseline available for comparison
- The "baseline" user identifier is used for consistency with existing transformation functions
