# In-Network vs Out-of-Network Feed Label Comparison Analysis

**Analysis Date**: September 2, 2025  
**Analysis Type**: Content Label Comparison Between Network Types

## Purpose

This analysis compares the content characteristics (labels) of posts that are in-network versus out-of-network for each user in the study. It examines whether there are systematic differences in content quality, toxicity, political orientation, and other characteristics between posts from users' social networks versus external sources.

## What This Analysis Does

### **Core Functionality**
The analysis examines content differences by:

1. **Network Type Filtering**: Separates posts into two categories:
   - **In-Network Posts**: Posts from users' social connections (friends, follows)
   - **Out-of-Network Posts**: Posts from users outside the social network

2. **Content Label Analysis**: Analyzes the content characteristics using multiple ML classifiers:
   - **Perspective API**: Toxic vs. constructive content
   - **Sociopolitical**: Political ideology (left, moderate, right, unclear) and political vs. non-political
   - **IME (Intergroup, Moral, Emotion)**: Content categorization by social dynamics
   - **Valence Classifier**: Emotional tone (positive, negative, neutral)

3. **Time Aggregation**: Provides both daily and weekly aggregated metrics per user

### **Usage**

The analysis is run with a command line flag to specify which network type to analyze:

```bash
# Analyze in-network posts
python main.py --network_type in_network

# Analyze out-of-network posts  
python main.py --network_type out_of_network
```

### **Data Flow**

```
Generated Feeds → Parse JSON → Filter by is_in_network flag → Extract Post URIs → 
Load Labels → Calculate Metrics → Daily/Weekly Aggregation → CSV Export
```

## Output Files

### **Data Files**

The analysis generates timestamped CSV files in a `results/{timestamp}/` directory:

- **Daily Results**: `daily_{network_type}_feed_content_analysis_{timestamp}.csv`
- **Weekly Results**: `weekly_{network_type}_feed_content_analysis_{timestamp}.csv`

### **Visualization Files**

The visualization script generates comparison plots:

- **Daily Visualizations**: `daily_{label}_in_network_vs_out_of_network.png`
- **Weekly Visualizations**: `weekly_{label}_in_network_vs_out_of_network.png`

Each plot shows:
- Time series comparing in-network vs out-of-network for a specific content label
- Separate lines for each study condition (engagement, representative_diversification, reverse_chronological)
- Solid lines for in-network, dashed lines for out-of-network
- Color coding by condition (red=engagement, green=representative_diversification, black=reverse_chronological)

## Key Metrics Generated

### **Per-User, Per-Day Metrics:**
- Average toxicity scores for in-network vs out-of-network posts
- Proportion of constructive content in each network type
- Political orientation distributions
- Emotional valence patterns
- IME categorization differences

### **Per-User, Per-Week Metrics:**
- Weekly aggregated versions of all daily metrics
- Smoothed trends over time

## Expected Insights

This analysis can reveal:

1. **Echo Chamber Effects**: Whether in-network content is more homogeneous
2. **Content Quality Differences**: If one network type has higher/lower quality content
3. **Political Polarization**: Whether in-network content is more politically aligned
4. **Toxicity Patterns**: If toxicity levels differ between network types
5. **Temporal Trends**: How these differences change over the study period

## File Structure

```
in_network_out_of_network_feed_label_comparison_2025_09_02/
├── main.py                           # Main analysis script
├── visualize_results.py              # Full visualization script (all traits)
├── plot_toxicity_focused.py          # Focused toxicity visualization script
├── submit_in_network_analysis.sh     # Slurm script for in-network analysis
├── submit_out_of_network_analysis.sh # Slurm script for out-of-network analysis
├── README.md                         # This documentation
└── results/                          # Output directory
    └── {timestamp}/                  # Timestamped results
        ├── daily_in_network_feed_content_analysis_{timestamp}.csv
        ├── weekly_in_network_feed_content_analysis_{timestamp}.csv
        ├── daily_out_of_network_feed_content_analysis_{timestamp}.csv
        ├── weekly_out_of_network_feed_content_analysis_{timestamp}.csv
        └── *.png                     # Visualization files
    └── visualizations/               # Visualization outputs
        └── {timestamp}/              # Timestamped visualization runs
            ├── weekly_toxicity_averages_focused.png  # Focused toxicity plot
            └── ...                    # Other visualization files
```

## Dependencies

- pandas
- matplotlib
- numpy
- Standard project libraries (lib.helper, lib.log, services.calculate_analytics.shared.*)

## Running the Analysis

### **Using Slurm Scripts (Recommended)**

The analysis should be run using the provided Slurm scripts on the cluster:

1. **Submit In-Network Analysis**:
   ```bash
   cd services/calculate_analytics/analyses/in_network_out_of_network_feed_label_comparison_2025_09_02
   sbatch submit_in_network_analysis.sh
   ```

2. **Submit Out-of-Network Analysis**:
   ```bash
   sbatch submit_out_of_network_analysis.sh
   ```

3. **Generate Visualizations** (after both analyses complete):
   ```bash
   # Generate all trait visualizations (720+ plots)
   python visualize_results.py
   
   # OR generate focused toxicity visualization only
   python plot_toxicity_focused.py
   ```

**Important**: You must run both Slurm scripts (in-network and out-of-network) before running the visualization script, as it needs both datasets to create comparison plots.

### **Manual Execution (Alternative)**

If running locally without Slurm:

1. **Run In-Network Analysis**:
   ```bash
   cd services/calculate_analytics/analyses/in_network_out_of_network_feed_label_comparison_2025_09_02
   python main.py --network_type in_network
   ```

2. **Run Out-of-Network Analysis**:
   ```bash
   python main.py --network_type out_of_network
   ```

3. **Generate Visualizations**:
   ```bash
   # Generate all trait visualizations (720+ plots)
   python visualize_results.py
   
   # OR generate focused toxicity visualization only (recommended)
   python plot_toxicity_focused.py
   ```

The visualization scripts will automatically find the most recent analysis results and create comparison plots.

## Visualization Scripts

### **`visualize_results.py` - Full Visualization Suite**
- Generates 720+ visualization files for all content traits
- Creates separate plots for each condition and trait combination
- Includes both daily and weekly time series
- Creates combined plots showing all conditions together
- **Use when**: You need comprehensive visualization of all traits

### **`plot_toxicity_focused.py` - Focused Toxicity Analysis**
- Generates a single, focused toxicity visualization
- Shows weekly toxicity averages by condition (RC, EB, DE)
- Compares in-network vs out-of-network with proper scaling
- Y-axis fixed at 0.0-0.2 for optimal toxicity data viewing
- **Use when**: You specifically want to analyze toxicity patterns
- **Benefits**: Fast execution, focused output, optimized scaling
