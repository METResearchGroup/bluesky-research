"""
Baseline Correlation Analysis for Toxicity-Constructiveness Study

This script implements the baseline correlation analysis across all Bluesky posts (20-30M posts)
to investigate the relationship between toxicity and constructiveness scores. The analysis
aims to determine whether observed correlations are real data patterns that replicate across
a wide sample of Bluesky posts, or if they are artifacts of data selection or processing.

Research Question: "Look at the correlation of toxicity x constructiveness on all posts we have,
to see if this is a trend that replicates across a wide sample of Bluesky posts."

The analysis will:
- Process all available posts using Slurm for large-scale data handling
- Calculate both Pearson and Spearman correlation coefficients
- Implement daily batch processing with garbage collection for memory management
- Generate CSV output with correlation results and statistical summaries
- Provide baseline understanding of toxicity-constructiveness relationships

This baseline analysis serves as Phase 1 of the correlation investigation project and will
be compared against feed selection bias analysis and calculation logic review in subsequent phases.
"""
