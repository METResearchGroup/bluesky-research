"""
Feed Selection Bias Analysis for Toxicity-Constructiveness Study

This script implements the feed selection bias analysis to investigate whether algorithmic
selection in feed generation creates artificial correlations between toxicity and constructiveness
scores. The analysis compares correlation patterns between posts used in feeds and the baseline
population to identify potential selection biases.

Research Question: "Assuming the baseline analysis comes out clean, look at the correlation of
toxicity x constructiveness on all posts used in feeds, to see if there's anything in the
algorithmic selection that causes this bias."

The analysis will:
- Load posts used in feeds data locally (manageable volume)
- Calculate correlations between toxicity and constructiveness for feed-selected posts
- Compare correlation patterns with baseline analysis results
- Implement bias detection metrics and statistical analysis
- Generate comparison reports and visualizations
- Identify whether feed selection algorithms introduce artificial correlations

This feed bias analysis serves as Phase 2 of the correlation investigation project and
depends on the completion of baseline correlation analysis (Phase 1).
"""
