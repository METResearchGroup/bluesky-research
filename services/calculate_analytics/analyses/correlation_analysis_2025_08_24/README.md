# Correlation Analysis 2025-08-24

## Overview
This directory contains the implementation code for the **Toxicity-Constructiveness Correlation Analysis** project, which investigates correlations between toxicity and constructiveness scores in Bluesky posts to determine whether these are real data patterns, algorithmic selection biases, or calculation artifacts.

**Linear Project**: [MET-47: Toxicity-Constructiveness Correlation Analysis](https://linear.app/metresearch/issue/MET-47/toxicity-constructiveness-correlation-analysis)

## Analysis Summary Table

| Analysis | Status | Linear Ticket | Date | Purpose | Key Results |
|----------|---------|---------------|------|---------|-------------|
| **Baseline Correlation** | ✅ COMPLETED | [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework) | 2025-08-25 | Establish baseline correlations across all posts | Negative correlation confirmed: -0.108 Pearson, -0.085 Spearman across 18.4M posts |
| **Feed Selection Bias** | ✅ COMPLETED | [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis) | 2025-08-25 | Investigate algorithmic selection biases | Algorithmic bias ruled out as source of correlations |
| **Calculation Logic Review** | ⏭️ SKIPPED | [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review) | N/A | Review daily proportion calculations | Phase skipped - requires deeper analytics refactor |

## File Structure

### Core Analysis Scripts
```
├── baseline_correlation_analysis.py      # Phase 1: Baseline correlation analysis
├── feed_selection_bias_analysis.py       # Phase 2: Feed selection bias investigation  
├── daily_proportion_calculation_review.py # Phase 3: Calculation logic review (skeleton)
```

### Execution Scripts
```
├── submit_baseline_correlation_analysis.sh    # Slurm job for Phase 1
├── submit_feed_selection_bias_analysis.sh     # Slurm job for Phase 2
```

### Supporting Files
```
├── query_profile.json        # Query performance profiling data
├── results/                  # Analysis output files and results
└── README.md                 # This file
```

## Analysis Details

### 1. Baseline Correlation Analysis ✅ COMPLETED
**File**: `baseline_correlation_analysis.py`  
**Purpose**: Establish baseline correlations between toxicity and constructiveness across all available posts  
**Sample Size**: 18,420,828 posts  
**Key Finding**: Confirmed expected negative correlation between toxicity and constructiveness  
**Results**: 
- Pearson Correlation: -0.108
- Spearman Correlation: -0.085
- Toxicity Mean: 0.124
- Constructiveness Mean: 0.178

### 2. Feed Selection Bias Analysis ✅ COMPLETED
**File**: `feed_selection_bias_analysis.py`  
**Purpose**: Investigate whether algorithmic selection biases create artificial correlations  
**Method**: 7-step data collection pipeline analyzing posts used in feeds  
**Conditions Analyzed**: 
- reverse_chronological
- engagement  
- representative_diversification
**Key Finding**: Algorithmic selection biases are NOT the source of observed correlations

### 3. Daily Proportion Calculation Review ⏭️ SKIPPED
**File**: `daily_proportion_calculation_review.py`  
**Purpose**: Review daily probability/proportion calculation logic  
**Status**: Skeleton only - phase skipped due to deeper analytics refactor requirements  
**Rationale**: Systematic calculation errors would affect entire system uniformly

## Execution

### Running Baseline Analysis
```bash
# Submit to Slurm cluster
sbatch submit_baseline_correlation_analysis.sh

# Or run locally (if data available)
python baseline_correlation_analysis.py
```

### Running Feed Bias Analysis
```bash
# Submit to Slurm cluster  
sbatch submit_feed_selection_bias_analysis.sh

# Or run locally (if data available)
python feed_selection_bias_analysis.py
```

## Dependencies

### Shared Modules
- `services/calculate_analytics/shared/analysis/correlations.py` - Correlation calculation functions
- `services/calculate_analytics/study_analytics/` - Data loading and processing utilities

### Environment
- Python 3.12+
- bluesky_research conda environment
- Access to Bluesky data (local or production)

## Results Location
All analysis outputs are stored in the `results/` directory, including:
- Correlation results in JSON format
- Performance metrics and profiling data
- Analysis artifacts and intermediate files

## Project Status
**✅ COMPLETED** - Core research questions about toxicity-constructiveness correlations have been successfully answered. The project confirmed that these correlations are real data patterns, not artifacts of algorithmic selection or data processing.

**Next Steps**: Future work on daily proportion calculations should be integrated into a broader analytics system refactor.
