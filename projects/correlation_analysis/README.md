# Toxicity-Constructiveness Correlation Analysis

## Project Overview
Investigate confusing correlations between toxicity and constructiveness scores in Bluesky posts to determine whether these are real data patterns, algorithmic selection biases, or calculation artifacts.

## Linear Project
- **Project**: [MET-47: Toxicity-Constructiveness Correlation Analysis](https://linear.app/metresearch/issue/MET-47/toxicity-constructiveness-correlation-analysis)
- **Team**: Northwestern
- **Priority**: High

## Research Questions & Implementation
This project addresses 3 key research questions through 3 implementation tickets:

### 1. Baseline Correlation Analysis & Framework Implementation ✅ COMPLETED
- **Ticket**: [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework)
- **Research Question**: "Look at the correlation of toxicity x constructiveness on all posts we have, to see if this is a trend that replicates across a wide sample of Bluesky posts."
- **Effort**: 2 weeks
- **Status**: ✅ COMPLETED
- **Results**: 
  ```json
  {
    "pearson_correlation": -0.1084108810843165,
    "spearman_correlation": -0.08536489169351506,
    "sample_size": 18420828,
    "toxicity_mean": 0.12372691150354528,
    "constructiveness_mean": 0.17767073811275552
  }
  ```
- **Key Finding**: Confirmed expected negative correlation between toxicity and constructiveness across ~18.4M posts, validating the baseline relationship.

### 2. Feed Selection Bias Analysis 🔄 NEXT UP
- **Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)
- **Research Question**: "Assuming the above comes out clean, look at the correlation of toxicity x constructiveness on all posts used in feeds, to see if there's anything in the algorithmic selection that causes this bias."
- **Effort**: 1 week
- **Status**: 🔄 NEXT UP
- **Implementation Approach**:
  1. For each feed, get the URIs
  2. For each user, get their feeds
  3. Get the condition for each user
  4. For each condition, get all feeds across users
  5. Then for each feed, get post URIs
  6. Then load all labels (same as before), then split up post URIs by condition
  7. Then recalculate analysis

- **Expected Results**:
  - Get the Spearman/Pearson correlation across all posts that are used in feeds
  - Get the Spearman/Pearson correlation across all posts that are used in feeds, split by condition (reverse_chronological, engagement, representative_diversification)

### 3. Daily Proportion Calculation Logic Review
- **Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)
- **Research Question**: "Assuming the above two check out, review the logic for calculating the daily probability/proportion checks. I'd be surprised if it were at this step, mostly because the problem would be more systematic since I use the same calculation logic across all the fields."
- **Effort**: 1 week
- **Status**: 📋 PLANNED

## Project Structure

### Project Planning & Documentation
```
projects/correlation_analysis/
├── spec.md                           # Project specification
├── braindump.md                      # Initial brain dump session
├── tickets/                          # Ticket documentation
│   ├── ticket-001.md                # Baseline analysis & framework
│   ├── ticket-002.md                # Feed selection bias
│   └── ticket-003.md                # Calculation logic review
├── README.md                         # Project overview
├── plan_correlation_analysis.md      # Task plan with subtasks and deliverables
├── todo.md                           # Checklist synchronized with Linear issues
├── logs.md                           # Progress log file
├── lessons_learned.md                # Insights and process improvements
├── metrics.md                        # Performance metrics and completion times
└── retrospective/                    # Ticket retrospectives
```

### Implementation Code
```
services/calculate_analytics/
├── shared/                           # Reusable components (existing)
└── analyses/
    └── correlation_analysis_2025_08_24/  # Implementation code
        └── README.md                 # Implementation overview
```

## Success Criteria
- Clear understanding of baseline correlations across 20-30M posts ✅
- Identification or ruling out of algorithmic selection biases
- Validation of daily proportion calculations
- Reproducible analysis scripts integrated with shared modules ✅
- Comprehensive documentation for future research

## Technical Requirements
- Python 3.12+, daily batch processing with garbage collection
- Pearson and Spearman correlations, CSV output format
- Integration with existing shared modules in analytics system
- Memory management for large dataset processing

## Total Timeline
4 weeks total implementation (2 + 1 + 1 weeks)
- **Phase 1**: ✅ COMPLETED (2 weeks)
- **Phase 2**: 🔄 IN PROGRESS (1 week)
- **Phase 3**: 📋 PLANNED (1 week)