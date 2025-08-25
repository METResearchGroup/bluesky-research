# Correlation Analysis Implementation

This folder contains the actual implementation code for the toxicity-constructiveness correlation analysis project.

## Project Planning
The project planning, tickets, and documentation are located in:
`projects/correlation_analysis/`

## Implementation Structure
- **Code Location**: `services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`
- **Shared Modules**: `services/calculate_analytics/shared/` (existing)
- **Linear Project**: [MET-47: Toxicity-Constructiveness Correlation Analysis](https://linear.app/metresearch/issue/MET-47/toxicity-constructiveness-correlation-analysis)

## Implementation Phases

### 1. Phase 1: Baseline Correlation Analysis & Framework Implementation ✅ COMPLETED
- **Ticket**: [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework)
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

### 2. Phase 2: Feed Selection Bias Analysis 🔄 NEXT UP
- **Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)
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

### 3. Phase 3: Daily Proportion Calculation Logic Review 📋 PLANNED
- **Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)
- **Status**: 📋 PLANNED

## Notes
- Follow the analytics system refactor spec structure
- Integrate with existing shared modules in `services/calculate_analytics/shared/`
- Focus on shipping quickly for research purposes
- All code changes go here or in the shared modules
- **Phase 1 completed successfully** - baseline correlation analysis framework is working and producing expected results
