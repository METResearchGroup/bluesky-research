# Progress Log: Toxicity-Constructiveness Correlation Analysis

## Project Setup - 2025-08-24

### Initial Setup Completed
- [x] Brain dump session completed and documented
- [x] Comprehensive specification document created
- [x] Linear project MET-47 created with Northwestern team
- [x] 3 implementation tickets created and linked to project:
  - MET-48: Baseline Correlation Analysis & Framework Implementation
  - MET-49: Feed Selection Bias Analysis  
  - MET-50: Daily Proportion Calculation Logic Review
- [x] Project folder structure created following analytics system refactor spec
- [x] Ticket documentation files created
- [x] Project README and tracking files created

### Project Structure Established
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
├── logs.md                           # This progress log file
├── lessons_learned.md                # Insights and process improvements (to be populated)
├── metrics.md                        # Performance metrics and completion times (to be populated)
└── retrospective/                    # Ticket retrospectives (to be populated)
```

### Implementation Structure
```
services/calculate_analytics/
├── shared/                           # Reusable components (existing)
└── analyses/
    └── correlation_analysis_2025_08_24/  # Implementation code
        └── README.md                 # Implementation overview
```

## Framework Implementation - 2025-08-24

### MET-48 Initial Framework Files Created
- [x] Created baseline_correlation_analysis.py with comprehensive docstring
- [x] Created feed_selection_bias_analysis.py with comprehensive docstring
- [x] Created daily_proportion_calculation_review.py with comprehensive docstring
- [x] Updated analyses README.md to reflect new structure

### GitHub Operations Completed
- [x] Created feature branch: feature/MET-48_correlation_analysis_framework
- [x] Committed all framework files with comprehensive docstrings
- [x] Pushed branch to remote repository
- [x] Created Pull Request #205: "(MET-48) Correlation Analysis Framework Files"
- [x] PR includes detailed description, Linear links, and completed subtasks

### PR Details
- **URL**: https://github.com/METResearchGroup/bluesky-research/pull/205
- **Status**: Open, needs review
- **Labels**: feature, needs-review
- **Files Added**: 4 new files with comprehensive documentation

### Next Steps
- [ ] Wait for PR review and approval
- [ ] Begin manual implementation of baseline correlation analysis logic
- [ ] Implement Slurm job design for large-scale processing
- [ ] Add actual correlation calculation methods and data processing logic

### Notes
- Project successfully follows analytics system refactor spec structure
- All 3 research questions clearly defined and mapped to tickets
- Dependencies properly established between phases
- Focus on shipping quickly for research purposes
- Integration with existing shared modules is key priority

## Implementation Phase 1 - 2025-08-25 ✅ COMPLETED

### MET-48 Baseline Correlation Analysis Completed
- [x] Baseline correlation analysis framework implemented and tested
- [x] Successfully processed ~18.4M posts across all available data
- [x] Generated comprehensive correlation results with expected negative correlation
- [x] Results validated and confirmed baseline relationship between toxicity and constructiveness

### Key Results Achieved
```json
{
  "pearson_correlation": -0.1084108810843165,
  "spearman_correlation": -0.08536489169351506,
  "sample_size": 18420828,
  "toxicity_mean": 0.12372691150354528,
  "constructiveness_mean": 0.17767073811275552
}
```

### Phase 1 Deliverables Completed
- [x] ✅ Baseline correlation analysis framework
- [x] ✅ Processing of 20-30M posts (achieved 18.4M)
- [x] ✅ Pearson and Spearman correlations calculated
- [x] ✅ CSV output format implemented
- [x] ✅ Integration with existing shared modules
- [x] ✅ Memory management for large dataset processing
- [x] ✅ Comprehensive documentation and examples

### Phase 1 Status: ✅ COMPLETED
**Research Question Answered**: "Look at the correlation of toxicity x constructiveness on all posts we have, to see if this is a trend that replicates across a wide sample of Bluesky posts."

**Finding**: Confirmed expected negative correlation between toxicity and constructiveness across ~18.4M posts, validating the baseline relationship. The correlation is consistent with expectations and provides a solid foundation for Phase 2 analysis.

### Next Steps for Phase 2
- [ ] Begin MET-49: Feed Selection Bias Analysis
- [ ] Analyze correlation patterns in feed-selected posts vs. baseline
- [ ] Implement bias detection metrics and analysis

## Implementation Phase 2 - 🔄 IN PROGRESS
*Phase 2 (MET-49) is now the active focus*

### MET-49 Feed Selection Bias Analysis Implementation Plan
**Date**: 2025-08-25
**Status**: 🔄 IMPLEMENTATION PLANNING

#### Detailed Implementation Approach
Based on user requirements, Phase 2 will implement the following data flow:

1. **Data Collection Pipeline**:
   - For each feed, get the URIs
   - For each user, get their feeds
   - Get the condition for each user
   - For each condition, get all feeds across users
   - Then for each feed, get post URIs
   - Then load all labels (same as before), then split up post URIs by condition
   - Then recalculate analysis

#### Expected Results
The analysis will produce:
- **Aggregate Feed Correlation**: Spearman/Pearson correlation across all posts that are used in feeds
- **Condition-Specific Correlations**: Spearman/Pearson correlation across all posts that are used in feeds, split by condition:
  - reverse_chronological condition
  - engagement condition
  - representative_diversification condition

#### Implementation Strategy
- **Data Processing**: 2 days for data collection and processing pipeline
- **Analysis Implementation**: 2 days for correlation calculations and bias detection
- **Results Generation**: 1 day for reports and visualizations
- **Documentation**: 1 day for methodology documentation

#### Next Steps
- [ ] Implement data collection pipeline following the 7-step approach
- [ ] Create correlation analysis framework for feed posts
- [ ] Implement condition-specific correlation analysis
- [ ] Compare results with Phase 1 baseline correlations

## Implementation Phase 3 - 📋 PLANNED
*Phase 3 (MET-50) depends on Phase 2 completion*

## Project Completion - TBD
*To be populated when project is completed*
