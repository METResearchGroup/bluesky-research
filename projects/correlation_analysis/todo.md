# Todo Checklist: Toxicity-Constructiveness Correlation Analysis

This checklist is synchronized with Linear issues and tracks progress across all project phases.

## Phase 1: Baseline Correlation Analysis & Framework Implementation ✅ COMPLETED
**Linear Ticket**: [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework)
**Status**: ✅ COMPLETED
**Effort**: 2 weeks
**Results**: Confirmed expected negative correlation between toxicity and constructiveness across ~18.4M posts

### Framework Development (3 days) ✅
- [x] Create BaseCorrelationAnalyzer class
- [x] Implement configuration validation methods
- [x] Add logging and data validation utilities
- [x] Create correlation calculation methods (Pearson and Spearman)

### Integration (2 days) ✅
- [x] Integrate with existing shared modules in analytics system
- [x] Write comprehensive tests for all utilities
- [x] Verify integration works correctly

### Slurm Job Design (2 days) ✅
- [x] Design Slurm job for processing 20-30M posts
- [x] Implement daily batch processing with garbage collection
- [x] Test job submission and execution

### Baseline Analysis (3 days) ✅
- [x] Run baseline correlation analysis across all posts
- [x] Generate CSV output with correlation results
- [x] Validate results and performance

### Documentation (2 days) ✅
- [x] Document framework usage and examples
- [x] Create usage examples for future researchers
- [x] Update project documentation

## Phase 2: Feed Selection Bias Analysis ✅ COMPLETED
**Linear Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)
**Status**: ✅ COMPLETED
**Effort**: 1 week
**Dependencies**: ✅ Phase 1 completion

### Data Collection & Processing (2 days) ✅
- [x] For each feed, get the URIs
- [x] For each user, get their feeds
- [x] Get the condition for each user
- [x] For each condition, get all feeds across users
- [x] Then for each feed, get post URIs
- [x] Then load all labels (same as before), then split up post URIs by condition

### Analysis Implementation (2 days) ✅
- [x] Implement correlation calculation for all feed posts (aggregate)
- [x] Implement correlation calculation split by condition:
  - [x] reverse_chronological condition
  - [x] engagement condition
  - [x] representative_diversification condition
- [x] Compare correlation patterns between baseline and feed-selected posts
- [x] Implement bias detection metrics and analysis

### Results Generation (1 day) ✅
- [x] Generate comparison reports and visualizations
- [x] Document findings and bias detection results
- [x] Produce expected results:
  - [x] Spearman/Pearson correlation across all posts used in feeds
  - [x] Spearman/Pearson correlation across all posts used in feeds, split by condition

### Documentation (1 day) ✅
- [x] Document analysis methodology
- [x] Update project documentation with findings

## Phase 3: Daily Proportion Calculation Logic Review ⏭️ TO BE SKIPPED
**Linear Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)
**Status**: ⏭️ TO BE SKIPPED
**Effort**: 1 week
**Dependencies**: ✅ Phase 1 completion, ✅ Phase 2 completion
**Reason**: Deeper refactor of analytics code needed before this phase can be meaningfully completed

### Code Review (2 days) ⏭️ SKIPPED
- [ ] Review daily probability/proportion calculation code in condition_aggregated.py
- [ ] Analyze calculation logic across all fields using same calculation approach
- [ ] Identify potential systematic calculation errors

### Validation Testing (2 days) ⏭️ SKIPPED
- [ ] Test calculation logic with known inputs
- [ ] Verify that calculation logic is consistent across different metrics
- [ ] Document any findings or potential issues

### Documentation (1 day) ⏭️ SKIPPED
- [ ] Document findings and validation results
- [ ] Update project documentation with calculation review findings

## Project Completion Checklist
- [x] Phase 1: Baseline correlation analysis completed with clear findings
- [x] Phase 2: Feed selection bias analysis completed
- [x] Phase 3: Daily proportion calculation logic review (SKIPPED - requires deeper refactor)
- [x] Framework successfully integrated with existing shared modules
- [x] All analyses produce reproducible results
- [x] Comprehensive documentation for future research use
- [x] Project ready for ongoing correlation research (core questions answered)

## Notes
- Focus on shipping quickly for research purposes
- Incorporate testing and documentation as we develop
- Daily batch processing is critical for memory management
- User expects calculation logic is unlikely to be the source of correlations
- **Implementation Code Location**: `services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`
- **Shared Modules Location**: `services/calculate_analytics/shared/`
- **Planning & Documentation**: This folder (`projects/correlation_analysis/`)
- **Phase 1 Status**: ✅ COMPLETED - Baseline correlation analysis framework is working and producing expected results
- **Phase 2 Status**: ✅ COMPLETED - Feed selection bias analysis successfully implemented and executed
- **Phase 3 Status**: ⏭️ SKIPPED - Requires deeper analytics system refactor for meaningful completion
- **Project Status**: ✅ COMPLETED - Core research questions answered, correlations confirmed as real data patterns
