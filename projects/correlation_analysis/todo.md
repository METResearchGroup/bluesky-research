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

## Phase 2: Feed Selection Bias Analysis 🔄 NEXT UP
**Linear Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)
**Status**: 🔄 NEXT UP
**Effort**: 1 week
**Dependencies**: ✅ Phase 1 completion

### Data Collection & Processing (2 days)
- [ ] For each feed, get the URIs
- [ ] For each user, get their feeds
- [ ] Get the condition for each user
- [ ] For each condition, get all feeds across users
- [ ] Then for each feed, get post URIs
- [ ] Then load all labels (same as before), then split up post URIs by condition

### Analysis Implementation (2 days)
- [ ] Implement correlation calculation for all feed posts (aggregate)
- [ ] Implement correlation calculation split by condition:
  - [ ] reverse_chronological condition
  - [ ] engagement condition
  - [ ] representative_diversification condition
- [ ] Compare correlation patterns between baseline and feed-selected posts
- [ ] Implement bias detection metrics and analysis

### Results Generation (1 day)
- [ ] Generate comparison reports and visualizations
- [ ] Document findings and bias detection results
- [ ] Produce expected results:
  - [ ] Spearman/Pearson correlation across all posts used in feeds
  - [ ] Spearman/Pearson correlation across all posts used in feeds, split by condition

### Documentation (1 day)
- [ ] Document analysis methodology
- [ ] Update project documentation with findings

## Phase 3: Daily Proportion Calculation Logic Review 📋 PLANNED
**Linear Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)
**Status**: 📋 PLANNED
**Effort**: 1 week
**Dependencies**: ✅ Phase 1 completion, Phase 2 completion

### Code Review (2 days)
- [ ] Review daily probability/proportion calculation code in condition_aggregated.py
- [ ] Analyze calculation logic across all fields using same calculation approach
- [ ] Identify potential systematic calculation errors

### Validation Testing (2 days)
- [ ] Test calculation logic with known inputs
- [ ] Verify that calculation logic is consistent across different metrics
- [ ] Document any findings or potential issues

### Documentation (1 day)
- [ ] Document findings and validation results
- [ ] Update project documentation with calculation review findings

## Project Completion Checklist
- [x] Phase 1: Baseline correlation analysis completed with clear findings
- [ ] Phase 2: Feed selection bias analysis completed
- [ ] Phase 3: Daily proportion calculation logic review completed
- [x] Framework successfully integrated with existing shared modules
- [x] All analyses produce reproducible results
- [x] Comprehensive documentation for future research use
- [ ] Project ready for ongoing correlation research

## Notes
- Focus on shipping quickly for research purposes
- Incorporate testing and documentation as we develop
- Daily batch processing is critical for memory management
- User expects calculation logic is unlikely to be the source of correlations
- **Implementation Code Location**: `services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`
- **Shared Modules Location**: `services/calculate_analytics/shared/`
- **Planning & Documentation**: This folder (`projects/correlation_analysis/`)
- **Phase 1 Status**: ✅ COMPLETED - Baseline correlation analysis framework is working and producing expected results
