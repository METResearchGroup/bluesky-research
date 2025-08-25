# Todo Checklist: Toxicity-Constructiveness Correlation Analysis

This checklist is synchronized with Linear issues and tracks progress across all project phases.

## Phase 1: Baseline Correlation Analysis & Framework Implementation
**Linear Ticket**: [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework)
**Status**: Backlog
**Effort**: 2 weeks

### Framework Development (3 days)
- [ ] Create BaseCorrelationAnalyzer class
- [ ] Implement configuration validation methods
- [ ] Add logging and data validation utilities
- [ ] Create correlation calculation methods (Pearson and Spearman)

### Integration (2 days)
- [ ] Integrate with existing shared modules in analytics system
- [ ] Write comprehensive tests for all utilities
- [ ] Verify integration works correctly

### Slurm Job Design (2 days)
- [ ] Design Slurm job for processing 20-30M posts
- [ ] Implement daily batch processing with garbage collection
- [ ] Test job submission and execution

### Baseline Analysis (3 days)
- [ ] Run baseline correlation analysis across all posts
- [ ] Generate CSV output with correlation results
- [ ] Validate results and performance

### Documentation (2 days)
- [ ] Document framework usage and examples
- [ ] Create usage examples for future researchers
- [ ] Update project documentation

## Phase 2: Feed Selection Bias Analysis
**Linear Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)
**Status**: Backlog
**Effort**: 1 week
**Dependencies**: Phase 1 completion

### Data Loading (1 day)
- [ ] Load posts used in feeds data locally
- [ ] Implement local processing for feed data analysis

### Analysis Implementation (2 days)
- [ ] Calculate correlations between toxicity and constructiveness for feed posts
- [ ] Implement bias detection metrics and analysis
- [ ] Compare correlation patterns between baseline and feed-selected posts

### Results Generation (1 day)
- [ ] Generate comparison reports and visualizations
- [ ] Document findings and bias detection results

### Documentation (1 day)
- [ ] Document analysis methodology
- [ ] Update project documentation with findings

## Phase 3: Daily Proportion Calculation Logic Review
**Linear Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)
**Status**: Backlog
**Effort**: 1 week
**Dependencies**: Phase 1 and Phase 2 completion

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
- [ ] All 3 research questions answered with clear findings
- [ ] Framework successfully integrated with existing shared modules
- [ ] All analyses produce reproducible results
- [ ] Comprehensive documentation for future research use
- [ ] Project ready for ongoing correlation research

## Notes
- Focus on shipping quickly for research purposes
- Incorporate testing and documentation as we develop
- Daily batch processing is critical for memory management
- User expects calculation logic is unlikely to be the source of correlations
- **Implementation Code Location**: `services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`
- **Shared Modules Location**: `services/calculate_analytics/shared/`
- **Planning & Documentation**: This folder (`projects/correlation_analysis/`)
