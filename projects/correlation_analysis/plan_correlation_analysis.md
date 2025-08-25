# Task Plan: Toxicity-Constructiveness Correlation Analysis

## Project Overview
Investigate confusing correlations between toxicity and constructiveness scores in Bluesky posts through systematic analysis across different data subsets.

## Subtasks and Deliverables

### Phase 1: Baseline Correlation Analysis & Framework Implementation (2 weeks)
**Linear Ticket**: [MET-48](https://linear.app/metresearch/issue/MET-48/phase-1-implement-shared-correlation-analysis-framework)

#### Subtasks
1. **Framework Development** (3 days)
   - Create BaseCorrelationAnalyzer class
   - Implement configuration validation methods
   - Add logging and data validation utilities
   - Create correlation calculation methods (Pearson and Spearman)

2. **Integration** (2 days)
   - Integrate with existing shared modules
   - Write comprehensive tests
   - Verify integration works correctly

3. **Slurm Job Design** (2 days)
   - Design Slurm job for processing 20-30M posts
   - Implement daily batch processing with garbage collection
   - Test job submission and execution

4. **Baseline Analysis** (3 days)
   - Run baseline correlation analysis
   - Generate CSV output
   - Validate results and performance

5. **Documentation** (2 days)
   - Document framework usage
   - Create usage examples
   - Update project documentation

#### Deliverables
- BaseCorrelationAnalyzer class with shared utilities
- Slurm job script for baseline analysis
- Baseline correlation results in CSV format
- Comprehensive test suite
- Framework documentation

#### Effort Estimate
- Framework Development: 3 days
- Integration: 2 days
- Slurm Job Design: 2 days
- Baseline Analysis: 3 days
- Documentation: 2 days
- **Total**: 12 days (2.4 weeks)

### Phase 2: Feed Selection Bias Analysis (1 week)
**Linear Ticket**: [MET-49](https://linear.app/metresearch/issue/MET-49/feed-selection-bias-analysis)

#### Implementation Approach
Based on user requirements, Phase 2 implements a comprehensive data flow to analyze algorithmic selection biases:

1. **Data Collection Pipeline** (2 days):
   - For each feed, get the URIs
   - For each user, get their feeds
   - Get the condition for each user
   - For each condition, get all feeds across users
   - Then for each feed, get post URIs
   - Then load all labels (same as before), then split up post URIs by condition
   - Then recalculate analysis

#### Subtasks
1. **Data Collection & Processing** (2 days)
   - Implement the 7-step data collection pipeline
   - Load posts used in feeds data locally
   - Implement local processing for feed data analysis
   - Split post URIs by condition (reverse_chronological, engagement, representative_diversification)

2. **Analysis Implementation** (2 days)
   - Calculate correlations for feed posts (aggregate)
   - Implement correlation calculation split by condition
   - Implement bias detection metrics
   - Compare with baseline correlations from Phase 1

3. **Results Generation** (1 day)
   - Generate comparison reports
   - Create visualizations
   - Document findings
   - Produce expected results:
     - Spearman/Pearson correlation across all posts used in feeds
     - Spearman/Pearson correlation across all posts used in feeds, split by condition

4. **Documentation** (1 day)
   - Document analysis methodology
   - Update project documentation

#### Expected Results
The analysis will produce two key correlation metrics:
- **Aggregate Feed Correlation**: Overall correlation across all posts used in feeds
- **Condition-Specific Correlations**: Correlations split by feed algorithm condition:
  - reverse_chronological condition
  - engagement condition
  - representative_diversification condition

#### Deliverables
- Feed selection bias analysis results
- Comparison reports between baseline and feed correlations
- Bias detection metrics
- Analysis documentation
- Condition-specific correlation breakdowns

#### Effort Estimate
- Data Collection & Processing: 2 days
- Analysis Implementation: 2 days
- Results Generation: 1 day
- Documentation: 1 day
- **Total**: 6 days (1.2 weeks)

### Phase 3: Daily Proportion Calculation Logic Review (1 week)
**Linear Ticket**: [MET-50](https://linear.app/metresearch/issue/MET-50/daily-proportion-calculation-logic-review)

#### Subtasks
1. **Code Review** (2 days)
   - Review daily probability/proportion calculation code
   - Analyze calculation logic across all fields
   - Identify potential systematic errors

2. **Validation Testing** (2 days)
   - Test calculation logic with known inputs
   - Verify consistency across different metrics
   - Document any issues found

3. **Documentation** (1 day)
   - Document findings and validation results
   - Update project documentation

#### Deliverables
- Calculation logic review report
- Validation test results
- Documentation of findings

#### Effort Estimate
- Code Review: 2 days
- Validation Testing: 2 days
- Documentation: 1 day
- **Total**: 5 days (1 week)

## Dependencies
- **Phase 2** depends on **Phase 1** completion
- **Phase 3** depends on **Phase 1** and **Phase 2** completion

## Parallel Execution Opportunities
- Framework development and Slurm job design can be done in parallel
- Documentation can be done incrementally throughout each phase

## Risk Mitigation
- **Memory Management**: Daily batch processing with garbage collection prevents overflow
- **Statistical Accuracy**: Comprehensive testing ensures correlation calculations are correct
- **Integration Issues**: Incremental development and testing prevents major integration problems

## Success Metrics
- All 3 research questions answered with clear findings
- Framework successfully integrated with existing shared modules
- All analyses produce reproducible results
- Comprehensive documentation for future research use

## Implementation Structure
- **Implementation Code Location**: `services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`
- **Shared Modules Location**: `services/calculate_analytics/shared/` (existing)
- **Planning & Documentation**: This folder (`projects/correlation_analysis/`)
- **Focus**: Ship quickly for research purposes, integrate with existing shared modules
