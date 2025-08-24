# Toxicity-Constructiveness Correlation Analysis Specification

## 1. Problem Definition and Stakeholder Identification

### Problem Statement
The current analytics system has identified confusing correlations between toxicity and constructiveness scores in Bluesky posts that require investigation to determine whether these are real data patterns, algorithmic selection biases, or calculation artifacts.

### Core Problem
1. **Unexpected Correlations**: Toxicity and constructiveness scores show correlations that seem counterintuitive or unexpected
2. **Data Source Uncertainty**: Need to determine if correlations exist across the broader post population or only in selected subsets
3. **Algorithmic Bias Investigation**: Potential bias in feed selection algorithms that could create artificial correlations
4. **Calculation Validation**: Verify that daily probability/proportion calculations are not introducing systematic errors

### Stakeholders
- **Primary**: Research team - needs to understand correlation patterns for ongoing studies
- **Secondary**: Data engineers - may need to fix algorithmic biases or calculation issues
- **Tertiary**: Future researchers - will benefit from documented methodology and findings

### Success Criteria
- **Baseline Understanding**: Clear picture of toxicity-constructiveness correlations across 20-30M posts
- **Bias Detection**: Identification of any algorithmic selection biases in feed generation
- **Calculation Validation**: Verification that daily proportion calculations are correct
- **Reproducibility**: All analyses can be re-run with identical results
- **Documentation**: Comprehensive methodology and findings documentation

## 2. Success Metrics and Validation Criteria

### Functional Requirements
- [ ] Baseline correlation analysis completed across all posts (20-30M posts)
- [ ] Feed selection bias analysis completed for posts used in feeds
- [ ] Daily proportion calculation logic reviewed and validated
- [ ] All analyses produce reproducible results
- [ ] Comprehensive documentation created

### Non-Functional Requirements
- **Performance**: Slurm processing handles 20-30M posts within reasonable time limits
- **Memory Management**: Daily batch processing prevents memory overflow
- **Data Quality**: Correlation calculations are statistically sound and accurate
- **Maintainability**: Analysis scripts follow new analytics system structure

### Validation Criteria
1. **Statistical Validation**: Correlation coefficients calculated correctly using both Pearson and Spearman methods
2. **Data Coverage**: Analysis covers complete dataset without missing significant portions
3. **Reproducibility**: All results can be regenerated from source data
4. **Performance**: Slurm jobs complete successfully without memory issues
5. **Integration**: Analysis integrates with existing shared modules and follows new system patterns

## 3. Scope Boundaries and Technical Requirements

### In Scope
- Baseline correlation analysis across all Bluesky posts (20-30M posts)
- Feed selection bias analysis for posts used in feeds
- Daily proportion calculation logic review
- Implementation of rolling correlation calculation methods
- Creation of reproducible analysis scripts
- Integration with new analytics system shared modules

### Out of Scope
- Changing underlying data sources or formats
- Modifying feed generation algorithms (only analysis)
- Creating new ML models or scoring systems
- Changing external API contracts or interfaces

### Technical Requirements
- **Python 3.12+**: Maintain compatibility with current environment
- **Data Processing**: Handle 20-30M posts with daily batch processing
- **Statistical Analysis**: Implement Pearson and Spearman correlations
- **Infrastructure**: Slurm for baseline analysis, local processing for feed analysis
- **Output Format**: CSV files for all correlation results
- **Memory Management**: Garbage collection between daily batches

### Dependencies
- **Internal**: Existing shared modules in `services/calculate_analytics/study_analytics/shared/`
- **Data Sources**: `preprocessed_posts` and `ml_inference_perspective_api` services
- **Infrastructure**: Slurm cluster for large-scale processing
- **Utilities**: `manage_local_data.py` for data loading, `service_constants.py` for metadata

## 4. User Experience Considerations

### Developer Experience
- **Clear Module Structure**: Analysis follows new analytics system patterns
- **Simple Import Patterns**: Easy to import and use shared functionality
- **Comprehensive Examples**: Working examples for correlation analysis
- **Error Messages**: Clear, actionable error messages with context

### Analysis Workflow
- **Quick Start**: New correlation analyses can be created using shared modules
- **Reproducibility**: All analyses can be re-run with identical results
- **Configuration**: Easy to modify analysis parameters without code changes
- **Output Management**: Consistent CSV output formats and file organization

### Maintenance Experience
- **Clear Documentation**: Every analysis script has comprehensive documentation
- **Testing**: Easy to run tests and verify functionality
- **Debugging**: Clear logging and error reporting for troubleshooting
- **Extensibility**: Simple to add new correlation analyses or modify existing ones

## 5. Technical Feasibility and Estimation

### Architecture Overview

The analysis system will use **simple, organized analysis classes** following the analytics system refactor spec to provide consistency while maintaining flexibility for different correlation analyses. The focus is on **modular, reusable components** organized through **simple inheritance for shared utilities**.

```
correlation_analysis_2025_08_24/
├── shared/                           # Reusable components
│   ├── data_loading/                # Data loading modules
│   ├── processing/                   # Data processing modules
│   ├── analyzers/                    # Simple analysis classes with shared utilities
│   ├── config/                       # Configuration management
│   └── utils/                        # Utility functions
├── analyses/                         # One-off analyses
│   ├── baseline_correlation/         # Baseline correlation across all posts
│   ├── feed_selection_bias/         # Feed selection bias analysis
│   └── calculation_validation/       # Daily proportion calculation review
├── tests/                            # Comprehensive test suite
├── config/                           # Configuration files
├── slurm/                            # Slurm job scripts
└── README.md                         # Updated documentation
```

### Simple Class-Based Design Pattern

The analysis framework will use simple classes with inheritance for shared utilities to ensure consistency across different correlation analyses:

- **BaseCorrelationAnalyzer**: Simple base class with shared utility methods (configuration validation, logging, data validation, correlation calculation)
- **Specific Analysis Classes**: Each analysis implements concrete classes that inherit common utilities

This approach provides:
- **Consistent Utilities**: Common functionality shared through inheritance
- **Flexible Implementation**: Each analysis can customize data loading, processing, and output generation
- **Code Reuse**: Common functionality shared through base class
- **Maintainability**: Clear contracts for what utilities are available

### Rolling Correlation Implementation Options

The system will support multiple approaches for rolling correlation calculations:

1. **Daily Correlations**: Calculate correlations within each day's data, then aggregate
2. **Rolling Window**: Maintain rolling window correlations (e.g., 7-day window) that update each day
3. **Cumulative Correlations**: Build correlations that accumulate over time
4. **Configurable Approach**: Allow users to specify preferred method through configuration

### Implementation Phases
1. **Phase 1 (Week 1)**: Implement shared correlation analysis framework
2. **Phase 2 (Week 2)**: Create baseline correlation analysis with Slurm integration
3. **Phase 3 (Week 3)**: Implement feed selection bias analysis
4. **Phase 4 (Week 4)**: Add calculation validation and testing
5. **Phase 5 (Week 5)**: Documentation and integration testing

### Risk Assessment
- **Low Risk**: Implementing correlation calculation utilities and shared modules
- **Medium Risk**: Slurm job design and memory management for large datasets
- **High Risk**: Ensuring statistical accuracy across different correlation methods

### Mitigation Strategies
- **Incremental Approach**: Small, testable changes with validation at each step
- **Comprehensive Testing**: Statistical validation tests ensure calculation accuracy
- **Memory Management**: Daily batch processing with garbage collection prevents overflow
- **Rollback Plan**: Ability to revert to previous version if issues arise

### Effort Estimation
- **Total Effort**: 5 weeks (1 developer)
- **Phase 1**: 1 week (shared framework implementation)
- **Phase 2**: 1 week (baseline analysis with Slurm)
- **Phase 3**: 1 week (feed bias analysis)
- **Phase 4**: 1 week (validation and testing)
- **Phase 5**: 1 week (documentation and integration)

## 6. Acceptance Criteria

### Phase 1 Acceptance
- [ ] Shared correlation analysis framework implemented and tested
- [ ] BaseCorrelationAnalyzer class with common utilities created
- [ ] All tests pass with same outputs
- [ ] Integration with existing shared modules verified

### Phase 2 Acceptance
- [ ] Baseline correlation analysis implemented with Slurm integration
- [ ] Daily batch processing with garbage collection working correctly
- [ ] Both Pearson and Spearman correlations implemented
- [ ] CSV output generation working correctly

### Phase 3 Acceptance
- [ ] Feed selection bias analysis implemented
- [ ] Local processing for feed data working correctly
- [ ] Comparison between baseline and feed correlations working
- [ ] Bias detection metrics implemented

### Phase 4 Acceptance
- [ ] Calculation validation logic implemented
- [ ] Daily proportion calculation review completed
- [ ] Statistical validation tests passing
- [ ] Performance benchmarks met

### Phase 5 Acceptance
- [ ] All analysis scripts documented comprehensively
- [ ] Integration testing completed successfully
- [ ] Final validation completed
- [ ] Project ready for ongoing use

## 7. Success Definition

This correlation analysis project will be considered successful when:

1. **Baseline correlations are clearly understood** across the full 20-30M post dataset
2. **Feed selection biases are identified or ruled out** through systematic analysis
3. **Daily proportion calculations are validated** as not being the source of correlations
4. **All analyses are reproducible** and can be re-run with identical results
5. **The system integrates seamlessly** with the new analytics system architecture
6. **Future researchers can easily extend** the analysis framework for new correlation studies

The refactored system should serve as a foundation for ongoing correlation research while maintaining the reliability and reproducibility of existing research outputs.
