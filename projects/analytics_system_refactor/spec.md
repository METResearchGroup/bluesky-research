# Analytics System Refactoring Specification

## 1. Problem Definition and Stakeholder Identification

### Problem Statement
The current `services/calculate_analytics/` system has evolved from one-off research scripts into a critical production analytics platform, but it suffers from severe architectural and maintainability issues that impede ongoing research and development.

### Core Problems
1. **Monolithic Scripts**: Single files with 1000+ lines mixing multiple concerns
2. **Code Duplication**: Similar logic repeated across multiple files
3. **Hardcoded Values**: Study dates, feature lists, and file paths scattered throughout
4. **Tight Coupling**: Data loading, processing, and export logic tightly intertwined
5. **Limited Testing**: Minimal test coverage making changes risky
6. **Poor Documentation**: Unclear purpose statements and limited usage examples

### Stakeholders
- **Primary**: Research team using analytics for Bluesky studies
- **Secondary**: Data engineers maintaining the analytics pipeline
- **Tertiary**: Future researchers who need to extend or modify analyses

### Success Criteria
- **Reproducibility**: All component CSV files can be regenerated with identical results
- **Test Coverage**: >80% test coverage across all shared functionality
- **Maintainability**: No single file >500 lines, clear separation of concerns
- **Reusability**: Shared modules can be imported and used independently

## 2. Success Metrics and Validation Criteria

### Functional Requirements
- [ ] All existing analytics outputs can be reproduced exactly
- [ ] New analyses can be created using shared modules
- [ ] Configuration changes don't require code modifications
- [ ] Error handling provides clear, actionable feedback

### Non-Functional Requirements
- **Performance**: Processing time reduced by >40% through caching and optimization
- **Reliability**: <1% error rate in data processing
- **Maintainability**: Code duplication reduced by >50%
- **Testability**: All shared modules have unit tests with >80% coverage

### Validation Criteria
1. **Regression Testing**: All existing CSV outputs match exactly after refactoring
2. **Integration Testing**: Full analytics pipeline runs end-to-end without errors
3. **Performance Testing**: Processing time and memory usage meet targets
4. **Code Quality**: Linting passes, no critical code smells detected
5. **Output Consistency**: 100% output consistency verified between old and new scripts
6. **Production Testing**: New scripts tested in Slurm environment (production-like conditions)
7. **Validation Framework**: Comprehensive validation checklist completed for all analyses
8. **Migration Testing**: Migration testing folder structure created with validation tracking

## 3. Scope Boundaries and Technical Requirements

### In Scope
- Refactoring existing analytics scripts into shared modules
- Creating simple, consistent patterns for data processing
- Implementing configuration management system
- Adding comprehensive testing infrastructure
- Reorganizing one-off analyses into dated folders
- Maintaining backward compatibility during transition

### Out of Scope
- Changing the underlying data sources or formats
- Modifying the business logic of existing analyses
- Creating new analytics capabilities (beyond refactoring)
- Changing the external API or interface contracts

### Technical Requirements
- **Python 3.12+**: Maintain compatibility with current environment
- **Pandas/NumPy**: Preserve existing data processing capabilities
- **Configuration Files**: YAML-based configuration management
- **Testing Framework**: pytest with comprehensive coverage reporting
- **Documentation**: Markdown-based documentation with examples

### Dependencies
- **Internal**: Existing data loading utilities, logging infrastructure
- **External**: No new external dependencies beyond current requirements
- **Infrastructure**: Local file system for data storage, no database changes

## 4. User Experience Considerations

### Developer Experience
- **Clear Module Structure**: Intuitive organization of shared vs. analysis code
- **Simple Import Patterns**: Easy to import and use shared functionality
- **Comprehensive Examples**: Working examples for common use cases
- **Error Messages**: Clear, actionable error messages with context

### Analysis Workflow
- **Quick Start**: New analyses can be created in minutes using shared modules
- **Reproducibility**: All analyses can be re-run with identical results
- **Configuration**: Easy to modify analysis parameters without code changes
- **Output Management**: Consistent output formats and file organization

### Maintenance Experience
- **Clear Documentation**: Every shared module has comprehensive documentation
- **Testing**: Easy to run tests and verify functionality
- **Debugging**: Clear logging and error reporting for troubleshooting
- **Extensibility**: Simple to add new features or modify existing ones

## 5. Technical Feasibility and Estimation

### Architecture Overview

The refactored system will use **simple, organized analysis classes** to provide consistency while maintaining flexibility for different experimental analyses. The focus is on **modular, reusable components** organized through **simple inheritance for shared utilities** rather than complex frameworks.

```
services/calculate_analytics/
├── shared/                           # Reusable components
│   ├── data_loading/                # Data loading modules
│   ├── processing/                   # Data processing modules
│   ├── analyzers/                    # Simple analysis classes with shared utilities
│   ├── config/                       # Configuration management
│   └── utils/                        # Utility functions
├── analyses/                         # One-off analyses
│   ├── description_YYYY_MM_DD/      # Dated analysis folders (one analysis per folder)
│   └── ...
├── tests/                            # Comprehensive test suite
├── config/                           # Configuration files
└── README.md                         # Updated documentation
```

### Simple Class-Based Design Pattern

The analysis framework will use simple classes with inheritance for shared utilities to ensure consistency across different analyses while allowing flexibility in implementation:

- **BaseAnalyzer**: Simple base class with shared utility methods (configuration validation, logging, data validation)
- **Specific Analysis Classes**: Each analysis implements concrete classes that inherit common utilities

This approach provides:
- **Consistent Utilities**: Common functionality shared through inheritance
- **Flexible Implementation**: Each analysis can customize data loading, processing, and output generation
- **Code Reuse**: Common functionality shared through base class
- **Maintainability**: Clear contracts for what utilities are available

### Interface Design Recommendations

The class-based design will include these key patterns:

- **Simple Method Signatures**: Direct analysis methods like `analyze_partition_date()` instead of complex pipeline interfaces
- **Configuration Integration**: Standardized configuration loading and validation patterns
- **Shared Utilities**: Common methods for data validation, logging, and statistical calculations
- **Clear Responsibility**: Each class has a single, obvious purpose

Each experimental analysis will implement these patterns according to their specific research needs while maintaining consistency in the overall structure.

### Analysis Folder Structure

Each analysis folder follows a consistent structure to ensure clarity and reproducibility:

```
analyses/
├── description_YYYY_MM_DD/           # One analysis per folder
│   ├── README.md                     # Description of analysis purpose and methodology
│   ├── investigation.ipynb           # Jupyter notebook with preliminary investigation
│   ├── main.py                       # Main implementation generating analytical assets
│   └── assets/                       # Generated outputs (CSV files, images, etc.)
├── another_analysis_YYYY_MM_DD/     # Another distinct analysis
│   ├── README.md
│   ├── investigation.ipynb
│   ├── main.py
│   └── assets/
└── ...
```

**Folder Naming Convention**: `<description>_<YYYY_MM_DD>` where description is a brief, clear name for the specific analysis being performed. This format ensures valid Python import paths.

**Key Principles**:
- **One Analysis Per Folder**: Each folder contains exactly one distinct analysis
- **Clear Separation**: Investigation, implementation, and outputs are clearly separated
- **Reproducibility**: All components needed to reproduce the analysis are contained within the folder
- **Documentation**: README.md explains what was analyzed and why

### Implementation Phases
1. **Phase 1 (Week 1)**: Extract shared data loading & processing
2. **Phase 2 (Week 2)**: Implement simple analysis framework with shared utilities
3. **Phase 3 (Week 3)**: Reorganize one-off analyses
4. **Phase 4 (Week 4)**: Implement testing & validation
5. **Phase 5 (Week 5)**: Documentation & cleanup

### Risk Assessment
- **Low Risk**: Extracting utility functions and constants
- **Medium Risk**: Refactoring data processing logic
- **High Risk**: Maintaining exact output compatibility during refactoring

### Mitigation Strategies
- **Incremental Approach**: Small, testable changes with validation at each step
- **Comprehensive Testing**: Regression tests ensure output consistency
- **Backward Compatibility**: Maintain existing interfaces during transition
- **Rollback Plan**: Ability to revert to previous version if issues arise

### Effort Estimation
- **Total Effort**: 5 weeks (1 developer)
- **Phase 1**: 1 week (extracting shared modules)
- **Phase 2**: 1 week (analysis framework)
- **Phase 3**: 1 week (analysis reorganization)
- **Phase 4**: 1 week (testing implementation)
- **Phase 5**: 1 week (documentation & cleanup)

## 6. Acceptance Criteria

### Phase 1 Acceptance
- [ ] Shared data loading modules created and tested
- [ ] Existing scripts import from shared modules
- [ ] All tests pass with same outputs
- [ ] Configuration constants externalized

### Phase 2 Acceptance
- [ ] Simple analysis framework implemented with shared utilities
- [ ] Core analytics converted to concrete analysis class implementations
- [ ] Analysis tests pass independently
- [ ] Performance meets or exceeds targets

### Phase 3 Acceptance
- [ ] All analyses moved to focused, dated folders (one analysis per folder)
- [ ] Analysis scripts updated to use shared modules
- [ ] All analyses can be re-run successfully
- [ ] Output files match previous versions exactly
- [ ] Each analysis folder contains README.md, investigation.ipynb, main.py, and assets/

### Phase 4 Acceptance
- [ ] >80% test coverage achieved
- [ ] Integration tests pass end-to-end
- [ ] Performance benchmarks met
- [ ] Error handling tested and validated

### Phase 5 Acceptance
- [ ] Old monolithic scripts removed
- [ ] Documentation updated and comprehensive
- [ ] Migration guide created
- [ ] Final validation completed

## 7. Success Definition

This refactoring will be considered successful when:

1. **All existing analytics outputs can be reproduced exactly** using the new shared modules
2. **New analyses can be created quickly** by importing and combining shared functionality
3. **The codebase is maintainable** with clear separation of concerns and comprehensive testing
4. **Performance is improved** through better caching and optimization
5. **The system is extensible** for future research needs

The refactored system should serve as a foundation for ongoing analytics work while maintaining the reliability and reproducibility of existing research outputs.
