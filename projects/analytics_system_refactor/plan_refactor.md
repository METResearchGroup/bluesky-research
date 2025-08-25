# Analytics System Refactor Plan

## Overview

This document outlines the plan to refactor the `services/calculate_analytics/` system from monolithic scripts to simple, organized analysis classes using modular components with shared utilities.

## Current State

The analytics system has evolved from one-off research scripts into a critical production analytics platform, but suffers from:

- **Monolithic Scripts**: Single files with 500+ lines mixing multiple concerns
- **Code Duplication**: Similar logic repeated across multiple files  
- **Hardcoded Values**: Study dates, feature lists, and file paths scattered throughout
- **Tight Coupling**: Data loading, processing, and export logic tightly intertwined
- **Limited Testing**: Minimal test coverage making changes risky
- **Poor Documentation**: Unclear purpose statements and limited usage examples

## Target Architecture

### Simple Class-Based Design Pattern

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

### Key Design Principles

- **Simple Inheritance**: Base class provides shared utility methods (configuration validation, logging, data validation)
- **Clear Responsibility**: Each analysis class has a single, obvious purpose
- **Direct Methods**: Simple method signatures like `analyze_partition_date()` instead of complex pipeline interfaces
- **Configuration-Driven**: All parameters externalized to YAML configuration files
- **Research-Focused**: Designed for reproducibility and academic scrutiny, not enterprise orchestration

## Implementation Phases

### Phase 1: Extract Shared Data Loading & Processing (Week 1)
- [x] Create shared configuration structure
- [x] Implement configuration loading utilities  
- [x] Extract all hardcoded constants
- [x] Create shared data loading modules
- [x] Implement unified data loading interfaces
- [x] Extract feature calculation logic
- [x] Extract threshold calculation logic
- [x] Create reusable processing functions

### Phase 2: Implement Simple Analysis Framework (Week 2)
- [ ] **REVISED**: Implement simple analysis framework with shared utilities
- [ ] **REVISED**: Convert core analytics to concrete analysis class implementations
- [ ] **REVISED**: Focus on code organization and reusability, not pipeline orchestration
- [ ] **REVISED**: Remove complex ABC pipeline framework
- [ ] **REVISED**: Implement simple inheritance for shared utilities

### Phase 3: Reorganize One-Off Analyses (Week 3)
- [ ] Move all analyses to focused, dated folders (one analysis per folder)
- [ ] Update analysis scripts to use shared modules
- [ ] Ensure all analyses can be re-run successfully
- [ ] Verify output files match previous versions exactly
- [ ] Each analysis folder contains README.md, investigation.ipynb, main.py, and assets/

### Phase 4: Implement Testing & Validation (Week 4)
- [ ] Achieve >80% test coverage
- [ ] Integration tests pass end-to-end
- [ ] Performance benchmarks met
- [ ] Error handling tested and validated

### Phase 5: Documentation & Cleanup (Week 5)
- [ ] Remove old monolithic scripts
- [ ] Update documentation and make comprehensive
- [ ] Create migration guide
- [ ] Complete final validation

## Key Changes from Original Plan

### **Architecture Simplification**
- **Original**: ABC-based pipeline framework with complex orchestration
- **Revised**: Simple analysis classes with shared utility inheritance
- **Rationale**: Research code doesn't need enterprise pipeline orchestration; focus on code organization and reusability

### **Design Philosophy**
- **Original**: Enterprise software engineering patterns (pipelines, state management, lifecycle hooks)
- **Revised**: Research-focused patterns (simple classes, shared utilities, configuration management)
- **Rationale**: Academic research needs transparency and reproducibility, not workflow orchestration

### **Implementation Approach**
- **Original**: Complex setup/execute/teardown lifecycle with state tracking
- **Revised**: Direct method execution with shared utility methods
- **Rationale**: Research is iterative and stateless; simple execution is better than complex lifecycle management

## Linear Project and MET-40 Updates

### **MET-40 Title and Description Update Required**

**Current MET-40 Title:**
"Phase 2: Implement Simple Pipeline Framework for Consistent Analysis Patterns"

**Proposed MET-40 Title:**
"Phase 2: Implement Simple Analysis Framework with Shared Utilities"

**Current MET-40 Description:**
Focuses on ABC-based pipeline framework with complex orchestration

**Proposed MET-40 Description:**
Focus on simple analysis classes with shared utility inheritance for code organization and reusability

### **Key Changes to Communicate in Linear**

1. **Architecture Simplification**: Removed ABC pipeline complexity in favor of simple classes
2. **Focus Shift**: From enterprise orchestration to research-focused code organization
3. **Implementation Approach**: Direct method execution instead of setup/execute lifecycle
4. **Benefits**: Better transparency, easier debugging, simpler testing for academic research

### **Rationale for Architecture Change**

**Academic Research Requirements:**
- **Transparency**: Code must be easy to understand for peer reviewers
- **Reproducibility**: Results must be identical every time
- **Simplicity**: Research is iterative and stateless
- **Academic Scrutiny**: Nature-level papers require clear, maintainable code

**Research Workflow Alignment:**
- **One-off execution**: Run analyses, not orchestrate workflows
- **Iterative development**: Modify and rerun, not maintain long-running processes
- **Stateless operation**: No need for pipeline state management
- **Direct execution**: Simple method calls are better than complex interfaces

**Code Organization Benefits:**
- **Shared utilities**: Common functionality through simple inheritance
- **Clear responsibility**: Each class has single, obvious purpose
- **Easy testing**: Simple unit tests without complex mocking
- **Maintainability**: Less code to maintain and debug

## Success Criteria

- [ ] All existing analytics outputs can be reproduced exactly
- [ ] New analyses can be created quickly using shared modules
- [ ] Codebase is maintainable with clear separation of concerns
- [ ] Performance improved through better caching and optimization
- [ ] System is extensible for future research needs
- [ ] **NEW**: Code is transparent and easy to understand for peer reviewers
- [ ] **NEW**: Architecture supports academic rigor and reproducibility

## Risk Mitigation

- **Incremental Approach**: Small, testable changes with validation at each step
- **Comprehensive Testing**: Regression tests ensure output consistency
- **Backward Compatibility**: Maintain existing interfaces during transition
- **Rollback Plan**: Ability to revert to previous version if issues arise
- **Simplified Architecture**: Reduced complexity means fewer things can go wrong

## Notes

- This refactoring maintains backward compatibility throughout the process
- All changes are incremental and testable
- Focus is on research-grade reliability and reproducibility, not production complexity
- Simple, organized patterns provide consistency while maintaining flexibility
- The revised approach better aligns with academic research requirements and peer review scrutiny
- **ACTION REQUIRED**: Update Linear project and MET-40 to reflect simplified architecture
