# 🚀 Execution Plan for MET-41: Phase 3 - Reorganize One-Off Analyses

## 🚀 Execution Plan - Approved for Implementation

This document outlines the approved execution plan for reorganizing all existing one-off analysis scripts into a structured, dated folder system using the newly implemented simplified analyzer architecture.

---

## 1. Executive Summary

**Objective**: Reorganize all existing one-off analysis scripts into a structured, dated folder system using the newly implemented simplified analyzer architecture.

**Current State**: 
- ✅ Simplified architecture implemented (MET-40 complete)
- ✅ BaseAnalyzer, FeedAnalyzer, EngagementAnalyzer, WeeklyThresholdsAnalyzer classes ready
- ✅ All pipeline complexity removed
- ✅ Direct method execution working

**Proposed Approach**: Create a systematic migration process that converts 15+ existing analysis scripts to use the new analyzer classes while maintaining 100% output consistency and creating a standardized folder structure for future analyses.

**Estimated Effort**: 1 week (1 developer)
**Risk Level**: Low (architecture is proven, focus is on systematic migration)

---

## 2. Context Analysis

### What is the ticket asking me to implement?
The ticket requires reorganizing existing one-off analysis scripts into a structured, dated folder system using the new simplified analyzer architecture. This involves:

1. **Creating Analysis Folder Structure**: Implement one analysis per folder with standardized naming
2. **Migrating Existing Scripts**: Convert all scripts to use new analyzer classes
3. **Updating Imports**: Ensure absolute imports throughout codebase
4. **Creating Templates**: Standardize documentation and structure

### What context and constraints do I need to consider?

**Existing Architecture (Already Implemented):**
- `BaseAnalyzer` class with shared utilities (validation, logging, data helpers)
- `FeedAnalyzer` for feed content analysis
- `EngagementAnalyzer` for user engagement metrics
- `WeeklyThresholdsAnalyzer` for threshold calculations
- Shared data loading, processing, and configuration modules

**Current Analysis Scripts Identified:**
- **Core Analytics**: `feed_analytics.py` (540 lines), `calculate_weekly_thresholds_per_user.py` (517 lines)
- **User Engagement**: `get_agg_labels_for_engagements.py` (1062 lines), `get_aggregate_metrics.py` (365 lines)
- **Report Generation**: `condition_aggregated.py`, `binary_classifications_averages.py`, `weekly_user_logins.py`
- **Data Consolidation**: `consolidate_feeds.py`, `consolidate_user_session_logs.py`
- **Visualizations**: `time_series.py`, `plot_toxicity.py`

**Technical Constraints:**
- No relative imports - use absolute imports only
- Maintain backward compatibility - all existing outputs must be reproducible
- Use conda environment `bluesky-research` for testing
- Follow pre-commit hooks - ruff formatting and linting must pass
- Test locally first before any deployment

### What has already been done or planned?

**Completed (MET-40):**
- ✅ Simplified analyzer architecture implemented
- ✅ BaseAnalyzer class with shared utilities
- ✅ All pipeline complexity removed
- ✅ Direct method execution working

**Current State:**
- Scripts still use old patterns and imports
- No standardized folder structure
- Mixed organization across multiple directories
- Inconsistent documentation and structure

### What are the key assumptions I'm making?

1. **Output Consistency**: New analyzer classes produce identical results to old scripts
2. **Import Compatibility**: All existing imports can be updated to use new analyzer classes
3. **Script Independence**: Each analysis script can be migrated independently
4. **Template Standardization**: Standard templates will improve consistency across analyses
5. **Backward Compatibility**: Old scripts can be kept temporarily for validation

---

## 3. Implementation Strategy

### What is my high-level approach?

**Systematic Migration Strategy**: I will implement a phased approach that creates the new structure while systematically migrating existing scripts, ensuring each step is validated before proceeding.

**Key Principles:**
1. **Create Structure First**: Establish the new folder system and templates
2. **Migrate Incrementally**: Convert one script at a time with validation
3. **Maintain Consistency**: Ensure all analyses follow the same pattern
4. **Validate Outputs**: Verify 100% consistency with previous versions

### Why am I choosing this approach?

**Benefits of Systematic Migration:**
- **Risk Mitigation**: Each migration is validated independently
- **Consistency**: All analyses follow the same structure and patterns
- **Maintainability**: Standardized templates make future analyses easier
- **Validation**: Output consistency can be verified at each step
- **Rollback Capability**: Issues can be isolated and fixed without affecting other analyses

**Alternative Approaches Considered:**
- **Big Bang Migration**: Convert all scripts at once (rejected - too risky)
- **Parallel Development**: Keep old and new systems running (rejected - maintenance overhead)
- **Selective Migration**: Only migrate some scripts (rejected - doesn't meet ticket requirements)

### What alternatives did I consider?

1. **Big Bang Migration**: Convert all scripts simultaneously
   - **Pros**: Faster completion, consistent state
   - **Cons**: High risk, difficult to debug, potential for widespread issues
   - **Rejection**: Too risky for production analytics system

2. **Parallel Development**: Maintain both old and new systems
   - **Pros**: No downtime, easy comparison
   - **Cons**: Maintenance overhead, confusion about which system to use
   - **Rejection**: Increases complexity rather than reducing it

3. **Selective Migration**: Only migrate high-priority scripts
   - **Pros**: Lower risk, focused effort
   - **Cons**: Doesn't meet ticket requirements for complete reorganization
   - **Rejection**: Doesn't achieve the stated objective

### What are the key design decisions?

1. **Folder Structure**: One analysis per folder with dated naming convention
2. **Template Standardization**: Consistent README, notebook, main.py, and assets structure
3. **Migration Order**: Start with core analytics, then expand to other areas
4. **Validation Strategy**: Output comparison and functional testing for each migration
5. **Import Strategy**: Update all imports to use new analyzer classes with absolute paths

---

## 4. Detailed Implementation Plan

### **Phase 1: Setup & Preparation (Day 1)**

#### **Step 1.1: Create Analysis Folder Structure**
```
services/calculate_analytics/study_analytics/analyses/
├── README.md                           # Main analyses documentation
├── templates/                          # Standardized templates
│   ├── README.md                      # Template for analysis documentation
│   ├── main.py                        # Template for main implementation
│   └── investigation.ipynb            # Template for exploratory work
└── [analysis_folders_created_during_migration]
```

#### **Step 1.2: Create Standardized Templates**
- **README.md Template**: Analysis purpose, methodology, usage instructions
- **main.py Template**: Standard structure using new analyzer classes
- **investigation.ipynb Template**: Jupyter notebook for exploratory work
- **assets/ Template**: Output files directory structure

#### **Step 1.3: Environment Setup**
- Verify conda environment `bluesky-research` is active
- Install any missing dependencies
- Run existing tests to ensure current system is working
- Create backup of current analysis scripts

### **Phase 2: Core Analytics Migration (Days 2-3)**

#### **Step 2.1: Migrate Feed Analytics**
**Source**: `services/calculate_analytics/study_analytics/calculate_analytics/feed_analytics.py`
**Target**: `services/calculate_analytics/study_analytics/analyses/feed_analytics_2024_12_01/`

**Migration Process:**
1. Create `feed_analytics_2024_12_01/` folder
2. Copy template files and customize for feed analytics
3. Convert script to use `FeedAnalyzer` class
4. Replace complex logic with direct method calls
5. Update imports to use absolute paths
6. Test output consistency with original script
7. Validate all functionality works correctly

**Key Changes:**
- Replace monolithic script with structured analysis class usage
- Use `FeedAnalyzer.analyze_partition_date()` method
- Leverage shared configuration and data loading modules
- Maintain exact output format and content

#### **Step 2.2: Migrate Weekly Thresholds**
**Source**: `services/calculate_analytics/study_analytics/calculate_analytics/calculate_weekly_thresholds_per_user.py`
**Target**: `services/calculate_analytics/study_analytics/analyses/weekly_thresholds_2024_12_01/`

**Migration Process:**
1. Create `weekly_thresholds_2024_12_01/` folder
2. Copy template files and customize for weekly thresholds
3. Convert script to use `WeeklyThresholdsAnalyzer` class
4. Replace complex date mapping logic with analyzer methods
5. Update imports and dependencies
6. Test output consistency
7. Validate threshold calculations

**Key Changes:**
- Use `WeeklyThresholdsAnalyzer.calculate_thresholds()` method
- Leverage shared threshold processing modules
- Maintain exact date mapping and calculation logic
- Ensure wave-based week assignments are preserved

### **Phase 3: User Engagement Migration (Days 4-5)**

#### **Step 3.1: Migrate Engagement Analysis**
**Source**: `services/calculate_analytics/study_analytics/get_user_engagement/get_agg_labels_for_engagements.py`
**Target**: `services/calculate_analytics/study_analytics/analyses/user_engagement_2024_12_01/`

**Migration Process:**
1. Create `user_engagement_2024_12_01/` folder
2. Convert to use `EngagementAnalyzer` class
3. Replace complex aggregation logic with analyzer methods
4. Update imports and dependencies
5. Test output consistency
6. Validate engagement metrics

**Key Changes:**
- Use `EngagementAnalyzer` methods for engagement calculations
- Leverage shared processing modules for metrics
- Maintain exact aggregation logic and output format

#### **Step 3.2: Migrate Aggregate Metrics**
**Source**: `services/calculate_analytics/study_analytics/get_user_engagement/get_aggregate_metrics.py`
**Target**: `services/calculate_analytics/study_analytics/analyses/aggregate_metrics_2024_12_01/`

**Migration Process:**
1. Create `aggregate_metrics_2024_12_01/` folder
2. Convert to use shared processing modules
3. Update imports and dependencies
4. Test output consistency
5. Validate metrics calculations

### **Phase 4: Report Generation Migration (Day 6)**

#### **Step 4.1: Migrate Report Scripts**
**Sources**: 
- `condition_aggregated.py`
- `binary_classifications_averages.py`
- `weekly_user_logins.py`

**Migration Process:**
1. Create dated folders for each report type
2. Convert to use shared modules where applicable
3. Update imports and dependencies
4. Test output consistency
5. Validate report generation

### **Phase 5: Data Consolidation & Visualization Migration (Day 7)**

#### **Step 5.1: Migrate Consolidation Scripts**
**Sources**:
- `consolidate_feeds.py`
- `consolidate_user_session_logs.py`
- `migrate_feeds_to_db.py`
- `migrate_user_session_logs_to_db.py`

#### **Step 5.2: Migrate Visualization Scripts**
**Sources**:
- `time_series.py`
- `plot_toxicity.py`

**Migration Process:**
1. Create dated folders for each script type
2. Convert to use shared modules where applicable
3. Update imports and dependencies
4. Test output consistency
5. Validate functionality

### **Phase 6: Import Updates & Dependencies (Day 8)**

#### **Step 6.1: Update All Import Statements**
**Scope**: Entire codebase that references old analysis scripts

**Files to Update**:
- All Python files that import from old script locations
- Configuration files that reference old paths
- Documentation that mentions old script locations
- Test files that import old modules

**Update Strategy**:
1. Search for all import statements referencing old locations
2. Replace with absolute imports to new analyzer classes
3. Update any hardcoded file paths
4. Ensure all imports use absolute paths from project root

#### **Step 6.2: Update Dependencies**
**Scope**: Any scripts that reference old pipeline framework

**Update Strategy**:
1. Search for references to old pipeline classes
2. Replace with new analyzer class references
3. Update method calls to use new interfaces
4. Ensure backward compatibility during transition

### **Phase 7: Testing & Validation (Day 9)**

#### **Step 7.1: Individual Analysis Testing**
**Testing Strategy**: Test each migrated analysis individually

**Test Cases**:
1. **Output Consistency**: Verify outputs match previous versions exactly
2. **Functionality**: Ensure all features work as expected
3. **Performance**: Verify performance meets or exceeds previous benchmarks
4. **Error Handling**: Test error scenarios and edge cases

**Validation Process**:
1. Run original script and capture outputs
2. Run migrated analysis and capture outputs
3. Compare outputs for exact consistency
4. Document any discrepancies and resolve them

#### **Step 7.2: Integration Testing**
**Testing Strategy**: Test end-to-end workflows

**Test Cases**:
1. **Import Compatibility**: Ensure all imports work correctly
2. **Configuration Loading**: Verify configuration system works
3. **Data Loading**: Test data loading from various sources
4. **Processing Pipeline**: Verify complete analysis workflows

#### **Step 7.3: Code Quality Validation**
**Quality Checks**:
1. **Pre-commit Hooks**: Run ruff formatting and linting
2. **Import Validation**: Ensure no relative imports exist
3. **Documentation**: Verify all analyses have proper documentation
4. **Template Compliance**: Ensure all analyses follow standardized structure

### **Phase 8: Documentation & Cleanup (Day 10)**

#### **Step 8.1: Update Documentation**
**Documentation Updates**:
1. **Main README**: Update to reflect new structure
2. **Analysis Templates**: Finalize and document templates
3. **Migration Guide**: Document migration process for future reference
4. **Usage Examples**: Update examples to use new structure

#### **Step 8.2: Cleanup Old Scripts**
**Cleanup Strategy**:
1. **Archive Old Scripts**: Move to deprecated folder for reference
2. **Update References**: Ensure no active references to old locations
3. **Documentation**: Update any documentation that references old locations
4. **Validation**: Ensure all functionality is preserved in new structure

---

## 5. Code Implementation Strategy

### What specific code will I write?

**New Directory Structure**:
```
services/calculate_analytics/study_analytics/analyses/
├── README.md                           # Main analyses documentation
├── templates/                          # Standardized templates
│   ├── README.md                      # Template for analysis documentation
│   ├── main.py                        # Template for main implementation
│   └── investigation.ipynb            # Template for exploratory work
├── feed_analytics_2024_12_01/         # Migrated feed analytics
│   ├── README.md                      # Analysis-specific documentation
│   ├── main.py                        # Main implementation using FeedAnalyzer
│   ├── investigation.ipynb            # Exploratory work notebook
│   └── assets/                        # Output files directory
├── weekly_thresholds_2024_12_01/      # Migrated weekly thresholds
│   ├── README.md                      # Analysis-specific documentation
│   ├── main.py                        # Main implementation using WeeklyThresholdsAnalyzer
│   ├── investigation.ipynb            # Exploratory work notebook
│   └── assets/                        # Output files directory
├── user_engagement_2024_12_01/        # Migrated user engagement
│   ├── README.md                      # Analysis-specific documentation
│   ├── main.py                        # Main implementation using EngagementAnalyzer
│   ├── investigation.ipynb            # Exploratory work notebook
│   └── assets/                        # Output files directory
└── [additional_migrated_analyses]
```

**Template Files**:
1. **README.md Template**: Standardized structure for analysis documentation
2. **main.py Template**: Standard structure using new analyzer classes
3. **investigation.ipynb Template**: Jupyter notebook template for exploratory work
4. **assets/ Template**: Standardized output directory structure

**Migration Scripts**:
1. **Import Update Scripts**: Automated scripts to update import statements
2. **Validation Scripts**: Scripts to compare outputs between old and new versions
3. **Testing Scripts**: Automated testing for migrated analyses

### How will I structure the code?

**Directory Organization**:
- **One Analysis Per Folder**: Each analysis gets its own dated folder
- **Standardized Naming**: `<description>_<YYYY_MM_DD>` convention
- **Consistent Structure**: All folders contain the same file types
- **Template-Based**: All analyses use standardized templates

**File Organization**:
- **README.md**: Analysis purpose, methodology, usage instructions
- **main.py**: Main implementation using new analyzer classes
- **investigation.ipynb**: Jupyter notebook for exploratory work
- **assets/**: Output files and generated content

**Code Structure**:
- **Main Entry Point**: Each main.py provides clear entry point for analysis
- **Configuration Integration**: Use shared configuration system
- **Error Handling**: Consistent error handling across all analyses
- **Logging**: Structured logging using shared utilities

### What patterns and conventions will I follow?

**Design Patterns**:
1. **Template Method Pattern**: Standardized structure across all analyses
2. **Dependency Injection**: Use shared modules and configuration
3. **Single Responsibility**: Each analysis focuses on one specific area
4. **Configuration-Driven**: Use configuration files for parameters

**Coding Standards**:
1. **Absolute Imports**: No relative imports, use absolute paths from project root
2. **Type Hints**: Complete type annotations for all public APIs
3. **Documentation**: Comprehensive docstrings and inline comments
4. **Error Handling**: Robust error handling with meaningful messages
5. **Logging**: Structured logging with correlation IDs

**Naming Conventions**:
1. **Folder Names**: `<description>_<YYYY_MM_DD>` (e.g., `feed_analytics_2024_12_01`)
2. **File Names**: Descriptive names that indicate purpose
3. **Function Names**: Clear, descriptive names that explain functionality
4. **Variable Names**: Self-documenting names that indicate purpose

### Why am I choosing this code structure?

**Benefits of This Structure**:
1. **Consistency**: All analyses follow the same pattern, making them easy to understand
2. **Maintainability**: Standardized structure reduces cognitive load for developers
3. **Scalability**: Easy to add new analyses following the established pattern
4. **Documentation**: Built-in documentation requirements ensure knowledge preservation
5. **Reproducibility**: Clear structure makes it easy to reproduce analyses
6. **Collaboration**: Standardized format makes it easy for team members to work together

**Research Workflow Alignment**:
1. **Academic Requirements**: Structure supports peer review and reproducibility
2. **Iterative Development**: Easy to modify and rerun analyses
3. **Knowledge Sharing**: Clear documentation supports team collaboration
4. **Quality Assurance**: Standardized structure supports testing and validation

---

## 6. Success Criteria & Validation

### How will I know each step is complete?

**Phase 1 Completion Criteria**:
- [ ] `analyses/` directory created with proper structure
- [ ] Standardized templates created and documented
- [ ] Environment setup verified and working

**Phase 2-5 Completion Criteria**:
- [ ] Each analysis script successfully migrated to new structure
- [ ] All imports updated to use new analyzer classes
- [ ] Output consistency verified with previous versions
- [ ] All functionality working as expected

**Phase 6-7 Completion Criteria**:
- [ ] All imports updated throughout codebase
- [ ] No references to old pipeline framework remain
- [ ] All analyses can be re-run successfully using new structure
- [ ] Output files match previous versions exactly

**Phase 8 Completion Criteria**:
- [ ] Documentation updated to reflect new structure
- [ ] Old scripts archived and references updated
- [ ] All templates finalized and documented
- [ ] Migration guide created for future reference

### What are the acceptance criteria?

**Functional Requirements**:
- [ ] All existing analytics outputs can be reproduced exactly
- [ ] New analyses can be created using shared modules
- [ ] Configuration changes don't require code modifications
- [ ] Error handling provides clear, actionable feedback

**Non-Functional Requirements**:
- [ ] Performance meets or exceeds current benchmarks
- [ ] Code complexity reduced through shared modules
- [ ] Test coverage maintained or improved
- [ ] Documentation is clear and comprehensive

**Quality Requirements**:
- [ ] No single file >500 lines
- [ ] Clear separation of concerns
- [ ] Comprehensive error handling
- [ ] Easy to understand and maintain

### How will I test and validate the implementation?

**Testing Strategy**:
1. **Unit Testing**: Test individual components and functions
2. **Integration Testing**: Test complete analysis workflows
3. **Output Validation**: Compare outputs with previous versions
4. **Performance Testing**: Benchmark execution time and resource usage
5. **Error Testing**: Test error scenarios and edge cases

**Validation Process**:
1. **Output Consistency**: Verify exact match with previous versions
2. **Functionality Verification**: Ensure all features work as expected
3. **Import Compatibility**: Verify all imports work correctly
4. **Configuration Validation**: Test configuration system functionality
5. **Documentation Review**: Ensure all documentation is accurate and complete

**Quality Assurance**:
1. **Code Review**: Self-review using established checklist
2. **Pre-commit Hooks**: Ensure ruff formatting and linting pass
3. **Import Validation**: Verify no relative imports exist
4. **Template Compliance**: Ensure all analyses follow standardized structure

### What metrics or benchmarks will I use?

**Performance Metrics**:
- **Execution Time**: Should meet or exceed previous benchmarks
- **Memory Usage**: Should be comparable to or better than previous versions
- **Resource Efficiency**: Should use shared modules effectively

**Quality Metrics**:
- **Code Duplication**: Should be reduced through shared modules
- **File Size**: No single file should exceed 500 lines
- **Test Coverage**: Should maintain or improve current coverage
- **Documentation Coverage**: All analyses should have complete documentation

**Consistency Metrics**:
- **Output Consistency**: 100% match with previous versions
- **Structure Consistency**: All analyses follow standardized structure
- **Import Consistency**: All imports use absolute paths
- **Documentation Consistency**: All analyses have consistent documentation format

---

## 7. Risk Assessment

### Potential Issues and Mitigation Strategies

**High-Risk Areas**:
1. **Output Inconsistency**: Risk that migrated analyses produce different results
   - **Mitigation**: Comprehensive testing and validation at each step
   - **Fallback**: Keep old scripts available for comparison

2. **Import Breakage**: Risk that import updates break existing functionality
   - **Mitigation**: Systematic testing of each import update
   - **Fallback**: Incremental updates with validation at each step

3. **Performance Degradation**: Risk that new structure is slower than old scripts
   - **Mitigation**: Performance benchmarking and optimization
   - **Fallback**: Identify and resolve performance bottlenecks before completion

**Medium-Risk Areas**:
1. **Configuration Issues**: Risk that shared configuration doesn't work correctly
   - **Mitigation**: Comprehensive testing of configuration system
   - **Fallback**: Maintain fallback configuration options

2. **Template Standardization**: Risk that templates are too rigid or too flexible
   - **Mitigation**: Iterative template development with user feedback
   - **Fallback**: Adjust templates based on actual usage patterns

**Low-Risk Areas**:
1. **Directory Structure**: Risk that folder organization doesn't meet needs
   - **Mitigation**: Clear documentation and examples
   - **Fallback**: Easy to reorganize if needed

2. **Documentation**: Risk that documentation is incomplete or unclear
   - **Mitigation**: Comprehensive documentation review
   - **Fallback**: Iterative documentation improvement

### Risk Mitigation Strategies

**Testing Strategy**:
1. **Incremental Testing**: Test each migration step independently
2. **Output Validation**: Verify exact consistency with previous versions
3. **Integration Testing**: Test complete workflows end-to-end
4. **Performance Testing**: Benchmark against established baselines

**Rollback Strategy**:
1. **Backup Creation**: Maintain backups of all original scripts
2. **Incremental Migration**: Migrate one script at a time
3. **Validation Checkpoints**: Verify each migration before proceeding
4. **Fallback Options**: Keep old scripts available during transition

**Communication Strategy**:
1. **Progress Updates**: Regular updates on migration progress
2. **Issue Reporting**: Immediate reporting of any problems
3. **User Feedback**: Regular feedback on migration quality
4. **Documentation**: Clear documentation of all changes

---

## 8. Success Criteria

### Clear Completion Criteria and Validation Approach

**Primary Success Criteria**:
- [ ] `analyses/` directory created with proper structure
- [ ] All existing analysis scripts migrated to new architecture
- [ ] Each analysis folder contains required files (README, notebook, main.py, assets)
- [ ] All imports updated to use new analyzer classes
- [ ] No references to old pipeline framework remain
- [ ] All analyses can be re-run successfully using new structure
- [ ] Output files match previous versions exactly

**Secondary Success Criteria**:
- [ ] Standardized templates created and documented
- [ ] All analyses follow consistent structure and patterns
- [ ] Documentation is comprehensive and accurate
- [ ] Performance meets or exceeds previous benchmarks
- [ ] Code quality meets established standards

**Validation Approach**:
1. **Output Consistency**: Compare outputs between old and new versions
2. **Functionality Testing**: Verify all features work as expected
3. **Performance Benchmarking**: Measure execution time and resource usage
4. **Code Quality Review**: Ensure all code meets quality standards
5. **Documentation Review**: Verify all documentation is accurate and complete

**Success Metrics**:
- **Functional Completeness**: 100% of existing analyses successfully migrated
- **Output Consistency**: 100% match with previous versions
- **Performance**: Meet or exceed previous benchmarks
- **Quality**: All code passes pre-commit hooks and quality checks
- **Documentation**: Complete documentation for all analyses

---

## 9. Implementation Readiness

✅ **EXECUTION PLAN APPROVED**

This execution plan has been reviewed and approved for implementation. The systematic migration approach will ensure all existing analysis scripts are successfully reorganized into a structured, dated folder system using the new simplified analyzer architecture.

---

## 10. Integration with Other Prompts

This execution planning prompt works with:
- **CRITICAL_ANALYSIS_PROMPT.md**: Use to critically evaluate the proposed approach
- **HOW_TO_EXECUTE_A_TICKET.md**: Use to guide actual implementation after approval

**Note**: This prompt is for pre-implementation planning only. The `ARCHITECTURE_DECISION_PROMPT.md` is used after implementation during code review to explain the design decisions that were actually made.

The goal is to ensure you have full visibility into the AI's implementation strategy and can provide feedback before any code is written, reducing rework and improving implementation quality.
