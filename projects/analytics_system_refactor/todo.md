# Analytics System Refactor - Todo Checklist

## Phase 1: Extract Shared Data Loading & Processing ✅ **COMPLETED**

### Configuration Management ✅
- [x] Create shared configuration structure
- [x] Implement configuration loading utilities
- [x] Extract all hardcoded constants
- [x] Create YAML configuration files for studies and features

### Data Loading Layer ✅
- [x] Create shared data loading modules
- [x] Implement unified data loading interfaces
- [x] Add error handling and validation
- [x] Extract post loading logic
- [x] Extract label loading logic
- [x] Extract feed loading logic
- [x] Extract user loading logic

### Processing Logic ✅
- [x] Extract feature calculation logic
- [x] Extract threshold calculation logic
- [x] Create reusable processing functions
- [x] Extract engagement analysis logic
- [x] Create common utility functions

### Integration & Testing ✅
- [x] Update existing scripts to use shared modules
- [x] Verify all functionality preserved
- [x] Run comprehensive tests
- [x] Validate output consistency

## Phase 2: Implement Simple Analysis Framework 🔄 **REVISED & IN PROGRESS**

### **ARCHITECTURE REVISION - ABC Pipeline Framework Removed**

**Original Plan (REMOVED):**
- [ ] ~~Create base pipeline ABCs~~
- [ ] ~~Implement BaseResearchPipeline abstract base class~~
- [ ] ~~Implement BaseFeedAnalysisPipeline abstract base class~~
- [ ] ~~Define clear interfaces for data loading, processing, and output generation~~
- [ ] ~~Add basic pipeline state management~~
- [ ] ~~Implement error handling and recovery~~
- [ ] ~~Add logging and progress tracking~~

**Revised Plan (Simple Analysis Classes):**
- [ ] **NEW**: Simplify base class to remove pipeline complexity
- [ ] **NEW**: Remove ABC abstract methods (setup, execute)
- [ ] **NEW**: Remove pipeline state management and lifecycle hooks
- [ ] **NEW**: Focus on shared utility methods (validation, logging, data helpers)
- [ ] **NEW**: Implement simple inheritance for common functionality
- [ ] **NEW**: Create clear, focused analysis classes
- [ ] **NEW**: Ensure each class has single, obvious responsibility

### Analysis Class Implementation
- [ ] **REVISED**: Convert feed analytics to simple analysis class (not pipeline)
- [ ] **REVISED**: Convert weekly thresholds to simple analysis class (not pipeline)
- [ ] **REVISED**: Convert engagement analysis to simple analysis class (not pipeline)
- [ ] **REVISED**: Focus on code organization and reusability, not orchestration

### Testing & Validation
- [ ] **REVISED**: Test analysis classes independently (not pipeline framework)
- [ ] **REVISED**: Ensure performance meets or exceeds current benchmarks
- [ ] **REVISED**: Validate that simplified architecture maintains functionality

### **IMPLEMENTATION DETAILS FOR SIMPLIFIED ARCHITECTURE**

#### Step 1: Create Simplified Base Class
- [ ] **NEW**: Replace BaseResearchPipeline ABC with simple BaseAnalyzer class
- [ ] **NEW**: Remove PipelineState, PipelineResult, PipelineError classes
- [ ] **NEW**: Remove setup(), execute(), cleanup(), validate() abstract methods
- [ ] **NEW**: Remove complex run() orchestration method
- [ ] **NEW**: Remove state tracking and metadata management
- [ ] **NEW**: Implement simple utility methods (validate_config, log_execution, validate_data)

#### Step 2: Simplify Analysis Classes
- [ ] **NEW**: Convert FeedAnalysisPipeline to FeedAnalyzer
- [ ] **NEW**: Convert EngagementAnalysisPipeline to EngagementAnalyzer
- [ ] **NEW**: Convert WeeklyThresholdsPipeline to WeeklyThresholdsAnalyzer
- [ ] **NEW**: Replace pipeline lifecycle with direct method execution
- [ ] **NEW**: Implement simple methods like analyze_partition_date() instead of complex orchestration

#### Step 3: Update File Structure
- [ ] **NEW**: Rename pipelines/ directory to analyzers/
- [ ] **NEW**: Update all import statements throughout codebase
- [ ] **NEW**: Remove old pipeline framework files
- [ ] **NEW**: Update __init__.py files with new class names

#### Step 4: Update Usage Patterns
- [ ] **NEW**: Replace complex pipeline.run() calls with direct method calls
- [ ] **NEW**: Update example_usage.py to demonstrate simplified approach
- [ ] **NEW**: Update all existing scripts to use new analyzer classes
- [ ] **NEW**: Ensure backward compatibility during transition

#### Step 5: Testing & Validation
- [ ] **NEW**: Test simplified analyzer classes independently
- [ ] **NEW**: Verify all functionality preserved from pipeline framework
- [ ] **NEW**: Ensure performance meets or exceeds current benchmarks
- [ ] **NEW**: Validate that simplified architecture maintains functionality

## Phase 3: Reorganize One-Off Analyses

### Analysis Folder Structure
- [ ] Create `analyses/` directory with proper structure
- [ ] Move existing analysis scripts to appropriate dated folders
- [ ] Implement one analysis per folder structure
- [ ] Ensure each folder follows naming convention: `<description>_<YYYY_MM_DD>`

### Analysis Script Updates
- [ ] Modify analysis scripts to use shared modules
- [ ] Update imports and dependencies
- [ ] Ensure all analyses can be re-run successfully
- [ ] Verify output files match previous versions exactly

### Standardization
- [ ] Add `README.md` to each analysis folder
- [ ] Create `investigation.ipynb` for exploratory work
- [ ] Organize `assets/` folder for outputs
- [ ] Standardize folder contents across all analyses

## Phase 4: Implement Testing & Validation

### Test Suite Creation
- [ ] Create comprehensive test suite for shared modules
- [ ] Implement unit tests for all analysis classes
- [ ] Add integration tests for end-to-end functionality
- [ ] Ensure >80% test coverage across all functionality

### Data Validation
- [ ] Implement input validation for all data loading functions
- [ ] Add output validation for all processing functions
- [ ] Create data quality checks and monitoring
- [ ] Validate data integrity throughout the pipeline

### Performance & Quality
- [ ] Benchmark current performance
- [ ] Implement caching and optimization where beneficial
- [ ] Validate performance improvements
- [ ] Ensure error handling is robust and user-friendly

## Phase 5: Documentation & Cleanup

### Documentation Updates
- [ ] Update comprehensive README for shared modules
- [ ] Create usage examples and tutorials
- [ ] Write migration guide for existing users
- [ ] Document all configuration options and parameters

### Code Cleanup
- [ ] Remove deprecated monolithic scripts
- [ ] Clean up unused imports and dependencies
- [ ] Archive old analysis outputs
- [ ] Ensure code follows project standards

### Final Validation
- [ ] Conduct end-to-end testing of complete system
- [ ] Validate all outputs and functionality
- [ ] Verify performance and reliability
- [ ] Complete final documentation review

## **ARCHITECTURE REVISION NOTES**

### **Why ABC Pipeline Framework Was Removed**

**Academic Research Requirements:**
- **Transparency**: Complex pipeline interfaces hide what's actually happening
- **Reproducibility**: Pipeline state management adds variables that can affect results
- **Simplicity**: Research code doesn't need enterprise orchestration
- **Peer Review**: Simple classes are easier for reviewers to understand

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

### **What Replaces the Pipeline Framework**

**Simple Analysis Classes:**
- Direct method execution without setup/execute phases
- Simple inheritance for shared utility methods
- Clear, focused responsibility for each class
- Configuration-driven parameters

**Shared Utility Methods:**
- Configuration validation
- Common logging patterns
- Data validation helpers
- Statistical calculation utilities

**Benefits of Simplified Approach:**
- **Reduced complexity**: 50%+ less code to maintain
- **Better transparency**: Easy to see what's happening
- **Easier debugging**: Simple execution without framework complexity
- **Research-focused**: Aligned with academic needs and peer review

## **NEXT STEPS**

1. **Complete Phase 2 Revision**: Simplify the analysis framework architecture
2. **Remove Pipeline Complexity**: Strip out unnecessary ABC and state management
3. **Focus on Shared Utilities**: Implement simple inheritance for common functionality
4. **Validate Simplified Approach**: Ensure functionality is maintained
5. **Proceed to Phase 3**: Begin analysis reorganization with simplified architecture

## **Success Metrics (Updated)**

- [ ] **Code Complexity**: Reduced from ABC pipelines to simple classes
- [ ] **Maintainability**: Easier to understand and modify
- [ ] **Transparency**: Code is clear for peer reviewers
- [ ] **Reproducibility**: Results are identical every time
- [ ] **Performance**: Maintained or improved through simplification
- [ ] **Testing**: Simpler to test without complex framework mocking
