# Analytics System Refactor - Todo Checklist

## Phase 1: Extract Shared Data Loading & Processing (Week 1)

> **Status**: ✅ COMPLETED  
> **Linear Issue**: [MET-39](https://linear.app/metresearch/issue/MET-39/phase-1-extract-shared-data-loading-and-processing)  
> **Implementation Plan**: [plan_phase1_implementation.md](plan_phase1_implementation.md)

### Data Loading Modules
- [x] Create `shared/data_loading/` directory
- [x] Extract post loading logic from `load_data.py`
- [x] Extract label loading logic from `load_labels.py`
- [x] Extract feed loading logic from `load_feeds.py`
- [x] Extract user loading logic from participant data modules
- [x] Test all data loading modules independently

### Processing Modules
- [x] Create `shared/processing/` directory
- [x] Extract feature calculation logic from `feed_analytics.py`
- [x] Extract threshold calculation logic from `weekly_thresholds.py`
- [x] Extract engagement analysis logic from engagement scripts
- [x] Test all processing modules independently

### Configuration Management
- [x] Create `shared/config/` directory
- [x] Extract constants to YAML configuration files
- [x] Create configuration loading and validation utilities
- [x] Implement environment-specific configuration support
- [x] Test configuration management system
- [x] **NEW**: Implement Pydantic v2-based configuration validation with enforced structure
- [x] **NEW**: Complete type-safe configuration access with comprehensive validation

### Integration
- [x] Update all existing scripts to import from shared modules
- [x] Verify all existing scripts still work after refactoring
- [x] Ensure no code duplication between shared modules
- [x] Validate configuration externalization

## Phase 2: Implement Simple Pipeline Framework (Week 2)

> **Status**: 🔄 IN PROGRESS - First pass complete, orchestration removed, needs testing and review

### Base Pipeline ABCs
- [x] Create `shared/pipelines/` directory
- [x] Implement `BaseResearchPipeline` abstract base class
- [x] Implement `BaseFeedAnalysisPipeline` abstract base class
- [x] Define clear interfaces for data loading, processing, and output generation
- [ ] **REQUIRED**: Test base pipeline ABCs thoroughly
- [ ] **REQUIRED**: Review and validate pipeline interfaces

### Concrete Pipeline Implementations
- [x] Convert feed analytics to concrete pipeline implementation
- [x] Convert weekly thresholds to concrete pipeline implementation
- [x] Convert engagement analysis to concrete pipeline implementation
- [ ] Test all concrete pipeline implementations

### Simple Pipeline Execution Patterns
- [x] Add basic pipeline state management
- [x] Implement error handling and recovery
- [x] Add logging and progress tracking
- [x] **COMPLETED**: Remove unnecessary orchestration layer
- [ ] Test simple pipeline execution patterns

### Validation
- [ ] Verify all existing analytics can be run through pipeline framework
- [ ] Ensure pipeline tests pass independently
- [ ] Validate performance meets or exceeds current benchmarks

## Phase 3: Reorganize One-Off Analyses (Week 3)

### Analysis Folder Structure
- [ ] Create `analyses/` directory with proper structure
- [ ] Move existing analysis scripts to appropriate dated folders
- [ ] Implement one analysis per folder structure
- [ ] Validate folder organization

### Analysis Script Updates
- [ ] Modify analysis scripts to use shared modules
- [ ] Update imports and dependencies
- [ ] Ensure all analyses can be re-run successfully
- [ ] Test all updated analysis scripts

### Standardization
- [ ] Add `README.md` to each analysis folder
- [ ] Create `investigation.ipynb` for exploratory work
- [ ] Organize `assets/` folder for outputs
- [ ] Validate standardized structure

### Final Validation
- [ ] Verify all analyses moved to focused, dated folders
- [ ] Ensure each analysis folder contains required components
- [ ] Confirm all analyses can be re-run with identical outputs

## Phase 4: Implement Testing & Validation (Week 4)

### Test Suite Creation
- [ ] Create unit tests for all shared modules
- [ ] Create integration tests for pipeline framework
- [ ] Create regression tests for output consistency
- [ ] Achieve >80% test coverage

### Data Validation
- [ ] Implement input validation for all data loading functions
- [ ] Implement output validation for all processing functions
- [ ] Add data quality checks and monitoring
- [ ] Test validation systems

### Performance Testing
- [ ] Benchmark current performance
- [ ] Implement caching and optimization
- [ ] Validate performance improvements
- [ ] Document performance metrics

### Quality Assurance
- [ ] Ensure all tests pass consistently
- [ ] Validate performance targets met or exceeded
- [ ] Verify data quality monitoring works correctly

## Phase 5: Documentation & Cleanup (Week 5)

### Documentation Updates
- [ ] Create comprehensive README for shared modules
- [ ] Write usage examples and tutorials
- [ ] Create migration guide for existing users
- [ ] Review and finalize all documentation

### Code Cleanup
- [ ] Remove deprecated monolithic scripts
- [ ] Clean up unused imports and dependencies
- [ ] Archive old analysis outputs
- [ ] Validate clean codebase

### Final Validation
- [ ] Conduct end-to-end testing of complete system
- [ ] Validate all outputs and functionality
- [ ] Verify performance and reliability
- [ ] Complete final testing checklist

### Project Completion
- [ ] Review all success criteria
- [ ] Validate all deliverables completed
- [ ] Final code review and approval
- [ ] Project handoff and documentation

## Notes
- Each checkbox corresponds to a Linear issue
- Update checkboxes as Linear issues are completed
- Use this checklist for daily progress tracking
- Mark items as blocked if dependencies are not met

## Recent Achievements
- ✅ **Configuration System**: Successfully implemented Pydantic v2-based configuration with enforced structure and type safety
- ✅ **Pipeline Framework**: First pass of ABC-based pipeline framework is complete
- 🔄 **Next Priority**: Thorough testing and review of pipeline ABCs before proceeding with concrete implementations
