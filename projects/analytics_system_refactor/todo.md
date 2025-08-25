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

## Phase 3: Reorganize One-Off Analyses 🔄 **IN PROGRESS - MET-41**

### **MIGRATION CHECKLIST - MET-41 IMPLEMENTATION**

#### **Phase 3a: Setup & Preparation**
- [ ] Create `analytics_system_refactor/analyses/` directory
- [ ] Set up folder structure template
- [ ] Create migration tracking spreadsheet
- [ ] Verify Phase 2 completion status

#### **Phase 3b: User Engagement Analysis Migration**
- [ ] **HIGH PRIORITY**: Migrate `get_agg_labels_for_engagements.py` → `user_engagement_analysis_2025_06_16/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **HIGH PRIORITY**: Migrate `get_aggregate_metrics.py` → `user_engagement_metrics_2025_06_16/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **MEDIUM PRIORITY**: Migrate `experiment_get_agg_labels_for_engagement.py` → `user_engagement_experiment_2025_05_12/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **MEDIUM PRIORITY**: Migrate `qa_spammy_handles.py` → `spam_handle_analysis_2025_05_12/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

#### **Phase 3c: Report Generation Migration**
- [ ] **MEDIUM PRIORITY**: Migrate `weekly_user_logins.py` → `weekly_user_logins_2025_04_23/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **HIGH PRIORITY**: Migrate `binary_classifications_averages.py` → `binary_classification_averages_2025_06_16/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **LOWER PRIORITY**: Migrate `condition_aggregated.py` → `condition_aggregated_analysis_2025_04_08/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **MEDIUM PRIORITY**: Migrate `get_total_users_in_study.py` → `study_user_count_2025_05_05/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

#### **Phase 3d: Core Analytics Migration**
- [ ] **HIGH PRIORITY**: Migrate `feed_analytics.py` → `feed_analytics_2025_06_16/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **LOWER PRIORITY**: Migrate `calculate_weekly_thresholds_per_user.py` → `weekly_thresholds_analysis_2025_04_08/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

#### **Phase 3e: Data Consolidation Migration**
- [ ] **LOWEST PRIORITY**: Migrate `consolidate_feeds.py` → `feed_consolidation_2025_03_10/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **LOWEST PRIORITY**: Migrate `consolidate_user_session_logs.py` → `user_session_consolidation_2025_03_10/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **LOWEST PRIORITY**: Migrate `migrate_feeds_to_db.py` → `feed_migration_2025_03_10/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

- [ ] **LOWEST PRIORITY**: Migrate `migrate_user_session_logs_to_db.py` → `user_session_migration_2025_03_10/`
  - [ ] Create folder structure
  - [ ] Move main script
  - [ ] Update imports to use shared modules
  - [ ] Create README.md
  - [ ] Create investigation.ipynb
  - [ ] Create results/ folder
  - [ ] Test script functionality
  - [ ] Validate raw data consistency

#### **Phase 3f: Supporting Files Migration**
- [ ] Migrate configuration files (constants.py, query_profile.json)
- [ ] Migrate shell scripts (submit_*.sh)
- [ ] Migrate test files (test_*.py)
- [ ] Migrate model definitions (model.py)
- [ ] Migrate documentation (README.md files)

#### **Phase 3g: Validation & Testing**
- [ ] **NEW**: Identify all output files from existing scripts
- [ ] **NEW**: Create validation scripts for output comparison
- [ ] **NEW**: Run new scripts in Slurm environment (production testing)
- [ ] **NEW**: Verify 100% output consistency with old scripts
- [ ] **NEW**: Document validation results and any discrepancies
- [ ] **NEW**: Keep old scripts available for validation purposes

#### **Phase 3h: Migration Testing & Validation Planning**
- [ ] **NEW**: Write new analysis files first (before validation begins)
- [ ] **NEW**: Create a migration testing folder structure
- [ ] **NEW**: Create a validation checklist with:
  - [ ] Old version of analytics file
  - [ ] New version of analytics file  
  - [ ] What output file needs to be tested
  - [ ] Validation status (completed/pending)
- [ ] **NEW**: Note: Detailed validation plan (`met_41_validation_plan.md`) needs to be created manually later

### **MIGRATION PRIORITY SUMMARY**

**HIGH PRIORITY (June 2025 - Recent Activity):**
- User engagement analysis files (2 files)
- Feed analytics
- Binary classification averages

**MEDIUM PRIORITY (May 2025 - Recent Activity):**
- User engagement experiments
- Spam handle analysis
- Study user count

**LOWER PRIORITY (April 2025 - Older Activity):**
- Weekly user logins
- Condition aggregated analysis
- Weekly thresholds analysis

**LOWEST PRIORITY (March 2025 - Older Activity):**
- Data consolidation scripts (4 files - these may be more stable/complete)

### **FOLDER STRUCTURE TEMPLATE**
Each analysis folder will contain:
```
analyses/
├── <analysis_name>_<YYYY_MM_DD>/
│   ├── README.md
│   ├── main.py (or original script name)
│   ├── investigation.ipynb
│   ├── results/
│   ├── constants.py (if applicable)
│   ├── submit_*.sh (if applicable)
│   └── test_*.py (if applicable)
```

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
- [ ] **MET-41 Migration**: All 16 analysis scripts successfully migrated with consistent structure
