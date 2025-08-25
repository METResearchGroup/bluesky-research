# MET-41 Implementation Plan: Phase 3 - Reorganize One-Off Analyses

## Overview

This document outlines the detailed implementation plan for MET-41, which involves reorganizing all existing analysis scripts from `services/calculate_analytics/study_analytics/` into a clean, dated folder structure within `projects/analytics_system_refactor/analyses/`.

## GitHub Pull Request

**PR #206**: [https://github.com/METResearchGroup/bluesky-research/pull/206](https://github.com/METResearchGroup/bluesky-research/pull/206)

## Key Objectives

1. **Organize Analysis Scripts**: Move all analysis scripts into focused, dated folders
2. **Standardize Structure**: Ensure every analysis folder has consistent layout
3. **Update Dependencies**: Modify scripts to use shared modules from Phase 2
4. **Maintain Functionality**: Ensure all analyses can be re-run successfully
5. **Focus on Raw Data**: Prioritize data consistency over output file matching

## Migration Priority Based on Git Activity

### **HIGH PRIORITY (June 2025 - Recent Activity)**
- User engagement analysis files (2 files)
- Feed analytics
- Binary classification averages

### **MEDIUM PRIORITY (May 2025 - Recent Activity)**
- User engagement experiments
- Spam handle analysis
- Study user count

### **LOWER PRIORITY (April 2025 - Older Activity)**
- Weekly user logins
- Condition aggregated analysis
- Weekly thresholds analysis

### **LOWEST PRIORITY (March 2025 - Older Activity)**
- Data consolidation scripts (4 files - these may be more stable/complete)

## Detailed Migration Checklist

### **Phase 3a: Setup & Preparation**
- [ ] Create `analytics_system_refactor/analyses/` directory
- [ ] Set up folder structure template
- [ ] Create migration tracking spreadsheet
- [ ] Verify Phase 2 completion status

### **Phase 3b: User Engagement Analysis Migration**

#### **HIGH PRIORITY**: `get_agg_labels_for_engagements.py` → `user_engagement_analysis_2025_06_16/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **HIGH PRIORITY**: `get_aggregate_metrics.py` → `user_engagement_metrics_2025_06_16/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **MEDIUM PRIORITY**: `experiment_get_agg_labels_for_engagement.py` → `user_engagement_experiment_2025_05_12/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **MEDIUM PRIORITY**: `qa_spammy_handles.py` → `spam_handle_analysis_2025_05_12/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

### **Phase 3c: Report Generation Migration**

#### **MEDIUM PRIORITY**: `weekly_user_logins.py` → `weekly_user_logins_2025_04_23/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **HIGH PRIORITY**: `binary_classifications_averages.py` → `binary_classification_averages_2025_06_16/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **LOWER PRIORITY**: `condition_aggregated.py` → `condition_aggregated_analysis_2025_04_08/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **MEDIUM PRIORITY**: `get_total_users_in_study.py` → `study_user_count_2025_05_05/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

### **Phase 3d: Core Analytics Migration**

#### **HIGH PRIORITY**: `feed_analytics.py` → `feed_analytics_2025_06_16/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **LOWER PRIORITY**: `calculate_weekly_thresholds_per_user.py` → `weekly_thresholds_analysis_2025_04_08/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

### **Phase 3e: Data Consolidation Migration**

#### **LOWEST PRIORITY**: `consolidate_feeds.py` → `feed_consolidation_2025_03_10/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **LOWEST PRIORITY**: `consolidate_user_session_logs.py` → `user_session_consolidation_2025_03_10/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **LOWEST PRIORITY**: `migrate_feeds_to_db.py` → `feed_migration_2025_03_10/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

#### **LOWEST PRIORITY**: `migrate_user_session_logs_to_db.py` → `user_session_migration_2025_03_10/`
- [ ] Create folder structure
- [ ] Move main script
- [ ] Update imports to use shared modules
- [ ] Create README.md
- [ ] Create investigation.ipynb
- [ ] Create results/ folder
- [ ] Test script functionality
- [ ] Validate raw data consistency

### **Phase 3f: Supporting Files Migration**
- [ ] Migrate configuration files (constants.py, query_profile.json)
- [ ] Migrate shell scripts (submit_*.sh)
- [ ] Migrate test files (test_*.py)
- [ ] Migrate model definitions (model.py)
- [ ] Migrate documentation (README.md files)

### **Phase 3g: Validation & Testing**
- [ ] **NEW**: Identify all output files from existing scripts
- [ ] **NEW**: Create validation scripts for output comparison
- [ ] **NEW**: Run new scripts in Slurm environment (production testing)
- [ ] **NEW**: Verify 100% output consistency with old scripts
- [ ] **NEW**: Document validation results and any discrepancies
- [ ] **NEW**: Keep old scripts available for validation purposes

### **Phase 3h: Migration Testing & Validation Planning**
- [ ] **NEW**: Write new analysis files first (before validation begins)
- [ ] **NEW**: Create a migration testing folder structure
- [ ] **NEW**: Create a validation checklist with:
  - [ ] Old version of analytics file
  - [ ] New version of analytics file  
  - [ ] What output file needs to be tested
  - [ ] Validation status (completed/pending)
- [ ] **NEW**: Note: Detailed validation plan (`met_41_validation_plan.md`) needs to be created manually later

## Folder Structure Template

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

## Implementation Strategy

### **Step 1: Create Base Structure**
1. Create `analytics_system_refactor/analyses/` directory
2. Set up folder structure template
3. Create migration tracking spreadsheet

### **Step 2: High Priority Migrations**
1. Start with June 2025 files (most recent activity)
2. Create folders and migrate scripts
3. Update imports to use shared modules
4. Test functionality and validate raw data consistency

### **Step 3: Medium Priority Migrations**
1. Continue with May 2025 files
2. Follow same process as high priority
3. Ensure consistent structure and functionality

### **Step 4: Lower Priority Migrations**
1. Handle April 2025 files
2. Focus on stability and consistency
3. Validate all functionality preserved

### **Step 5: Lowest Priority Migrations**
1. Complete March 2025 files
2. These may be more stable/complete
3. Ensure backward compatibility

### **Step 6: Validation & Testing**
1. Create comprehensive validation framework
2. Test all migrated scripts
3. Verify raw data consistency
4. Document any issues or discrepancies

## Success Criteria

- [ ] All 16 analysis scripts successfully migrated
- [ ] Each analysis has dedicated, dated folder
- [ ] Consistent folder structure across all analyses
- [ ] All scripts updated to use shared modules
- [ ] Raw data consistency maintained
- [ ] Functionality preserved for all analyses
- [ ] Comprehensive documentation created
- [ ] Validation framework established

## Dependencies

- ✅ **Phase 2 Completion**: Simplified analyzer framework completed and ready
- ⏳ **Shared Modules**: All shared data loading and processing modules must be available
- ⏳ **Testing Environment**: Access to Slurm environment for production testing

## Timeline Estimate

- **Setup & High Priority**: 2-3 days
- **Medium Priority**: 2-3 days  
- **Lower Priority**: 1-2 days
- **Lowest Priority**: 1-2 days
- **Validation & Testing**: 2-3 days
- **Total**: 8-13 days (1.5-2.5 weeks)

## Risk Mitigation

- **Phase 2 Dependency**: Monitor completion status and adjust timeline accordingly
- **Script Interdependencies**: Careful analysis before migration to identify dependencies
- **Import Path Updates**: Comprehensive testing of all import statement changes
- **Raw Data Validation**: Focus on data consistency rather than output file matching
- **Folder Naming Conflicts**: Ensure unique, descriptive names for each analysis

## Next Steps

1. **Await Phase 2 Completion**: Ensure simplified analyzer framework is ready
2. **Create Migration Tracking**: Set up spreadsheet to track progress
3. **Begin High Priority Migrations**: Start with most recent activity files
4. **Iterative Validation**: Test each migration before proceeding to next
5. **Documentation Updates**: Keep all documentation current throughout process
