# MET-41 Migration Tracking Spreadsheet

## GitHub Pull Request

**PR #206**: [https://github.com/METResearchGroup/bluesky-research/pull/206](https://github.com/METResearchGroup/bluesky-research/pull/206)

## Migration Progress Overview

**Total Files to Migrate**: 16 analysis scripts + supporting files  
**Current Status**: 🔄 **IN PROGRESS**  
**Phase 2 Dependency**: ⏳ **WAITING FOR COMPLETION**  
**Start Date**: TBD  
**Target Completion**: TBD  

## Migration Status Legend

- 🔄 **NOT STARTED** - File not yet migrated
- 🚧 **IN PROGRESS** - Migration currently being worked on
- ✅ **COMPLETED** - Migration finished and validated
- ❌ **BLOCKED** - Migration blocked by dependency or issue
- ⚠️ **ISSUES** - Migration completed but with issues to resolve

## High Priority Migrations (June 2025 - Recent Activity)

| Source File | Target Folder | Priority | Status | Assigned To | Start Date | Completion Date | Notes |
|-------------|---------------|----------|---------|-------------|------------|-----------------|-------|
| `get_agg_labels_for_engagements.py` | `user_engagement_analysis_2025_06_16/` | HIGH | 🔄 NOT STARTED | TBD | TBD | TBD | User engagement analysis with valence classifications |
| `get_aggregate_metrics.py` | `user_engagement_metrics_2025_06_16/` | HIGH | 🔄 NOT STARTED | TBD | TBD | TBD | Aggregate user engagement metrics |
| `feed_analytics.py` | `feed_analytics_2025_06_16/` | HIGH | 🔄 NOT STARTED | TBD | TBD | TBD | Core feed analytics functionality |
| `binary_classifications_averages.py` | `binary_classification_averages_2025_06_16/` | HIGH | 🔄 NOT STARTED | TBD | TBD | TBD | Binary classification averages with valence |

## Medium Priority Migrations (May 2025 - Recent Activity)

| Source File | Target Folder | Priority | Status | Assigned To | Start Date | Completion Date | Notes |
|-------------|---------------|----------|---------|-------------|------------|-----------------|-------|
| `experiment_get_agg_labels_for_engagement.py` | `user_engagement_experiment_2025_05_12/` | MEDIUM | 🔄 NOT STARTED | TBD | TBD | TBD | User engagement experiment scripts |
| `qa_spammy_handles.py` | `spam_handle_analysis_2025_05_12/` | MEDIUM | 🔄 NOT STARTED | TBD | TBD | TBD | Spam handle quality analysis |
| `get_total_users_in_study.py` | `study_user_count_2025_05_05/` | MEDIUM | 🔄 NOT STARTED | TBD | TBD | TBD | Study user count calculation |
| `weekly_user_logins.py` | `weekly_user_logins_2025_04_23/` | MEDIUM | 🔄 NOT STARTED | TBD | TBD | TBD | Weekly user login reporting |

## Lower Priority Migrations (April 2025 - Older Activity)

| Source File | Target Folder | Priority | Status | Assigned To | Start Date | Completion Date | Notes |
|-------------|---------------|----------|---------|-------------|------------|-----------------|-------|
| `condition_aggregated.py` | `condition_aggregated_analysis_2025_04_08/` | LOWER | 🔄 NOT STARTED | TBD | TBD | TBD | Condition aggregated analysis |
| `calculate_weekly_thresholds_per_user.py` | `weekly_thresholds_analysis_2025_04_08/` | LOWER | 🔄 NOT STARTED | TBD | TBD | TBD | Weekly thresholds per user |

## Lowest Priority Migrations (March 2025 - Older Activity)

| Source File | Target Folder | Priority | Status | Assigned To | Start Date | Completion Date | Notes |
|-------------|---------------|----------|---------|-------------|------------|-----------------|-------|
| `consolidate_feeds.py` | `feed_consolidation_2025_03_10/` | LOWEST | 🔄 NOT STARTED | TBD | TBD | TBD | Feed consolidation scripts |
| `consolidate_user_session_logs.py` | `user_session_consolidation_2025_03_10/` | LOWEST | 🔄 NOT STARTED | TBD | TBD | TBD | User session log consolidation |
| `migrate_feeds_to_db.py` | `feed_migration_2025_03_10/` | LOWEST | 🔄 NOT STARTED | TBD | TBD | TBD | Feed migration to database |
| `migrate_user_session_logs_to_db.py` | `user_session_migration_2025_03_10/` | LOWEST | 🔄 NOT STARTED | TBD | TBD | TBD | User session log migration |

## Supporting Files Migration

| File Type | Source Location | Status | Assigned To | Start Date | Completion Date | Notes |
|-----------|-----------------|---------|-------------|------------|-----------------|-------|
| Configuration files | Various constants.py, query_profile.json | 🔄 NOT STARTED | TBD | TBD | TBD | Configuration and query files |
| Shell scripts | Various submit_*.sh | 🔄 NOT STARTED | TBD | TBD | TBD | Submission and execution scripts |
| Test files | Various test_*.py | 🔄 NOT STARTED | TBD | TBD | TBD | Test and validation scripts |
| Model definitions | Various model.py | 🔄 NOT STARTED | TBD | TBD | TBD | Data model definitions |
| Documentation | Various README.md | 🔄 NOT STARTED | TBD | TBD | TBD | Documentation files |

## Migration Checklist Status

### **Phase 3a: Setup & Preparation**
- [ ] Create `analytics_system_refactor/analyses/` directory
- [ ] Set up folder structure template
- [ ] Create migration tracking spreadsheet
- [ ] Verify Phase 2 completion status

### **Phase 3b: User Engagement Analysis Migration**
- [ ] **HIGH PRIORITY**: Migrate `get_agg_labels_for_engagements.py` → `user_engagement_analysis_2025_06_16/`
- [ ] **HIGH PRIORITY**: Migrate `get_aggregate_metrics.py` → `user_engagement_metrics_2025_06_16/`
- [ ] **MEDIUM PRIORITY**: Migrate `experiment_get_agg_labels_for_engagement.py` → `user_engagement_experiment_2025_05_12/`
- [ ] **MEDIUM PRIORITY**: Migrate `qa_spammy_handles.py` → `spam_handle_analysis_2025_05_12/`

### **Phase 3c: Report Generation Migration**
- [ ] **MEDIUM PRIORITY**: Migrate `weekly_user_logins.py` → `weekly_user_logins_2025_04_23/`
- [ ] **HIGH PRIORITY**: Migrate `binary_classifications_averages.py` → `binary_classification_averages_2025_06_16/`
- [ ] **LOWER PRIORITY**: Migrate `condition_aggregated.py` → `condition_aggregated_analysis_2025_04_08/`
- [ ] **MEDIUM PRIORITY**: Migrate `get_total_users_in_study.py` → `study_user_count_2025_05_05/`

### **Phase 3d: Core Analytics Migration**
- [ ] **HIGH PRIORITY**: Migrate `feed_analytics.py` → `feed_analytics_2025_06_16/`
- [ ] **LOWER PRIORITY**: Migrate `calculate_weekly_thresholds_per_user.py` → `weekly_thresholds_analysis_2025_04_08/`

### **Phase 3e: Data Consolidation Migration**
- [ ] **LOWEST PRIORITY**: Migrate `consolidate_feeds.py` → `feed_consolidation_2025_03_10/`
- [ ] **LOWEST PRIORITY**: Migrate `consolidate_user_session_logs.py` → `user_session_consolidation_2025_03_10/`
- [ ] **LOWEST PRIORITY**: Migrate `migrate_feeds_to_db.py` → `feed_migration_2025_03_10/`
- [ ] **LOWEST PRIORITY**: Migrate `migrate_user_session_logs_to_db.py` → `user_session_migration_2025_03_10/`

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

## Progress Summary

| Priority Level | Total Files | Not Started | In Progress | Completed | Blocked | Issues |
|----------------|-------------|-------------|-------------|-----------|---------|---------|
| **HIGH PRIORITY** | 4 | 4 | 0 | 0 | 0 | 0 |
| **MEDIUM PRIORITY** | 4 | 4 | 0 | 0 | 0 | 0 |
| **LOWER PRIORITY** | 2 | 2 | 0 | 0 | 0 | 0 |
| **LOWEST PRIORITY** | 4 | 4 | 0 | 0 | 0 | 0 |
| **SUPPORTING FILES** | 5 | 5 | 0 | 0 | 0 | 0 |
| **TOTAL** | **19** | **19** | **0** | **0** | **0** | **0** |

## Blockers and Issues

| Blocker/Issue | Description | Impact | Resolution Plan | Assigned To | Target Resolution Date |
|---------------|-------------|---------|-----------------|-------------|------------------------|
| ✅ **Phase 2 Complete** | Simplified analyzer framework completed | N/A - READY TO PROCEED | N/A | N/A | N/A |
| ⏳ **Shared Modules** | All shared data loading and processing modules must be available | BLOCKS MIGRATION | Verify module availability | TBD | TBD |
| ⏳ **Testing Environment** | Access to Slurm environment for production testing | BLOCKS VALIDATION | Secure testing environment access | TBD | TBD |

## Next Actions

1. ✅ **Phase 2 Complete** - Simplified analyzer framework ready for use
2. **Begin High Priority Migrations** - Start with June 2025 files (most recent activity)
3. **Follow Iterative Validation** - Test each migration before proceeding to next
4. **Track Progress** - Use migration tracking spreadsheet
5. **Validate Shared Modules** - Ensure all required modules are available

## Notes and Observations

- **Git Activity Analysis**: Most recent activity is June 2025, indicating active development
- **Priority Strategy**: Focus on high-priority files first to maximize impact
- **Dependency Management**: All migrations depend on Phase 2 completion
- **Validation Approach**: Focus on raw data consistency rather than output file matching
- **Risk Mitigation**: Keep old scripts available for validation and rollback if needed
