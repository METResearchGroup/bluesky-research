# Analytics System Refactor

## Project Overview
Refactor the `services/calculate_analytics/` system from monolithic scripts to simple, organized analysis classes using modular components with shared utilities.

## Project Status
**Current Phase**: Phase 1 Complete, Phase 2 Planning  
**Overall Progress**: 25%  
**Next Milestone**: Begin Phase 2 Implementation (Simple Analysis Framework)  
**Risk Level**: Low (Simplified architecture reduces complexity)

## Quick Links
- [Project Specification](spec.md)
- [Linear Project](https://linear.app/metresearch/project/analytics-system-refactoring-4af511ffca6b)
- [Starting Point Analysis](starting_point_analysis.md) - Baseline state before refactoring
- [Task Plan](plan_refactor.md)
- [Todo Checklist](todo.md)
- [Progress Logs](logs.md)
- [Lessons Learned](lessons_learned.md)
- [Performance Metrics](metrics.md)

## Linear Tickets
- [MET-39](https://linear.app/metresearch/issue/MET-39/phase-1-extract-shared-data-loading-and-processing) - Phase 1: Extract Shared Data Loading & Processing ✅ **COMPLETED**
- [MET-40](https://linear.app/metresearch/issue/MET-40/phase-2-implement-simple-pipeline-framework-for-consistent-analysis) - Phase 2: Implement Simple Analysis Framework with Shared Utilities 🔄 **REVISED & IN PROGRESS**
- [MET-41](https://linear.app/metresearch/issue/MET-41/phase-3-reorganize-one-off-analyses) - Phase 3: Reorganize One-Off Analyses
- [MET-42](https://linear.app/metresearch/issue/MET-42/phase-4-implement-testing-and-validation) - Phase 4: Implement Testing & Validation
- [MET-43](https://linear.app/metresearch/issue/MET-43/phase-5-documentation-and-cleanup) - Phase 5: Documentation & Cleanup

## Project Structure
```
projects/analytics_system_refactor/
├── spec.md                           # Project specification (REVISED)
├── plan_refactor.md                  # Detailed task plan (REVISED)
├── todo.md                           # Todo checklist
├── logs.md                           # Progress logs
├── lessons_learned.md                # Lessons learned
├── metrics.md                        # Performance metrics
└── retrospective/                    # Phase retrospectives
    └── README.md                     # Retrospective overview
```

## Implementation Phases
1. **Phase 1 (Week 1)**: Extract shared data loading & processing ✅ **COMPLETED**
2. **Phase 2 (Week 2)**: Implement simple analysis framework with shared utilities 🔄 **REVISED & IN PROGRESS**
3. **Phase 3 (Week 3)**: Reorganize one-off analyses
4. **Phase 4 (Week 4)**: Implement testing & validation
5. **Phase 5 (Week 5)**: Documentation & cleanup

## **ARCHITECTURE REVISION - KEY CHANGES**

### **Original Plan vs. Revised Approach**

**Original Architecture (ABC Pipeline Framework):**
- Complex abstract base classes with setup/execute lifecycle
- Pipeline state management and orchestration
- Enterprise software engineering patterns
- Over-engineered for research needs

**Revised Architecture (Simple Analysis Classes):**
- Simple classes with inheritance for shared utilities
- Direct method execution without complex lifecycle
- Research-focused patterns for transparency and reproducibility
- Focus on code organization and reusability

### **Why the Architecture Was Simplified**

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

## Analysis Folder Structure

Each analysis folder follows a consistent structure to ensure clarity and reproducibility:

```
analyses/
├── feed_content_analysis_2024_12_01/           # One analysis per folder
│   ├── README.md                     # Description of analysis purpose and methodology
│   ├── investigation.ipynb           # Jupyter notebook with preliminary investigation
│   ├── main.py                       # Main implementation generating analytical assets
│   └── assets/                       # Generated outputs (CSV files, images, etc.)
├── user_engagement_analysis_2024_12_01/     # Another distinct analysis
│   ├── README.md
│   ├── investigation.ipynb
│   ├── main.py
│   └── assets/
└── ...
```

**Folder Naming Convention**: `<description>_<YYYY_MM_DD>` where description is a brief, clear name for the specific analysis being performed. This format ensures valid Python import paths.

## Success Criteria
- All existing analytics outputs can be reproduced exactly
- New analyses can be created quickly using shared modules
- Codebase is maintainable with clear separation of concerns
- Performance improved through better caching and optimization
- System is extensible for future research needs
- **NEW**: Code is transparent and easy to understand for peer reviewers
- **NEW**: Architecture supports academic rigor and reproducibility

## Key Metrics
- **Test Coverage**: >80% (Target)
- **Code Duplication**: Reduce by >50% (Target)
- **Performance**: >40% faster processing (Target)
- **File Size**: No files >500 lines (Target)
- **Architecture Complexity**: Simplified from ABC pipelines to simple classes (Target)

## Team
- **Lead Developer**: [TBD]
- **Reviewers**: [TBD]
- **Stakeholders**: Research team, data engineers

## Notes
- This project maintains backward compatibility throughout refactoring
- All changes are incremental and testable
- Focus is on research-grade reliability and reproducibility, not production complexity
- **REVISED**: Simple, organized patterns provide consistency while maintaining flexibility
- **REVISED**: Architecture better aligns with academic research requirements and peer review scrutiny
- **REVISED**: Removed unnecessary enterprise complexity in favor of research-focused simplicity
