# Analytics System Refactor

## Project Overview
Refactor the `services/calculate_analytics/` system from monolithic scripts to a modular, ABC-based pipeline framework.

## Project Status
**Current Phase**: Planning Complete  
**Overall Progress**: 0%  
**Next Milestone**: Begin Phase 1 Implementation  
**Risk Level**: Medium (Output compatibility concerns)

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
- [MET-39](https://linear.app/metresearch/issue/MET-39/phase-1-extract-shared-data-loading-and-processing) - Phase 1: Extract Shared Data Loading & Processing
- [MET-40](https://linear.app/metresearch/issue/MET-40/phase-2-implement-abc-based-pipeline-framework) - Phase 2: Implement ABC-Based Pipeline Framework
- [MET-41](https://linear.app/metresearch/issue/MET-41/phase-3-reorganize-one-off-analyses) - Phase 3: Reorganize One-Off Analyses
- [MET-42](https://linear.app/metresearch/issue/MET-42/phase-4-implement-testing-and-validation) - Phase 4: Implement Testing & Validation
- [MET-43](https://linear.app/metresearch/issue/MET-43/phase-5-documentation-and-cleanup) - Phase 5: Documentation & Cleanup

## Project Structure
```
projects/analytics_system_refactor/
├── spec.md                           # Project specification
├── linear_project_definition.md      # Linear project definition
├── plan_refactor.md                  # Detailed task plan
├── todo.md                           # Todo checklist
├── logs.md                           # Progress logs
├── lessons_learned.md                # Lessons learned
├── metrics.md                        # Performance metrics
└── retrospective/                    # Phase retrospectives
    └── README.md                     # Retrospective overview
```

## Implementation Phases
1. **Phase 1 (Week 1)**: Extract shared data loading & processing
2. **Phase 2 (Week 2)**: Implement ABC-based pipeline framework
3. **Phase 3 (Week 3)**: Reorganize one-off analyses
4. **Phase 4 (Week 4)**: Implement testing & validation
5. **Phase 5 (Week 5)**: Documentation & cleanup

## Success Criteria
- All existing analytics outputs can be reproduced exactly
- New analyses can be created quickly using shared modules
- Codebase is maintainable with clear separation of concerns
- Performance improved through better caching and optimization
- System is extensible for future research needs

## Key Metrics
- **Test Coverage**: >80% (Target)
- **Code Duplication**: Reduce by >50% (Target)
- **Performance**: >40% faster processing (Target)
- **File Size**: No files >500 lines (Target)

## Team
- **Lead Developer**: [TBD]
- **Reviewers**: [TBD]
- **Stakeholders**: Research team, data engineers

## Notes
- This project maintains backward compatibility throughout refactoring
- All changes are incremental and testable
- Focus is on research-grade reliability, not production complexity
- ABC-based design provides consistency while maintaining flexibility
