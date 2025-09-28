# Toxicity vs Join Date Analysis - Project Plan

## Project Overview
Exploratory analysis to investigate whether users who joined Bluesky toward the end of the study period (2024-09-30 to 2024-12-01) were more likely to post toxic/outrage content compared to users who joined earlier.

## Research Question
Are users who joined Bluesky later in the study period more likely to post toxic/outrage content than users who joined earlier?

## Timeline
- **Start Date**: Current
- **Deadline**: Wednesday afternoon
- **Status**: Ready for implementation

## Subtasks and Deliverables

### Phase 1: Project Planning ✅
- [x] Conduct brainstorming session and create brain dump
- [x] Create comprehensive specification following HOW_TO_WRITE_A_SPEC.md
- [x] Conduct multi-persona review process (5 specialized personas)
- [x] Create Linear project definition
- [x] Generate implementation ticket following HOW_TO_WRITE_LINEAR_TICKET.md
- [x] Set up project organization structure
- [x] Create project README and documentation

### Phase 2: Implementation (Pending)
- [ ] User frequency analysis implementation
- [ ] User sampling logic implementation
- [ ] Bluesky API integration with error handling
- [ ] Toxicity/outrage calculation implementation
- [ ] Histogram visualization generation
- [ ] Scatterplot visualization generation
- [ ] Data storage structure implementation
- [ ] Results validation and presentation preparation

## Effort Estimates
- **Planning Phase**: 4 hours (completed)
- **Implementation Phase**: 6 hours (estimated)
- **Total Project**: 10 hours

## Success Criteria
- ✅ User frequency analysis completed across all study days
- ✅ Sample of users selected based on post frequency threshold
- ✅ Bluesky API calls completed for join date collection
- ✅ Average toxicity/outrage calculated per user
- ✅ Histogram generated: joined month/date vs average probability
- ✅ Scatterplot generated: average toxicity/outrage vs months relative to study start
- ✅ Data and visualizations ready for presentation by Wednesday afternoon
- ✅ Analysis results stored in structured format

## PR Information
- **PR URL**: https://github.com/METResearchGroup/bluesky-research/pull/217
- **Branch**: feature/toxicity_join_date_analysis_setup
- **Status**: Open, needs review
- **Labels**: feature, needs-review

## Key Files
- **Specification**: `/projects/toxicity-join-date-analysis/spec.md`
- **Brain Dump**: `/projects/toxicity-join-date-analysis/braindump.md`
- **Implementation Ticket**: `/projects/toxicity-join-date-analysis/tickets/ticket-001.md`
- **Project README**: `/projects/toxicity-join-date-analysis/README.md`
