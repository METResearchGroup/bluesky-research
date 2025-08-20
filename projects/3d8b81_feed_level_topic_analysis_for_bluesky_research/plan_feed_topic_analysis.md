---
last_modified: 2025-01-19T19:20:00Z
project_id: 3d8b8125-5cc0-4816-b3bb-9ba177463d5e
---

# Task Plan: Feed-Level Topic Analysis for Bluesky Research

## Feature Overview
Systematic topic modeling analysis to understand how Bluesky feed algorithms affect topic distributions across experimental conditions and time periods, generating publication-ready research outputs.

## Subtasks and Deliverables

| Subtask | Deliverable | Dependencies | Effort (hrs) | Priority Score | Linear Issue ID | Linear Issue Identifier | PR URL |
|---------|-------------|--------------|--------------|----------------|-----------------|-------------------------|---------|
| Core BERTopic Pipeline | Generic text-to-topics pipeline with YAML config and quality monitoring | None | 6 | 5 | bd926157-063b-48a5-ba33-574efd45fdff | MET-34 | TBD |
| Feed Analysis & Stratification | Multi-level stratified analysis across conditions and time | MET-34 | 8 | 5 | 05df403a-fefb-48e0-86d1-45b8e6df15d5 | MET-35 | TBD |
| Publication Materials | Statistical tables and publication-quality figures | MET-35 | 6 | 5 | 6c099013-973e-44ff-b766-8b428873d533 | MET-36 | TBD |
| Interactive Dashboard (Optional) | Streamlit dashboard for topic exploration | MET-36 | 8 | 2 | 51edaa17-4d87-4cef-a8cc-15cf49d1d07f | MET-37 | TBD |

## Execution Order and Dependencies

### Sequential Execution Required:
1. **MET-34** (Core BERTopic Pipeline) - Must be completed first
2. **MET-35** (Feed Analysis & Stratification) - Depends on MET-34
3. **MET-36** (Publication Materials) - Depends on MET-35  
4. **MET-37** (Interactive Dashboard) - Optional, depends on MET-36

### Parallel Execution Opportunities:
- None - Each ticket has strict dependencies on the previous

## Risk Register

| Risk | Likelihood (1-5) | Impact (1-5) | Score | Mitigation |
|------|------------------|--------------|-------|------------|
| BERTopic scalability with 1M posts | 3 | 4 | 12 | GPU optimization and batch processing |
| Topic interpretability issues | 2 | 4 | 8 | Coherence metrics monitoring and manual validation |
| Memory constraints during processing | 3 | 3 | 9 | Configurable batch sizes and optimization strategies |
| Topic stability across conditions | 2 | 3 | 6 | Sensitivity analysis and validation checks |

## Success Metrics
- BERTopic model trained with coherence scores (c_v > 0.4, c_npmi > 0.1)
- Multi-level stratified analysis across 3 conditions and temporal periods
- Publication-ready statistical tables and PNG figures
- Reproducible analysis pipeline with random seed control

## Milestones
- **Week 1**: Core BERTopic pipeline with YAML config and quality monitoring (MET-34)
- **Week 2**: Feed-specific analysis code and stratified data processing (MET-35)  
- **Week 3**: Publication materials generation and topic evolution analysis (MET-36)
- **Week 4**: Optional Streamlit dashboard and final documentation (MET-37)
