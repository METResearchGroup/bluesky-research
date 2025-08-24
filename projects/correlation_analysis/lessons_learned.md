# Lessons Learned: Toxicity-Constructiveness Correlation Analysis

## Project Planning Insights

### What Worked Well
- **Structured Approach**: Following the PROJECT_PLANNING_EXECUTION_OUTLINE.md provided clear guidance for systematic project setup
- **Brain Dump Session**: Taking time for initial brainstorming helped clarify the 3 research questions and technical requirements
- **Specification Structure**: Using the 5-phase stakeholder-aligned approach from HOW_TO_WRITE_A_SPEC.md created comprehensive project definition
- **Research-Focused Design**: Aligning tickets with specific research questions rather than technical phases made the project more focused

### Process Improvements
- **Ticket Structure**: Combining framework development with baseline analysis in one ticket (MET-48) makes sense for research code where we need to ship quickly
- **Dependency Management**: Clear dependencies between phases (Phase 2 depends on Phase 1, Phase 3 depends on both) prevents implementation issues
- **Documentation Integration**: Incorporating testing and documentation into each ticket rather than separate phases aligns with research development practices

### Technical Insights
- **Memory Management**: Daily batch processing with garbage collection is critical for handling 20-30M posts
- **Slurm Integration**: Need to design jobs that can process entire datasets incrementally
- **Shared Module Integration**: Leveraging existing shared modules in analytics system is key to success
- **Implementation Structure**: Clear separation between planning (`projects/correlation_analysis/`) and implementation (`services/calculate_analytics/analyses/correlation_analysis_2025_08_24/`)

## Implementation Insights

*To be populated as implementation progresses*

## Research Methodology Insights

*To be populated as research questions are answered*

## Future Project Considerations

*To be populated based on project outcomes*
