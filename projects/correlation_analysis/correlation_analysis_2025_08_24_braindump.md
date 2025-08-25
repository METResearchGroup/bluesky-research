# Brain Dump: Toxicity-Constructiveness Correlation Analysis

## Project Overview
Investigating confusing correlations between toxicity and constructiveness in Bluesky posts to understand if this is a data artifact, algorithmic bias, or calculation error.

## Initial Thoughts and Context

### The Problem
- There are correlations between toxicity and constructiveness that seem confusing/unexpected
- Need to investigate whether this is:
  1. A real trend across the broader Bluesky post population
  2. An algorithmic bias in feed selection
  3. A calculation error in daily probability/proportion logic

### Investigation Strategy (User's Thinking)
1. **Baseline Check**: Look at correlation of toxicity x constructiveness on ALL posts to see if trend replicates across wide sample
2. **Algorithmic Bias Check**: If baseline is clean, examine correlation on posts used in feeds to see if selection bias exists  
3. **Calculation Logic Review**: If above checks out, review daily probability/proportion calculation logic (user suspects this is unlikely since same logic used across all fields)

**Note**: User expects the issue is likely NOT in the calculation logic since the same calculation approach is used across all fields, making systematic errors less probable.

### Project Structure
- Following the analytics system refactor spec approach
- Will be placed in `correlation_analysis_2025_08_24` folder
- Should follow the new modular, testable structure

## Questions and Knowledge Gaps

### Data Understanding
- **Toxicity**: Perspective API scores (Float64, 0-1 range)
- **Constructiveness**: Perspective API scores (Float64, 0-1 range) 
- **Data Structure**: Parquet files with posts in `preprocessed_posts` service and labels in `ml_inference_perspective_api` service
- **Volume**: 20-30 million posts total
- **Format**: All data in .parquet format, loaded via `manage_local_data.py` utilities

### Current System Understanding
- **Daily Proportions**: Calculated in `condition_aggregated.py` 
- **Feed Posts**: Available via `get_posts_in_feeds_per_day.py` and similar utilities
- **Data Pipeline**: Posts loaded via `manage_local_data.py` with service constants defined in `service_constants.py`
- **Feed Generation**: Posts used in feeds are tracked and can be loaded locally (not requiring Slurm)

### Technical Questions
- **Analytics System Status**: Refactor not complete, but shared modules are implemented in `shared/` directory
- **Shared Modules**: Available in `services/calculate_analytics/study_analytics/shared/`
- **Data Loading**: Will need custom data loader logic (user will implement, but needs to be designed)
- **Statistical Tools**: Need to determine what's available for correlation analysis

### Expected Outcomes
- **Baseline Check**: No specific "clean" baseline expected - this is exploratory research
- **Success Criteria**: Determine if correlations replicate across different data subsets
- **Investigation Goal**: Understand whether observed correlations are data artifacts, algorithmic biases, or calculation errors

## Initial Scope Ideas

### Phase 1: Baseline Correlation Analysis (Slurm Required)
- **Data Loading**: Custom data loader logic for 20-30M posts across multiple days
- **Processing Strategy**: Load one day at a time, calculate correlations, garbage collect, then load next day
- **Correlation Methods**: Both Pearson and Spearman correlations
- **Output**: CSV format with daily correlation coefficients and aggregate statistics
- **Infrastructure**: Single Slurm job that processes entire dataset incrementally

### Phase 2: Feed Selection Analysis (Local Processing)
- **Data Loading**: Load posts used in feeds locally (manageable volume)
- **Processing Strategy**: Similar batch processing per day approach
- **Comparison**: Contrast correlation patterns between full dataset and feed-selected posts
- **Output**: Bias detection metrics and analysis

### Phase 3: Calculation Logic Review
- **Code Review**: Examine `condition_aggregated.py` daily proportion calculations
- **Testing**: Validate calculation logic with known inputs
- **Systematic Analysis**: Check for calculation errors across all fields

### Phase 4: Documentation and Reporting
- **Methodology**: Document batch processing approach and statistical methods
- **Reproducibility**: Create scripts that can be re-run with clear parameters
- **Analysis Script**: One main analysis script for documentation and reproducibility
- **Visualizations**: Generate correlation plots and summary statistics

## Potential Risks and Considerations

### Data Quality
- Missing or corrupted toxicity/constructiveness scores
- Selection bias in the data collection process
- Temporal changes in scoring algorithms

### Statistical Considerations
- Correlation vs. causation
- Multiple testing issues
- Effect sizes and practical significance

### Technical Challenges
- **Data Volume**: 20-30M posts requiring Slurm processing and batch processing per day
- **Memory Management**: Need rolling correlation calculations to avoid loading all data at once
- **Data Loading**: Custom loader logic needed for both Slurm and local processing
- **Integration**: Work with existing shared modules while extending functionality as needed

## Stakeholders
- **Primary**: Research team (user) - needs to understand the correlation
- **Secondary**: Data engineers - may need to fix calculation issues
- **Tertiary**: Future researchers - will benefit from documented methodology

## Success Criteria (Initial Thoughts)
- Clear understanding of whether correlation is real or artifact
- Reproducible analysis that can be re-run
- Actionable next steps based on findings
- Integration with the new analytics system structure

## Next Steps
- **Data Loader Design**: Design the custom data loading logic for both Slurm and local processing
- **Rolling Calculation Strategy**: Define multiple approaches for rolling correlation calculations (to be finalized later)
- **Slurm Job Design**: Plan the single Slurm job that processes entire dataset incrementally
- **Shared Module Integration**: Determine how to extend existing shared modules for this analysis
- **Statistical Methods**: Implement both Pearson and Spearman correlation methods with CSV output
- **Analysis Script**: Design one main analysis script for documentation and reproducibility

## Rolling Calculation Options (To Be Finalized)
- **Option 1**: Daily correlations within each day's data, then aggregate
- **Option 2**: Rolling window correlations (e.g., 7-day window) that update each day
- **Option 3**: Cumulative correlations that build up over time
- **Option 4**: Other approaches to be determined during specification phase
