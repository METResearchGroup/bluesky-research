# MET-53: Implement Hashtag analysis module

## Context & Motivation
This ticket implements hashtag analysis to extract and analyze hashtag usage patterns across experimental conditions and time periods. This analysis provides insights into how hashtag adoption and popularity vary across different feed conditions and temporal periods, enabling understanding of viral content patterns and topic trends in political discourse.

See `/projects/content_analysis_2025_09_22/spec.md` for complete requirements and `/projects/content_analysis_2025_09_22/plan_content_analysis.md` for implementation strategy.

## Detailed Description & Requirements

### Functional Requirements:
- **Inputs**: All posts used in feeds from existing topic modeling results
- **Processing**: 
  - Extract hashtags using regex patterns (#hashtag format)
  - Apply case normalization and deduplication
  - Filter out very rare hashtags (e.g., < 5 occurrences)
  - Create frequency analysis by condition and time periods
  - Generate stratified analysis (overall, pre/post, by condition, by condition pre/post)
- **Outputs**:
  - Standardized CSV format: `condition | pre/post_election | hashtag | count | proportion`
  - Pre-sliced CSV files for easy visualization loading
  - Filenames following timestamped pattern: `YYYY-MM-DD_HH:MM:SS`
  - Basic visualization (top hashtags by condition side by side)
  - Metadata files with generation tracking

### Non-Functional Requirements:
- **Performance**: Efficient regex processing for large-scale datasets
- **Data Quality**: Consistent hashtag normalization and deduplication
- **Output Format**: Standardized CSV schema compatible with visualization tools
- **Timestamp Format**: Consistent YYYY-MM-DD_HH:MM:SS formatting across all outputs
- **Integration**: Seamless integration with existing data loading infrastructure

### Validation & Error Handling:
- **Edge Cases**: Handle malformed hashtags, empty posts, special characters
- **Regex Validation**: Ensure hashtag extraction accuracy and completeness
- **Frequency Validation**: Verify filtering thresholds work correctly
- **CSV Validation**: Ensure schema compliance and data integrity
- **Error Logging**: Log processing failures, data quality issues, and performance metrics

## Success Criteria
- [ ] Hashtag extraction successfully identifies hashtags from post text
- [ ] Frequency analysis covers all experimental conditions and time periods
- [ ] Standardized CSV output format: `condition | pre/post_election | hashtag | count | proportion`
- [ ] Pre-sliced CSV files are created for easy visualization loading
- [ ] Filenames follow timestamped pattern: `YYYY-MM-DD_HH:MM:SS`
- [ ] Visualization shows top hashtags by condition side by side
- [ ] Module documentation is complete and clear
- [ ] Integration with existing data pipeline works correctly
- [ ] All tests written and passing
- [ ] Code reviewed and merged

## Test Plan
- **`test_hashtag_extraction`**: Test regex pattern matching → Correct hashtag identification
- **`test_hashtag_normalization`**: Test case normalization and deduplication → Consistent hashtag formatting
- **`test_hashtag_frequency_filtering`**: Test rare hashtag filtering → Low-frequency hashtags removed correctly
- **`test_hashtag_stratified_analysis`**: Test condition and time period analysis → Correct frequency rankings per dimension
- **`test_hashtag_csv_schema_compliance`**: Validate CSV output format → Schema compliance with all required columns
- **`test_hashtag_filename_patterns`**: Verify timestamped filename generation → Correct YYYY-MM-DD_HH:MM:SS format
- **`test_hashtag_visualization_quality`**: Validate PNG output quality → Publication-ready visualizations
- **`test_hashtag_integration`**: Test data loading from existing topic modeling results → Seamless integration
- **`test_hashtag_edge_cases`**: Test malformed hashtags, empty posts → Graceful error handling

📁 Test file: `services/calculate_analytics/analyses/content_analysis_2025_09_22/hashtags/tests/test_hashtag_analysis.py`

## Dependencies
- **Depends on**: Project structure setup (content_analysis_2025_09_22 directory creation)
- **Requires**: Existing topic modeling results from `calculate_feed_topic_models_2025_08_18`
- **Requires**: matplotlib/seaborn for visualizations
- **Requires**: Access to all posts used in feeds
- **Requires**: Regex pattern libraries for hashtag extraction

## Suggested Implementation Plan
1. **Hashtag Extraction**: Implement `HashtagExtractor` class with regex patterns and validation
2. **Normalization**: Create `HashtagNormalizer` for case normalization and deduplication
3. **Frequency Analysis**: Implement `HashtagFrequencyAnalyzer` with configurable filtering thresholds
4. **Stratified Analysis**: Create `HashtagStratifiedAnalyzer` for condition and temporal period analysis
5. **CSV Export**: Implement `HashtagCsvExporter` with standardized schema and pre-sliced files
6. **Visualization**: Create `HashtagVisualizer` for comparative charts and rankings
7. **Metadata Tracking**: Implement `HashtagMetadataManager` for audit trail and reproducibility
8. **Integration**: Ensure seamless integration with existing project patterns and file organization

## Effort Estimate
- **Estimated effort**: **4 hours**
- **Assumes**: Existing topic modeling infrastructure is stable and accessible
- **Assumes**: Project structure is already set up with proper directory organization
- **Assumes**: AI agent implementation (faster than human development time)

## Priority & Impact
- **Priority**: **Medium**
- **Rationale**: Supporting analysis module, can be developed in parallel with other modules

## Acceptance Checklist
- [ ] Hashtag extraction module implemented
- [ ] Regex pattern matching working correctly
- [ ] Hashtag normalization and deduplication implemented
- [ ] Frequency analysis by condition and time periods working
- [ ] Standardized CSV export with correct schema
- [ ] Pre-sliced CSV files for visualization created
- [ ] Timestamped filename patterns implemented
- [ ] Basic visualization (top hashtags by condition) created
- [ ] Module documentation written
- [ ] Integration with existing topic modeling pipeline tested
- [ ] Tests written and passing
- [ ] Code reviewed and merged

## Links & References
- **Spec**: `/projects/content_analysis_2025_09_22/spec.md`
- **Plan**: `/projects/content_analysis_2025_09_22/plan_content_analysis.md`
- **Linear Issue**: https://linear.app/metresearch/issue/MET-53
- **Linear Project**: https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0
- **Existing Topic Modeling**: `services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/`
- **Related tickets**: MET-51 (TF-IDF), MET-52 (NER), MET-54 (Mentions)
