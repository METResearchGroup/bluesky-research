# MET-54: Implement Mention analysis module

## Context & Motivation
This ticket implements mention analysis to extract and analyze user mention patterns (@username) across experimental conditions and time periods. This analysis provides insights into how user interactions and social connections vary across different feed conditions and temporal periods, enabling understanding of influence patterns, user engagement, and social network dynamics in political discourse.

See `/projects/content_analysis_2025_09_22/spec.md` for complete requirements and `/projects/content_analysis_2025_09_22/plan_content_analysis.md` for implementation strategy.

## Detailed Description & Requirements

### Functional Requirements:
- **Inputs**: All posts used in feeds from existing topic modeling results
- **Processing**: 
  - Extract @mentions using regex patterns (@username format)
  - Apply case normalization and deduplication
  - Filter out very rare mentions (e.g., < 5 occurrences)
  - Create frequency analysis by condition and time periods
  - Generate stratified analysis (overall, pre/post, by condition, by condition pre/post)
- **Outputs**:
  - Standardized CSV format: `condition | pre/post_election | mention | count | proportion`
  - Pre-sliced CSV files for easy visualization loading
  - Filenames following timestamped pattern: `YYYY-MM-DD_HH:MM:SS`
  - Basic visualization (top mentions by condition side by side)
  - Metadata files with generation tracking

### Non-Functional Requirements:
- **Performance**: Efficient regex processing for large-scale datasets
- **Data Quality**: Consistent mention normalization and deduplication
- **Output Format**: Standardized CSV schema compatible with visualization tools
- **Timestamp Format**: Consistent YYYY-MM-DD_HH:MM:SS formatting across all outputs
- **Integration**: Seamless integration with existing data loading infrastructure
- **Privacy Considerations**: Handle user mentions with appropriate data governance

### Validation & Error Handling:
- **Edge Cases**: Handle malformed mentions, empty posts, special characters
- **Regex Validation**: Ensure mention extraction accuracy and completeness
- **Frequency Validation**: Verify filtering thresholds work correctly
- **CSV Validation**: Ensure schema compliance and data integrity
- **Privacy Validation**: Ensure user mention data is handled appropriately
- **Error Logging**: Log processing failures, data quality issues, and performance metrics

## Success Criteria
- [ ] Mention extraction successfully identifies user mentions from post text
- [ ] Frequency analysis covers all experimental conditions and time periods
- [ ] Standardized CSV output format: `condition | pre/post_election | mention | count | proportion`
- [ ] Pre-sliced CSV files are created for easy visualization loading
- [ ] Filenames follow timestamped pattern: `YYYY-MM-DD_HH:MM:SS`
- [ ] Visualization shows top mentions by condition side by side
- [ ] Module documentation is complete and clear
- [ ] Integration with existing data pipeline works correctly
- [ ] All tests written and passing
- [ ] Code reviewed and merged

## Test Plan
- **`test_mention_extraction`**: Test regex pattern matching → Correct @mention identification
- **`test_mention_normalization`**: Test case normalization and deduplication → Consistent mention formatting
- **`test_mention_frequency_filtering`**: Test rare mention filtering → Low-frequency mentions removed correctly
- **`test_mention_stratified_analysis`**: Test condition and time period analysis → Correct frequency rankings per dimension
- **`test_mention_csv_schema_compliance`**: Validate CSV output format → Schema compliance with all required columns
- **`test_mention_filename_patterns`**: Verify timestamped filename generation → Correct YYYY-MM-DD_HH:MM:SS format
- **`test_mention_visualization_quality`**: Validate PNG output quality → Publication-ready visualizations
- **`test_mention_integration`**: Test data loading from existing topic modeling results → Seamless integration
- **`test_mention_edge_cases`**: Test malformed mentions, empty posts → Graceful error handling
- **`test_mention_privacy_compliance`**: Test user mention data handling → Appropriate privacy safeguards

📁 Test file: `services/calculate_analytics/analyses/content_analysis_2025_09_22/mentions/tests/test_mention_analysis.py`

## Dependencies
- **Depends on**: Project structure setup (content_analysis_2025_09_22 directory creation)
- **Requires**: Existing topic modeling results from `calculate_feed_topic_models_2025_08_18`
- **Requires**: matplotlib/seaborn for visualizations
- **Requires**: Access to all posts used in feeds
- **Requires**: Regex pattern libraries for mention extraction
- **Requires**: Privacy and data governance policies for user mention handling

## Suggested Implementation Plan
1. **Mention Extraction**: Implement `MentionExtractor` class with regex patterns and validation
2. **Normalization**: Create `MentionNormalizer` for case normalization and deduplication
3. **Frequency Analysis**: Implement `MentionFrequencyAnalyzer` with configurable filtering thresholds
4. **Stratified Analysis**: Create `MentionStratifiedAnalyzer` for condition and temporal period analysis
5. **CSV Export**: Implement `MentionCsvExporter` with standardized schema and pre-sliced files
6. **Visualization**: Create `MentionVisualizer` for comparative charts and rankings
7. **Privacy Handling**: Implement `MentionPrivacyHandler` for appropriate data governance
8. **Metadata Tracking**: Implement `MentionMetadataManager` for audit trail and reproducibility
9. **Integration**: Ensure seamless integration with existing project patterns and file organization

## Effort Estimate
- **Estimated effort**: **4 hours**
- **Assumes**: Existing topic modeling infrastructure is stable and accessible
- **Assumes**: Project structure is already set up with proper directory organization
- **Assumes**: Privacy policies are established for user mention handling
- **Assumes**: AI agent implementation (faster than human development time)

## Priority & Impact
- **Priority**: **Medium**
- **Rationale**: Supporting analysis module, can be developed in parallel with other modules

## Acceptance Checklist
- [ ] Mention extraction module implemented
- [ ] Regex pattern matching working correctly
- [ ] Mention normalization and deduplication implemented
- [ ] Frequency analysis by condition and time periods working
- [ ] Standardized CSV export with correct schema
- [ ] Pre-sliced CSV files for visualization created
- [ ] Timestamped filename patterns implemented
- [ ] Basic visualization (top mentions by condition) created
- [ ] Privacy handling for user mentions implemented
- [ ] Module documentation written
- [ ] Integration with existing topic modeling pipeline tested
- [ ] Tests written and passing
- [ ] Code reviewed and merged

## Links & References
- **Spec**: `/projects/content_analysis_2025_09_22/spec.md`
- **Plan**: `/projects/content_analysis_2025_09_22/plan_content_analysis.md`
- **Linear Issue**: https://linear.app/metresearch/issue/MET-54
- **Linear Project**: https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0
- **Existing Topic Modeling**: `services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/`
- **Related tickets**: MET-51 (TF-IDF), MET-52 (NER), MET-53 (Hashtags)
