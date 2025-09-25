# MET-52: Implement Named Entity Recognition (NER) analysis with visualizations

## Context & Motivation
This ticket implements comprehensive NER analysis on all posts used in feeds to extract and analyze political/sociopolitical entities (People, Organizations, Locations, Dates) across experimental conditions and time periods. The analysis includes advanced entity normalization, frequency filtering, and sophisticated visualizations to understand how entity salience patterns shift across feed conditions and around critical events like elections.

See `/projects/content_analysis_2025_09_22/spec.md` for complete requirements and `/projects/content_analysis_2025_09_22/plan_content_analysis.md` for implementation strategy.

## Detailed Description & Requirements

### Functional Requirements:
- **Inputs**: All posts used in feeds 
- **Processing**: 
  - Apply spaCy NER pipeline with en_core_web_sm model
  - Implement entity normalization (case-folding, punctuation stripping, lemmatization, alias mapping)
  - Record both normalized and original surface forms
  - Apply configurable frequency threshold filtering
  - Create stratified analysis (overall, pre/post, by condition, by condition pre/post)
- **Outputs**:
  - Extended hash map format: `{"<date>": {"<condition>": [{"entity_normalized":"<keyword>","entity_raws":["..."],"count":<count>}]}}`
  - Standardized CSV files: `date, condition, entity_normalized, entity_raws (comma-separated), count, pre_post_flag`
  - Filenames following pattern: `top10_overall_YYYY-MM-DD_HH:MM:SS.csv`
  - Comprehensive visualization suite (dumbbell plots, bar charts, heatmaps, line charts, synthesis dashboard)

### Non-Functional Requirements:
- **Entity Types**: Focus on PERSON, ORG, GPE (locations), DATE entities for political relevance
- **Performance**: Handle large-scale datasets (all posts used in feeds) efficiently
- **PII Handling**: Implement redaction/pseudonymization workflows, retention policy, access controls, IRB/consent tracking
- **Data Governance**: Document redaction methods, storage locations, downstream data sharing constraints
- **Visualization Quality**: Export high-resolution PNG files suitable for research publication

### Validation & Error Handling:
- **Edge Cases**: Handle malformed text, missing entities, low-frequency entity filtering
- **Entity Validation**: Verify normalization accuracy and alias mapping effectiveness
- **Frequency Filtering**: Ensure configurable thresholds work correctly across different analysis dimensions
- **PII Detection**: Implement safeguards for personally identifiable information
- **Error Logging**: Log processing failures, entity extraction issues, and performance metrics

## Success Criteria
- [ ] spaCy NER pipeline successfully identifies People, Organizations, Locations, and Dates
- [ ] Entity normalization implemented: case-folding, strip surrounding punctuation, simple lemmatization/canonicalization, map common aliases
- [ ] Both normalized and original surface forms recorded in data structure
- [ ] Configurable frequency_threshold parameter implemented for filtering low-frequency entities
- [ ] Extended hash map output format implemented and validated
- [ ] Standardized CSV output format with consistent column headers
- [ ] Filenames follow clear pattern: `top10_overall_YYYY-MM-DD_HH:MM:SS.csv`
- [ ] Comprehensive visualization suite implemented:
  - [ ] Dumbbell plots for Pre vs. Post Top 10 entities
  - [ ] Small multiples bar charts for Top 10 by Condition
  - [ ] Heatmap matrix for Cross-Condition Proportions
  - [ ] Line charts for Entity trajectories over time
  - [ ] Synthesis dashboard layout combining all views
- [ ] PII handling workflows implemented: redaction/pseudonymization, retention policy, access controls, IRB/consent tracking
- [ ] Data governance documentation completed: redaction methods, storage locations, downstream data sharing constraints
- [ ] Module documentation is complete and clear
- [ ] All visualizations exported as PNG for research publication
- [ ] All tests written and passing
- [ ] Code reviewed and merged

## Test Plan
- **`test_ner_entity_extraction`**: Verify spaCy extracts correct entity types → PERSON, ORG, GPE, DATE entities identified
- **`test_ner_entity_normalization`**: Test case-folding, punctuation stripping, alias mapping → Correct normalized entities
- **`test_ner_surface_form_tracking`**: Verify both normalized and raw forms recorded → Complete entity mapping
- **`test_ner_frequency_filtering`**: Test configurable threshold filtering → Low-frequency entities filtered correctly
- **`test_ner_stratified_analysis`**: Test overall, pre/post, by condition analysis → Correct entity rankings per dimension
- **`test_ner_hash_map_format`**: Validate extended hash map structure → Correct nested data format
- **`test_ner_csv_schema_compliance`**: Test CSV output format → Schema compliance with all required columns
- **`test_ner_filename_patterns`**: Verify timestamped filename generation → Correct YYYY-MM-DD_HH:MM:SS format
- **`test_ner_visualization_quality`**: Validate PNG output quality → Publication-ready visualizations
- **`test_ner_pii_handling`**: Test PII detection and handling → Sensitive data properly managed
- **`test_ner_integration`**: Test data loading from existing topic modeling results → Seamless integration
- **`test_ner_edge_cases`**: Test malformed text, empty posts → Graceful error handling

📁 Test file: `services/calculate_analytics/analyses/content_analysis_2025_09_22/named_entity_recognition/tests/test_ner_analysis.py`

## Dependencies
- **Depends on**: Project structure setup (content_analysis_2025_09_22 directory creation)
- **Requires**: spaCy library with en_core_web_sm model
- **Requires**: Existing topic modeling results from `calculate_feed_topic_models_2025_08_18`
- **Requires**: matplotlib/seaborn for visualizations
- **Requires**: Access to all posts used in feeds (not topic-filtered)
- **Requires**: PII handling procedures and data governance policies

## Suggested Implementation Plan
1. **Entity Extraction**: Implement `NerExtractor` class with spaCy pipeline and entity type filtering
2. **Entity Normalization**: Create `EntityNormalizer` with case-folding, punctuation handling, lemmatization, and alias lookup sets
3. **Surface Form Tracking**: Implement `EntityTracker` to record both normalized and original forms
4. **Frequency Filtering**: Create `FrequencyFilter` with configurable thresholds for different analysis dimensions
5. **Stratified Analysis**: Implement `NerStratifiedAnalyzer` for overall, pre/post, by condition analysis
6. **Data Export**: Create `NerDataExporter` with hash map and CSV output formats
7. **Visualization Suite**: Implement `NerVisualizer` with dumbbell plots, bar charts, heatmaps, line charts, and synthesis dashboard
8. **PII Handling**: Create `PiiHandler` for redaction, retention policy, and data governance
9. **Integration**: Ensure seamless integration with existing project patterns and file organization

## Effort Estimate
- **Estimated effort**: **12 hours**
- **Assumes**: spaCy model is available and properly configured
- **Assumes**: Entity normalization lookup sets are predefined
- **Assumes**: PII handling procedures are established
- **Assumes**: AI agent implementation (faster than human development time)

## Priority & Impact
- **Priority**: **High**
- **Rationale**: Core analysis module with complex visualizations, blocks downstream analysis work

## Acceptance Checklist
- [ ] spaCy NER pipeline implemented and tested
- [ ] Entity normalization with lookup sets working correctly
- [ ] Surface form tracking implemented
- [ ] Frequency filtering with configurable thresholds implemented
- [ ] Stratified analysis (overall, pre/post, by condition) working
- [ ] Extended hash map output format implemented
- [ ] Standardized CSV output with correct schema
- [ ] Timestamped filename patterns implemented
- [ ] Comprehensive visualization suite implemented
- [ ] PII handling workflows implemented
- [ ] Data governance documentation completed
- [ ] Module documentation written
- [ ] Integration with existing topic modeling pipeline tested
- [ ] Tests written and passing
- [ ] Code reviewed and merged

## Links & References
- **Spec**: `/projects/content_analysis_2025_09_22/spec.md`
- **Plan**: `/projects/content_analysis_2025_09_22/plan_content_analysis.md`
- **Linear Issue**: https://linear.app/metresearch/issue/MET-52
- **Linear Project**: https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0
- **Existing Topic Modeling**: `services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/`
- **Related tickets**: MET-51 (TF-IDF), MET-53 (Hashtags), MET-54 (Mentions)
