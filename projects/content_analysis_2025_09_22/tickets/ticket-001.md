# MET-51: Implement TF-IDF analysis module

## Context & Motivation
This ticket implements TF-IDF analysis for the "Political Opinions and Perspectives" topic to enable keyword extraction and comparative analysis across experimental conditions and pre/post election periods. This analysis is essential for understanding how political discourse patterns shift across different feed conditions and temporal periods, providing actionable insights for research into social media's role in political communication.

See `/projects/content_analysis_2025_09_22/spec.md` for complete requirements and `/projects/content_analysis_2025_09_22/plan_content_analysis.md` for implementation strategy.

## Detailed Description & Requirements

### Functional Requirements:
- **Inputs**: Posts filtered by "Political Opinions and Perspectives" topic assignment from existing topic modeling results
- **Processing**: Apply TF-IDF vectorization with stratified analysis by condition (control, treatment, etc.) and pre/post election periods
- **Outputs**: 
  - Standardized CSV files with schema: `post_id, user_id, topic, tfidf_vector (or vector components as separate columns), top_terms, timestamp_YYYY-MM-DD_HH:MM:SS, condition, period`
  - Top 10 keywords rankings for each analysis dimension
  - Comparative visualizations (side-by-side pre/post election keywords)
  - Metadata files with generation_time, source_topic_model_version, data_query_parameters, CSV schema version

### Non-Functional Requirements:
- **Reproducibility**: Fixed random_seed, library version pinning (scikit-learn X.Y.Z), serialization with versioned filenames
- **Performance**: Handle large-scale datasets without significant performance degradation
- **Data Integrity**: Ensure consistent timestamp formatting (YYYY-MM-DD_HH:MM:SS) and CSV schema compliance
- **Integration**: Seamlessly load data from existing topic modeling infrastructure without disrupting current workflows

### Validation & Error Handling:
- **Edge Cases**: Handle empty topic assignments, malformed post data, missing condition mappings
- **Data Validation**: Verify topic filtering accuracy and CSV schema compliance
- **Error Logging**: Log processing failures, data quality issues, and performance metrics
- **Fallback Behavior**: Graceful handling of missing or corrupted topic modeling results

## Success Criteria
- [ ] TF-IDF analysis successfully extracts keywords from political topic posts
- [ ] Results are stratified by condition and pre/post election periods
- [ ] Standardized CSV output format implemented and validated
- [ ] Timestamped output filenames following YYYY-MM-DD_HH:MM:SS pattern
- [ ] Explicit output metadata file with complete audit trail
- [ ] Vectorizer reproducibility features implemented (random seed, version pinning, serialization)
- [ ] Visualization shows top 10 keywords pre/post election side by side
- [ ] Module documentation is complete and clear
- [ ] Integration with existing topic modeling data pipeline works correctly
- [ ] All tests written and passing
- [ ] Code reviewed and merged

## Test Plan
- **`test_tfidf_political_topic_filtering`**: Verify correct filtering of posts by "Political Opinions and Perspectives" topic → Accurate post subset
- **`test_tfidf_stratified_analysis`**: Test condition and pre/post election stratification → Correct keyword rankings per dimension
- **`test_tfidf_csv_schema_compliance`**: Validate CSV output format → Schema compliance with all required columns
- **`test_tfidf_reproducibility`**: Test fixed random seed and serialization → Identical results across runs
- **`test_tfidf_metadata_generation`**: Verify metadata file creation → Complete audit trail with all required fields
- **`test_tfidf_visualization_quality`**: Validate PNG output quality → Publication-ready visualizations
- **`test_tfidf_integration`**: Test data loading from existing topic modeling results → Seamless integration
- **`test_tfidf_edge_cases`**: Test empty datasets, malformed data → Graceful error handling

📁 Test file: `services/calculate_analytics/analyses/content_analysis_2025_09_22/tf_idf/tests/test_tfidf_analysis.py`

## Dependencies
- **Depends on**: Project structure setup (content_analysis_2025_09_22 directory creation)
- **Requires**: Existing topic modeling results from `calculate_feed_topic_models_2025_08_18`
- **Requires**: scikit-learn library with version pinning
- **Requires**: matplotlib/seaborn for visualizations
- **Requires**: Access to "Political Opinions and Perspectives" topic assignments

## File Structure

```
tf_idf/
├── load_data.py                    # DataLoader class for loading topic modeling results
├── model.py                        # TfidfModel class with vectorizer and analysis logic
├── train.py                        # Training script and pipeline orchestration
├── visualizations.py               # Visualization generation and export
├── tests/                          # Unit tests
│   └── test_tfidf_analysis.py
├── results/                        # All analysis outputs
│   ├── training/                   # Training artifacts and metadata
│   │   └── <timestamp_YYYY_MM_DD_HH_MM_SS>/
│   │       ├── metadata.json       # Training run metadata (config, versions, params)
│   │       ├── vectorizer.pkl      # Serialized TfidfVectorizer for reproducibility
│   │       ├── feature_names.json  # Vocabulary and feature mapping
│   │       ├── training_log.txt    # Processing logs and performance metrics
│   │       └── data_summary.json   # Dataset statistics and filtering results
│   └── visualization/              # Generated visualizations
│       └── <timestamp_YYYY_MM_DD_HH_MM_SS>/
│           ├── metadata.json       # Visualization run metadata
│           ├── condition/          # Analysis by experimental condition
│           │   ├── top_keywords_by_condition.png
│           │   ├── tfidf_scores_by_condition.csv
│           │   └── condition_comparison_heatmap.png
│           ├── election_date/      # Pre/post election analysis
│           │   ├── pre_vs_post_keywords.png
│           │   ├── election_timeline_keywords.csv
│           │   └── temporal_keyword_evolution.png
│           ├── overall/            # Overall analysis results
│           │   ├── top_10_keywords_overall.png
│           │   ├── overall_tfidf_scores.csv
│           │   └── keyword_frequency_distribution.png
│           └── combined/           # Cross-dimensional analysis
│               ├── stratified_keyword_analysis.png
│               ├── condition_election_cross_analysis.csv
│               └── comprehensive_keyword_rankings.csv
└── README.md                       # Module documentation and usage
```

## Suggested Implementation Plan
1. **Data Integration**: Implement `DataLoader` class in `load_data.py` to leverage existing topic modeling infrastructure
2. **Topic Filtering**: Create efficient filtering by "Political Opinions and Perspectives" topic assignment
3. **TF-IDF Pipeline**: Implement `TfidfModel` class in `model.py` with scikit-learn's TfidfVectorizer, fixed random seed, and configuration management
4. **Training Orchestration**: Create `train.py` script for pipeline execution and result management
5. **Stratified Analysis**: Implement condition and temporal period analysis within the model class
6. **CSV Export**: Create standardized schema export with timestamp formatting in results directories
7. **Visualization**: Implement `TfidfVisualizer` in `visualizations.py` for comparative charts using matplotlib/seaborn
8. **Metadata Tracking**: Create comprehensive metadata files for audit trail and reproducibility tracking
9. **Integration**: Ensure seamless integration with existing project patterns and file organization

## Effort Estimate
- **Estimated effort**: **6 hours**
- **Assumes**: Existing topic modeling infrastructure is stable and accessible
- **Assumes**: Project structure is already set up with proper directory organization
- **Assumes**: AI agent implementation (faster than human development time)

## Priority & Impact
- **Priority**: **High**
- **Rationale**: Foundation module for content analysis extension, blocks downstream NER and visualization work

## Acceptance Checklist
- [ ] TF-IDF analysis module implemented
- [ ] Topic filtering for "Political Opinions and Perspectives" working correctly
- [ ] Stratified analysis by condition and pre/post election implemented
- [ ] Standardized CSV export with correct schema
- [ ] Timestamped output filenames implemented
- [ ] Metadata file generation working
- [ ] Vectorizer reproducibility features implemented
- [ ] Basic visualization (top 10 keywords side by side) created
- [ ] Module documentation written
- [ ] Integration with existing topic modeling pipeline tested
- [ ] Tests written and passing
- [ ] Code reviewed and merged

## Links & References
- **Spec**: `/projects/content_analysis_2025_09_22/spec.md`
- **Plan**: `/projects/content_analysis_2025_09_22/plan_content_analysis.md`
- **Linear Issue**: https://linear.app/metresearch/issue/MET-51
- **Linear Project**: https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0
- **Existing Topic Modeling**: `services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18/`
- **Related tickets**: MET-52 (NER), MET-53 (Hashtags), MET-54 (Mentions)
