# 🧾 Spec: Content Analysis Extension for Topic Modeling

## 1. Problem Statement

**Who is affected?** Researchers and analysts working with the existing topic modeling system (`calculate_feed_topic_models_2025_08_18`) need deeper content analysis capabilities beyond basic topic distributions and UMAP visualizations.

**What's the pain point?** The current system provides topic-level analysis but lacks granular keyword analysis (TF-IDF) and entity extraction (NER) that would enable more nuanced understanding of political discourse patterns across experimental conditions and time periods.

**Why now?** The existing topic modeling infrastructure is mature and stable, with the "Political Opinions and Perspectives" topic identified as the largest and most relevant for political analysis. The system has proven capable of handling production-scale datasets and provides a solid foundation for extending analysis capabilities.

**Strategic link?** This extension enables research into how experimental feed conditions affect political discourse patterns, entity salience, and keyword evolution around critical events like elections - providing actionable insights for understanding social media's role in political communication.

## 2. Desired Outcomes & Metrics

**Primary Outcomes:**
- TF-IDF analysis of political content across experimental conditions and pre/post election periods
- Named Entity Recognition across all posts used in feeds with comprehensive entity tracking
- Hashtag analysis across experimental conditions and time periods
- Mention analysis across experimental conditions and time periods
- Sophisticated visualizations suitable for research publication (PNG format)
- Pre-sliced CSV files for easy visualization and paper inclusion

**Success Metrics:**
- **TF-IDF Success**: Extract top keywords for "Political Opinions and Perspectives" topic by condition and pre/post election, with comparative visualizations and pre-sliced CSV outputs
- **NER Success**: Extract and normalize entities (PERSON, ORG, GPE, DATE) from all posts, create structured hash map with date/condition/entity counts, generate top 10 rankings with pre-sliced CSV outputs
- **Hashtag Success**: Extract and analyze hashtags across conditions and time periods with pre-sliced CSV outputs
- **Mention Success**: Extract and analyze @mentions across conditions and time periods with pre-sliced CSV outputs
- **Visualization Success**: Produce publication-quality PNG visualizations including dumbbell plots, heatmaps, small multiples, and trend lines
- **Integration Success**: Seamlessly load data from existing topic modeling results without disrupting current workflows
- **Performance Success**: Process large-scale datasets without significant performance degradation

**Acceptance Criteria:**
- All visualizations export as high-resolution PNG files suitable for academic publication
- TF-IDF analysis covers condition-level and pre/post election comparisons
- NER analysis provides comprehensive entity tracking with structured data export
- Integration follows existing project patterns and file organization
- Documentation supports future maintenance and extension

## 3. In Scope / Out of Scope

**In Scope:**
- TF-IDF analysis focused on "Political Opinions and Perspectives" topic only
- NER analysis on all posts used in feeds (not topic-filtered) with entity normalization
- Hashtag analysis on all posts used in feeds
- Mention analysis on all posts used in feeds
- Analysis by experimental conditions (control, treatment, etc.)
- Analysis by pre/post election periods (election date: 2024-11-05)
- Standardized CSV export with exact schema: `post_id, user_id, topic, tfidf_vector (or vector components as separate columns), top_terms, timestamp_YYYY-MM-DD_HH:MM:SS, condition, period`
- Timestamped output filenames and explicit output metadata files with generation_time, source_topic_model_version, data_query_parameters, and CSV schema version
- Vectorizer reproducibility: fixed random_seed, library and version (scikit-learn X.Y.Z), tokenizer/settings, n_features, ngram_range, hashing/normalization choices, and serialization instructions (pickle or joblib with versioned filename)
- NER entity normalization: case-folding, strip surrounding punctuation, simple lemmatization/canonicalization, map common aliases, record both normalized and original surface forms
- Configurable frequency_threshold parameter to filter low-frequency entities before producing top-N lists
- Extended hash map with normalized keys: `{"<date>": {"<condition>": [{"entity_normalized":"<keyword>","entity_raws":["..."],"count":<count>}]}}`
- Standardized CSV files with consistent column headers: `date, condition, entity_normalized, entity_raws (comma-separated), count, pre_post_flag`
- Filenames follow clear pattern: `top10_overall_YYYY-MM-DD_HH:MM:SS.csv`
- PII handling: redaction/pseudonymization workflows, retention policy (storage duration, deletion/archival procedures), access controls and encryption (least-privilege, audit logging), IRB/consent tracking (consent capture, provenance, approval notes)
- Publication-quality PNG visualizations with professional styling
- Integration with existing topic modeling data and export patterns
- Pre-sliced CSV files with condition/pre-post election columns for easy analysis
- Entity normalization with lookup sets for common political entities
- Text preprocessing for social media content (URLs, mentions, hashtags)
- Top 10 entity/keyword/hashtag/mention rankings across all analysis dimensions

**Out of Scope:**
- TF-IDF analysis on topics other than "Political Opinions and Perspectives"
- Real-time processing or streaming analysis
- Interactive visualizations or web interfaces
- Additional entity types beyond PERSON, ORG, GPE, DATE
- Advanced entity linking or disambiguation beyond basic normalization
- Performance optimization beyond basic efficiency
- Integration with external APIs or services
- User-facing UI components or dashboards
- Statistical significance testing or validation (exploratory analysis only)

## 4. Stakeholders & Dependencies

**Primary Stakeholders:**
- **Researchers**: Need interpretable results for academic analysis and publication
- **Analysts**: Want actionable insights about content patterns and experimental effects
- **Technical Team**: Requires maintainable, well-documented code following existing patterns

**System Dependencies:**
- **Existing Topic Modeling System**: `calculate_feed_topic_models_2025_08_18` for data loading and topic assignments
- **Data Sources**: 
  - `train/trained_models/{mode}/{timestamp}/topics.csv` for topic information
  - Existing inference results for topic assignments
  - Post data from sociopolitical labels and feed mappings
- **Libraries**: scikit-learn (TF-IDF), spaCy (NER), matplotlib/seaborn (visualizations)

**Cross-Functional Dependencies:**
- Access to production data for NER analysis on all posts used in feeds
- Integration with existing export and visualization patterns
- Following established project structure and naming conventions

## 5. Risks / Unknowns

**Technical Risks:**
- **Memory Usage**: TF-IDF matrices may be large for big datasets, requiring efficient processing
- **NER Accuracy**: Entity recognition quality on social media text with casual language and slang
- **Topic Filtering**: Efficient identification and filtering of posts from "Political Opinions and Perspectives" topic
- **Data Loading**: Accessing and parsing existing topic modeling results and post assignments

**Implementation Risks:**
- **Visualization Complexity**: Creating sophisticated visualizations like dumbbell plots and heatmaps may require additional libraries or custom implementations
- **Performance**: NER processing on large datasets (all posts used in feeds) may be computationally intensive
- **Integration**: Ensuring seamless integration with existing pipeline without disrupting current workflows

**Research Risks:**
- **Entity Relevance**: Filtering entities to focus on political/sociopolitical relevance may require manual curation or additional heuristics
- **Temporal Analysis**: Tracking entity/keyword evolution over time may reveal patterns that require additional interpretation

## 6. UX Notes & Accessibility

**User Experience:**
- **Primary Users**: Researchers and analysts working with command-line tools and data files
- **Output Format**: PNG visualizations for research paper publication with clean, professional styling
- **Documentation**: Clear README and code documentation for future maintenance and extension
- **File Organization**: Consistent with existing project structure for familiarity and maintainability

**Accessibility:**
- **Color Accessibility**: Ensure visualizations are readable in grayscale for accessibility compliance
- **Data Accessibility**: Export results in standard formats (CSV, JSON) for further analysis
- **Documentation**: Comprehensive documentation supporting users with varying technical backgrounds

## 7. Technical Notes

**Architecture:**
- **Modular Design**: Separate modules for TF-IDF (`tf_idf/`) and NER (`named_entity_recognition/`) analysis
- **Data Integration**: Leverage existing data structures from topic modeling system
- **Extensibility**: Design to allow easy addition of other text analysis methods in the future

**Implementation Approach:**
- **TF-IDF Module**: Filter posts by topic assignment, apply stratified analysis, compute TF-IDF scores, generate comparative visualizations and pre-sliced CSV files
- **NER Module**: Process all posts with spaCy, normalize entities using lookup sets, create structured hash map, generate rankings and trend visualizations with pre-sliced CSV files
- **Hashtag Module**: Extract hashtags from all posts, apply stratified analysis, generate rankings and visualizations with pre-sliced CSV files
- **Mention Module**: Extract @mentions from all posts, apply stratified analysis, generate rankings and visualizations with pre-sliced CSV files
- **Text Preprocessing**: Handle URLs, mentions, hashtags, stopwords, and lowercase normalization for improved analysis quality
- **Visualization**: Use matplotlib/seaborn for publication-quality charts with consistent styling

**File Structure:**
```
content_analysis_2025_09_22/
├── tf_idf/
│   ├── load_data.py    
│   ├── tf_idf_analysis.py
│   ├── visualization.py
│   └── results/
├── named_entity_recognition/
│   ├── load_data.py    
│   ├── ner_analysis.py
│   ├── visualization.py
│   ├── entity_normalization.py
│   └── results/
├── hashtags/
│   ├── load_data.py    
│   ├── hashtag_analysis.py
│   ├── visualization.py
│   └── results/
├── mentions/
│   ├── load_data.py    
│   ├── mention_analysis.py
│   ├── visualization.py
│   └── results/
├── README.md
└── requirements.in
```

**Technical Constraints:**
- Must work with existing Python 3.12 environment and dependency management
- Follow existing logging and error handling patterns
- Maintain compatibility with current data loading and export systems
- Pin specific spaCy version for reproducibility
- Include entity normalization lookup sets for common political entities
- Generate pre-sliced CSV files with standardized format: `condition | pre/post_election | entity/keyword/hashtag/mention | count | proportion`

## 8. Compliance, Cost, GTM

**Compliance:**
- **Data Privacy**: Processing existing sociopolitical post data within established privacy frameworks
- **Research Ethics**: Analysis focused on aggregate patterns rather than individual user content
- **Publication Standards**: Visualizations must meet academic publication quality standards

**Cost Considerations:**
- **Computational**: NER processing on large datasets may require significant compute resources
- **Storage**: Additional results and visualization files will require storage space
- **Dependencies**: New libraries (spaCy, additional visualization tools) may increase environment complexity

**Go-to-Market:**
- **Research Publication**: Primary deliverable is research-quality visualizations and analysis for academic publication
- **Internal Use**: Analysis results support ongoing research into social media's role in political communication
- **Future Extension**: Foundation for additional content analysis capabilities

## 9. Success Criteria

**Measurable Definition of "Done":**

1. **TF-IDF Analysis Complete:**
   - Successfully extract top keywords for "Political Opinions and Perspectives" topic
   - Generate comparative visualizations by condition and pre/post election
   - Export pre-sliced CSV files with standardized format for easy analysis
   - Export results in structured format following existing patterns

2. **NER Analysis Complete:**
   - Process all posts used in feeds with spaCy entity recognition
   - Normalize entities using lookup sets for common political entities
   - Create structured hash map with date/condition/entity counts
   - Generate top 10 entity rankings across all analysis dimensions
   - Export pre-sliced CSV files with standardized format
   - Produce sophisticated visualizations (dumbbell plots, heatmaps, trend lines)

3. **Hashtag Analysis Complete:**
   - Extract hashtags from all posts used in feeds
   - Apply stratified analysis by condition and pre/post election
   - Generate top 10 hashtag rankings across all analysis dimensions
   - Export pre-sliced CSV files with standardized format
   - Produce comparative visualizations

4. **Mention Analysis Complete:**
   - Extract @mentions from all posts used in feeds
   - Apply stratified analysis by condition and pre/post election
   - Generate top 10 mention rankings across all analysis dimensions
   - Export pre-sliced CSV files with standardized format
   - Produce comparative visualizations

5. **Integration Complete:**
   - Seamlessly load data from existing topic modeling results
   - Follow established project structure and export patterns
   - Maintain performance standards for production datasets
   - Pin specific spaCy version for reproducibility

6. **Documentation Complete:**
   - Comprehensive README with usage instructions
   - Clear code documentation and examples
   - Integration guide for future extensions
   - Entity normalization lookup set documentation

7. **Quality Assurance:**
   - All visualizations export as high-resolution PNG files
   - Results are reproducible and well-documented
   - Code follows existing project standards and patterns
   - Pre-sliced CSV files follow standardized format for paper inclusion

**Sign-off Requirements:**
- Technical review of integration with existing systems
- Validation of visualization quality for research publication
- Confirmation of analysis accuracy and reproducibility
