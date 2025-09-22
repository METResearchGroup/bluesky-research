# Content Analysis Extension - Brain Dump

## Project Overview
Create a new content analysis project (`content_analysis_2025_09_22`) that extends the existing topic modeling analysis system with TF-IDF and Named Entity Recognition (NER) capabilities to provide deeper content analysis beyond basic topic modeling.

## Project Structure
```
content_analysis_2025_09_22/
├── tf_idf/
├── named_entity_recognition/
└── README.md
```

## Initial Thoughts and Requirements

### Core Functionality

#### TF-IDF Analysis
- **Scope**: Focus on posts from the biggest topic "Political Opinions and Perspectives" only
- **Analysis Levels**:
  - By Condition (control, treatment, etc.)
  - By Pre/Post Election periods
- **Integration**: Use existing topic modeling results from `calculate_feed_topic_models_2025_08_18`
- **Data Source**: Load posts filtered by topic assignment from existing results

#### NER Analysis
- **Scope**: All posts used in feeds (not topic-filtered)
- **Entity Types**: People, Organizations, Locations, Dates (focused on political/sociopolitical content)
- **Data Structure**: Hash map format: `{"<date>": {"<condition>": [{"entity": "<keyword>", "count": <count>}]}}`
- **Analysis Levels**:
  - Top 10 entities overall
  - Top 10 entities pre/post election
  - Top 10 entities per condition
  - Top 10 entities per condition pre/post election
- **Output**: Save results in `named_entity_recognition/results/` folder structure

### Technical Context
- **Existing System**: Well-structured topic modeling pipeline with hybrid training/inference
- **Data Pipeline**: Documents deduplicated by stable hash, mapped to topics, stratified by condition/time
- **Current Analyses**: Topic distribution by condition, UMAP embeddings, topic evolution
- **Export System**: Comprehensive CSV/JSON export with timestamped results

### Visualization Requirements

#### TF-IDF Visualizations
- **Top 10 keywords pre/post election**: Side-by-side comparison
- **Top 10 keywords by condition**: Side-by-side comparison  
- **Cross-period analysis**: For top 10 pre-election keywords, show their proportion post-election (and vice versa)
- **Cross-condition analysis**: For top 10 keywords per condition, show their proportion in other conditions

#### NER Visualizations
- **Snapshots**: Ranking + Comparison
  - Pre vs. Post Top 10 (Global): Dumbbell plots with entity on y-axis, dots for pre/post frequencies
  - Top 10 by Condition: Small multiples of bar charts with consistent color scheme
- **Cross-Condition Proportions**: Heatmap matrix with rows=top entities, columns=conditions
- **Trends Over Time**: Entity trajectories as line charts, election day marked as vertical line
- **Synthesis Dashboard**: Combined view of snapshots, heatmaps, and trends

### Key Questions to Explore
1. **Data Integration**: How to efficiently load and filter posts by topic assignment from existing results?
2. **NER Model Selection**: Which NER model performs best on social media political content?
3. **Performance**: How to handle NER processing on large datasets efficiently?
4. **Visualization Libraries**: What libraries to use for dumbbell plots, heatmaps, and trend visualizations?
5. **Entity Filtering**: How to filter entities to focus on political/sociopolitical relevance?
6. **Dependencies**: What additional libraries/requirements are needed beyond existing topic modeling stack?

### Potential Challenges
- **Memory Usage**: TF-IDF matrices can be large for big datasets
- **Entity Recognition Quality**: NER accuracy on social media text (casual language, slang)
- **Topic Filtering**: Efficiently identifying and filtering posts from "Political Opinions and Perspectives" topic
- **Data Loading**: Accessing existing topic modeling results and post assignments
- **NER Performance**: Processing NER on large datasets (all posts used in feeds)
- **Visualization Complexity**: Creating sophisticated visualizations like dumbbell plots and heatmaps

### Stakeholder Considerations
- **Researchers**: Need interpretable results for academic analysis
- **Analysts**: Want actionable insights about content patterns
- **Technical Team**: Requires maintainable, well-documented code
- **Performance**: Must handle production-scale datasets efficiently

### Success Criteria
- **TF-IDF Analysis**:
  - Extract top keywords for "Political Opinions and Perspectives" topic by condition and pre/post election
  - Generate comparative visualizations showing keyword shifts across periods and conditions
- **NER Analysis**:
  - Extract entities (people, organizations, locations, dates) from all posts used in feeds
  - Create structured hash map with date/condition/entity counts
  - Generate top 10 entity rankings across all analysis dimensions
  - Produce sophisticated visualizations (dumbbell plots, heatmaps, trend lines)
- **Integration**:
  - Seamlessly load data from existing topic modeling results
  - Follow existing project structure and export patterns
  - Maintain performance standards for production datasets

### Technical Dependencies
- **TF-IDF**: scikit-learn (already available in topic modeling project)
- **NER**: spaCy with political/news model (need to add)
- **Text Processing**: Additional preprocessing for NER on social media text
- **Visualization**: 
  - Dumbbell plots (matplotlib/seaborn)
  - Heatmaps (seaborn)
  - Small multiples (matplotlib subplots)
  - Trend lines with election day markers
- **Data Integration**: Load existing topic assignments and post mappings

### Research Questions This Enables
- **TF-IDF Questions**:
  - Which keywords are most distinctive for political content in different experimental conditions?
  - How do keyword salience patterns shift from pre-election to post-election periods?
  - Do treatment conditions produce different keyword patterns than control?
- **NER Questions**:
  - Which entities consistently dominate political discourse (top 10), and which "break in" around the election?
  - Do treatment conditions produce different top entities, or just different weights on the same ones?
  - Do entities persist across time windows, or do they rise/fall sharply?
  - Are changes gradual (trend lines) or sudden (election shock)?
- **Comparative Questions**:
  - Are there entities that are localized to one feed condition vs. broadly present?
  - How stable/unstable were entity mentions over time?

### Implementation Considerations
- **Modularity**: Create separate modules for TF-IDF and NER
- **Reusability**: Design to work with existing data structures
- **Extensibility**: Allow easy addition of other text analysis methods
- **Testing**: Need comprehensive testing on both local and production data
- **Documentation**: Clear documentation for new analysis components

## Implementation Details

### Data Flow
1. **Load topic information** from `train/trained_models/{mode}/{timestamp}/topics.csv`
2. **Identify "Political Opinions and Perspectives" topic ID** from the CSV
3. **Load topic assignments** from existing inference results
4. **Filter posts** by topic assignment to "Political Opinions and Perspectives" 
5. **Apply stratified analysis** (condition + pre/post election) to filtered posts
6. **Run TF-IDF** on stratified subsets
7. **Load all posts used in feeds** for NER analysis
8. **Run NER** on all posts with date/condition tracking
9. **Generate PNG visualizations** and export results

### File Structure
```
content_analysis_2025_09_22/
├── tf_idf/
│   ├── tf_idf_analysis.py
│   ├── visualization.py
│   └── results/
├── named_entity_recognition/
│   ├── ner_analysis.py
│   ├── visualization.py
│   └── results/
├── README.md
└── requirements.in
```

## Clarifications Resolved

### Topic ID Resolution
- **Data Source**: Load topic information from `train/trained_models/{mode}/{timestamp}/topics.csv`
- **Topic Identification**: The "Political Opinions and Perspectives" topic is present in the most recent training run
- **Topic Names**: Topics have both original names and OpenAI-generated names in the CSV

### NER Model Selection
- **Model Choice**: Use basic spaCy model (e.g., `en_core_web_sm`)
- **Entity Types**: Focus on PERSON, ORG, GPE (locations), DATE entities
- **Performance**: No specific performance constraints

### Visualization Requirements
- **Export Format**: PNG format for research paper publication
- **Quality**: High-resolution images suitable for academic publication
- **Layout**: Clean, professional styling for research presentation

## Background Context
- Existing system already handles large-scale social media analysis
- Focus on sociopolitical content with experimental conditions  
- Strong foundation in topic modeling and stratified analysis
- Need to extend rather than replace existing functionality
- Election date: 2024-11-05 for pre/post analysis
