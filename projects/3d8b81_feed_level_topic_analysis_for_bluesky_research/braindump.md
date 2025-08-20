# Feed-Level Topic Analysis Project - Brain Dump

## Project Overview
Analytical project for follow-up analysis on a research paper, focusing on feed-level topic modeling using BERTopic to understand differences in information environments across experimental conditions.

## Core Concept
**High-level flow**: `load tweets → fit BERT Topic model → splice + analyze`

**Unit of Analysis**: Individual tweets as atomic units, with feeds as collections of tweets containing metadata (feed_id, user_id, condition, timestamp).

## Detailed Analysis Plan

### Step 1: Define Unit of Analysis
- **Atomic unit**: Individual tweets
- **Collections**: Feeds (treatment vs control, temporal metadata)
- **Metadata**: feed_id, user_id, condition, timestamp

### Step 2: Consolidate the Corpus
- Collect all tweets shown in feeds across all users, conditions, and time
- Preserve metadata alongside each tweet
- Structure: tweet_text + [feed_id, user_id, condition, timestamp]

### Step 3: Train Global Topic Model
- Run BERTopic on entire tweet corpus (not separated by condition/time)
- Ensures shared "topic space" across dataset
- Output: topic assignments for each tweet (including noise labels)

### Step 4: Reattach Metadata
- Merge topic labels back to original tweet metadata
- Final structure: [tweet_text, topic_id, feed_id, user_id, condition, timestamp]

### Step 5: Aggregate to Feed Level
- Group tweets by feed_id
- Build topic distributions (counts/proportions) for each feed
- Output: feed-level topic distribution vectors

### Step 6: Data Slicing for Analysis
- **Condition-level**: Compare topic distributions across treatment vs control
- **Time-level**: Compare distributions over time slices (weekly, etc.)
- **Condition × Time**: Track topic prevalence differences across conditions over time
- **Event-based**: Pre vs post periods around specific events (e.g., elections)

### Step 7: Statistical Comparisons
- Chi-square tests for topic distribution differences
- Permutation tests for robustness
- KL divergence for distribution comparisons
- Assess stability vs event-driven differences

### Step 8: Visualization
- Topic prevalence curves over time
- Stacked bar charts/heatmaps by condition
- Sankey diagrams for topic shifts across time
- Alluvial plots for temporal transitions

### Step 9: Sensitivity Checks
- Run BERTopic separately within each condition as comparison
- Check for condition-specific patterns missed by global model
- Augment global analysis with condition-specific insights if needed

### Step 10: Interpret and Report
- Differences in topic distributions = differences in information environment
- Connect to experimental conditions, user exposure, outcomes
- Generate research paper-ready results

## Proposed Technical Architecture

### Core ML Tooling (Reusable Components)
```
ml_tooling/topic_modeling/
├── __init__.py
├── bertopic_wrapper.py       # BERTopic model interface
├── topic_analyzer.py         # Generic topic analysis utilities  
├── visualization.py          # Reusable plotting functions
└── tests/
```

### Analysis-Specific Implementation
```
services/calculate_analytics/2025-08-18_calculate_feed_topic_models/
├── README.md
├── requirements.in
├── config/
│   └── feed_analysis_config.py
├── src/
│   ├── __init__.py
│   ├── feed_data_loader.py   # Feed-specific data loading (user implements)
│   ├── feed_preprocessor.py  # Feed-specific preprocessing  
│   ├── feed_aggregator.py    # Feed-level topic distributions
│   └── feed_analyzer.py      # Feed-specific statistical analysis
├── notebooks/
│   └── feed_topic_analysis.ipynb
├── streamlit_app/
│   ├── app.py               # Interactive topic model visualization
│   ├── components/          # Streamlit UI components
│   └── utils/              # Visualization utilities
└── scripts/
    └── run_feed_analysis.py
```

## Technical Environment
- **Python Environment**: conda env "bluesky-research"
- **Package Manager**: uv for dependencies
- **Team**: Northwestern (Linear)
- **Data Format**: .parquet files (following project conventions)
- **Orchestration**: One-time analysis, no Prefect needed
- **Storage**: No Postgres, prefer SQLite if database needed

## Confirmed Project Details

### Data Source & Scale
- [x] **Data Source**: Bluesky posts from existing research
- [x] **Volume**: ~1M posts/tweets
- [x] **Time Range**: 2 months of data
- [x] **Data Access**: Raw feed data already available
- [x] **Data Format**: User's existing .parquet data lake
- [x] **Data Loading**: User will handle DataLoader implementation (to be created)

### Experimental Context  
- [x] **Conditions**: Three experimental conditions (specifics TBD by user)
- [ ] What was the original experiment this follows up on?
- [ ] Are there specific events to consider (elections, etc.)?
- [ ] How many users/feeds in each condition?

### Technical Requirements
- [x] **Prefect Integration**: No - one-time analysis, no orchestration needed
- [x] **Data Connections**: User handles all parquet data lake connections
- [x] **Statistical Rigor**: High - research publication quality required
- [ ] Compute requirements (GPU, memory constraints)?
- [ ] Performance/scalability requirements for 1M posts?

### Research Objectives
- [x] **Primary Research Question**: Do different feeds show different topics when stratified by:
  - Condition (3 experimental conditions)
  - Time (temporal analysis across 2 months)
  - Condition × Time (interaction effects)
- [x] **Expected Deliverables**:
  - Statistical tables for research paper
  - Figures for research paper publication
  - Interactive visualization (Streamlit app) of topic models across all stratifications
- [ ] Target publication venue or format?

### Technical Resources
- [x] **Compute Resources**: GPU access available for BERTopic training
- [x] **Interactive Visualization**: Streamlit app for topic model exploration

### Timeline & Dependencies
- [ ] Research paper deadlines?
- [ ] Dependencies on other team members?
- [ ] Coordination with other analysis projects?

## Potential Risks & Considerations

### Technical Risks
- [ ] BERTopic scalability with 1M Bluesky posts corpus
- [ ] Memory requirements for global topic modeling on 1M posts
- [ ] Topic coherence across three experimental conditions
- [ ] Computational time for large-scale analysis (2 months of data)
- [ ] GPU/CPU requirements for BERTopic on this scale

### Methodological Risks
- [ ] Global vs condition-specific topic models trade-offs
- [ ] Topic stability across 2-month time periods
- [ ] Statistical power for three-way condition comparisons
- [ ] Multiple comparison corrections needed for research publication
- [ ] Topic interpretability with 1M diverse Bluesky posts

### Data Quality Risks
- [ ] Missing or incomplete feed metadata across 2 months
- [ ] Temporal gaps in Bluesky data collection
- [ ] Unbalanced conditions (user counts, post volumes across 3 conditions)
- [ ] Bluesky-specific text preprocessing challenges (handles, links, etc.)

## Integration Points

### Existing Project Infrastructure
- [ ] Connection to current data pipelines
- [ ] Reuse of existing preprocessing utilities
- [ ] Integration with monitoring/alerting systems
- [ ] Coordination with other analytical projects

### Deliverable Integration
- [ ] Research paper figure generation pipeline
- [ ] Statistical result export formats
- [ ] Reproducibility requirements
- [ ] Code review and documentation standards

## Success Criteria (Initial)
- [ ] Successful BERTopic model training on 1M Bluesky posts using GPU
- [ ] Clear topic assignments for all posts with metadata (condition, time)
- [ ] Feed-level topic distributions calculated for all stratifications
- [ ] Statistical comparisons across:
  - Conditions (3-way comparison)
  - Time periods (temporal trends)
  - Condition × Time interactions
- [ ] Research publication-ready statistical tables generated
- [ ] High-quality figures for research paper created
- [ ] Interactive Streamlit app for topic model exploration deployed
- [ ] Sensitivity analysis validates global vs condition-specific approach
- [ ] Reproducible analysis pipeline with GPU optimization

## Next Steps
1. Address clarification questions above
2. Finalize scope and requirements based on answers
3. Create detailed specification document
4. Set up Linear project and tickets
5. Begin implementation planning
