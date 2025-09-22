# Content Analysis Extension - TODO Checklist

## Project Setup ✅
- [x] Create Linear project
- [x] Create Linear tickets (MET-51, MET-52, MET-53, MET-54)
- [x] Set up project directory structure
- [x] Move spec and braindump files to project folder
- [x] Create tracking files (plan, todo, logs, etc.)
- [ ] Create GitHub PR for project setup

## Implementation Tasks

### MET-51: TF-IDF Analysis Module
- [ ] Set up project structure and data integration
- [ ] Implement TF-IDF analysis for political topic
- [ ] Create stratified analysis by condition and time periods
- [ ] Implement CSV export with standardized format
- [ ] Create basic visualization (top 10 keywords side by side)
- [ ] Write module documentation

### MET-52: NER Analysis Module
- [ ] Set up spaCy NER pipeline
- [ ] Implement entity normalization and lookup sets
- [ ] Run NER on all posts used in feeds
- [ ] Create stratified analysis (overall, pre/post, by condition)
- [ ] Export results in hash map and CSV formats
- [ ] Create comprehensive visualization suite:
  - [ ] Dumbbell plots for Pre vs. Post Top 10 entities
  - [ ] Small multiples bar charts for Top 10 by Condition
  - [ ] Heatmap matrix for Cross-Condition Proportions
  - [ ] Line charts for Entity trajectories over time
  - [ ] Synthesis dashboard layout
- [ ] Write module documentation

### MET-53: Hashtag Analysis Module
- [ ] Implement hashtag extraction from posts
- [ ] Create frequency analysis by condition and time periods
- [ ] Export pre-sliced CSV files
- [ ] Create basic visualization (top hashtags by condition)
- [ ] Write module documentation

### MET-54: Mention Analysis Module
- [ ] Implement mention extraction from posts
- [ ] Create frequency analysis by condition and time periods
- [ ] Export pre-sliced CSV files
- [ ] Create basic visualization (top mentions by condition)
- [ ] Write module documentation

## Quality Assurance
- [ ] Test all modules with existing data
- [ ] Validate CSV output formats
- [ ] Verify visualization quality for publication
- [ ] Review all documentation
- [ ] Integration testing with existing topic modeling pipeline

## Documentation
- [ ] Update main project README
- [ ] Create usage examples
- [ ] Document data formats and schemas
- [ ] Create troubleshooting guide
