# TODO: Feed-Level Topic Analysis

## Project Status: In Progress
**Linear Project**: [Feed-Level Topic Analysis for Bluesky Research](https://linear.app/metresearch/project/feed-level-topic-analysis-for-bluesky-research-6b2e884b3dd9)

## Core Tasks (Sequential Execution Required)

### 🔄 Phase 1: Core Infrastructure
- [ ] **MET-34**: Implement core BERTopic pipeline with YAML configuration
  - [ ] BERTopic wrapper class implemented with YAML configuration
  - [ ] Sentence Transformer model selection configurable  
  - [ ] Coherence metrics (c_v, c_npmi) monitoring implemented
  - [ ] GPU optimization and memory management included
  - [ ] Comprehensive test suite written and passing
  - [ ] Random seed reproducibility validated
  - [ ] Code reviewed and documentation complete

### 🔄 Phase 2: Analysis Engine  
- [ ] **MET-35**: Build feed-specific analysis and stratification code
  - [ ] Feed-level topic aggregation implemented
  - [ ] Multi-level stratified analysis (condition, time, condition×time) working
  - [ ] Topic evolution tracking across 2-month period
  - [ ] Topic co-occurrence analysis within feeds
  - [ ] Weekly temporal analysis and election boundary comparison
  - [ ] Jupyter notebook workflow documented and tested
  - [ ] Statistical validation framework implemented
  - [ ] Comprehensive test suite passing

### 🔄 Phase 3: Publication Materials
- [ ] **MET-36**: Generate publication-ready tables and figures
  - [ ] Statistical tables generated for all stratification levels
  - [ ] Publication-quality figures created (topic evolution, distributions, comparisons)
  - [ ] PNG format with appropriate DPI for publication
  - [ ] CSV and LaTeX table formats available
  - [ ] Accessibility standards met (color schemes, legends)
  - [ ] Automated generation pipeline working
  - [ ] Professional styling consistent across all outputs
  - [ ] Comprehensive test suite validating figure quality

### 🔄 Phase 4: Optional Enhancement
- [ ] **MET-37**: Create interactive Streamlit dashboard (Optional)
  - [ ] Streamlit dashboard framework implemented
  - [ ] Multi-level filtering interface (conditions, time, topics) working
  - [ ] Interactive visualizations with real-time updates
  - [ ] Drill-down capabilities for topic exploration
  - [ ] Representative post viewing functionality
  - [ ] Performance optimized for large datasets
  - [ ] Error handling and user input validation
  - [ ] Dashboard deployed and accessible

## Project Milestones
- [ ] **Week 1**: Core BERTopic pipeline complete
- [ ] **Week 2**: Feed analysis and stratification complete
- [ ] **Week 3**: Publication materials generated
- [ ] **Week 4**: Optional dashboard deployed

## Linear Issue Links
- [MET-34: Core BERTopic Pipeline](https://linear.app/metresearch/issue/MET-34)
- [MET-35: Feed Analysis & Stratification](https://linear.app/metresearch/issue/MET-35)  
- [MET-36: Publication Materials](https://linear.app/metresearch/issue/MET-36)
- [MET-37: Interactive Dashboard](https://linear.app/metresearch/issue/MET-37)
