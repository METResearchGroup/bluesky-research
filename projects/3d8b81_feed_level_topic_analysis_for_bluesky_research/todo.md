# Project Todo: Feed-Level Topic Analysis for Bluesky Research

## Phase 1: Core Infrastructure ✅ COMPLETED

- [x] **MET-34**: Core BERTopic pipeline with YAML configuration and text preprocessing ✅ COMPLETED
  - Generic, reusable BERTopic wrapper with quality monitoring
  - GPU optimization and comprehensive testing
  - YAML configuration for all parameters

## Phase 2: Data Loading Infrastructure 🔄 IN PROGRESS

- [ ] **MET-44**: Implement Local Data Loader for Topic Modeling Pipeline 🔄 IN PROGRESS
  - Abstract DataLoader interface with LocalDataLoader implementation
  - Integration with existing `load_data_from_local_storage` function
  - Configuration-driven data loader selection
  - End-to-end integration with BERTopic pipeline
  - **Status**: Implementation plan approved, starting development
  - **Estimated Effort**: 4 hours
  - **Dependencies**: ✅ MET-34 (Core BERTopic pipeline) COMPLETED

- [ ] **MET-45**: Implement Production Data Loader for Topic Modeling Pipeline
  - ProductionDataLoader implementation following DataLoader interface
  - Production data source integration with robust error handling
  - Performance optimization for production-scale data loading
  - Production configuration management
  - **Dependencies**: MET-44 (Local Data Loader)

## Phase 3: Analysis & Stratification

- [ ] **MET-46**: Implement Feed-Specific Analysis and Stratification for Topic Models
  - Multi-level stratified analysis (overall → condition → time → condition×time)
  - Topic evolution tracking across 2-month experimental period
  - Topic co-occurrence patterns within feeds
  - Weekly temporal analysis and pre/post election comparison
  - Publication-ready visualizations and statistical summaries
  - **Dependencies**: MET-44, MET-45 (Data Loading Infrastructure)

## Phase 4: Publication & Documentation

- [ ] **Publication Materials Generation**
  - Statistical tables and publication-quality figures
  - Topic evolution and co-occurrence analysis reports
  - Reproducible analysis pipeline documentation
  - **Dependencies**: MET-46 (Feed-Specific Analysis)

- [ ] **Final Documentation & Handoff**
  - Complete pipeline documentation
  - Usage examples and best practices
  - Performance benchmarks and optimization notes
  - **Dependencies**: All previous phases

## Implementation Notes

### MET-44 Implementation Plan (Current)
- **Package Structure**: `services/calculate_analytics/2025-08-18_calculate_feed_topic_models/src/data_loading/`
- **Core Components**: Abstract DataLoader interface, LocalDataLoader implementation, configuration management
- **Integration**: Seamless integration with existing BERTopicWrapper from MET-34
- **Testing**: Comprehensive test suite covering all functionality
- **Deliverables**: Working data loading infrastructure with demo notebook

### Next Steps After MET-44
1. **MET-45**: Production data loader implementation
2. **MET-46**: Feed-specific analysis and stratification
3. **Publication Materials**: Statistical analysis and visualizations

## Current Status: Week 2 - Data Loading Infrastructure
**Focus**: Building the foundational data loading infrastructure that will enable the complete topic modeling pipeline.
