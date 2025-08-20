# 🧾 Spec: Feed-Level Topic Analysis for Bluesky Research

## 1. Problem Statement

**Who is affected?** Research team conducting follow-up analysis for academic publication, focusing on understanding information environments across different feed algorithms.

**What's the pain point?** Need comprehensive exploratory data analysis (EDA) to understand how topic distributions vary across experimental conditions and time periods in Bluesky feed data. Current analysis lacks systematic topic modeling and stratified comparison capabilities.

**Why now?** Follow-up analysis for existing research paper requiring deeper insights into topic patterns across ~1M Bluesky posts over 2-month experimental period.

**Strategic alignment:** Core research objective to understand algorithmic influence on information exposure in social media feeds, contributing to academic understanding of feed algorithm impacts.

## 2. Desired Outcomes & Metrics

**Primary Success Criteria:**
- **Interpretable global topic model** trained on 1M Bluesky posts using BERTopic + Sentence Transformers
- **Multi-level stratified analysis** showing topic distributions across:
  - Overall corpus baseline
  - Three experimental conditions
  - Temporal periods (weekly + pre/post November 5, 2024 election)
  - Condition × Time interactions
- **Topic evolution tracking** demonstrating how topics change over time
- **Topic co-occurrence analysis** within feeds

**Deliverables Quality Metrics:**
- **Statistical tables** ready for research publication
- **Publication-quality figures** (PNG format) with proper visualization standards
- **Reproducible analysis** with consistent random seed results
- **Optional interactive dashboard** (Streamlit) for deeper exploration

**Success Definition:** Ability to answer "Do different feed algorithms expose users to different topic distributions?" through comprehensive descriptive statistics and visualizations.

## 3. In Scope / Out of Scope

### ✅ **In Scope**
- **Generic BERTopic pipeline** accepting pre-cleaned text DataFrames
- **Feed-specific analysis code** for Bluesky data loading and stratification
- **Topic evolution and co-occurrence analysis**
- **Multi-level descriptive statistical analysis** (overall → condition → time → condition×time)
- **Publication-ready visualizations** and statistical summaries
- **Jupyter notebook-based analysis workflow**
- **YAML-configurable BERTopic parameters**
- **Reproducible pipeline with random seed control**
- **Optional Streamlit dashboard** (nice-to-have, not critical path)

### ❌ **Out of Scope**
- **Individual user behavior analysis** (focus on feed-level aggregations)
- **Real-time or streaming analysis** (one-time historical analysis)
- **Integration with production systems** (standalone research tool)
- **Advanced network analysis** or clustering beyond topic modeling
- **Inferential statistical testing** (descriptive EDA focus)
- **Data cleaning or preprocessing** (assumes pre-cleaned inputs)
- **Intermediate artifact persistence** (direct processing pipeline)

## 4. Stakeholders & Dependencies

### **Primary Stakeholders**
- **Mark (Lead Researcher)** - Project owner, analysis execution, publication authoring
- **Research collaborators/co-authors** - Review findings and contribute to publication
- **Academic research community** - End consumers of published results

### **Technical Dependencies**
- **User-provided data loading** - Mark handles all parquet data lake connections
- **GPU resources** - Available for BERTopic training acceleration
- **Northwestern Linear team** - Project tracking and coordination
- **Python environment** - conda env "bluesky-research" with uv package management

### **External Dependencies**
- **Bluesky data access** - Already secured, 1M posts over 2 months
- **Research publication timeline** - TBD based on broader research schedule

## 5. Risks / Unknowns

### **Technical Risks**
- **BERTopic scalability** with 1M posts (mitigated by GPU access)
- **Memory requirements** for large-scale topic modeling (monitor during development)
- **Topic interpretability** across diverse Bluesky content (validate with sample outputs)
- **Model training time** even with GPU optimization

### **Methodological Risks**
- **Topic quality and coherence** with automatic topic number selection
- **Topic stability** across different time periods within 2-month window
- **Meaningful stratification** across three experimental conditions

### **Project Risks**
- **Scope creep** into advanced network analysis (explicitly out of scope)
- **Streamlit feature creep** (marked as optional nice-to-have)

## 6. UX Notes & Accessibility

### **Primary User Journey (Researcher Workflow)**
1. **Data Loading** → Python script loads and prepares Bluesky feed data
2. **Model Training** → BERTopic pipeline processes 1M posts with GPU acceleration
3. **Analysis Exploration** → Jupyter notebooks for stratified topic analysis
4. **Publication Generation** → Export statistical tables and PNG figures
5. **Optional Exploration** → Streamlit dashboard for interactive analysis

### **Interface Design**
- **Core Infrastructure**: Python scripts for data loading and model training
- **Analysis Interface**: Jupyter notebooks for flexible exploratory analysis
- **Configuration**: YAML files for all BERTopic parameters and settings
- **Output Format**: PNG figures, statistical tables, optional interactive dashboard

### **Research Workflow Optimization**
- **Reproducibility**: Random seed control for consistent results
- **Modularity**: Generic BERTopic pipeline + feed-specific analysis separation
- **Flexibility**: Configurable for reuse with different datasets

## 7. Technical Notes

### **Architecture Overview**
```
ml_tooling/topic_modeling/          # Generic reusable components
├── bertopic_wrapper.py             # BERTopic model interface
├── topic_analyzer.py               # Generic topic analysis utilities
└── visualization.py                # Reusable plotting functions

services/calculate_analytics/2025-08-18_calculate_feed_topic_models/
├── config/
│   └── feed_analysis_config.yaml   # YAML configuration (BERTopic params, embedding model, coherence metrics)
├── src/
│   ├── feed_data_loader.py         # Bluesky-specific data loading (user implements)
│   ├── feed_preprocessor.py        # Feed-specific preprocessing
│   ├── feed_aggregator.py          # Feed-level topic distributions
│   └── feed_analyzer.py            # Feed-specific statistical analysis
├── notebooks/
│   └── feed_topic_analysis.ipynb   # Main analysis workflow
├── streamlit_app/                  # Optional interactive dashboard
└── scripts/
    └── run_feed_analysis.py        # Orchestration script
```

### **Core Technical Specifications**
- **Topic Modeling**: BERTopic with Sentence Transformers embeddings
- **Embedding Model**: Configurable Sentence Transformer model (default: 'all-MiniLM-L6-v2' for efficiency, 'all-mpnet-base-v2' for quality)
- **Input Interface**: Pre-cleaned text DataFrame (no text preprocessing)
- **Model Persistence**: Standard Python serialization (pickle/joblib)
- **Configuration**: YAML-based parameter management for all BERTopic and embedding parameters
- **Topic Quality Monitoring**: Automated coherence metrics (c_v, c_npmi) tracking during model training
- **Compute**: GPU-optimized for 1M post processing
- **Temporal Boundary**: November 5, 2024 (election date) for pre/post analysis

### **Implementation Constraints**
- **Generic Pipeline**: Text-agnostic, accepts any DataFrame with text column
- **No Intermediate Storage**: Direct processing without artifact persistence
- **Reproducibility**: Random seed configuration for consistent results

## 8. Compliance, Cost, GTM

### **Research Ethics & Publication**
- **Data Usage**: Existing approved research data, no additional permissions needed
- **Publication Standards**: Academic research publication quality requirements
- **Open Science**: Code and methodology should be reproducible for peer review

### **Infrastructure Costs**
- **GPU Usage**: Limited to model training phase, one-time analysis cost
- **Storage**: Minimal, no persistent intermediate artifacts
- **Dependencies**: Standard open-source Python packages via uv

### **Timeline Considerations**
- **Research Publication Cycle**: Flexible timeline based on broader research project needs
- **One-time Analysis**: No ongoing maintenance or operational costs

## 9. Success Criteria

### **Technical Success**
- [ ] **BERTopic model successfully trained** on 1M Bluesky posts with interpretable topics and quality metrics
- [ ] **Topic quality validation** shows coherence scores (c_v, c_npmi) meeting research standards
- [ ] **Topic assignments completed** for all posts with feed metadata preserved
- [ ] **Multi-level stratified analysis** generates meaningful topic distributions across:
  - Overall corpus
  - Three experimental conditions  
  - Weekly temporal periods
  - Pre/post November 5, 2024 election
  - Condition × Time interactions
- [ ] **Topic evolution analysis** tracks topic changes over 2-month period
- [ ] **Topic co-occurrence patterns** identified within feeds

### **Research Deliverables**
- [ ] **Statistical tables** ready for academic publication
- [ ] **Publication-quality figures** (PNG format) showing key findings
- [ ] **Reproducible analysis pipeline** with consistent random seed results
- [ ] **Jupyter notebook documentation** of complete analysis workflow

### **Optional Enhancements**
- [ ] **Interactive Streamlit dashboard** with filtering and drill-down capabilities
- [ ] **Generic pipeline** validated for reuse with other text datasets

### **Quality Validation**
- [ ] **Topic interpretability** confirmed through manual inspection of sample topics
- [ ] **Topic quality metrics** show acceptable coherence scores (c_v > 0.4, c_npmi > 0.1)
- [ ] **Analysis robustness** validated through sensitivity checks
- [ ] **Statistical validity** of descriptive comparisons across stratifications
- [ ] **Code quality** meets research reproducibility standards

**Final Success Definition:** Research team can confidently answer "How do topic distributions vary across feed algorithms, conditions, and time?" with publication-ready statistical evidence and visualizations.
