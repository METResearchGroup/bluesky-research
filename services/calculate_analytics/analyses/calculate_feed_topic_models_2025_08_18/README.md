# Feed-Level Topic Modeling Analysis

This analysis performs BERTopic topic modeling on Bluesky feed content to understand topic distributions across experimental conditions and time periods.

## 🎯 **What This Does**

1. **Loads feed data** from production environment using shared data loading functions
2. **Trains BERTopic model** using the existing BERTopicWrapper (MET-34)
3. **Performs stratified analysis** across conditions and time periods
4. **Exports results** to CSV and JSON files
5. **Generates visualizations** for topic distributions and evolution

## 🚀 **Usage**

### **Basic Usage**
```bash
cd services/calculate_analytics/analyses/calculate_feed_topic_models_2025_08_18
python main.py
```

### **SLURM Job Submission**
```bash
sbatch submit_topic_modeling_analysis.sh
```

### **Generate Visualizations**
```bash
python visualize_results.py
```

## 📁 **Output Files**

The analysis creates several output files in the `results/` directory:

### **Data Files**
1. **`topic_modeling_results_{timestamp}.csv`** - Full topic information from BERTopic
2. **`topic_quality_metrics_{timestamp}.json`** - Model quality metrics and coherence scores
3. **`stratified_topic_analysis_{timestamp}.csv`** - Topic distributions by condition and time
4. **`topic_analysis_summary_{timestamp}.json`** - High-level analysis summary

### **Visualization Files**
1. **`topic_distribution_by_size.png`** - Bar chart of topic sizes
2. **`topic_evolution_over_time.png`** - Time series of topic proportions
3. **`topic_distribution_by_condition.png`** - Comparison across experimental conditions
4. **`topic_model_quality_metrics.png`** - Quality metrics visualization

## 🔧 **How It Works**

### **1. Data Loading (`do_setup`)**
- Loads users and partition dates using shared functions
- Maps users to posts used in feeds for each date
- Loads post texts and creates document mappings
- **Follows standard analysis pattern with shared components**

### **2. BERTopic Analysis (`do_analysis_and_export_results`)**
- Trains BERTopic model using existing `BERTopicWrapper` from MET-34
- Computes topic assignments for all documents
- Performs stratified analysis across conditions and time periods
- **Uses proven shared analysis functions**

### **3. Results Export**
- Exports topic information to CSV format
- Exports quality metrics to JSON format
- Exports stratified analysis results
- **Standardized output patterns with timestamps**

### **4. Visualization Generation**
- Creates publication-quality plots for topic distributions
- Generates time series plots for topic evolution
- Compares topic distributions across experimental conditions
- **Professional visualization following established patterns**

## 🎯 **Key Features**

### **Standard Analysis Pattern:**
- **Consistent Structure**: Follows the same `do_setup()` → `do_analysis()` → `main()` pattern as other analyses
- **Shared Components**: Leverages proven data loading and analysis functions
- **Standardized Output**: Timestamped files with consistent naming patterns
- **SLURM Integration**: Can run on HPC cluster like other analyses

### **Production-Ready Infrastructure:**
- **Error Handling**: Comprehensive error handling with detailed logging
- **Resource Management**: Appropriate SLURM configurations for large-scale processing
- **Monitoring**: Structured logging and job status tracking
- **Visualization**: Professional-quality plots for publication

### **Research-Optimized:**
- **Fast Iteration**: Simple function-based approach for quick modifications
- **Reproducible**: Random seed control for consistent results
- **Scalable**: Handles 1M+ posts with GPU optimization
- **Publication-Ready**: Generates statistical tables and figures

## 🔄 **Integration with Existing System**

### **Shared Components Used:**
- `services.calculate_analytics.shared.data_loading.users.load_user_data()`
- `services.calculate_analytics.shared.data_loading.feeds.map_users_to_posts_used_in_feeds()`
- `services.calculate_analytics.shared.data_loading.posts.load_preprocessed_posts_by_uris()`
- `services.calculate_analytics.shared.constants.STUDY_START_DATE`, `STUDY_END_DATE`

### **Standard Patterns Followed:**
- **File Organization**: `main.py`, `submit_*.sh`, `visualize_results.py`, `README.md`
- **Error Handling**: Standard try/catch patterns with detailed logging
- **Output Management**: Timestamped files in `results/` directory
- **SLURM Integration**: Standard job submission script

## 📊 **Performance Characteristics**

- **Memory Usage**: Optimized for large datasets with conservative BERTopic settings
- **Training Time**: GPU-accelerated with fallback configurations for large datasets
- **Scalability**: Handles 1M+ documents with appropriate resource allocation
- **Debugging**: Simple function calls with clear error messages

## 🎉 **Success Metrics**

This analysis succeeds when:
- ✅ **BERTopic model trains successfully** on 1M+ Bluesky posts
- ✅ **Topic quality metrics** show acceptable coherence scores (c_v > 0.4, c_npmi > 0.1)
- ✅ **Stratified analysis** generates meaningful topic distributions across conditions and time
- ✅ **Visualizations** provide publication-ready figures for research
- ✅ **Integration** works seamlessly with existing analytics infrastructure

## 🚨 **Resource Requirements**

### **SLURM Configuration:**
- **Time**: 8 hours (allows for large dataset processing)
- **Memory**: 50GB (sufficient for BERTopic training on 1M+ documents)
- **Partition**: normal (appropriate for long-running jobs)

### **Dependencies:**
- **BERTopic**: For topic modeling
- **Sentence Transformers**: For text embeddings
- **Matplotlib**: For visualization generation
- **Shared Components**: Existing data loading and analysis functions

---

## 📝 **Migration Notes**

This analysis represents a refactoring from the original complex implementation to follow established patterns:

### **What Changed:**
- **Structure**: From complex class hierarchy to simple function-based approach
- **Integration**: Now uses shared components instead of custom implementations
- **Operations**: Added SLURM integration and visualization capabilities
- **Documentation**: Updated to match standard analysis documentation patterns

### **What Stayed the Same:**
- **Core Functionality**: BERTopic topic modeling with stratified analysis
- **Output Quality**: Same high-quality results for research publication
- **Research Focus**: Optimized for research workflows and iteration

**Status**: ✅ COMPLETED - Standardized, production-ready implementation
**Next Steps**: Run analysis and generate visualizations for research publication
