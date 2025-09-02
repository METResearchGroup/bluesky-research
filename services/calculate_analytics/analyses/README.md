# Analytics System - Research Analyses

This directory contains a comprehensive suite of analytics scripts for analyzing Bluesky social media data as part of a research study on engagement-based algorithms and their impact on content amplification. The system provides both foundational data processing and advanced analytical capabilities.

## Overview

The analytics system is designed to support research on how different feed algorithms affect content distribution, user engagement patterns, and content characteristics. It processes large-scale social media data to understand the impact of algorithmic personalization on content exposure and user behavior.

## Analysis Categories

### **Core Data Processing Analyses**

#### 1. **User Feed Analysis** (`user_feed_analysis_2025_04_08/`)
- **Purpose**: Analyzes content that appeared in users' personalized feeds
- **Key Features**: Daily/weekly aggregation, multiple ML classifiers, condition comparison
- **Output**: CSV files with feed content metrics per user per time period
- **Migration**: Refactored from legacy scripts for better maintainability

#### 2. **User Engagement Analysis** (`user_engagement_analysis_2025_06_16/`)
- **Purpose**: Analyzes content that users actively engaged with (likes, reposts, replies)
- **Key Features**: Engagement type analysis, content classification, temporal trends
- **Output**: CSV files with engagement patterns and content characteristics
- **Migration**: Migrated from legacy engagement analysis scripts

#### 3. **Baseline Measures Analysis** (`get_baseline_measures_across_all_posts_2025_09_02/`)
- **Purpose**: Calculates baseline measures across ALL labeled posts in the dataset
- **Key Features**: Complete dataset coverage, memory-efficient processing, reference baseline
- **Output**: CSV files with baseline metrics for comparison with user-specific analyses
- **Unique Value**: Provides comprehensive baseline without user-specific filtering

### **Network and Content Analysis**

#### 4. **In-Network vs Out-of-Network Analysis** (`in_network_out_of_network_feed_label_comparison_2025_09_02/`)
- **Purpose**: Compares content characteristics between in-network and out-of-network posts
- **Key Features**: Network type filtering, echo chamber analysis, content quality comparison
- **Output**: CSV files and visualization plots comparing network types
- **Research Value**: Examines echo chamber effects and content diversity

#### 5. **Proportion of In-Network Content** (`prop_in_network_feed_content_2025_09_02/`)
- **Purpose**: Calculates the proportion of in-network posts in user feeds
- **Key Features**: Daily/weekly aggregation, condition comparison, echo chamber metrics
- **Output**: CSV files and time series visualizations
- **Research Value**: Quantifies content diversity and network effects

### **Statistical and Visualization Analyses**

#### 6. **Histogram Analysis** (`histogram_analyses_2025_09_01/`)
- **Purpose**: Generates distribution visualizations for content label metrics
- **Key Features**: Multi-dataset analysis, statistical summaries, publication-ready plots
- **Output**: 40 high-resolution PNG files across 8 analysis categories
- **Research Value**: Provides distribution understanding and quality assessment

#### 7. **Correlation Analysis** (`correlation_analysis_2025_08_24/`)
- **Purpose**: Analyzes correlations between various feed and engagement metrics
- **Key Features**: Correlation matrices, heatmap visualizations, condition-based analysis
- **Output**: JSON files with correlation data and PNG heatmaps
- **Research Value**: Identifies relationships between different content characteristics

### **Advanced Analytics**

#### 8. **Topic Modeling Analysis** (`calculate_feed_topic_models_2025_08_18/`)
- **Purpose**: Performs BERTopic topic modeling on feed content
- **Key Features**: Simplified implementation, direct execution, topic discovery
- **Output**: CSV files with topic information and quality metrics
- **Research Value**: Discovers thematic patterns in feed content

#### 9. **Manuscript R Analyses** (`manuscript_R_analyses_2025_09_01/`)
- **Purpose**: R-based statistical analysis for research manuscript
- **Key Features**: Time series analysis, LOESS smoothing, condition comparison
- **Output**: High-resolution PNG plots for publication
- **Research Value**: Formal statistical analysis for academic publication

## Shared Framework

### **Common Architecture**
All analyses follow consistent patterns:
- **Modular Design**: Clear separation of data loading, analysis, and export
- **Shared Components**: Reusable functions from `services.calculate_analytics.shared.*`
- **Error Handling**: Comprehensive error handling with detailed logging
- **Timestamped Output**: Organized results with proper file naming
- **Documentation**: Comprehensive README files for each analysis

### **Shared Data Loading**
- **User Data**: Standardized user loading with study participant filtering
- **Feed Data**: Consistent feed data access patterns
- **Engagement Data**: Unified engagement data processing
- **ML Labels**: Standardized access to content classification labels

### **Shared Analysis Functions**
- **Content Analysis**: Common functions for content label processing
- **Temporal Aggregation**: Standardized daily/weekly aggregation
- **Data Transformation**: Consistent data transformation patterns
- **Export Functions**: Standardized CSV export with proper formatting

## Content Classification System

### **ML Classifiers Used**
- **Perspective API**: Toxicity and constructiveness scores
- **Sociopolitical**: Political orientation (left, right, moderate, unclear)
- **IME (Intergroup, Moral, Emotion)**: Social dynamics classification
- **Valence Classifier**: Emotional tone (positive, negative, neutral)

### **Metric Types**
- **Average Metrics**: Mean values of continuous scores
- **Proportion Metrics**: Proportions of binary classifications
- **Temporal Metrics**: Daily and weekly aggregated values

## Research Applications

### **Primary Research Questions**
1. **Algorithm Impact**: How do different feed algorithms affect content distribution?
2. **Echo Chambers**: Do personalized feeds create echo chamber effects?
3. **Content Quality**: How do algorithms affect content quality and diversity?
4. **User Behavior**: How do users engage with different types of content?

### **Study Conditions**
- **Reverse Chronological**: Standard chronological feed (control)
- **Engagement-Based**: Algorithm optimized for user engagement
- **Representative Diversification**: Algorithm designed to diversify content exposure

## Usage Patterns

### **Typical Workflow**
1. **Run Core Analyses**: Start with feed and engagement analyses
2. **Generate Baseline**: Create baseline measures for comparison
3. **Network Analysis**: Analyze in-network vs out-of-network effects
4. **Statistical Analysis**: Perform correlation and distribution analysis
5. **Visualization**: Generate plots and visualizations for publication
6. **Advanced Analytics**: Apply topic modeling and R-based statistical analysis

### **Dependencies**
- **Prerequisites**: Some analyses depend on results from others
- **Data Requirements**: Consistent data format and availability
- **Resource Management**: Proper memory management for large datasets

## Technical Features

### **Performance Optimizations**
- **Memory Management**: Day-by-day processing for large datasets
- **Efficient Data Loading**: Optimized data access patterns
- **Parallel Processing**: Where applicable, parallel execution
- **Resource Monitoring**: Proper resource allocation and cleanup

### **Quality Assurance**
- **Error Handling**: Comprehensive error handling and recovery
- **Data Validation**: Input data validation and quality checks
- **Logging**: Detailed logging for debugging and monitoring
- **Testing**: Unit tests for critical functions

### **Deployment**
- **Slurm Integration**: Production deployment scripts for cluster execution
- **Environment Management**: Proper conda environment setup
- **Resource Allocation**: Appropriate memory and time limits
- **Output Management**: Organized result storage and retrieval

## File Organization

### **Directory Structure**
```
analyses/
├── README.md                                    # This overview
├── user_feed_analysis_2025_04_08/              # Core feed analysis
├── user_engagement_analysis_2025_06_16/        # Core engagement analysis
├── get_baseline_measures_across_all_posts_2025_09_02/  # Baseline analysis
├── in_network_out_of_network_feed_label_comparison_2025_09_02/  # Network analysis
├── prop_in_network_feed_content_2025_09_02/    # Network proportion analysis
├── histogram_analyses_2025_09_01/              # Distribution analysis
├── correlation_analysis_2025_08_24/            # Correlation analysis
├── calculate_feed_topic_models_2025_08_18/     # Topic modeling
└── manuscript_R_analyses_2025_09_01/           # R-based statistical analysis
```

### **Output Organization**
- **Timestamped Results**: Each analysis creates timestamped output directories
- **Consistent Naming**: Standardized file naming conventions
- **High-Resolution Output**: Publication-ready plots and visualizations
- **Comprehensive Documentation**: Detailed README files for each analysis

## Getting Started

### **Prerequisites**
1. **Data Availability**: Ensure required data files are available
2. **Environment Setup**: Proper Python/R environment configuration
3. **Dependencies**: Install required packages and libraries
4. **Permissions**: Appropriate file system permissions

### **Recommended Order**
1. Start with **User Feed Analysis** and **User Engagement Analysis**
2. Generate **Baseline Measures** for comparison
3. Run **Network Analyses** to understand echo chamber effects
4. Perform **Statistical Analyses** for deeper insights
5. Generate **Visualizations** for publication

### **Documentation**
Each analysis includes comprehensive documentation:
- **Purpose and Overview**: Clear explanation of what the analysis does
- **Usage Instructions**: Step-by-step execution guidance
- **Output Description**: Detailed explanation of results
- **Technical Details**: Implementation specifics and dependencies

## Contributing

### **Adding New Analyses**
- Follow established patterns and conventions
- Use shared components where possible
- Include comprehensive documentation
- Add appropriate error handling and logging
- Test with sample data before full execution

### **Modifying Existing Analyses**
- Maintain backward compatibility where possible
- Update documentation for any changes
- Test modifications thoroughly
- Consider impact on dependent analyses

## Support

For questions about specific analyses, refer to the individual README files in each analysis directory. Each analysis includes detailed documentation, usage instructions, and technical specifications.

## Research Context

This analytics system supports research on the impact of algorithmic personalization on social media content distribution. The analyses provide insights into how different feed algorithms affect content exposure, user engagement patterns, and the formation of echo chambers in social media environments.

The system is designed to support academic research and publication, with high-quality outputs suitable for peer-reviewed journals and conference presentations.