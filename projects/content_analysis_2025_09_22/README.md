# Content Analysis Extension for Topic Modeling

**Project ID**: content_analysis_2025_09_22  
**Linear Project**: [Content Analysis Extension for Topic Modeling](https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0)  
**Status**: Planning Complete, Implementation Ready  

## Overview
This project extends the existing topic modeling system (`calculate_feed_topic_models_2025_08_18`) with advanced content analysis capabilities including TF-IDF, Named Entity Recognition (NER), Hashtag analysis, and Mention analysis.

## Project Structure
```
content_analysis_2025_09_22/
├── spec.md                    # Complete project specification
├── braindump.md              # Initial brainstorming session
├── plan_content_analysis.md  # Task plan with subtasks and deliverables
├── todo.md                   # Checklist synchronized with Linear issues
├── logs.md                   # Progress tracking and issue resolution
├── metrics.md                # Performance metrics and success criteria
├── lessons_learned.md        # Insights and process improvements
├── tickets/                  # Individual ticket documentation
└── retrospective/           # Post-implementation retrospectives
    └── README.md
```

## Linear Tickets
- **[MET-51](https://linear.app/metresearch/issue/MET-51)**: Implement TF-IDF analysis module
- **[MET-52](https://linear.app/metresearch/issue/MET-52)**: Implement Named Entity Recognition (NER) analysis with visualizations
- **[MET-53](https://linear.app/metresearch/issue/MET-53)**: Implement Hashtag analysis module
- **[MET-54](https://linear.app/metresearch/issue/MET-54)**: Implement Mention analysis module

## Key Features
- **TF-IDF Analysis**: Keyword extraction for "Political Opinions and Perspectives" topic
- **NER Analysis**: Entity recognition with political/sociopolitical focus and comprehensive visualizations
- **Hashtag Analysis**: Hashtag frequency analysis across experimental conditions
- **Mention Analysis**: User mention patterns across conditions and time periods
- **Standardized Output**: Consistent CSV format across all analysis modules
- **Publication-Ready Visualizations**: PNG outputs suitable for research papers

## Technical Stack
- **Python 3.12+**
- **spaCy** for Named Entity Recognition
- **scikit-learn** for TF-IDF analysis
- **Matplotlib/Seaborn** for visualizations
- **Integration** with existing topic modeling infrastructure

## Getting Started
1. Review the [specification](spec.md) for detailed requirements
2. Check [Linear project](https://linear.app/metresearch/project/38487fd5-fe72-4ff9-a731-01a7f77755d0) for current status
3. Follow [task plan](plan_content_analysis.md) for implementation approach
4. Track progress using [todo checklist](todo.md)

## Documentation
- [Complete Specification](spec.md)
- [Task Plan](plan_content_analysis.md)
- [Progress Logs](logs.md)
- [Performance Metrics](metrics.md)
- [Lessons Learned](lessons_learned.md)

## Status
✅ **Planning Complete** - Specification, Linear project, and tickets created  
🔄 **Ready for Implementation** - All requirements defined and tracked  
⏳ **Implementation Pending** - Awaiting development start
