# Simple Pipeline Framework

This directory contains a simple pipeline framework that standardizes how analytics pipelines work across the analytics system. The framework provides abstract base classes and concrete implementations to ensure consistency and maintainability for one-off research analyses.

## Overview

The pipeline framework converts existing monolithic analytics scripts into standardized, reusable pipeline classes that follow a consistent interface and lifecycle. This eliminates code duplication and provides a foundation for building new analytics by combining existing components.

## Architecture

### Core Components

```
shared/pipelines/
├── __init__.py              # Main module exports
├── base.py                  # Abstract base classes
├── feed_analysis.py         # Feed analytics pipeline
├── weekly_thresholds.py     # Weekly thresholds pipeline
├── engagement_analysis.py   # Engagement analysis pipeline
├── README.md                # This documentation
├── example_usage.py         # Usage examples
└── test_pipelines.py        # Pipeline testing suite
```

### Design Principles

1. **Abstract Base Classes**: Define clear interfaces that all pipelines must implement
2. **Lifecycle Management**: Standardized setup, execute, validate, and cleanup phases
3. **State Tracking**: Basic state management and progress tracking
4. **Error Handling**: Robust error handling with graceful degradation
5. **Configuration Integration**: Seamless integration with shared configuration system
6. **Logging & Monitoring**: Structured logging and execution monitoring

## Base Classes

### BaseResearchPipeline

The core abstract base class that all analytics pipelines must implement.

**Key Methods:**
- `setup()`: Initialize resources and validate configuration
- `execute()`: Run the main pipeline logic
- `validate()`: Verify outputs and results
- `cleanup()`: Release resources and reset state
- `run()`: Orchestrate the complete pipeline lifecycle

**Features:**
- Automatic state management (pending, running, completed, failed, cancelled)
- Execution timing and metadata tracking
- Comprehensive error handling and recovery
- Logging integration with correlation IDs

### BaseFeedAnalysisPipeline

Extends BaseResearchPipeline with feed-specific functionality.

**Additional Methods:**
- `load_feed_data()`: Load feed data for specific partition dates
- `calculate_feed_features()`: Calculate features from feed posts
- `aggregate_feed_results()`: Combine results from multiple users
- `validate_feed_data()`: Validate feed data quality

## Concrete Implementations

### FeedAnalysisPipeline

Converts the original `feed_analytics.py` script into a standardized pipeline.

**Capabilities:**
- Load and validate feed data for specific partition dates
- Calculate ML feature averages (toxicity, IME, political, valence)
- Calculate feature proportions using configurable thresholds
- Aggregate results across multiple users
- Export results in various formats

**Configuration:**
```python
config = {
    "exclude_partition_dates": ["2024-10-08"],
    "default_label_threshold": 0.5,
    "load_unfiltered_posts": True,
}
```

**Usage:**
```python
from shared.pipelines import FeedAnalysisPipeline

pipeline = FeedAnalysisPipeline("feed_analysis_2024_10_15")
pipeline.set_partition_date("2024-10-15")
result = pipeline.run()

if result.success:
    print(f"Processed {len(result.data)} users")
    print(f"Execution time: {result.execution_time:.2f}s")
```

### WeeklyThresholdsPipeline

Converts the original `weekly_thresholds.py` script into a standardized pipeline.

**Capabilities:**
- Load weekly threshold data for specific time periods
- Calculate user engagement thresholds
- Generate threshold analysis reports
- Export results in standardized formats

**Usage:**
```python
from shared.pipelines import WeeklyThresholdsPipeline

pipeline = WeeklyThresholdsPipeline("weekly_thresholds_2024_10")
pipeline.set_time_period("2024-10-01", "2024-10-31")
result = pipeline.run()

if result.success:
    print(f"Generated thresholds for {len(result.data)} weeks")
```

### EngagementAnalysisPipeline

Converts engagement analysis scripts into a standardized pipeline.

**Capabilities:**
- Load user engagement data
- Calculate engagement metrics and patterns
- Generate engagement analysis reports
- Export results in standardized formats

**Usage:**
```python
from shared.pipelines import EngagementAnalysisPipeline

pipeline = EngagementAnalysisPipeline("engagement_analysis_2024_10")
pipeline.set_analysis_period("2024-10-01", "2024-10-31")
result = pipeline.run()

if result.success:
    print(f"Analyzed engagement for {len(result.data)} users")
```

## Simple Pipeline Usage

The framework is designed for **one-off research analyses** where you run each analysis individually:

### Basic Pattern

```python
# 1. Create pipeline instance
pipeline = SpecificPipeline("analysis_name")

# 2. Configure parameters
pipeline.set_parameters(param1="value1", param2="value2")

# 3. Execute pipeline
result = pipeline.run()

# 4. Check results
if result.success:
    print(f"Analysis completed successfully")
    print(f"Data: {result.data}")
    print(f"Execution time: {result.execution_time:.2f}s")
else:
    print(f"Analysis failed: {result.error}")
```

### Configuration

Each pipeline can be configured with specific parameters:

```python
config = {
    "partition_date": "2024-10-15",
    "feature_threshold": 0.5,
    "output_format": "csv",
    "include_metadata": True
}

pipeline = FeedAnalysisPipeline("custom_analysis", config=config)
```

### Error Handling

The framework provides comprehensive error handling:

```python
try:
    result = pipeline.run()
    if result.success:
        # Process successful result
        pass
    else:
        # Handle pipeline failure
        print(f"Pipeline failed: {result.error}")
except Exception as e:
    # Handle unexpected errors
    print(f"Unexpected error: {e}")
```

## Testing

The framework includes comprehensive testing:

```bash
# Run all pipeline tests
python -m pytest test_pipelines.py

# Run specific test categories
python -m pytest test_pipelines.py::TestBaseResearchPipeline
python -m pytest test_pipelines.py::TestFeedAnalysisPipeline
```

## Creating New Pipelines

To create a new pipeline:

1. **Extend BaseResearchPipeline** or appropriate base class
2. **Implement required abstract methods**
3. **Add configuration validation**
4. **Implement error handling**
5. **Add comprehensive testing**

Example:
```python
class MyAnalysisPipeline(BaseResearchPipeline):
    def setup(self):
        # Validate configuration
        # Initialize resources
        pass
    
    def execute(self):
        # Load data
        # Process data
        # Generate outputs
        return PipelineResult(success=True, data=output_data)
    
    def validate(self):
        # Validate outputs
        return True
    
    def cleanup(self):
        # Clean up resources
        pass
```

## Best Practices

1. **Keep pipelines focused**: Each pipeline should do one thing well
2. **Validate inputs**: Always validate configuration and data inputs
3. **Handle errors gracefully**: Provide clear error messages and recovery
4. **Log operations**: Use structured logging for debugging and monitoring
5. **Test thoroughly**: Write comprehensive tests for all pipeline logic
6. **Document usage**: Provide clear examples and documentation

## Migration from Existing Scripts

To migrate existing scripts:

1. **Extract common logic** into shared modules (Phase 1)
2. **Create pipeline class** that implements the required interface
3. **Move business logic** into the `execute()` method
4. **Add configuration** parameters instead of hardcoded values
5. **Test thoroughly** to ensure identical outputs
6. **Update documentation** with usage examples

This approach ensures that existing functionality is preserved while providing a consistent, maintainable structure for future development.
