# Shared Pipeline Framework

This directory contains the ABC-based pipeline framework that standardizes how analytics pipelines work across the analytics system. The framework provides abstract base classes, concrete implementations, and orchestration utilities to ensure consistency and maintainability.

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
├── orchestration.py         # Pipeline orchestration utilities
├── README.md                # This documentation
├── example_usage.py         # Usage examples
└── test_pipelines.py        # Pipeline testing suite
```

### Design Principles

1. **Abstract Base Classes**: Define clear interfaces that all pipelines must implement
2. **Lifecycle Management**: Standardized setup, execute, validate, and cleanup phases
3. **State Tracking**: Comprehensive state management and progress tracking
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

Converts the original `calculate_weekly_thresholds_per_user.py` script into a standardized pipeline.

**Capabilities:**
- Load user demographic information
- Calculate static week thresholds based on user waves
- Calculate dynamic week thresholds based on survey completion
- Combine static and dynamic results
- Validate week assignments and wave information

**Configuration:**
```python
config = {
    "exclude_partition_dates": ["2024-10-08"],
    "calculate_static": True,
    "calculate_dynamic": True,
    "include_wave_info": True,
}
```

**Usage:**
```python
from shared.pipelines import WeeklyThresholdsPipeline

pipeline = WeeklyThresholdsPipeline("weekly_thresholds")
result = pipeline.run()

if result.success:
    thresholds_df = result.data
    print(f"Calculated thresholds for {len(thresholds_df)} user-date combinations")
```

### EngagementAnalysisPipeline

Converts the original `get_aggregate_metrics.py` script into a standardized pipeline.

**Capabilities:**
- Load study users and generate partition dates
- Calculate daily engagement metrics (likes, posts, follows, reposts)
- Generate engagement summaries per user
- Export engagement data in various formats
- Configurable metric types and calculations

**Configuration:**
```python
config = {
    "exclude_partition_dates": ["2024-10-08"],
    "include_likes": True,
    "include_posts": True,
    "include_follows": True,
    "include_reposts": True,
    "calculate_rates": True,
    "calculate_summaries": True,
}
```

**Usage:**
```python
from shared.pipelines import EngagementAnalysisPipeline

pipeline = EngagementAnalysisPipeline("engagement_analysis")
result = pipeline.run()

if result.success:
    engagement_data = result.data
    print(f"Processed {len(engagement_data['users'])} users")
    print(f"Generated {len(engagement_data['partition_dates'])} partition dates")
```

## Pipeline Orchestration

### PipelineOrchestrator

Centralized management of multiple pipeline executions.

**Key Features:**
- Pipeline registration and management
- Sequential and parallel execution
- Execution history and statistics
- Pipeline cancellation and monitoring
- Export execution results

**Usage:**
```python
from shared.pipelines import PipelineOrchestrator, FeedAnalysisPipeline, WeeklyThresholdsPipeline

# Create orchestrator
orchestrator = PipelineOrchestrator("analytics_orchestrator")

# Register pipelines
feed_pipeline = FeedAnalysisPipeline("feed_analysis")
thresholds_pipeline = WeeklyThresholdsPipeline("weekly_thresholds")

orchestrator.register_pipeline(feed_pipeline)
orchestrator.register_pipeline(thresholds_pipeline)

# Execute pipelines sequentially
results = orchestrator.execute_pipelines_sequential([
    "feed_analysis",
    "weekly_thresholds"
])

# Get execution summary
summary = orchestrator.get_execution_summary()
print(f"Success rate: {summary['success_rate']:.2%}")
```

## Configuration Integration

All pipelines integrate with the shared configuration system:

```python
from shared.config import get_config

config = get_config()
feature_config = config.features
study_config = config.study
week_config = config.weeks
```

**Configuration Sources:**
- `analytics.yaml`: Feature definitions and study parameters
- Environment variables: Override default values
- Pipeline-specific config: Runtime configuration overrides

## Error Handling

The framework provides comprehensive error handling:

**PipelineError**: Custom exception with pipeline context
**Graceful Degradation**: Continue processing when possible
**Error Recovery**: Automatic cleanup on failures
**Detailed Logging**: Structured error information

**Example Error Handling:**
```python
try:
    result = pipeline.run()
    if result.success:
        process_results(result.data)
    else:
        handle_pipeline_failure(result.error)
except PipelineError as e:
    logger.error(f"Pipeline {e.pipeline_name} failed at {e.stage}: {e.message}")
    # Handle specific pipeline errors
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    # Handle unexpected errors
```

## State Management

Pipelines maintain comprehensive state information:

**Pipeline States:**
- `PENDING`: Pipeline created but not yet started
- `RUNNING`: Pipeline currently executing
- `COMPLETED`: Pipeline finished successfully
- `FAILED`: Pipeline encountered an error
- `CANCELLED`: Pipeline execution was cancelled

**State Information:**
```python
status = pipeline.get_status()
print(f"State: {status['state']}")
print(f"Start time: {status['start_time']}")
print(f"Execution time: {status['execution_time']}")
print(f"Error: {status['error']}")
```

## Testing

The framework includes comprehensive testing support:

**Test Structure:**
- Unit tests for individual pipeline components
- Integration tests for pipeline interactions
- Mock-based testing for external dependencies
- Performance benchmarking

**Running Tests:**
```bash
# Test all pipelines
python -m pytest test_pipelines.py -v

# Test specific pipeline
python -m pytest test_pipelines.py::test_feed_analysis_pipeline -v

# Test with coverage
python -m pytest test_pipelines.py --cov=shared.pipelines --cov-report=html
```

## Performance Considerations

**Optimization Features:**
- Lazy loading of data and resources
- Efficient memory management
- Configurable batch processing
- Progress tracking for long-running operations

**Monitoring:**
- Execution time tracking
- Memory usage monitoring
- Progress indicators
- Performance metrics export

## Migration Guide

### From Monolithic Scripts

**Before (feed_analytics.py):**
```python
# Manual execution flow
def main():
    partition_date = "2024-10-15"
    user_posts_map = get_hydrated_feed_posts_per_user(partition_date)
    
    user_averages = []
    for user, posts_df in user_posts_map.items():
        averages = calculate_averages(posts_df, user)
        user_averages.append(averages)
    
    results_df = pd.DataFrame(user_averages)
    results_df.to_csv("feed_analytics.csv", index=False)

if __name__ == "__main__":
    main()
```

**After (using pipeline framework):**
```python
from shared.pipelines import FeedAnalysisPipeline

# Create and configure pipeline
pipeline = FeedAnalysisPipeline("feed_analysis")
pipeline.set_partition_date("2024-10-15")

# Execute with full lifecycle management
result = pipeline.run()

if result.success:
    # Export results
    result.data.to_csv("feed_analytics.csv", index=False)
    print(f"Pipeline completed in {result.execution_time:.2f}s")
```

### Benefits of Migration

1. **Standardization**: Consistent interface across all analytics
2. **Error Handling**: Robust error handling and recovery
3. **Monitoring**: Comprehensive execution tracking and logging
4. **Reusability**: Pipelines can be combined and orchestrated
5. **Testing**: Isolated, testable components
6. **Configuration**: Centralized parameter management
7. **Maintenance**: Single source of truth for each analysis type

## Best Practices

### Pipeline Design

1. **Single Responsibility**: Each pipeline should have one clear purpose
2. **Configuration-Driven**: Use configuration for parameters and behavior
3. **Error Handling**: Implement comprehensive error handling and recovery
4. **Resource Management**: Properly manage resources and cleanup
5. **Validation**: Validate inputs, outputs, and intermediate results
6. **Logging**: Use structured logging with appropriate levels

### Configuration Management

1. **Default Values**: Provide sensible defaults for all parameters
2. **Validation**: Validate configuration at setup time
3. **Environment Overrides**: Support environment variable overrides
4. **Documentation**: Document all configuration options

### Error Handling

1. **Specific Exceptions**: Use PipelineError for pipeline-specific issues
2. **Graceful Degradation**: Continue processing when possible
3. **Error Context**: Include relevant context in error messages
4. **Recovery**: Implement recovery mechanisms where appropriate

### Testing

1. **Unit Tests**: Test individual pipeline methods
2. **Integration Tests**: Test pipeline interactions
3. **Mock Dependencies**: Mock external dependencies for isolated testing
4. **Performance Tests**: Benchmark pipeline performance
5. **Error Scenarios**: Test error handling and recovery

## Examples

### Complete Analytics Workflow

```python
from shared.pipelines import (
    PipelineOrchestrator,
    FeedAnalysisPipeline,
    WeeklyThresholdsPipeline,
    EngagementAnalysisPipeline
)

# Create orchestrator
orchestrator = PipelineOrchestrator("complete_analytics")

# Create and configure pipelines
feed_pipeline = FeedAnalysisPipeline("feed_analysis")
feed_pipeline.set_partition_date("2024-10-15")

thresholds_pipeline = WeeklyThresholdsPipeline("weekly_thresholds")

engagement_pipeline = EngagementAnalysisPipeline("engagement_analysis")

# Register pipelines
orchestrator.register_pipeline(feed_pipeline)
orchestrator.register_pipeline(thresholds_pipeline)
orchestrator.register_pipeline(engagement_pipeline)

# Execute complete workflow
results = orchestrator.execute_pipelines_sequential([
    "feed_analysis",
    "weekly_thresholds", 
    "engagement_analysis"
])

# Process results
for result in results:
    if result.success:
        print(f"Pipeline {result.pipeline_name} completed successfully")
        print(f"Execution time: {result.execution_time:.2f}s")
    else:
        print(f"Pipeline {result.pipeline_name} failed: {result.error}")

# Get execution summary
summary = orchestrator.get_execution_summary()
print(f"Overall success rate: {summary['success_rate']:.2%}")
```

### Custom Pipeline Configuration

```python
# Custom configuration for feed analysis
feed_config = {
    "exclude_partition_dates": ["2024-10-08", "2024-10-09"],
    "default_label_threshold": 0.7,
    "load_unfiltered_posts": False,
}

feed_pipeline = FeedAnalysisPipeline("custom_feed_analysis", config=feed_config)

# Custom configuration for weekly thresholds
thresholds_config = {
    "exclude_partition_dates": ["2024-10-08"],
    "calculate_static": True,
    "calculate_dynamic": False,  # Skip dynamic thresholds
    "include_wave_info": True,
}

thresholds_pipeline = WeeklyThresholdsPipeline("custom_thresholds", config=thresholds_config)
```

### Pipeline Monitoring and Control

```python
# Monitor pipeline status
status = feed_pipeline.get_status()
print(f"Pipeline state: {status['state']}")

# Cancel running pipeline
if feed_pipeline.state == PipelineState.RUNNING:
    feed_pipeline.cancel()

# Get orchestrator statistics
summary = orchestrator.get_execution_summary()
print(f"Total executions: {summary['total_executions']}")
print(f"Successful: {summary['successful_executions']}")
print(f"Failed: {summary['failed_executions']}")
print(f"Average execution time: {summary['average_execution_time']:.2f}s")

# Export execution history
orchestrator.export_execution_history(format="csv")
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure absolute imports from project root
2. **Configuration Errors**: Verify configuration files and environment variables
3. **Data Loading Failures**: Check data availability and permissions
4. **Validation Failures**: Review input data quality and pipeline validation logic
5. **Performance Issues**: Monitor execution times and resource usage

### Debug Mode

Enable debug logging for detailed pipeline execution:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Pipeline execution will show detailed logs
result = pipeline.run()
```

### Error Investigation

Use pipeline status and error information for debugging:

```python
# Get detailed pipeline status
status = pipeline.get_status()
print(f"Pipeline: {status['name']}")
print(f"State: {status['state']}")
print(f"Error: {status['error']}")
print(f"Metadata: {status['metadata']}")

# Check execution history
execution_results = orchestrator.execution_history
for result in execution_results:
    if not result.success:
        print(f"Failed execution: {result.pipeline_name}")
        print(f"Error: {result.error}")
        print(f"Execution time: {result.execution_time:.2f}s")
```

## Contributing

When adding new pipelines:

1. **Follow Base Class Contract**: Implement all required abstract methods
2. **Configuration Integration**: Use shared configuration system
3. **Error Handling**: Implement comprehensive error handling
4. **Testing**: Add unit and integration tests
5. **Documentation**: Update README and add docstrings
6. **Validation**: Implement proper input/output validation

## Related Documentation

- [Configuration Management](../config/README.md)
- [Data Loading](../data_loading/README.md)
- [Processing Modules](../processing/README.md)
- [Coding Standards](../../../ai-rules/agents/task_instructions/execution/CODING_RULES.md)
- [Project Planning](../../../../projects/analytics_system_refactor/plan_refactor.md)

## Conclusion

The ABC-based pipeline framework provides a robust, standardized foundation for analytics operations. By converting existing monolithic scripts into pipeline classes, the system gains:

- **Consistency**: Standardized interfaces and behavior
- **Maintainability**: Isolated, testable components
- **Reusability**: Pipelines can be combined and orchestrated
- **Monitoring**: Comprehensive execution tracking and logging
- **Error Handling**: Robust error handling and recovery
- **Configuration**: Centralized parameter management

This foundation enables Phase 3 (Reorganize One-Off Analyses) and sets the stage for building new analytics by combining existing pipeline components rather than duplicating code.
