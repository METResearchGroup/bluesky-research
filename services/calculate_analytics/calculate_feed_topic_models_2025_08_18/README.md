# Feed-Level Topic Analysis for Bluesky Research

## Overview

This package provides the data loading infrastructure and pipeline integration for topic modeling analysis of Bluesky feed data. It implements the foundational data loading system required for MET-44 and serves as the foundation for the complete topic modeling pipeline.

## Features

- **Abstract DataLoader Interface**: Clean contract for all data loaders with extensible design
- **LocalDataLoader Implementation**: Concrete implementation using existing `load_data_from_local_storage` function
- **Configuration Management**: YAML-based configuration for data loader selection and parameters
- **Pipeline Integration**: Seamless integration between data loading and topic modeling components
- **Data Validation**: Robust validation for date ranges, data quality, and study period constraints
- **Error Handling**: Comprehensive error handling with meaningful error messages

## Architecture

### Package Structure

```
src/
├── data_loading/
│   ├── base.py              # Abstract DataLoader interface
│   ├── local.py             # LocalDataLoader implementation
│   └── config.py            # Configuration management
├── pipeline/
│   └── topic_modeling.py    # Pipeline integration
└── __init__.py              # Main package exports

tests/
├── test_data_loading/
│   ├── test_base.py         # Interface tests
│   ├── test_local.py        # LocalDataLoader tests
│   ├── test_config.py       # Configuration tests
│   └── test_pipeline.py     # Pipeline tests

config/
└── data_loader.yaml         # Default configuration

notebooks/
└── data_loading_demo.ipynb  # Jupyter demo

demo_data_loading.py          # Command-line demo script
```

### Core Components

#### 1. Abstract DataLoader Interface (`base.py`)

The `DataLoader` abstract base class defines the contract that all data loaders must implement:

```python
@abstractmethod
def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
    """Load text data for the specified date range."""
    pass
```

**Features:**
- Abstract interface with required `load_text_data` method
- Built-in date range validation
- Comprehensive error handling with custom exceptions
- Extensible design for future loader implementations

#### 2. LocalDataLoader Implementation (`local.py`)

Concrete implementation that loads preprocessed posts from local storage:

```python
from src.data_loading.local import LocalDataLoader

loader = LocalDataLoader(
    service="preprocessed_posts",
    directory="cache",
    export_format="parquet"
)

df = loader.load_text_data("2024-10-01", "2024-10-31")
```

**Features:**
- Uses existing `load_data_from_local_storage` function
- Integrates with helper functions (`get_partition_dates`)
- Respects study period constants from `constants.py`
- Comprehensive data validation and cleaning
- Support for different data formats and source types

#### 3. Configuration Management (`config.py`)

YAML-based configuration system for data loader selection:

```python
from src.data_loading.config import DataLoaderConfig

config = DataLoaderConfig()
loader_config = config.get_loader_config("local")
```

**Features:**
- Automatic default configuration creation
- Dynamic configuration updates
- Loader registration and management
- Environment-specific configuration support

#### 4. Pipeline Integration (`pipeline/topic_modeling.py`)

Integration layer that connects data loading with topic modeling:

```python
from src.pipeline.topic_modeling import TopicModelingPipeline

pipeline = TopicModelingPipeline(data_loader)
df = pipeline.load_data("2024-10-01", "2024-10-31")
prepared_data = pipeline.prepare_for_bertopic()
```

**Features:**
- Seamless integration with DataLoader implementations
- Data preparation for BERTopic processing
- Comprehensive pipeline state information
- Error handling and validation

## Usage Examples

### Basic Data Loading

```python
from src.data_loading.local import LocalDataLoader

# Create loader with default settings
loader = LocalDataLoader()

# Load data for a date range
df = loader.load_text_data("2024-10-01", "2024-10-31")
print(f"Loaded {len(df)} posts")

# Check data quality
print(f"Columns: {list(df.columns)}")
print(f"Sample text: {df['text'].iloc[0][:100]}...")
```

### Configuration-Driven Loading

```python
from src.data_loading.config import DataLoaderConfig
from src.data_loading.local import LocalDataLoader

# Load configuration
config = DataLoaderConfig()

# Get loader configuration
loader_config = config.get_loader_config("local")
print(f"Service: {loader_config['service']}")
print(f"Directory: {loader_config['directory']}")

# Create loader with configuration
loader = LocalDataLoader(**loader_config)
```

### Pipeline Integration

```python
from src.data_loading.local import LocalDataLoader
from src.pipeline.topic_modeling import TopicModelingPipeline

# Create data loader
loader = LocalDataLoader()

# Create pipeline
pipeline = TopicModelingPipeline(loader)

# Load and prepare data
df = pipeline.load_data("2024-10-01", "2024-10-31")
prepared_data = pipeline.prepare_for_bertopic()

# Get pipeline information
info = pipeline.get_pipeline_info()
print(f"Pipeline status: {info}")
```

## Configuration

The default configuration is stored in `config/data_loader.yaml`:

```yaml
data_loader:
  type: "local"
  local:
    enabled: true
    service: "preprocessed_posts"
    directory: "cache"
    export_format: "parquet"
  
validation:
  max_date_range_days: 365
  min_text_length: 10
  max_text_length: 10000
  
performance:
  batch_size: 1000
  memory_limit_gb: 8
  enable_compression: true
```

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
pytest tests/ -v

# Run specific test modules
pytest tests/test_data_loading/test_base.py -v
pytest tests/test_data_loading/test_local.py -v
pytest tests/test_data_loading/test_config.py -v
pytest tests/test_data_loading/test_pipeline.py -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html
```

## Demo

### Command-Line Demo

```bash
python demo_data_loading.py
```

This script demonstrates:
- Abstract interface functionality
- Configuration management
- LocalDataLoader implementation
- Pipeline integration

### Jupyter Notebook Demo

```bash
jupyter notebook notebooks/data_loading_demo.ipynb
```

Interactive demonstration of all components with real examples.

## Integration with Existing Infrastructure

### Dependencies

- **Data Loading**: Uses existing `load_data_from_local_storage` function from `lib.db.manage_local_data`
- **Helper Functions**: Integrates with `get_partition_dates` from `lib.helper`
- **Constants**: Respects study period constants from `lib.constants`
- **BERTopic Pipeline**: Ready for integration with completed BERTopic implementation (MET-34)

### Data Flow

1. **Date Range Input** → Date validation (study period, format)
2. **Data Loading** → `load_data_from_local_storage` with proper parameters
3. **Data Validation** → Text column presence, data quality checks
4. **Data Cleaning** → Remove nulls, empty texts, very long texts
5. **Pipeline Integration** → Prepare data for BERTopic processing

## Error Handling

The system provides comprehensive error handling:

- **ValidationError**: Invalid date ranges, study period violations
- **DataLoadingError**: Data loading failures, missing columns, empty datasets
- **ValueError**: Invalid configuration parameters

All errors include meaningful messages and context for debugging.

## Performance Considerations

- **Memory Management**: Configurable memory limits and batch processing
- **Data Filtering**: Efficient filtering of invalid/empty texts
- **Lazy Loading**: Data is only loaded when explicitly requested
- **Caching**: Configuration and validation results are cached

## Future Extensibility

The modular design allows easy addition of:

- **Production Data Loader** (MET-45)
- **Database Data Loader**
- **API Data Loader**
- **Streaming Data Loader**

All new loaders implement the same `DataLoader` interface, ensuring consistency and compatibility.

## Status

**MET-44 Implementation Status**: ✅ COMPLETED

- [x] Abstract DataLoader interface implemented and documented
- [x] LocalDataLoader successfully loads data from local storage
- [x] Configuration-driven loader selection working
- [x] Integration with topic modeling pipeline functional
- [x] Comprehensive test suite passing
- [x] Demo scripts and documentation complete

**Next Steps**: Ready for MET-45 (Production Data Loader) implementation.

## Author

AI Agent implementing MET-44  
**Date**: 2025-08-22  
**Linear Issue**: [MET-44](https://linear.app/metresearch/issue/MET-44)
