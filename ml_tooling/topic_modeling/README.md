# Topic Modeling Module

This module provides a generic, reusable BERTopic modeling pipeline for text analysis with comprehensive quality monitoring and GPU optimization.

## Features

- **Generic Pipeline**: Accepts any DataFrame with text column
- **YAML Configuration**: Comprehensive parameter management
- **Quality Monitoring**: Real-time coherence metrics (c_v, c_npmi)
- **GPU Optimization**: Automatic device detection and memory management
- **Reproducibility**: Random seed control for consistent results
- **Error Handling**: Comprehensive validation and graceful error handling

## Installation

The module is part of the `bluesky-research` package. Install with:

```bash
# Install with analysis dependencies
pip install -e .[analysis]

# Or install all dependencies
pip install -e .[all]
```

## Quick Start

### Default Configuration

The wrapper automatically loads `config.yaml` from the same directory when no configuration is provided:

```python
# This will automatically load ml_tooling/topic_modeling/config.yaml
wrapper = BERTopicWrapper()
```

### Basic Usage

```python
from ml_tooling.topic_modeling import BERTopicWrapper
import pandas as pd

# Create sample data
data = pd.DataFrame({
    'text': [
        'machine learning artificial intelligence deep learning',
        'machine learning algorithms data science',
        'artificial intelligence neural networks',
        'data science machine learning statistics'
    ]
})

# Initialize with default configuration (loads config.yaml automatically)
wrapper = BERTopicWrapper()

# Train the model
wrapper.fit(data, 'text')

# Get results
topics = wrapper.get_topics()
topic_info = wrapper.get_topic_info()
quality_metrics = wrapper.get_quality_metrics()

print(f"Generated {len(topics)} topics")
print(f"Quality metrics: {quality_metrics}")
```

### Using YAML Configuration

```python
from ml_tooling.topic_modeling import BERTopicWrapper

# Load configuration from YAML file
wrapper = BERTopicWrapper(config_path='config_example.yaml')

# Or use dictionary configuration
config = {
    'embedding_model': {
        'name': 'all-mpnet-base-v2',  # Higher quality model
        'device': 'auto',  # Auto-detect GPU
        'batch_size': 64
    },
    'bertopic': {
        'top_n_words': 25,
        'min_topic_size': 20,
        'nr_topics': 'auto'
    },
    'quality_thresholds': {
        'c_v_min': 0.5,
        'c_npmi_min': 0.15
    },
    'random_seed': 42
}

wrapper = BERTopicWrapper(config_dict=config)
```

## Configuration Options

### Embedding Model

| Parameter | Default | Description |
|-----------|---------|-------------|
| `name` | `'all-MiniLM-L6-v2'` | Sentence Transformer model name |
| `device` | `'auto'` | Device: 'auto', 'cuda', 'mps', or 'cpu' |
| `batch_size` | `32` | Batch size for embedding generation |

**Recommended Models:**
- **Fast & Efficient**: `'all-MiniLM-L6-v2'` (384 dimensions)
- **High Quality**: `'all-mpnet-base-v2'` (768 dimensions)
- **Multilingual**: `'paraphrase-multilingual-MiniLM-L12-v2'`

### BERTopic Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `top_n_words` | `20` | Number of top words per topic |
| `min_topic_size` | `15` | Minimum documents per topic |
| `nr_topics` | `'auto'` | Number of topics or 'auto' |
| `calculate_probabilities` | `True` | Calculate topic probabilities |
| `verbose` | `True` | Verbose output during training |

### Quality Thresholds

| Parameter | Default | Description |
|-----------|---------|-------------|
| `c_v_min` | `0.4` | Minimum c_v coherence score |
| `c_npmi_min` | `0.1` | Minimum c_npmi coherence score |

### GPU Optimization

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable` | `True` | Enable GPU acceleration |
| `max_batch_size` | `128` | Maximum GPU batch size |
| `memory_threshold` | `0.8` | GPU memory usage threshold |

## Advanced Usage

### Custom UMAP and HDBSCAN Parameters

```python
config = {
    'bertopic': {
        'umap_model': {
            'n_neighbors': 15,
            'n_components': 5,
            'min_dist': 0.0,
            'metric': 'cosine'
        },
        'hdbscan_model': {
            'min_cluster_size': 15,
            'min_samples': 5,
            'metric': 'euclidean',
            'cluster_selection_method': 'eom'
        }
    }
}
```

### Quality Monitoring

```python
# Get comprehensive quality metrics
metrics = wrapper.get_quality_metrics()

print(f"Training time: {metrics['training_time']:.2f} seconds")
print(f"c_v coherence: {metrics['c_v_mean']:.3f}")
print(f"c_npmi coherence: {metrics['c_npmi_mean']:.3f}")
print(f"Meets thresholds: {metrics['meets_thresholds']}")

if metrics['warnings']:
    print("Quality warnings:")
    for warning in metrics['warnings']:
        print(f"  - {warning}")
```

### Model Persistence

```python
# Save trained model
wrapper.save_model('topic_model.pkl')

# Load saved model
loaded_wrapper = BERTopicWrapper.load_model('topic_model.pkl')

# Continue analysis
topics = loaded_wrapper.get_topics()
```

### Configuration Updates

```python
# Update configuration after initialization
wrapper.update_config({
    'embedding_model': {'batch_size': 128},
    'quality_thresholds': {'c_v_min': 0.6}
})

# Get current configuration
current_config = wrapper.get_config()
```

## Performance Considerations

### GPU Memory Management

- **Small datasets** (< 10K documents): Use default batch sizes
- **Medium datasets** (10K-100K documents): Increase batch size to 64-128
- **Large datasets** (> 100K documents): Monitor GPU memory and adjust batch size

### Batch Size Guidelines

```python
# For different dataset sizes
config = {
    'embedding_model': {
        'batch_size': 32,  # 10K-50K documents
        # 'batch_size': 64,  # 50K-200K documents
        # 'batch_size': 128, # 200K+ documents
    }
}
```

### Memory Optimization

- Use `'all-MiniLM-L6-v2'` for memory-constrained environments
- Set `calculate_probabilities=False` for large datasets if probabilities aren't needed
- Monitor GPU memory usage and adjust batch sizes accordingly

## Error Handling

The module provides comprehensive error handling:

```python
try:
    wrapper.fit(data, 'text')
except ValueError as e:
    print(f"Input validation error: {e}")
except RuntimeError as e:
    print(f"Training error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Common Error Scenarios

1. **Empty DataFrame**: Ensure input data contains documents
2. **Missing Text Column**: Verify text column name matches configuration
3. **GPU Memory Issues**: Reduce batch size or use CPU
4. **Invalid Configuration**: Check parameter ranges and types

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
pytest ml_tooling/topic_modeling/tests/ -v

# Run specific test categories
pytest ml_tooling/topic_modeling/tests/ -k "test_init" -v
pytest ml_tooling/topic_modeling/tests/ -k "test_pipeline" -v
pytest ml_tooling/topic_modeling/tests/ -k "test_quality" -v
```

### Test Coverage

The test suite covers:
- ✅ Input validation (DataFrame structure, text column validation)
- ✅ Configuration management (YAML loading, parameter validation)
- ✅ Pipeline functionality (end-to-end topic modeling)
- ✅ Error handling (invalid inputs, missing files, GPU failures)
- ✅ Reproducibility (same input + seed produces identical results)
- ✅ Quality monitoring (coherence metrics calculation)
- ✅ GPU optimization (device detection, memory management)

## Examples

### Example 1: Basic Topic Modeling

```python
from ml_tooling.topic_modeling import BERTopicWrapper
import pandas as pd

# Load data
data = pd.read_parquet('posts.parquet')

# Initialize wrapper
wrapper = BERTopicWrapper(config_dict={
    'embedding_model': {'name': 'all-MiniLM-L6-v2'},
    'random_seed': 42
})

# Train model
wrapper.fit(data, 'text')

# Analyze results
topics = wrapper.get_topics()
topic_info = wrapper.get_topic_info()

print("Topic Analysis Results:")
for topic_id, words in topics.items():
    if topic_id != -1:  # Skip outlier topic
        print(f"Topic {topic_id}: {[word for word, _ in words[:5]]}")
```

### Example 2: High-Quality Analysis

```python
# Configuration for high-quality analysis
config = {
    'embedding_model': {
        'name': 'all-mpnet-base-v2',
        'device': 'auto',
        'batch_size': 64
    },
    'bertopic': {
        'top_n_words': 30,
        'min_topic_size': 25,
        'nr_topics': 'auto',
        'calculate_probabilities': True
    },
    'quality_thresholds': {
        'c_v_min': 0.5,
        'c_npmi_min': 0.15
    },
    'random_seed': 42
}

wrapper = BERTopicWrapper(config_dict=config)
wrapper.fit(data, 'text')

# Check quality
metrics = wrapper.get_quality_metrics()
if not metrics['meets_thresholds']:
    print("Quality below thresholds, consider adjusting parameters")
```

### Example 3: Large Dataset Processing

```python
# Configuration for large datasets
config = {
    'embedding_model': {
        'name': 'all-MiniLM-L6-v2',
        'device': 'auto',
        'batch_size': 128
    },
    'bertopic': {
        'top_n_words': 20,
        'min_topic_size': 50,
        'nr_topics': 'auto',
        'calculate_probabilities': False  # Save memory
    },
    'gpu_optimization': {
        'enable': True,
        'max_batch_size': 256,
        'memory_threshold': 0.7
    }
}

wrapper = BERTopicWrapper(config_dict=config)
wrapper.fit(large_dataset, 'text')
```

## Troubleshooting

### Common Issues

1. **CUDA Out of Memory**
   - Reduce batch size
   - Use smaller embedding model
   - Set `calculate_probabilities=False`

2. **Poor Topic Quality**
   - Increase `min_topic_size`
   - Use higher quality embedding model
   - Check text preprocessing

3. **Slow Performance**
   - Enable GPU acceleration
   - Increase batch size
   - Use faster embedding model

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

wrapper = BERTopicWrapper(config_dict={})
wrapper.fit(data, 'text')
```

## Contributing

When contributing to this module:

1. Follow the established code style and patterns
2. Add comprehensive tests for new functionality
3. Update documentation for new features
4. Ensure all tests pass before submitting changes

## License

This module is part of the `bluesky-research` project and follows the same license terms.
