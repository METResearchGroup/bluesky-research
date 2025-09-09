# Load Testing for Topic Modeling Analysis

This directory contains load testing functionality for the topic modeling analysis to evaluate performance across different dataset sizes.

## Files

- `main.py` - Main topic modeling analysis script (modified to support sample size truncation/upsampling)
- `load_testing.py` - Load testing script that runs analysis across multiple sample sizes
- `test_load_testing.py` - Test script to validate load testing functionality
- `LOAD_TESTING_README.md` - This documentation file

## Features

### Sample Size Handling

The `main.py` script now supports intelligent sample size handling:

- **Truncation**: If the dataset is larger than the target sample size, it randomly samples down to the target size
- **Upsampling**: If the dataset is smaller than the target sample size, it samples with replacement to reach the target size
- **Exact Match**: If the dataset size matches the target, no changes are made
- **Logging**: All operations are logged with original and final dataset sizes

### Load Testing Script

The `load_testing.py` script provides comprehensive load testing:

- Runs analysis across multiple sample sizes (default: 1000, 10,000, 100,000, 1,000,000, 5,000,000)
- Measures execution time for each run
- Collects quality metrics (c_v coherence, c_npmi coherence)
- Extracts discovered topics and their keywords
- Outputs results in structured JSON format
- Provides detailed logging and error handling

## Usage

### Running Load Tests

```bash
# Run with default sample sizes (1000, 10K, 100K, 1M, 5M)
python load_testing.py

# Run with custom sample sizes
python load_testing.py --sample-sizes 1000 5000 10000

# Run in production mode (requires production data access)
python load_testing.py --mode prod

# Specify custom output file
python load_testing.py --output my_load_test_results.json
```

### Running Individual Analysis

```bash
# Run with specific sample size
python main.py --sample-size 10000 --mode local

# Force fallback configuration for large datasets
python main.py --sample-size 1000000 --force-fallback
```

### Testing the Load Testing Script

```bash
# Run validation tests
python test_load_testing.py
```

## Output Format

The load testing script generates JSON output with the following structure:

```json
{
  "date": "2025-01-19T10:30:00.000000",
  "mode": "local",
  "runs": [
    {
      "n_samples": 1000,
      "metrics": {
        "c_v": 0.456,
        "c_npmi": 0.234,
        "total_topics": 15,
        "total_documents": 1000
      },
      "topics": {
        "topic_0": ["word1", "word2", "word3"],
        "topic_1": ["word4", "word5", "word6"]
      },
      "time": "2 minutes, 15 seconds",
      "success": true
    }
  ]
}
```

## Performance Considerations

### Memory Usage

- Large sample sizes (1M+ documents) may require significant memory
- The script automatically uses fallback configuration for datasets > 100K documents
- Use `--force-fallback` flag to force conservative settings

### Execution Time

- Small datasets (1K-10K): 1-5 minutes
- Medium datasets (100K): 10-30 minutes  
- Large datasets (1M+): 1-3 hours
- Very large datasets (5M+): 3+ hours

### Timeout Handling

- Each run has a 1-hour timeout to prevent hanging
- Failed runs are logged with error details
- The script continues with remaining sample sizes even if some fail

## Troubleshooting

### Common Issues

1. **Memory Errors**: Use `--force-fallback` flag or reduce sample sizes
2. **Timeout Errors**: Large datasets may exceed 1-hour timeout; consider running individual tests
3. **Data Loading Errors**: Ensure data is available in the specified mode (local/prod)

### Logging

All operations are logged with detailed information:
- Dataset size changes (truncation/upsampling)
- Execution times
- Quality metrics
- Error details for failed runs

### Error Handling

- Failed runs are captured with error messages
- The script continues processing remaining sample sizes
- All results (successful and failed) are included in the output JSON

## Example Workflow

1. **Quick Test**: Start with small sample sizes to verify setup
   ```bash
   python load_testing.py --sample-sizes 1000 5000
   ```

2. **Full Load Test**: Run complete test suite
   ```bash
   python load_testing.py
   ```

3. **Production Test**: Test with production data (if available)
   ```bash
   python load_testing.py --mode prod --sample-sizes 10000 100000
   ```

4. **Analyze Results**: Review the generated JSON file for performance insights

## Integration with CI/CD

The load testing script can be integrated into CI/CD pipelines:

```bash
# Run load test and check for failures
python load_testing.py --sample-sizes 1000 10000
if [ $? -ne 0 ]; then
    echo "Load test failed"
    exit 1
fi
```

## Future Enhancements

Potential improvements for the load testing system:

- Parallel execution of multiple sample sizes
- Real-time progress monitoring
- Performance regression detection
- Integration with monitoring systems
- Automated performance benchmarking
