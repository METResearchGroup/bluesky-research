# Baseline Load Testing Specification

## Overview

This document specifies the implementation of baseline load testing for the firehose sync stream service. This establishes performance metrics for the **current system** (without batching) to serve as a baseline for comparison after implementing batch writes (see [Issue #251](https://github.com/METResearchGroup/bluesky-research/issues/251)).

**Related Documents:**
- [Load Testing Plan](./LOAD_TESTING_PLAN.md) - Overall phased approach and test scenarios

**GitHub Issues:**
- [Issue #252](https://github.com/METResearchGroup/bluesky-research/issues/252) - Baseline load testing (this spec)
- [Issue #251](https://github.com/METResearchGroup/bluesky-research/issues/251) - Implement batch writes (future work)

## Goals

1. **Establish Baseline Metrics**: Measure current system performance without batching
2. **Identify Bottlenecks**: Understand where the system struggles under load
3. **Create Reproducible Tests**: Generate consistent, repeatable test results
4. **Enable Comparison**: Provide metrics format that can be compared post-batching

## Architecture

The load testing system will follow **Phase 1** from the [Load Testing Plan](./LOAD_TESTING_PLAN.md):

- **Synthetic Data Generation**: Generate realistic `operations_by_type` dictionaries
- **Direct Callback Invocation**: Bypass firehose client, call `operations_callback` directly
- **Metrics Collection**: Instrument and collect performance metrics
- **Result Export**: Save results in structured format for analysis

## Implementation Structure

### Directory Structure

```
services/sync/stream/load_testing/
├── LOAD_TESTING_PLAN.md          # Overall plan (existing)
├── BASELINE_LOAD_TESTING_SPEC.md  # This file
├── synthetic_data_factory.py      # Generate realistic operations_by_type dicts
├── synthetic_load_generator.py    # Load generator with rate control
├── metrics_collector.py           # Collect and aggregate metrics
├── load_test_runner.py            # Main test orchestration script
└── <YYYY_MM_DD-HH:MM:SS>/        # Output directories (one per test run)
    ├── config.json                # Test configuration used
    └── output.json                # Test results and metrics
```

### Output Directory Format

Each test run creates a timestamped directory:
- **Format**: `YYYY_MM_DD-HH:MM:SS` (e.g., `2024_12_12-14:30:45`)
- **Location**: `services/sync/stream/load_testing/<timestamp>/`
- **Files**:
  - `config.json`: Complete test configuration (rates, durations, ratios, etc.)
  - `output.json`: Test results, metrics, and analysis

## Components

### 1. Synthetic Data Factory

**File**: `synthetic_data_factory.py`

**Purpose**: Generate realistic `operations_by_type` dictionaries matching firehose format.

**Key Requirements** (from [Load Testing Plan](./LOAD_TESTING_PLAN.md)):
- Default to **1:10:4 ratio** (posts:likes:follows) matching production
- Randomly emit events according to specified ratios (not deterministic per commit)
- Generate realistic record structures matching firehose format
- Support configurable distribution for edge case testing
- Support study user vs. generic user ratios

**Interface**:
```python
class SyntheticDataFactory:
    def __init__(self, study_user_ratio: float = 0.1):
        """Initialize factory with study user ratio."""
    
    def generate_commit(
        self,
        record_type_ratios: dict[str, float] | None = None,
    ) -> dict:
        """Generate a single commit's operations_by_type dict.
        
        Args:
            record_type_ratios: Dict mapping record types to ratios.
                Default: {"posts": 1/15, "likes": 10/15, "follows": 4/15}
        
        Returns:
            operations_by_type dict matching firehose format:
            {
                'posts': {'created': [...], 'deleted': []},
                'likes': {'created': [...], 'deleted': []},
                'follows': {'created': [...], 'deleted': []},
                ...
            }
        """
    
    def generate_commits_at_rate(
        self,
        total_rate: float,  # total records/sec
        duration: float,  # seconds
        record_type_ratios: dict[str, float] | None = None,
    ) -> Iterator[dict]:
        """Generate commits at specified rate with proper distribution.
        
        Yields operations_by_type dicts at the specified rate, maintaining
        the record type ratios across the duration.
        """
```

**Implementation Notes**:
- Use `random.choices()` or similar to randomly select record types according to ratios
- Reuse patterns from `tests/mock_firehose_data.py` for realistic record structures
- Generate unique URIs, DIDs, and timestamps for each record
- Ensure records are valid for processing (correct structure, required fields)

### 2. Synthetic Load Generator

**File**: `synthetic_load_generator.py`

**Purpose**: Generate configurable load patterns and invoke `operations_callback` at specified rates.

**Key Requirements**:
- Precise rate control (within 5% of target total rate)
- Maintain record type ratios across all load patterns (1:10:4 by default)
- Support various load patterns (sustained, burst, ramp-up)
- Thread-safe invocation of `operations_callback`
- Graceful shutdown and cleanup

**Interface**:
```python
class SyntheticLoadGenerator:
    def __init__(
        self,
        operations_callback: Callable,
        context: CacheWriteContext,
        metrics_collector: MetricsCollector,
    ):
        """Initialize load generator with callback and context."""
    
    def run_sustained_load(
        self,
        total_rate: float,  # total records/sec
        duration: int,  # seconds
        record_type_ratios: dict[str, float] | None = None,  # defaults to 1:10:4
    ) -> dict:
        """Run sustained load test with specified total rate and ratios.
        
        Returns:
            Summary dict with test results
        """
    
    def run_burst_load(
        self,
        base_rate: float,  # total records/sec
        burst_rate: float,  # total records/sec
        burst_duration: int,  # seconds
        burst_interval: int,  # seconds between bursts
        total_duration: int,  # seconds
        record_type_ratios: dict[str, float] | None = None,  # defaults to 1:10:4
    ) -> dict:
        """Run burst load test maintaining ratios throughout."""
```

**Rate Control Implementation**:
- Use `time.sleep()` to control timing between commits
- Calculate inter-commit delay: `1.0 / commits_per_second`
- Track actual rate and adjust if drift > 5%
- Use `time.perf_counter()` for precise timing

### 3. Metrics Collector

**File**: `metrics_collector.py`

**Purpose**: Collect, aggregate, and export performance metrics.

**Key Metrics** (from [Load Testing Plan](./LOAD_TESTING_PLAN.md)):

**Primary Metrics**:
- **Throughput**: Total records/sec, posts/sec, likes/sec, follows/sec
- **Latency**: p50, p95, p99 callback processing time
- **Error Rates**: Processing errors, failed cache writes, error rate %
- **Resource Usage**: CPU utilization, memory usage, disk I/O, file descriptor count

**Secondary Metrics**:
- Cache directory size (growth rate)
- Records written vs. received (completeness)
- File count

**Interface**:
```python
class MetricsCollector:
    def __init__(self):
        """Initialize metrics collector."""
    
    def record_callback_start(self) -> float:
        """Mark start of callback processing. Returns timestamp."""
    
    def record_callback_end(self, start_time: float, success: bool = True):
        """Record callback processing completion."""
    
    def record_throughput(self, records_by_type: dict[str, int], duration: float):
        """Record throughput metrics.
        
        Args:
            records_by_type: Dict like {"posts": 10, "likes": 100, "follows": 40}
            duration: Time period in seconds
        """
    
    def record_error(self, error_type: str, error_message: str):
        """Record processing error."""
    
    def record_resource_usage(self, cpu: float, memory: float, disk_io: dict):
        """Record system resource usage."""
    
    def get_summary(self) -> dict:
        """Get aggregated metrics summary.
        
        Returns:
            {
                "throughput": {
                    "total_records_per_sec": float,
                    "posts_per_sec": float,
                    "likes_per_sec": float,
                    "follows_per_sec": float,
                },
                "latency": {
                    "p50": float,
                    "p95": float,
                    "p99": float,
                    "mean": float,
                    "min": float,
                    "max": float,
                },
                "errors": {
                    "total_errors": int,
                    "error_rate_percent": float,
                    "errors_by_type": dict[str, int],
                },
                "resources": {
                    "cpu_avg": float,
                    "cpu_peak": float,
                    "memory_avg_mb": float,
                    "memory_peak_mb": float,
                    "disk_writes_per_sec": float,
                    "disk_bytes_per_sec": float,
                },
                "cache": {
                    "total_files": int,
                    "cache_size_mb": float,
                },
            }
        """
```

**Implementation Notes**:
- Use `collections.deque` for latency tracking (efficient append/pop)
- Use `threading.Lock` if needed (though initially single-threaded)
- Sample resource usage periodically (every 1-5 seconds) using `psutil`
- Track file count by scanning cache directory

### 4. Load Test Runner

**File**: `load_test_runner.py`

**Purpose**: Main orchestration script that runs tests and exports results.

**Interface**:
```python
class LoadTestRunner:
    def __init__(
        self,
        output_dir: str | None = None,
        context: CacheWriteContext | None = None,
    ):
        """Initialize test runner.
        
        Args:
            output_dir: Optional output directory. If None, creates timestamped dir.
            context: Optional CacheWriteContext. If None, creates via setup.
        """
    
    def run_baseline_test(
        self,
        total_rate: float = 174.0,  # 1x expected load
        duration: int = 600,  # 10 minutes
        record_type_ratios: dict[str, float] | None = None,
    ) -> dict:
        """Run baseline sustained load test.
        
        Returns:
            Test results dict
        """
    
    def run_test_suite(self) -> dict:
        """Run full test suite (all scenarios from plan).
        
        Returns:
            Combined results from all tests
        """
    
    def export_results(self, results: dict, config: dict) -> str:
        """Export results to output directory.
        
        Args:
            results: Test results dict
            config: Test configuration dict
        
        Returns:
            Path to output directory
        """
```

**Output Directory Creation**:
```python
from datetime import datetime

def create_output_directory(base_dir: str = "load_testing") -> str:
    """Create timestamped output directory.
    
    Returns:
        Path to created directory (e.g., "load_testing/2024_12_12-14:30:45")
    """
    timestamp = datetime.now().strftime("%Y_%m_%d-%H:%M:%S")
    output_dir = os.path.join(base_dir, timestamp)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir
```

## Test Scenarios

The baseline test will run **Scenario 1** from the [Load Testing Plan](./LOAD_TESTING_PLAN.md):

### Baseline Sustained Load

- **Total Rate**: 280 records/sec (1.6x expected average)
- **Distribution**: 1:10:4 ratio (posts:likes:follows)
  - ~19 posts/sec, ~190 likes/sec, ~76 follows/sec
- **Duration**: 10 minutes (600 seconds)
- **Goal**: Establish baseline performance with production-like ratios

**Additional Scenarios** (optional, can be added later):
- Scenario 2: Burst Handling
- Scenario 3: Stress Test
- Scenario 4: Sustained Stability

## Output Format

### config.json

```json
{
  "test_name": "baseline_sustained_load",
  "timestamp": "2024-12-12T14:30:45Z",
  "scenario": {
    "total_rate": 280.0,
    "duration_seconds": 600,
    "record_type_ratios": {
      "posts": 0.0667,
      "likes": 0.6667,
      "follows": 0.2667
    }
  },
  "system": {
    "cpu_count": 8,
    "memory_gb": 16,
    "disk_type": "SSD",
    "python_version": "3.12.0"
  },
  "cache_config": {
    "batching_enabled": false,
    "cache_path": "services/sync/stream/__local_cache__"
  }
}
```

### output.json

```json
{
  "test_summary": {
    "test_name": "baseline_sustained_load",
    "start_time": "2024-12-12T14:30:45Z",
    "end_time": "2024-12-12T14:40:45Z",
    "duration_seconds": 600,
    "status": "completed"
  },
  "throughput": {
    "total_records_processed": 168000,
    "total_records_per_sec": 280.0,
    "posts_per_sec": 18.7,
    "likes_per_sec": 186.5,
    "follows_per_sec": 74.8,
    "target_rate": 280.0,
    "rate_accuracy_percent": 100.0
  },
  "latency": {
    "callback_processing_time_ms": {
      "p50": 45.2,
      "p95": 98.5,
      "p99": 245.3,
      "mean": 52.1,
      "min": 12.3,
      "max": 512.8
    }
  },
  "errors": {
    "total_errors": 5,
    "error_rate_percent": 0.003,
    "errors_by_type": {
      "processing_error": 3,
      "cache_write_error": 2
    }
  },
  "resources": {
    "cpu": {
      "avg_percent": 65.2,
      "peak_percent": 89.5
    },
    "memory": {
      "avg_mb": 245.3,
      "peak_mb": 312.8
    },
    "disk_io": {
      "writes_per_sec": 280.0,
      "bytes_per_sec": 1250000,
      "io_wait_percent": 2.3
    },
    "file_descriptors": {
      "peak": 150
    }
  },
  "cache": {
    "total_files": 168000,
    "cache_size_mb": 125.3,
    "files_per_second": 280.0
  },
  "bottlenecks": [
    {
      "component": "file_io",
      "severity": "high",
      "description": "Synchronous file writes are the primary bottleneck",
      "recommendation": "Implement batching to reduce file I/O overhead"
    }
  ]
}
```

## Usage

### Running Baseline Test

```bash
# From project root
cd services/sync/stream/load_testing

# Run baseline test with default settings
python load_test_runner.py --baseline

# Run with custom settings
python load_test_runner.py \
  --baseline \
  --rate 280 \
  --duration 600 \
  --output-dir custom_output

# Run full test suite
python load_test_runner.py --suite
```

### Command-Line Interface

```python
# load_test_runner.py
import argparse

def main():
    parser = argparse.ArgumentParser(description="Run load tests for stream service")
    parser.add_argument("--baseline", action="store_true", help="Run baseline test")
    parser.add_argument("--suite", action="store_true", help="Run full test suite")
    parser.add_argument("--rate", type=float, default=280.0, help="Total records/sec")
    parser.add_argument("--duration", type=int, default=600, help="Duration in seconds")
    parser.add_argument("--output-dir", type=str, help="Custom output directory")
    
    args = parser.parse_args()
    
    runner = LoadTestRunner(output_dir=args.output_dir)
    
    if args.baseline:
        results = runner.run_baseline_test(
            total_rate=args.rate,
            duration=args.duration,
        )
        config = {...}  # Build config from args
        output_path = runner.export_results(results, config)
        print(f"Results exported to: {output_path}")
    elif args.suite:
        results = runner.run_test_suite()
        # Export suite results
```

## Instrumentation

### Adding Metrics Hooks

To collect metrics, we need to instrument `operations_callback`:

```python
# In load_test_runner.py or metrics_collector.py
from functools import wraps

def instrument_callback(original_callback, metrics_collector):
    """Wrap operations_callback to collect metrics."""
    @wraps(original_callback)
    def wrapped_callback(operations_by_type, context):
        start_time = metrics_collector.record_callback_start()
        try:
            result = original_callback(operations_by_type, context)
            metrics_collector.record_callback_end(start_time, success=True)
            return result
        except Exception as e:
            metrics_collector.record_error("callback_error", str(e))
            metrics_collector.record_callback_end(start_time, success=False)
            raise
    return wrapped_callback
```

### Resource Monitoring

Use `psutil` for system resource monitoring:

```python
import psutil
import time

def collect_resource_metrics(interval: float = 1.0) -> Iterator[dict]:
    """Collect resource metrics periodically."""
    process = psutil.Process()
    while True:
        cpu_percent = process.cpu_percent(interval=interval)
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        
        # Disk I/O (if available)
        try:
            io_counters = process.io_counters()
            disk_writes = io_counters.write_bytes
        except:
            disk_writes = 0
        
        yield {
            "cpu_percent": cpu_percent,
            "memory_mb": memory_mb,
            "disk_writes_bytes": disk_writes,
            "timestamp": time.time(),
        }
```

## Success Criteria

The baseline test should meet these criteria (from [Load Testing Plan](./LOAD_TESTING_PLAN.md)):

✅ **Functional Requirements:**
- System processes all synthetic records without data loss
- All cache writes complete successfully
- Error rate < 0.1%

✅ **Performance Requirements:**
- Sustains 280 total records/sec (1.6x expected average)
- p95 callback latency < 100ms (may not be achievable without batching)
- CPU utilization < 70% under sustained load
- Memory usage stable over test duration

✅ **Observability Requirements:**
- All primary metrics collected and exported
- Results in structured JSON format
- Clear identification of bottlenecks

## Next Steps

1. **Implement Components**: Create the four main components (factory, generator, collector, runner)
2. **Run Baseline Test**: Execute Scenario 1 and collect metrics
3. **Analyze Results**: Identify bottlenecks and performance characteristics
4. **Document Findings**: Create baseline performance report
5. **Implement Batching**: Proceed with [Issue #251](https://github.com/METResearchGroup/bluesky-research/issues/251)
6. **Re-test with Batching**: Run same tests and compare results

## Dependencies

- `psutil`: System resource monitoring
- `time`, `datetime`: Timing and timestamp generation
- `json`: Result export
- `os`, `pathlib`: File system operations
- Existing stream service components (for context setup)

## References

- [Load Testing Plan](./LOAD_TESTING_PLAN.md) - Overall approach and scenarios
- [Issue #252](https://github.com/METResearchGroup/bluesky-research/issues/252) - Baseline load testing
- [Issue #251](https://github.com/METResearchGroup/bluesky-research/issues/251) - Batch writes implementation
