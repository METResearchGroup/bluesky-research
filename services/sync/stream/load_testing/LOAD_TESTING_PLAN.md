# Load Testing Plan for Firehose Sync Stream Service

## Overview

This document outlines a phased approach to load testing the firehose sync stream service. The service processes real-time events from the Bluesky firehose, transforms them, and writes to a local JSON cache. Expected production load is approximately **15M records/day** (~174 records/sec average, with significant burst potential), distributed as:
- **1M posts/day** (~11.6 posts/sec)
- **10M likes/day** (~115.7 likes/sec)
- **4M follows/day** (~46.3 follows/sec)

This represents a **1:10:4 ratio** (posts:likes:follows).

## Architecture Context

The service has a two-phase architecture:
1. **Real-time Cache Writes**: Synchronous processing in `operations_callback` that writes JSON files to local cache
2. **Batch Exports**: Asynchronous cron job that reads cache and exports to Parquet (out of scope for initial load testing)

**Key Processing Path:**
```
Firehose Client → on_message_handler → operations_callback → 
  Record Processors → Handlers → File I/O (JSON cache writes)
```

## Phased Approach

### Phase 1: Synthetic Load Testing (Foundation)

**Goal**: Establish baseline performance metrics and identify bottlenecks without external dependencies.

**Approach**: Direct callback invocation with synthetic data generator
- Bypass firehose client entirely
- Generate synthetic `operations_by_type` dictionaries
- Invoke `operations_callback` directly from a load generator
- Control rate, burst patterns, and data distribution

**Why this approach first:**
- Fastest to implement and iterate
- Full control over load patterns
- No external dependencies (no real firehose connection needed)
- Easy to reproduce and debug
- Can test edge cases and failure modes

**Implementation Strategy:**
1. Create synthetic data generator that produces realistic `operations_by_type` dicts
2. Build load generator with configurable:
   - Sustained rate (records/sec)
   - Burst patterns (spike duration, magnitude)
   - Record type distribution (posts, likes, follows, etc.)
   - Study user vs. generic user ratios
3. Instrument `operations_callback` and downstream components with timing/metrics
4. Run tests with increasing load until we hit bottlenecks

**Deliverables:**
- `synthetic_load_generator.py`: Configurable load generator
- `synthetic_data_factory.py`: Factory for generating realistic operations_by_type dicts
- `load_test_runner.py`: Test orchestration and metrics collection
- Baseline performance report

---

### Phase 2: Enhanced Synthetic Testing (Stress & Edge Cases)

**Goal**: Validate system behavior under extreme conditions and edge cases.

**Approach**: Extend Phase 1 with:
- Stress testing (2x, 5x, 10x expected load)
- Sustained load testing (hours-long runs)
- Memory leak detection
- File system saturation scenarios
- Error injection (malformed records, I/O failures)

**Deliverables:**
- Stress test suite
- Long-running stability tests
- Resource usage profiling (memory, CPU, disk I/O)

---

### Phase 3: Real Firehose Integration Testing

**Goal**: Validate behavior with actual firehose data and network conditions.

**Approach**: Hybrid - real firehose + instrumentation
- Connect to real Bluesky firehose
- Add comprehensive instrumentation and metrics collection
- Implement rate limiting/throttling to control test conditions
- Monitor behavior under real-world data patterns

**Why graduate to this:**
- Validates assumptions about data distribution
- Tests network resilience and reconnection logic
- Exposes real-world edge cases in data
- Validates cursor management under load

**Implementation Strategy:**
1. Add metrics/monitoring hooks throughout the pipeline
2. Create test mode that can throttle/limit processing rate
3. Run controlled tests with real firehose connection
4. Compare results with Phase 1/2 synthetic tests

**Deliverables:**
- Real firehose test harness
- Comparison report (synthetic vs. real)
- Production readiness assessment

---

## Metrics to Measure

### Primary Metrics (Must Have)

#### 1. Throughput
- **Total records processed per second** (all record types combined)
- **Posts processed per second** (target: ~11.6/sec average)
- **Likes processed per second** (target: ~115.7/sec average)
- **Follows processed per second** (target: ~46.3/sec average)
- **Operations processed per second** (created + deleted)
- **Cache writes per second**

**Target**: 
- Sustain **174+ total records/sec** average (maintaining 1:10:4 ratio)
- Handle bursts of **500-1000 total records/sec** (maintaining ratio)
- Individual record type throughput should maintain expected ratios

#### 2. Latency
- **End-to-end latency**: Time from firehose message arrival to cache write completion
- **Callback processing time**: Time spent in `operations_callback`
- **Per-record processing time**: Time per individual record (p50, p95, p99)
- **File I/O latency**: Time for individual cache writes

**Target**: 
- p95 callback processing time < 100ms
- p99 callback processing time < 500ms
- No sustained latency degradation under load

#### 3. Error Rates
- **Processing errors per second**
- **Failed cache writes**
- **Exception counts by type**
- **Error rate as % of total records**

**Target**: < 0.1% error rate under normal load, < 1% under stress

#### 4. Resource Usage
- **CPU utilization** (average, peak)
- **Memory usage** (RSS, heap, file handles)
- **Disk I/O** (writes/sec, bytes/sec, I/O wait time)
- **File descriptor count**

**Target**: 
- CPU < 80% average under sustained load
- Memory growth stable (no leaks)
- Disk I/O not saturated

### Secondary Metrics (Should Have)

#### 5. Backpressure Indicators
- **Queue depth** (if we add queuing in future)
- **Processing lag** (time behind real-time)
- **Cache directory size** (growth rate)

#### 6. Data Quality
- **Records written vs. records received** (completeness)
- **Cache file integrity** (valid JSON, correct structure)
- **Path construction correctness**

#### 7. System Health
- **Thread health** (if we add threading)
- **Connection stability** (reconnects, timeouts)
- **Cursor update frequency** (should match `cursor_update_frequency`)

---

## Success Criteria

### Phase 1 Success Criteria

✅ **Functional Requirements:**
- System processes all synthetic records without data loss
- All cache writes complete successfully
- No exceptions in processing path (or < 0.1% error rate)
- Cache file structure matches expected format

✅ **Performance Requirements:**
- Sustains **280 total records/sec** sustained load (1.6x expected average, maintaining 1:10:4 ratio)
  - ~19 posts/sec, ~190 likes/sec, ~76 follows/sec
- Handles **870 total records/sec** burst for 30 seconds without degradation (5x expected, maintaining ratio)
  - ~58 posts/sec, ~580 likes/sec, ~232 follows/sec
- p95 callback latency < 100ms
- p99 callback latency < 500ms
- CPU utilization < 70% under sustained load
- Memory usage stable over 1-hour test run

✅ **Observability Requirements:**
- All primary metrics collected and reportable
- Metrics exportable to CSV/JSON for analysis
- Clear identification of bottlenecks

### Phase 2 Success Criteria

✅ **Stress Requirements:**
- Handles **5x expected load** (870 total records/sec, maintaining 1:10:4 ratio) for 5 minutes
  - ~58 posts/sec, ~580 likes/sec, ~232 follows/sec
- Handles **10x expected load** (1740 total records/sec, maintaining ratio) for 1 minute
  - ~116 posts/sec, ~1160 likes/sec, ~464 follows/sec
- Graceful degradation (errors logged, no crashes)
- System recovers within 30 seconds after stress test

✅ **Stability Requirements:**
- 4-hour sustained load test completes without memory leaks
- No file descriptor leaks
- Cache directory size growth predictable

### Phase 3 Success Criteria

✅ **Real-World Validation:**
- Performance matches or exceeds Phase 1 synthetic results
- Handles real firehose data patterns without issues
- Network reconnection logic works correctly
- Cursor management stable under load

---

## Implementation Details

### Phase 1 Implementation

#### Synthetic Data Generator

```python
# load_testing/synthetic_data_factory.py
class SyntheticDataFactory:
    """Generates realistic operations_by_type dictionaries for load testing."""
    
    def generate_commit(
        self,
        record_type_ratios: dict[str, float] | None = None,
        study_user_ratio: float = 0.1,
    ) -> dict:
        """Generate a single commit's operations_by_type dict.
        
        Args:
            record_type_ratios: Dict mapping record types to ratios.
                Default: {"posts": 1/15, "likes": 10/15, "follows": 4/15}
            study_user_ratio: Ratio of study users to generic users
        """
        # Implementation details...
    
    def generate_commits_at_rate(
        self,
        total_rate: float,  # total records/sec
        duration: float,  # seconds
        record_type_ratios: dict[str, float] | None = None,
    ) -> Iterator[dict]:
        """Generate commits at specified rate with proper distribution."""
        # Implementation details...
```

**Requirements:**
- Generate realistic record structures matching firehose format
- **Default to 1:10:4 ratio (posts:likes:follows)** matching production expectations
- Support configurable distribution of record types (for testing edge cases)
- Support study user vs. generic user ratios
- Generate realistic URIs, DIDs, timestamps
- **Randomly emit events according to specified ratios** (not deterministic per commit)

#### Load Generator

```python
# load_testing/synthetic_load_generator.py
class SyntheticLoadGenerator:
    """Generates configurable load patterns for testing."""
    
    def run_sustained_load(
        self,
        total_rate: float,  # total records/sec
        duration: int,  # seconds
        record_type_ratios: dict[str, float] | None = None,  # defaults to 1:10:4
    ):
        """Run sustained load test with specified total rate and ratios."""
        
    def run_burst_load(
        self,
        base_rate: float,  # total records/sec
        burst_rate: float,  # total records/sec
        burst_duration: int,
        total_duration: int,
        record_type_ratios: dict[str, float] | None = None,  # defaults to 1:10:4
    ):
        """Run burst load test maintaining ratios throughout."""
```

**Requirements:**
- Precise rate control (within 5% of target total rate)
- **Maintain record type ratios across all load patterns** (1:10:4 by default)
- **Randomly distribute record types per commit** according to ratios (not deterministic)
- Support for various load patterns (constant, ramp-up, burst, sine wave)
- Thread-safe invocation of `operations_callback`
- Graceful shutdown and cleanup

#### Metrics Collection

```python
# load_testing/metrics_collector.py
class MetricsCollector:
    """Collects and aggregates performance metrics."""
    
    def record_callback_latency(self, duration: float):
        """Record callback processing time."""
    
    def record_throughput(self, records: int, duration: float):
        """Record throughput metrics."""
    
    def get_summary(self) -> dict:
        """Get aggregated metrics summary."""
```

**Requirements:**
- Low-overhead metrics collection (minimal impact on performance)
- Thread-safe
- Export to CSV/JSON
- Real-time metrics display (optional)

#### Test Runner

```python
# load_testing/load_test_runner.py
class LoadTestRunner:
    """Orchestrates load tests and generates reports."""
    
    def run_test_suite(self):
        """Run full test suite and generate report."""
```

**Requirements:**
- Run multiple test scenarios
- Collect system metrics (CPU, memory, disk I/O)
- Generate comprehensive reports
- Compare results across runs

---

## Test Scenarios

### Scenario 1: Baseline Sustained Load
- **Total Rate**: 280 records/sec (1.6x expected average)
- **Distribution**: 1:10:4 ratio (posts:likes:follows)
  - ~19 posts/sec, ~190 likes/sec, ~76 follows/sec
- **Duration**: 10 minutes
- **Goal**: Establish baseline performance with production-like ratios

### Scenario 2: Burst Handling
- **Base rate**: 174 records/sec (1x expected, maintaining 1:10:4 ratio)
  - ~12 posts/sec, ~116 likes/sec, ~46 follows/sec
- **Burst**: 870 records/sec (5x expected, maintaining ratio) for 30 seconds, every 2 minutes
  - ~58 posts/sec, ~580 likes/sec, ~232 follows/sec
- **Duration**: 10 minutes
- **Goal**: Validate burst handling while maintaining record type ratios

### Scenario 3: Stress Test
- **Total Rate**: 870 records/sec (5x expected, maintaining 1:10:4 ratio)
  - ~58 posts/sec, ~580 likes/sec, ~232 follows/sec
- **Duration**: 5 minutes
- **Goal**: Find breaking point under production-like distribution

### Scenario 4: Sustained Stability
- **Total Rate**: 174 records/sec (1x expected, maintaining 1:10:4 ratio)
  - ~12 posts/sec, ~116 likes/sec, ~46 follows/sec
- **Duration**: 4 hours
- **Goal**: Detect memory leaks, stability issues under realistic load

### Scenario 5: Extreme Stress Test
- **Total Rate**: 1740 records/sec (10x expected, maintaining 1:10:4 ratio)
  - ~116 posts/sec, ~1160 likes/sec, ~464 follows/sec
- **Duration**: 1 minute
- **Goal**: Validate system behavior at extreme load

### Scenario 6: Skewed Distribution (Edge Case)
- **Distribution**: 80% likes, 15% posts, 5% follows (stress test for like-heavy scenarios)
- **Total Rate**: 200 records/sec
- **Duration**: 10 minutes
- **Goal**: Validate system handles non-standard distributions gracefully

---

## Reporting

### Test Report Format

Each test run should generate a report including:

1. **Test Configuration**
   - Load pattern (rate, duration, record distribution)
   - System configuration (CPU, memory, disk)

2. **Results Summary**
   - Total records processed (and breakdown by type: posts, likes, follows)
   - Throughput (avg, min, max) - both total and per record type
   - Latency percentiles (p50, p95, p99)
   - Error counts and rates (by record type)
   - Resource usage (CPU, memory, disk I/O)
   - Record type distribution verification (should match 1:10:4 ratio)

3. **Bottleneck Analysis**
   - Identified bottlenecks
   - Recommendations for optimization

4. **Comparison** (for Phase 3)
   - Synthetic vs. real firehose performance
   - Regression analysis vs. previous runs

---

## Next Steps

1. **Create Phase 1 implementation** (synthetic load generator)
2. **Add instrumentation hooks** to `operations_callback` and handlers
3. **Run initial baseline tests** to establish current performance
4. **Iterate on optimizations** based on findings
5. **Graduate to Phase 2** once Phase 1 success criteria met
6. **Graduate to Phase 3** once Phase 2 success criteria met

---

## Open Questions

- Should we test batch export performance separately, or as part of integrated tests?
- Do we need to test cursor management under load? (S3/DynamoDB writes)
- Should we test with different cache directory structures (many small files vs. fewer large files)?
- Do we need to test concurrent batch export + cache write scenarios?

---

## References

- Expected throughput: 
  - **1M posts/day** (~11.6 posts/sec)
  - **10M likes/day** (~115.7 likes/sec)
  - **4M follows/day** (~46.3 follows/sec)
  - **15M total records/day** (~173.6 records/sec)
  - Ratio: 1:10:4 (posts:likes:follows)
- Current cursor update frequency: 20,000 commits (from firehose.py)
- Architecture: Two-phase (cache writes + batch exports)

