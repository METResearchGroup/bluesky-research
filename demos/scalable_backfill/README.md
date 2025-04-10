# Scalable Backfill System

A distributed system for efficiently backfilling large numbers of Bluesky user data with built-in resilience, monitoring, and rate limit management.

## Overview

This system is designed to scale the backfill process from thousands to hundreds of thousands of users while providing:

- Distributed processing across multiple workers
- Efficient rate limit management
- Checkpointing and resumability
- Real-time monitoring and observability
- Failure recovery

## Architecture

The system uses a coordinator-worker architecture with Redis as a shared state store:

- **Coordinator**: Manages work distribution and tracks overall progress
- **Workers**: Process batches of users in parallel
- **Rate Limiter**: Ensures compliance with API rate limits
- **Monitor**: Provides real-time visibility into the process

For detailed architecture information, see [SYSTEM_DESIGN_V1.md](system_designs/SYSTEM_DESIGN_V1.md).

## Prerequisites

- Python 3.10+
- Redis server
- Existing Bluesky backfill code
- Python dependencies (specified in requirements.txt)

## Installation

1. Clone the repository
2. Install the dependencies:

```bash
pip install -r demos/scalable_backfill/requirements.txt
```

3. Ensure Redis is running:

```bash
redis-server
```

## Usage

### 1. Start the Coordinator

```bash
python demos/scalable_backfill/coordinator.py --input-file path/to/dids.txt --batch-size 50
```

### 2. Start Workers (can be on multiple machines)

```bash
python demos/scalable_backfill/worker.py --worker-id worker1 --coordinator-host localhost
```

### 3. Monitor Progress

```bash
python demos/scalable_backfill/monitor.py
```

### 4. Run Evaluation

After completion, evaluate the performance:

```bash
python demos/scalable_backfill/evaluate.py --output-dir results
```

## Configuration

The system can be configured via command-line arguments or a config file:

- `--batch-size`: Number of DIDs to process in each batch (default: 50)
- `--worker-count`: Number of workers to spawn locally (default: CPU count)
- `--redis-host`: Redis server hostname (default: localhost)
- `--redis-port`: Redis server port (default: 6379)
- `--rate-limit`: Maximum API requests per 5-minute window (default: 3000)
- `--checkpoint-interval`: How often to save progress (default: after each user)

See `--help` on each script for full options.

## Testing the System

For testing with a small dataset:

```bash
# Generate test data (1000 mock DIDs)
python demos/scalable_backfill/generate_test_data.py --count 1000

# Run the full system with simulated PDS responses
python demos/scalable_backfill/run_test.py --simulated --workers 4
```

## Component Documentation

- [Coordinator](docs/coordinator.md): Work distribution and progress tracking
- [Worker](docs/worker.md): DID processing and data export
- [Rate Limiter](docs/rate_limiter.md): API quota management
- [Monitor](docs/monitor.md): Real-time metrics and visualization
- [Evaluation](docs/evaluation.md): Performance analysis

## Benchmarking

Initial benchmarks with 3,000 users:

| Configuration | Workers | Completion Time | Throughput (users/min) |
|---------------|---------|-----------------|------------------------|
| Single process| 1       | ~30 minutes     | ~100                   |
| Distributed   | 4       | ~10 minutes     | ~300                   |
| Distributed   | 8       | ~6 minutes      | ~500                   |

Note: Actual performance will depend on hardware, network conditions, and PDS response times.

## Future Improvements

- Slurm integration for HPC environments
- Adaptive rate limiting based on PDS response times
- Multi-account support to increase throughput
- Advanced failure recovery strategies

## License

MIT
