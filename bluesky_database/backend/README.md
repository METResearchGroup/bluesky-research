# Redis Backend Setup

A comprehensive Redis setup for the Bluesky database backend with educational implementations and practical testing tools.

## What's Included

- **Redis Server**: Production-ready Redis configuration and Docker setup
- **Educational Implementation**: Python implementation of Redis core functionality for learning
- **Implementation Comparison**: Detailed comparison of Redis Lists vs Redis Streams for data pipelines
- **Testing Tools**: Comprehensive test scripts for Redis operations
- **Local Development**: Simple scripts to run Redis locally

## Quick Start

### Option 1: Automated Setup (Recommended)
Run everything with a single command:
```bash
./run_load_test.sh
```

This script will:
- Check Docker and conda environment
- Install all dependencies
- Start Redis container
- Test Redis functionality
- Run the complete Jetstream load test
- Show results and generated files

**Note**: The script includes robust error handling and debugging features:
- Automatic port conflict detection and resolution
- Enhanced Redis connection parameters for Docker environments
- Comprehensive prerequisite checking (Docker, Docker Compose, conda)
- Graceful cleanup and container management

### Option 2: Manual Setup
If you prefer to run steps individually:

#### 1. Start Redis Server
```bash
./start_local.sh
```

#### 2. Activate Conda Environment
```bash
conda activate bluesky-research
```

#### 3. Install Dependencies
```bash
uv pip install -r requirements.txt
```

#### 4. Test Redis Connection
```bash
python test_redis_server.py
```

#### 5. Run Jetstream Load Test
```bash
python jetstream_load_test.py
```

## Files Overview

### Core Infrastructure
- `docker-compose.yml` - Redis container configuration with volume mounting
- `redis.conf` - Redis server configuration optimized for buffer operations
- `start_local.sh` - Script to start Redis locally with Docker Compose
- `requirements.in` & `requirements.txt` - Python dependencies for Redis backend

### Testing & Development
- `test_redis_server.py` - Basic Redis connection and functionality test script
- `jetstream_load_test.py` - Complete Bluesky Jetstream firehose simulation with Redis Streams
- `run_load_test.sh` - Complete automation script for running the entire load test
- `.gitignore` - Git ignore rules for data files, logs, and temporary files

## Docker Setup

### Redis Container
The `docker-compose.yml` file provides a minimal Redis setup:

- **Redis 7.2-alpine** - Lightweight Redis container
- **Port 6379** - Bound to localhost only for security
- **Volume mounting** - Data persistence and Parquet file access
- **Health checks** - Automatic container health monitoring

### Configuration
The `redis.conf` file is optimized for the Bluesky data pipeline:

- **2GB memory limit** - Sufficient for 8-hour buffer capacity
- **AOF persistence** - Data durability with `appendfsync everysec`
- **LRU eviction** - `allkeys-lru` policy for buffer management
- **Stream optimization** - Configured for high-throughput stream operations

### Volume Mounting
The Docker setup mounts volumes for data persistence:

```yaml
volumes:
  - ./data/redis:/data          # Redis persistence
  - ./data:/app/data            # Parquet file output
```

This allows the Jetstream load test to write Parquet files to `./data/` which persists on the host.

## Learning Redis

### Redis Testing
The `test_redis_server.py` script tests basic Redis functionality:

1. **Connection test** - Verify Redis is accessible
2. **Basic operations** - Test SET/GET/DELETE operations
3. **Redis Streams** - Test stream operations for Jetstream load test
4. **Configuration validation** - Verify Redis settings

```bash
python test_redis_server.py
```

**Sample Output:**
```
🚀 Redis Server Test Script
==================================================
✅ Redis connection successful
📊 Redis version: 7.2.0
📊 Connected clients: 1
📊 Used memory: 890.36K
📊 Max memory: 2.0G

🧪 Testing basic operations...
✅ SET/GET operations work
✅ DELETE operations work

🧪 Testing Redis Streams...
✅ Added message to stream: 1703123456789-0
✅ Stream read operations work
✅ Stream cleanup successful

🎉 All Redis tests passed!
```
🔍 Starting retrieval job...
📊 Retrieving 100 records from Redis...
✅ Successfully retrieved 100 records

==================== STEP 4: VERIFY RECORDS ====================
📈 Retrieval Results:
----------------------------------------
Records retrieved: 100
Records still in Redis: 100

🔍 Verifying records still exist in Redis:
  record:001: ✅ Exists
  record:002: ✅ Exists
  record:003: ✅ Exists

📊 Retrieved Data Statistics:
  Unique users: 10
  Unique categories: 5
  Most common user: user_2 (10 records)
  Most common category: category_2 (20 records)

==================== STEP 5: DELETE RECORDS ====================
🗑️ Deleting 100 records from Redis...
✅ Successfully deleted 100 records

==================== STEP 6: VERIFY DELETION ====================
🔍 Verifying deletion:
------------------------------
Records deleted: 100
Records remaining in Redis: 0
✅ All checked keys have been successfully deleted

📊 Final Redis State:
  Total keys in Redis: 0
  Database empty: Yes

==================== FINAL SUMMARY ====================
📊 Test Results:
  Records written: 100
  Records retrieved: 100
  Records deleted: 100
  Final Redis size: 0
  Test completed: ✅ Success

🔌 Redis connection closed
```

## Jetstream Load Testing System

The `jetstream_load_test.py` script provides a complete simulation of the Bluesky Jetstream firehose with a production-ready data pipeline using Redis Streams. This is the most comprehensive test of the Redis backend infrastructure.

### What It Does

The system simulates a complete Bluesky data pipeline:

1. **Jetstream Firehose Simulator** - Generates realistic Bluesky events in bursts
2. **Data Ingestion** - Receives events and writes to Redis Streams
3. **Redis Streams** - Buffers the data with consumer groups
4. **Data Writers** - Parallel jobs that consume from streams and write to Parquet
5. **Telemetry** - Real-time monitoring of the entire pipeline

### Supported Event Types

The system generates realistic events for all major Bluesky collections:
- `app.bsky.feed.post` - User posts
- `app.bsky.feed.like` - Post likes
- `app.bsky.feed.repost` - Post reposts
- `app.bsky.graph.follow` - User follows
- `app.bsky.graph.block` - User blocks

### Running the Load Test

```bash
# Start Redis first
./start_local.sh

# Activate environment
conda activate bluesky-research

# Install dependencies
uv pip install redis polars

# Run the load test
python jetstream_load_test.py
```

### Live Telemetry Output

The system provides real-time telemetry showing the pipeline performance:

```
🚀 Jetstream Load Test - Live Telemetry
============================================================
⏱️  Elapsed: 35.1s
📊 Events Published: 1,750
📥 Events Ingested: 1,750
💾 Events Written: 1,701
❌ Errors: 0

📈 Rates (events/sec):
   Publish: 49.8
   Ingest: 49.8
   Write: 48.4

📋 Collection Breakdown:
   app.bsky.feed.post: 1,000
   app.bsky.feed.like: 750

🔄 Press Ctrl+C to stop...
```

### Generated Data Structure

The system creates organized Parquet files:

```
data/2025_08_07/
├── app.bsky.feed.post/
│   └── data_20250807_125807_0.parquet
├── app.bsky.feed.like/
│   └── data_20250807_125807_1.parquet
├── app.bsky.feed.repost/
│   └── data_20250807_125807_2.parquet
├── app.bsky.graph.follow/
│   └── data_20250807_125807_3.parquet
└── app.bsky.graph.block/
    └── data_20250807_125807_4.parquet
```

### Performance Characteristics

**Default Configuration:**
- **Events per collection**: 1,000
- **Burst size**: 50 events
- **Publish rate**: ~50 events/second
- **Parallel writers**: 5 (one per collection)
- **Buffer size**: 100 events before writing to Parquet
- **Redis Stream retention**: Last 100,000 messages per stream

**Sample Performance Results:**
```
📊 FINAL STATISTICS
============================================================
Total Events Published: 5,000
Total Events Ingested: 5,000
Total Events Written: 5,000
Total Errors: 0
Average Publish Rate: 49.8 events/sec
Average Ingest Rate: 49.8 events/sec
Average Write Rate: 48.4 events/sec
```

### System Architecture

```
JetstreamSimulator → Queue → DataIngestion → Redis Streams → DataWriters → Parquet Files
                                    ↓
                              TelemetryCollector
```

**Key Components:**

1. **JetstreamSimulator**: Generates realistic Bluesky events with proper URIs, CIDs, and metadata
2. **DataIngestion**: Thread-safe ingestion that writes to Redis Streams with automatic trimming
3. **Redis Streams**: Buffers data with consumer groups for parallel processing
4. **DataWriter**: Parallel writers that consume from streams and batch write to Parquet
5. **TelemetryCollector**: Real-time monitoring with per-second statistics

### Error Handling & Resilience

- **Graceful shutdown**: Handles Ctrl+C with proper cleanup
- **Error tracking**: Comprehensive error counting and logging
- **Message acknowledgment**: Redis Streams ensure no data loss
- **Buffer management**: Automatic flushing of pending data
- **Consumer groups**: Handles multiple writers per collection

### Customization Options

You can modify the script to test different scenarios:

```python
# In the main() function, adjust these parameters:
simulator.run_simulation(
    events_queue, 
    events_per_collection=1000,  # Total events per collection
    burst_size=50                # Events per burst
)
```

## Implementation Comparison

The `implementation_comparison.py` file provides a detailed comparison of using Redis Lists versus Redis Streams for data pipeline buffers. This is crucial for understanding the trade-offs in your Bluesky data pipeline implementation.

### Key Differences:

**Redis Lists (Current Approach):**
- ✅ Simple to implement
- ❌ No built-in acknowledgment
- ❌ Risk of data loss during failures
- ❌ Manual tracking of processed messages
- ❌ No consumer groups

**Redis Streams (Recommended):**
- ✅ Automatic message acknowledgment
- ✅ Consumer groups for parallel processing
- ✅ Better memory management with trimming
- ✅ Built-in deduplication
- ✅ Pending message handling for failures

```bash
python implementation_comparison.py
```

## Troubleshooting

### Redis Connection Issues
```bash
# Check if Redis is running
docker compose ps

# Check Redis logs
docker compose logs redis

# Restart Redis
docker compose restart redis
```

### Port Conflicts
If you encounter port conflicts (e.g., "Ports are not available: exposing port TCP 127.0.0.1:6379"):
```bash
# Check what's using port 6379
lsof -i :6379

# Stop any existing Redis processes
brew services stop redis  # If using Homebrew Redis
kill -9 <PID>            # For other processes

# Clean up Docker containers
docker compose down
```

### Enhanced Connection Parameters
The test script uses robust Redis connection parameters optimized for Docker environments:
- **Longer timeouts**: 10s instead of 5s for better reliability
- **Retry on timeout**: Automatic retry for transient connection issues
- **Health check interval**: 30s health checks for connection stability
- **Docker-aware configuration**: Optimized for containerized Redis instances

### Debugging Features
The `run_load_test.sh` script includes comprehensive debugging capabilities:
- **Prerequisite validation**: Checks for Docker, Docker Compose, and conda
- **Port conflict detection**: Automatically identifies and reports port conflicts
- **Connection testing**: Validates Redis connectivity before running tests
- **Graceful error handling**: Provides clear error messages and recovery steps
- **Container management**: Proper cleanup and container lifecycle management

### Python Dependencies
```bash
# Reinstall Redis client
uv pip install --force-reinstall redis

# Check installed packages
uv pip list | grep redis
```

### Test Redis Connection
```bash
# Basic connection test
redis-cli ping

# Check Redis info
redis-cli info keyspace
```

## Development Workflow

### Automated Workflow (Recommended)
1. **Run Everything**: `./run_load_test.sh`
2. **View Results**: Check `./data/` for generated Parquet files
3. **Stop Redis**: `docker-compose down` (when done)

### Manual Workflow
1. **Start Redis**: `./start_local.sh`
2. **Activate Environment**: `conda activate bluesky-research`
3. **Install Dependencies**: `uv pip install -r requirements.txt`
4. **Test Redis**: `python test_redis_server.py`
5. **Run Load Test**: `python jetstream_load_test.py`
6. **View Data**: Check `./data/` for generated Parquet files

## Next Steps

This foundation provides:
1. ✅ Basic Redis setup
2. ✅ Educational implementation
3. ✅ Comprehensive testing
4. ✅ Implementation comparison
5. ✅ Complete load testing system
6. 🔄 Database schema design
7. 🔄 API endpoints
8. 🔄 Data models
9. 🔄 Business logic 