# Scalable Backfill System Design V1

## Overview

This document outlines a distributed architecture for scaling the Bluesky backfill sync process from handling ~2,300 users to ~400,000 users efficiently. The current implementation processes 2,300 users in approximately 30 minutes, but scaling linearly would require ~100 hours for the full dataset, which is impractical and lacks necessary resilience.

## Architecture Components

![Architecture Diagram](../diagrams/architecture_v1.png)

### 1. Coordinator Service

The Coordinator is responsible for:
- Breaking down the full user list into manageable chunks
- Assigning work to available workers
- Tracking overall progress
- Handling retry logic for failed tasks
- Ensuring rate limits are respected globally

### 2. Worker Pool

Workers are distributed compute units that:
- Process assigned batches of DIDs
- Report progress back to the coordinator
- Handle individual user data retrieval and transformation
- Implement local rate limiting and backoff strategies
- Store checkpoints after completing each user

### 3. Shared State Store

A Redis-based state store that provides:
- Persistent job state tracking
- Atomic operations for coordination
- Distributed locking for critical sections
- Queue management for task distribution
- Fast in-memory operations with persistence

### 4. Rate Limit Manager

A service that:
- Maintains global rate limit budget (3,000 requests/5min per account)
- Distributes request quotas to workers
- Tracks PDS endpoints to optimize request distribution
- Alternates between available accounts to maximize throughput

### 5. Monitoring Dashboard

A real-time monitoring system that shows:
- Overall progress (% complete)
- Current throughput (users/minute)
- Error rates and types
- Estimated completion time
- Resource utilization across the cluster

## Proof of Concept Implementation

The proof of concept implements a simplified version of this architecture using:
- Redis for coordination and state management
- Python's multiprocessing for local worker pool simulation
- A mock PDS client with realistic rate limiting
- Basic monitoring and metrics collection

### Components Included in PoC

1. **Coordinator Script** (`coordinator.py`)
   - Divides work into chunks
   - Manages the task queue in Redis
   - Tracks global progress
   - Handles basic failure recovery

2. **Worker Script** (`worker.py`)
   - Pulls tasks from Redis queue
   - Processes users with existing backfill logic
   - Implements checkpointing
   - Reports progress metrics

3. **Rate Limiter** (`rate_limiter.py`)
   - Implements token bucket algorithm
   - Coordinates across workers
   - Handles PDS endpoint-specific limits

4. **Monitoring Script** (`monitor.py`)
   - Collects metrics from Redis
   - Displays real-time progress
   - Estimates completion time

### Key Metrics

1. **Throughput**
   - Users processed per minute
   - Records processed per minute
   - API requests per minute

2. **Reliability**
   - Success rate (% of users successfully processed)
   - Retry rate (% of tasks that required retries)
   - Error distribution by type

3. **Efficiency**
   - CPU utilization per worker
   - Memory usage per worker
   - Redis operation latency
   - Network I/O per user

4. **Progress**
   - Percentage of total users completed
   - Estimated time to completion
   - Rate limit utilization

## Success Criteria for PoC

The proof of concept will be considered successful if it demonstrates:

1. **Scalability**: Can process 3,000 users significantly faster than the current implementation when using equivalent resources
2. **Resilience**: Can recover from simulated failures (worker crashes, network issues)
3. **Observability**: Provides clear visibility into progress and bottlenecks
4. **Rate Limit Compliance**: Efficiently utilizes available rate limits without triggering blocks
5. **Data Accuracy**: Produces identical output to the current implementation

## Potential Bottlenecks

1. **Redis Performance**
   - If Redis becomes a bottleneck, consider partitioning or using a more distributed solution
   - Monitor Redis memory usage and operation latency

2. **Network I/O**
   - PDS API calls may dominate execution time
   - Consider implementing connection pooling and keep-alive connections

3. **Task Granularity**
   - Too fine-grained tasks increase coordination overhead
   - Too coarse-grained tasks reduce resilience and load balancing
   - Experiment with different batch sizes

4. **Rate Limit Management**
   - Central rate limiter could become a synchronization bottleneck
   - Consider more sophisticated quota allocation strategies

5. **State Store Size**
   - Checkpointing every user might grow the state store rapidly
   - Implement cleanup and compaction strategies

## Out of Scope for Initial PoC

1. **Full Slurm Integration**: The PoC will simulate distributed workers locally
2. **Sophisticated Failure Recovery**: Only basic retry logic will be implemented
3. **Advanced Monitoring**: Basic metrics will be collected, but no full dashboard
4. **Multi-Account Optimization**: Will demonstrate with a single account approach
5. **Security Measures**: Authentication and authorization will be minimal

## Next Steps After PoC

1. Evaluate performance metrics against success criteria
2. Identify actual bottlenecks from observed behavior
3. Refine architecture based on findings
4. Scale test to a larger subset (~30,000 users)
5. Implement full Slurm integration
6. Add comprehensive monitoring dashboard
7. Implement multi-account strategy

## Implementation Timeline

1. **PoC Development**: 1-2 days
2. **Initial Testing**: 1 day
3. **Performance Tuning**: 1-2 days
4. **Documentation & Analysis**: 1 day

Total: 4-6 days from concept to analyzed results 