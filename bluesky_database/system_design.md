# Bluesky Firehose Data Pipeline System Design

## Overview

This document outlines the system design for a high-performance, scalable data pipeline to ingest and process the Bluesky firehose data. The system replaces the current study-specific, file-based approach with a generic, Redis-buffered architecture optimized for real-time analytics and query performance.

## System Architecture

### High-Level Design

```
Jetstream Firehose → Redis Buffer → Batch Processing → Parquet Storage → Query Engine
```

### Key Components

- **Redis Buffer Layer**: High-performance in-memory buffer for real-time data ingestion
- **Batch Processing Service**: Scheduled service that processes buffered data every 5 minutes
- **Parquet Storage Layer**: Optimized columnar storage with intelligent partitioning
- **Query Engine**: Fast analytics queries against the partitioned data

## Data Volume Estimates

### Real-time Throughput

| Event Type | Rate (per minute) | Rate (per hour) |
|------------|-------------------|-----------------|
| Posts      | ~1,000-5,000      | 60K-300K        |
| Likes      | ~10,000-50,000    | 600K-3M         |
| Follows    | ~100-1,000        | 6K-60K          |
| Reposts    | ~500-2,000        | 30K-120K        |
| **Total**  | **~12K-58K**      | **~720K-3.5M**  |

### Storage Requirements

| Metric | Volume |
|--------|--------|
| Daily Volume | ~17M-86M events/day |
| Monthly Volume | ~500M-2.6B events/month |
| Storage per Event | ~2-5KB average |
| Daily Storage | ~34GB-430GB/day |
| Monthly Storage | ~1TB-13TB/month |

## Proposed File Structure

### Redis Buffer Structure

```
redis://localhost:6379/
├── firehose:posts:queue    # Post events buffer
├── firehose:likes:queue    # Like events buffer
├── firehose:follows:queue  # Follow events buffer
├── firehose:reposts:queue  # Repost events buffer
├── firehose:blocks:queue   # Block events buffer
└── firehose:metadata       # Processing metadata
```

### Parquet Storage Structure (Hybrid Partitioning)

#### Primary Partitioning (Date-based)

```
~/tmp/bluesky_data/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   ├── hour=14/
│   │   │   │   ├── posts.parquet
│   │   │   │   ├── likes.parquet
│   │   │   │   ├── follows.parquet
│   │   │   │   └── reposts.parquet
│   │   │   └── hour=15/
│   │   └── day=16/
│   └── month=02/
└── year=2025/
```

#### Secondary Partitioning (User-based for high-volume users)

```
~/tmp/bluesky_data/
├── year=2024/
│   ├── month=01/
│   │   ├── day=15/
│   │   │   ├── hour=14/
│   │   │   │   ├── posts/
│   │   │   │   │   ├── user_did=did:plc:high_volume_user1/
│   │   │   │   │   │   └── posts.parquet
│   │   │   │   │   └── user_did=other/
│   │   │   │   │       └── posts.parquet
│   │   │   │   └── likes/
│   │   │   │       ├── user_did=did:plc:high_volume_user1/
│   │   │   │       │   └── likes.parquet
│   │   │   │       └── user_did=other/
│   │   │   │           └── likes.parquet
```

## System Design Principles

### 1. Query Performance Optimization

- **Date-based primary partitioning**: Enables efficient time-range queries
- **User-based secondary partitioning**: Optimizes user-specific queries for high-volume users
- **Columnar storage (Parquet)**: Enables fast analytics and text search
- **Compression**: Reduces storage costs and improves I/O performance

### 2. Scalability Considerations

- **Redis buffer**: Handles high-throughput real-time ingestion
- **Batch processing**: Reduces storage overhead and enables efficient compression
- **Horizontal partitioning**: Enables parallel processing and querying
- **Metadata tracking**: Enables resumable processing and data consistency

### 3. Data Consistency

- **Idempotent processing**: Prevents duplicate data on retries
- **Transaction boundaries**: Ensures atomic batch operations
- **Checkpointing**: Enables recovery from failures
- **Data validation**: Ensures data quality at ingestion

## Implementation Plan

### Phase 1: Redis Buffer Setup

- Install and configure Redis for high-throughput buffering
- Implement Redis-based queue interface
- Modify jetstream connector to use Redis buffer
- Add monitoring and alerting for buffer health

### Phase 2: Batch Processing Service

- Create scheduled batch processing service
- Implement Parquet conversion logic
- Add intelligent partitioning logic
- Implement data validation and error handling

### Phase 3: Storage Optimization

- Implement hybrid partitioning strategy
- Add compression and optimization
- Create data lifecycle management
- Implement backup and recovery procedures

### Phase 4: Query Engine

- Implement fast query interface
- Add text search capabilities
- Create analytics dashboard
- Optimize query performance

## Performance Targets

### Throughput

- **Ingestion**: Handle 100K+ events/minute
- **Processing**: Process 5-minute batches in <30 seconds
- **Query**: Return results for 1-day queries in <5 seconds

### Storage

- **Compression**: Achieve 80%+ compression ratio
- **Query Performance**: Support concurrent queries without degradation
- **Cost**: Minimize storage costs through intelligent partitioning

### Reliability

- **Uptime**: 99.9% availability
- **Data Loss**: Zero data loss tolerance
- **Recovery**: <5 minute recovery from failures

## Monitoring and Observability

### Key Metrics

- **Buffer health**: Queue depth, processing latency
- **Storage metrics**: Volume, compression ratios, query performance
- **System health**: CPU, memory, disk I/O
- **Business metrics**: Events processed, data quality scores

### Alerting

- **Buffer overflow**: Queue depth >80% capacity
- **Processing delays**: Batch processing >2x expected time
- **Storage issues**: Disk usage >90%
- **Data quality**: Error rate >1%

## Migration Strategy

### From Current System

- **Parallel operation**: Run new system alongside existing system
- **Data validation**: Compare outputs between systems
- **Gradual migration**: Move traffic incrementally
- **Rollback plan**: Maintain ability to revert if issues arise

### Data Backfill

- **Historical data**: Process existing cache files
- **Incremental backfill**: Process data in chronological order
- **Validation**: Ensure data consistency across systems
- **Cleanup**: Remove old cache files after successful migration
