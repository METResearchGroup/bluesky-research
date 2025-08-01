# Bluesky Firehose Data Pipeline Backend - Brain Dump

## Project Context

Building a high-performance, scalable backend data pipeline for the Bluesky Post Explorer frontend. This project will replace the current study-specific, file-based approach with a generic, Redis-buffered architecture optimized for real-time analytics and query performance.

**Key Integration Points:**
- Powers the Bluesky Post Explorer frontend (from 1_ui_planning.md)
- Integrates with existing Jetstream connector for data ingestion
- Replaces current file-based storage with Redis + Parquet architecture
- Hosted on Hetzner (compute and storage)

## System Architecture Overview

```
Jetstream Firehose → Redis Buffer → Batch Processing → Parquet Storage → DuckDB Query Engine → REST API → Vercel Frontend
```

**Key Design Principles:**
- **Real-time Streaming**: Jetstream writes to Redis buffer in real-time
- **Batch Processing**: Scheduled jobs pull from Redis buffer and process in batches
- **OLAP Focus**: DuckDB for analytical queries against Parquet data
- **Cached Views**: Pre-computed common query patterns updated via cron
- **Docker Deployment**: All services containerized on Hetzner VMs
- **Permanent Retention**: All data stored permanently on Hetzner storage

### Key Components

1. **Redis Buffer Layer**: High-performance in-memory buffer for real-time data ingestion from Jetstream
2. **Batch Processing Service**: Scheduled service that processes buffered data every 5 minutes
3. **Parquet Storage Layer**: Optimized columnar storage with intelligent partitioning for OLAP workloads
4. **DuckDB Query Engine**: Fast analytical queries against Parquet data with SQL interface
5. **REST API**: RESTful endpoints for the Vercel-hosted Bluesky Post Explorer frontend
6. **Cached Views Service**: Pre-computed common query patterns updated via cron schedules
7. **Docker Orchestration**: Containerized deployment on Hetzner VMs

## Data Volume & Performance Requirements

### Data Schema (Initial Scope)
- **Posts**: text content, author information, timestamps, hashtags
- **Likes**: post interactions with timestamps
- **Author Information**: user handles, display names, DIDs
- **Hashtags**: extracted from post text
- **Timestamps**: creation and interaction times

### Real-time Throughput
- **Posts**: ~1,000-5,000 per minute
- **Likes**: ~10,000-50,000 per minute  
- **Follows**: ~100-1,000 per minute
- **Reposts**: ~500-2,000 per minute
- **Total**: ~12K-58K events/minute (720K-3.5M/hour)

### Storage Requirements
- **Daily Volume**: ~17M-86M events/day
- **Monthly Volume**: ~500M-2.6B events/month
- **Daily Storage**: ~34GB-430GB/day
- **Monthly Storage**: ~1TB-13TB/month

### Performance Targets
- **Ingestion**: Handle 100K+ events/minute
- **Processing**: Process 5-minute batches in <30 seconds
- **Query**: Return results for 1-day queries in <5 seconds
- **Compression**: Achieve 80%+ compression ratio
- **Availability**: 99.9% uptime
- **Data Loss**: Zero tolerance

## Technical Architecture

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

### Secondary Partitioning (User-based for high-volume users)
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
```

## Key Questions Requiring Answers

### Infrastructure & Deployment
1. ✅ **Deployment Strategy**: Docker containers on Hetzner VMs (single VMs, no load balancing)
2. ✅ **Storage**: All storage on Hetzner (compute + storage)
3. ✅ **Monitoring**: Prometheus + Grafana (Phase 6)
4. ✅ **Data Retention**: Permanent retention
5. ✅ **Timeline**: 1 week with team of 10 (ambitious but achievable)
6. ✅ **Batch Processing**: Every 5 minutes
7. ✅ **Cached Views**: Daily updates
8. ✅ **Pagination**: 50 results per page, max 1000 rows (feature flag enabled by default)
9. ✅ **Project Type**: Greenfield (no migration from existing system)
10. What specific Hetzner VM configurations do we need?
11. How do we handle Redis persistence and backup on Hetzner?
12. What's the cost optimization strategy for Hetzner resources?

### Data Architecture
1. ✅ **Partitioning Strategy**: Use hybrid partitioning from system_design.md (date-based primary, user-based secondary)
2. ✅ **Data Validation**: Stub module for later implementation (Phase 6)
3. How do we handle data consistency and idempotency?
4. What's the backup and recovery strategy?
5. How do we handle data quality checks? (stub for later)

### Integration Points
1. ✅ **Jetstream Integration**: Real-time stream writes to Redis buffer, batch processing pulls from buffer
2. ✅ **API Contract**: RESTful API for Vercel frontend to query data (internal only initially, auth later)
3. ✅ **Query Engine**: DuckDB for OLAP queries against Parquet data
4. ✅ **Cached Views**: Cron-scheduled updates for common query patterns
5. ✅ **Orchestration**: Prefect for job orchestration, Docker Compose for deployment
6. ✅ **Redis Persistence**: Transient/volatile (lossy is acceptable)
7. ✅ **Feature Flags**: Max 1000 rows restriction (enabled by default, disable in production)
8. How do we handle the transition from current service_constants.py to new configuration?
9. How do we integrate with existing data loading functions?

### Performance & Scaling
1. How do we handle the high throughput requirements?
2. What's the horizontal scaling strategy?
3. How do we optimize query performance?
4. How do we handle Redis memory management?
5. What's the batch processing error handling strategy?

### Migration & Rollback
1. How do we ensure zero data loss during migration?
2. What's the rollback strategy if issues arise?
3. How do we validate data consistency between old and new systems?
4. What's the timeline for the migration?
5. How do we handle historical data backfill?

## Technical Risks & Considerations

### Data Volume & Performance
- The system needs to handle 12K-58K events/minute (720K-3.5M/hour)
- Storage requirements: 34GB-430GB/day, 1TB-13TB/month
- Query performance requirements: <5 seconds for 1-day queries
- This is a significant scaling challenge

### Technology Stack Migration
- Moving from file-based to Redis + Parquet architecture
- Refactoring data loaders from current format to Polars
- Updating service constants and metadata
- This requires careful coordination to avoid data loss

### Infrastructure Complexity
- Hetzner hosting adds infrastructure management overhead
- Redis requires careful configuration for persistence and performance
- Batch processing introduces complexity for error handling and recovery
- Monitoring and alerting need to be comprehensive

### Integration Challenges
- Jetstream connector needs to be modified to work with Redis buffer
- Frontend API needs to be designed and implemented
- Data format changes need to be coordinated across systems
- Migration strategy needs to be carefully planned

## Implementation Phases

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

### Phase 4: Query Engine & Cached Views
- Implement DuckDB query interface
- Add text search capabilities
- Create cached views for common query patterns (see recommended patterns below)
- Implement cron schedules for view updates
- Optimize query performance

#### Recommended Cached View Patterns for Social Media Analytics

**Time-based Trends:**
- Daily post counts by hour (24-hour rolling window)
- Weekly post volume trends
- Monthly posting activity patterns
- Peak posting hours by day of week

**User Activity Metrics:**
- Most active users (daily/weekly/monthly)
- User engagement rates (posts per user)
- New user registration trends
- User retention metrics

**Content Analysis:**
- Trending hashtags (hourly/daily/weekly)
- Most liked posts by time period
- Content sentiment trends
- Post length distribution trends

**Engagement Metrics:**
- Like-to-post ratios over time
- Engagement velocity (likes per hour after posting)
- Viral content identification (posts with rapid like growth)
- Cross-platform content performance

**Geographic/Network Analysis:**
- User network growth patterns
- Influencer identification metrics
- Content spread patterns
- Community formation trends

### Phase 5: Data Loader Refactoring
- Migrate to Polars
- Update storage paths
- Refactor service constants
- Update data loading functions

### Phase 6: Frontend Integration & Monitoring
- Design and implement REST API endpoints for Vercel frontend
- Handle authentication and rate limiting
- Integrate with frontend requirements
- Performance testing and optimization
- Deploy Prometheus + Grafana monitoring stack

#### API Endpoints (Based on Frontend Requirements)
**GET /search**
- Parameters: `text`, `username` (optional), `start`, `end`, `exact`
- Response: Paginated results (50 per page, max 1000 rows with feature flag)
- Features: CSV export capability

**GET /export**
- Parameters: Same as search endpoint
- Response: CSV file download
- Features: Direct file download for large datasets

## Success Criteria

- Zero data loss during migration
- Meet performance targets (ingestion, processing, query)
- Achieve cost targets through efficient storage and compression
- Maintain 99.9% availability
- Enable fast analytics queries for the frontend
- Seamless integration with existing Jetstream connector
- Successful migration from current file-based system

## Dependencies & Prerequisites

- Existing Jetstream connector (services/sync/jetstream/)
- Current service_constants.py configuration
- load_data_from_local_storage function
- Bluesky Post Explorer frontend requirements (Vercel-hosted)
- Hetzner infrastructure setup (VMs, storage, networking)
- Redis installation and configuration
- Docker containerization setup
- DuckDB integration for OLAP queries

## Monitoring & Observability

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

## Infrastructure Sizing Recommendations

### Hetzner VM Configuration Recommendations

**Primary Data Processing VM (CX31 or CPX21):**
- **CPU**: 2-4 vCPUs (CX31: 2 vCPUs, CPX21: 3 vCPUs)
- **RAM**: 8-16 GB (CX31: 8GB, CPX21: 16GB)
- **Storage**: 80-160 GB SSD (CX31: 80GB, CPX21: 160GB)
- **Use Case**: Redis buffer, batch processing, DuckDB queries

**Storage VM (CX21 or dedicated storage):**
- **CPU**: 2 vCPUs
- **RAM**: 4-8 GB
- **Storage**: 500GB-1TB SSD for Parquet data
- **Use Case**: Parquet file storage, data archival

**API/Web Server VM (CX21):**
- **CPU**: 2 vCPUs
- **RAM**: 4 GB
- **Storage**: 40 GB SSD
- **Use Case**: REST API, Docker Compose orchestration

### Cost Optimization Strategy
- **Start with CX21/CX31 instances** (~$10-20/month each)
- **Scale up based on actual usage** (monitor CPU/memory usage)
- **Use Hetzner Cloud Volumes** for additional storage as needed
- **Consider object storage** for long-term archival (cheaper than SSD)

### Estimated Monthly Costs (Initial Setup)
- **3 VMs (CX21/CX31)**: ~$30-60/month
- **Storage**: ~$10-50/month (depending on volume)
- **Total**: ~$40-110/month for initial setup

## Next Steps

1. Validate this brain dump with stakeholders
2. Create detailed specification document
3. Set up Linear project with tickets
4. Begin Phase 1 implementation
5. Establish monitoring and alerting infrastructure 