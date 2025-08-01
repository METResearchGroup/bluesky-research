# Bluesky Firehose Data Pipeline Backend - Specification

## Project Information
- **Title**: Bluesky Firehose Data Pipeline Backend
- **Author**: Mark Torres
- **Date**: 2025-01-27
- **Project Type**: Greenfield backend infrastructure
- **Timeline**: 2 months with team of 10

---

## Phase 1: Problem Definition and Stakeholder Identification

### Problem Statement
The Bluesky Post Explorer frontend needs a high-performance, scalable backend data pipeline to replace the current study-specific, file-based approach. The current system writes files in real-time batches which does not scale well for high-volume data. We need to transition from study-specific data collection to generic data collection for all Bluesky users, with Redis buffering to handle the firehose throughput and batch processing for scalability. **The primary focus is on text-based queries and feature-based queries of posts, with post enrichment from in-house classifiers.**

### Stakeholders
1. **Primary Users**: Social media researchers and analysts using the Bluesky Post Explorer frontend
2. **Development Team**: 10-person engineering team building the system
3. **Data Scientists**: Users analyzing social media trends and patterns
4. **System Administrators**: Team managing Hetzner infrastructure
5. **Frontend Developers**: Team building the Vercel-hosted Bluesky Post Explorer UI

### Current Pain Points
- File-based storage lacks scalability for high-volume data
- Cannot manage the throughput required for full Bluesky firehose
- Limited query performance for analytics workloads
- Manual data management and processing
- Lack of proper monitoring and observability
- Study-specific approach limits broader usage

### Success Criteria
- Handle ~8.1M events/day from Bluesky firehose with Redis buffering
- Process configurable batches (default 5 minutes) in <30 seconds
- Return query results for 1-day queries in <30 seconds
- Achieve 80%+ compression ratio with Parquet
- Maintain 99.9% availability for data pipeline and API services
- Support batch processing with Redis buffering
- Enable fast analytics queries for frontend
- Scale to handle all Bluesky users (not just study participants)
- Cost under $100/month, ideally under $50/month
- **Initial Success**: Successfully capture and process 1 month of data (~240M events)
- **Primary Focus**: Fast text-based and feature-based queries of posts
- **Long-term**: Scale to handle 1TB every 3 months with data lifecycle management

### Incremental Validation Success Criteria
- **Phase 1 (1 Day)**: All components work correctly with 1 day of data
- **Phase 2 (1 Week)**: System handles 1 week of data without issues
- **Phase 3 (1 Month)**: System successfully handles 1 month of data
- **Phase 4 (Full Scale)**: Production-ready system with continuous ingestion

---

## Phase 2: Success Metrics and Validation Criteria

### Performance Metrics
- **Throughput**: Successfully ingest ~8.1M events/day from Bluesky firehose
- **Latency**: Process batches in <30 seconds, query response in <30 seconds
- **Storage Efficiency**: Achieve 80%+ compression with Parquet format
- **Availability**: Maintain 99.9% uptime for data pipeline and API services
- **Data Quality**: Acceptable data loss (velocity over volume)
- **Initial Success**: Successfully capture and process 1 month of data (~240M events)

### Storage Requirements (Updated Based on Current Bluesky Stats)
- **Current Bluesky Volume** (as of July 2025):
  - **Total Users**: 37.8M users
  - **Daily Posts**: ~2.3M posts/day
  - **Daily Likes**: ~2.1M likes/day  
  - **Daily Follows**: ~3.7M follows/day
  - **Total Daily Events**: ~8.1M events/day
- **Monthly Volume**: ~240M events/month
- **Storage Target**: 1TB every 3 months (as specified)
- **Initial Goal**: Successfully capture and process 1 month of data (~240M events)
- **Data Lifecycle Management**: Future consideration (Phase 6+)

### Business Metrics
- **User Experience**: API queries return results within 30 seconds
- **Cost Efficiency**: Stay within $100/month budget, ideally under $50/month
- **Scalability**: System can handle ~8.1M events/day and scale appropriately
- **Analytics Capability**: Support text-based post queries with basic LIKE functionality
- **Generic Access**: Make data available to researchers and analysts beyond study participants
- **Initial Success**: Successfully run for 1 month capturing full firehose data
- **Primary Use Case**: Fast querying of post text with future ML enrichment capability

### Validation Criteria
- **Functional Testing**: All API endpoints return expected results
- **Performance Testing**: Meet all latency and throughput requirements
- **Load Testing**: Handle peak data volumes without degradation
- **Integration Testing**: Seamless integration with Jetstream connector and frontend
- **Monitoring Validation**: Prometheus/Grafana provide actionable insights

---

## Phase 3: Scope Boundaries and Technical Requirements

### In Scope
- **Redis Buffer Layer**: High-performance in-memory buffering for firehose data (required for throughput)
- **Batch Processing Service**: Configurable processing (default 5 minutes)
- **Parquet Storage**: Optimized columnar storage with intelligent partitioning from system design
- **DuckDB Query Engine**: Fast analytical queries with SQL interface
- **REST API**: Endpoints for frontend integration (designed for future auth)
- **Docker Deployment**: Containerized services on Hetzner VMs
- **Monitoring**: Basic health checks with Prometheus + Grafana (Phase 6)
- **Local Development**: Environment that mirrors production
- **Scalable Data Loading**: Design for future scaling (PySpark, custom map-reduce, etc.)
- **Complete Data Collection**: Collect all firehose data (posts, likes, follows, etc.)
- **Post-Focused Querying**: Primary query functionality focused on posts with basic LIKE queries
- **Basic Network Storage**: Store follows/followers data for future analysis

### Out of Scope
- **Authentication**: Initial implementation is internal-only (auth added later)
- **Data Validation**: Stub module for future implementation
- **Load Balancing**: Single VM deployment initially
- **Real-time Streaming**: Frontend receives batch-processed data
- **Advanced Analytics**: Basic trend analysis only
- **Data Migration**: Greenfield project, no existing data migration
- **Cached Views**: Placeholder only, not implemented in initial version
- **Advanced Monitoring**: Basic health checks only initially
- **Secondary User Partitioning**: No hot user partitioning in initial version
- **Data Lifecycle Management**: Future consideration (Phase 6+)
- **Long-term Scaling**: Focus on 1 month of data initially, scale later
- **Network Analysis**: Complex network queries and graph analysis (future scope)
- **User Classification**: User-level classification and network-based user analysis (future scope)
- **ML Enrichment Pipeline**: Separate system to be implemented later
- **ML Model Serving**: Batched ML processing outside current scope
- **ML Model Versioning**: To be handled in separate ML infrastructure
- **Frontend Development**: Focus on backend API only, frontend development deferred

### Technical Requirements

#### Infrastructure
- **Platform**: Hetzner Cloud VMs
- **Deployment**: Docker containers with Docker Compose
- **Orchestration**: Prefect for job orchestration
- **Storage**: SSD volumes for Parquet data (~4TB annual growth)
- **Monitoring**: Prometheus + Grafana

#### Data Architecture
- **Ingestion**: Jetstream connector → Redis buffer
- **Processing**: Configurable batch processing (default 5 minutes)
- **Storage**: Parquet with intelligent partitioning strategy
- **Query Engine**: DuckDB for OLAP workloads
- **Caching**: Daily updates for common query patterns
- **Complete Data Collection**: All firehose data (posts, likes, follows, etc.)
- **Text Search**: Basic LIKE queries for post content (DuckDB)
- **Future ML Integration**: Schema designed to accommodate ML enrichment later

#### Partitioning Strategy
**Primary Partitioning (Date-based):**
```
~/tmp/bluesky_data/
├── year=2025/
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
└── year=2026/
```

**Post Data Schema (Primary Focus):**
- **Posts Table**: (uri, author_did, text, created_at, enriched_features, classifier_scores)
- **Enriched Features**: 
  - Political_score (in-house classifier)
  - Toxicity_score (in-house classifier)
  - Perspective API scores (toxicity, severe_toxicity, identity_attack, insult, profanity, threat, etc.)
- **Text Search**: Basic LIKE queries for post content (DuckDB)
- **Feature Queries**: Fast filtering and aggregation on enriched features

**Basic Network Storage (Secondary):**
- **Follows Table**: (follower_did, following_did, timestamp) - simple storage for future use
- **Partitioning**: By date for temporal queries
- **Compression**: Use Parquet with dictionary encoding for user DIDs

#### API Requirements
- **Endpoints**: GET /search, GET /export
- **Pagination**: Configurable (default 50 results per page)
- **Rate Limiting**: Feature flag for max 1000 rows (enabled by default, performance-based)
- **Response Format**: JSON for search, CSV for export
- **Parameters**: text, username, start, end, exact, feature_filters
- **Documentation**: OpenAPI/Swagger standard
- **Feature Flags**: Configurable via environment variables
- **Text Search**: Basic LIKE queries for post content (DuckDB)
- **Feature Queries**: Filtering and aggregation on enriched features (political_score, toxicity_score, Perspective API scores)

---

## Phase 4: User Experience Considerations

### API User Experience
- **Simple Query Interface**: Text search with optional filters
- **Fast Response Times**: <5 seconds for typical queries
- **Clear Error Messages**: Descriptive error responses
- **Consistent Response Format**: Standardized JSON structure
- **CSV Export**: Direct file download capability

### Developer Experience
- **Clear Documentation**: API documentation and examples
- **Docker Setup**: Easy local development environment
- **Monitoring**: Clear visibility into system health
- **Logging**: Comprehensive logging for debugging
- **Feature Flags**: Easy toggling of restrictions

### System Administrator Experience
- **Simple Deployment**: Docker Compose for easy deployment
- **Monitoring Dashboard**: Grafana dashboards for system health
- **Alerting**: Proactive alerts for system issues
- **Backup Strategy**: Automated backup procedures
- **Scaling**: Clear scaling guidelines and procedures

### Data Analyst Experience
- **Fast Queries**: Quick response times for analytics
- **Cached Views**: Pre-computed common metrics
- **Flexible Filtering**: Multiple filter options
- **Export Capability**: Easy data export for further analysis
- **Trend Analysis**: Support for time-based queries

---

## Phase 5: Technical Feasibility and Estimation

### Technical Feasibility Assessment

#### High Feasibility
- **Redis Buffer**: Well-established technology with proven performance
- **Parquet Storage**: Mature columnar format with excellent compression
- **DuckDB**: Fast analytical database with SQL interface
- **Docker Deployment**: Standard containerization approach
- **Hetzner Infrastructure**: Reliable cloud provider with good performance

#### Medium Feasibility
- **Real-time Streaming**: Requires careful Redis configuration
- **Batch Processing**: Needs robust error handling and recovery
- **Hybrid Partitioning**: Complex but achievable with proper design
- **Cached Views**: Requires careful design of update schedules

#### Risk Mitigation
- **Data Loss**: Redis persistence configuration and backup strategies
- **Performance**: Load testing and optimization
- **Scalability**: Monitoring and capacity planning
- **Integration**: Comprehensive testing with Jetstream connector

### Effort Estimation (2-Month Timeline)

#### Month 1: Core Infrastructure
- **Week 1-2**: Redis buffer setup and Jetstream integration
- **Week 3-4**: Batch processing service and Parquet storage

#### Month 2: Query Engine and Integration
- **Week 5-6**: DuckDB query engine and API development
- **Week 7-8**: Frontend integration, monitoring, and deployment

#### Detailed Breakdown:
- **Phase 1**: Redis Buffer Setup (2 weeks)
  - Redis installation and configuration for high throughput
  - Jetstream connector integration
  - Basic monitoring setup

- **Phase 2**: Batch Processing Service (2 weeks)
  - Configurable batch processing logic
  - Parquet conversion with intelligent partitioning
  - Error handling and recovery

- **Phase 3**: Query Engine & API (2 weeks)
  - DuckDB integration
  - REST API implementation with OpenAPI docs
  - Query optimization and performance tuning

- **Phase 4**: Integration & Monitoring (2 weeks)
  - Frontend integration
  - Prometheus/Grafana monitoring setup
  - Production deployment and testing

### Resource Requirements
- **Team Size**: 10 engineers
- **Infrastructure**: 3 Hetzner VMs (~$40-110/month, target under $50/month)
- **Storage**: ~1TB every 3 months (~$20-50/month additional storage costs)
- **Development Tools**: Docker, Prefect, monitoring stack
- **Timeline**: 2 months (realistic with proper testing and deployment)
- **Experience**: Team experienced with Docker, Prefect, Prometheus/Grafana; new to Hetzner
- **Initial Data Volume**: 1 month of data (~240M events, ~300-400GB compressed)

### Success Probability
- **High Confidence**: Core infrastructure components are well-established
- **Medium Risk**: Real-time streaming and batch processing complexity
- **Mitigation**: Phased approach with early testing and validation

---

## Implementation Plan

### Month 1: Core Infrastructure
- **Weeks 1-2**: Redis buffer setup and Jetstream integration
- **Weeks 3-4**: Batch processing service and Parquet storage

### Month 2: Query Engine and Integration
- **Weeks 5-6**: DuckDB query engine and API development
- **Weeks 7-8**: Frontend integration, monitoring, and deployment

### Incremental Testing & Validation Strategy

**Build for Scale, Test Incrementally**

The system is designed to handle the full Bluesky firehose at scale, but will be validated through incremental testing phases:

#### **Phase 1: One Day Validation (Week 2)**
- **Test Load**: 1 day of firehose data (~8.1M events, ~10-15GB)
- **Validation Checkpoints**:
  - ✅ Jetstream connector successfully ingests data to Redis buffer
  - ✅ Batch processing service processes all data without errors
  - ✅ Parquet storage correctly partitions and stores data
  - ✅ DuckDB can query the stored data
  - ✅ REST API returns correct results
  - ✅ Basic text search and feature filtering work
- **Success Criteria**: All components work correctly with 1 day of data
- **Next Step**: Only proceed to Phase 2 if Phase 1 is 100% successful

#### **Phase 2: One Week Validation (Week 4)**
- **Test Load**: 1 week of firehose data (~56.7M events, ~70-100GB)
- **Validation Checkpoints**:
  - ✅ All Phase 1 checkpoints continue to work
  - ✅ Memory usage remains stable over 7 days
  - ✅ Storage growth matches expected patterns
  - ✅ Query performance remains under 30 seconds
  - ✅ No data loss or corruption
- **Success Criteria**: System handles 1 week of data without issues
- **Next Step**: Only proceed to Phase 3 if Phase 2 is 100% successful

#### **Phase 3: One Month Validation (Week 6)**
- **Test Load**: 1 month of firehose data (~240M events, ~300-400GB)
- **Validation Checkpoints**:
  - ✅ All Phase 2 checkpoints continue to work
  - ✅ Storage efficiency meets expectations
  - ✅ Query performance scales appropriately
  - ✅ Enrichment pipeline processes all data
  - ✅ System uptime and reliability validated
- **Success Criteria**: System successfully handles 1 month of data
- **Next Step**: Production deployment ready

#### **Phase 4: Full Scale (Week 8)**
- **Test Load**: Continuous firehose ingestion
- **Validation Checkpoints**:
  - ✅ All Phase 3 checkpoints continue to work
  - ✅ Long-term stability validated
  - ✅ Monitoring and alerting functional
  - ✅ Performance optimization complete
- **Success Criteria**: Production-ready system

### Key Milestones
- **Week 2**: End-to-end data flow working (Redis → Parquet) + One day validation complete
- **Week 4**: Basic query functionality operational + One week validation complete
- **Week 6**: Full API integration complete + One month validation complete
- **Week 8**: Production deployment and monitoring + Full scale validation
- **Month 1**: Successfully capture 1 month of data (~240M events)
- **Future**: Scale to handle 1TB every 3 months with data lifecycle management

### Risk Mitigation
- **Parallel Development**: Multiple teams working on different components
- **Early Testing**: Continuous integration and testing with incremental validation phases
- **Rollback Plan**: Docker-based deployment enables quick rollbacks
- **Monitoring**: Comprehensive observability from day one
- **Incremental Scaling**: Test with 1 day → 1 week → 1 month → full scale
- **Validation Gates**: Each phase must be 100% successful before proceeding

---

## Data Collection and Storage Strategy

### Complete Data Collection
The system will collect all firehose data (posts, likes, follows, etc.) but focus query functionality on posts initially.

### **Post Data Schema (Primary Query Focus)**
**Storage Strategy:**
- **Posts Table**: (uri, author_did, text, created_at, raw_data)
- **Text Search**: Basic LIKE queries for post content (DuckDB)
- **Future Enrichment**: Schema designed to accommodate ML features later

**Query Optimization:**
- **DuckDB with Parquet**: Efficient columnar storage for post queries
- **Text Search**: Basic LIKE queries for post content
- **Partitioning**: By date for temporal queries
- **Future ML Integration**: Schema designed to accommodate enriched features later

**Scaling Limits:**
- **Current Approach**: Scales to handle ~2.3M posts/day
- **Query Performance**: <30 seconds for text queries
- **Storage**: ~300-400GB for 1 month of all firehose data
- **Processing**: Batch processing of raw firehose data

## Network Data Storage Strategy (Secondary/Future)

### Basic Network Storage
**Storage Strategy:**
- **Follows Table**: (follower_did, following_did, timestamp) - simple storage for future use
- **Partitioning**: By date for temporal queries
- **Compression**: Dictionary encoding for user DIDs

**Future Considerations:**
- **Network Analysis**: Complex network queries and graph analysis (future scope)
- **User Classification**: User-level classification and network-based user analysis (future scope)
- **Graph Databases**: Consider Neo4j or similar for complex network analysis
- **Scaling**: Will require specialized graph processing for large-scale network analysis

### **Post Data Storage (Primary Query Focus)**
- **Posts Table**: (uri, author_did, text, created_at, raw_data)
- **Text Search**: Basic LIKE queries for post content (DuckDB)
- **Partitioning**: By date for temporal analysis
- **Compression**: Dictionary encoding for user DIDs, efficient text compression
- **Indexing**: Indices for text search and temporal queries
- **Future ML Integration**: Schema designed to accommodate enriched features later

### **Basic Network Storage (Secondary/Future)**
- **Follows Table**: (follower_did, following_did, timestamp) - simple storage for future use
- **Partitioning**: By date for temporal queries
- **Compression**: Dictionary encoding for user DIDs
- **Future Use**: Will be used for network analysis in future phases

### **Post Query Use Cases (Primary Requirements)**
1. **Text Search**: Basic LIKE queries across post content (DuckDB)
2. **Temporal Analysis**: Query posts by date ranges
3. **Author Analysis**: Find posts by specific authors
4. **Basic Filtering**: Filter by post type, engagement metrics, etc.
5. **Future ML Integration**: Schema ready for enriched features when ML pipeline is added

### **Implementation Strategy for Post Queries**
**Text Search and Basic Queries:**
```sql
-- Example: Find posts containing specific text in date range
SELECT uri, author_did, text, created_at
FROM posts 
WHERE created_at BETWEEN '2025-01-01' AND '2025-01-31'
  AND text ILIKE '%election%'
ORDER BY created_at DESC
LIMIT 100;
```

**Query Optimization:**
- **Text Search**: Basic LIKE queries with DuckDB (no specialized indices needed)
- **Feature Indices**: Separate indices for each enriched feature (political_score, toxicity_score, Perspective API scores)
- **Caching**: Cache results for frequently accessed queries
- **Partitioning**: Efficient date-based partitioning for temporal queries

### **Scalability Considerations**
- **Memory Management**: Use streaming approaches for large text and feature queries
- **Caching Strategy**: Cache frequently accessed post queries and feature aggregations
- **Query Optimization**: Implement efficient LIKE queries and feature filtering in DuckDB
- **Batch Processing**: Separate enrichment pipeline with in-house classifiers and Perspective API
- **Future Scaling**: Consider specialized text search engines (Elasticsearch) for complex text analysis

## Conclusion

This specification provides a comprehensive roadmap for building a high-performance, scalable backend data pipeline for the Bluesky Post Explorer. The phased approach focuses on successfully capturing 1 month of data (~240M events) before scaling to handle the full 1TB every 3 months requirement.

**Primary Focus**: Text-based and feature-based queries of posts with enrichment from in-house classifiers. The system is optimized for fast post content search and feature filtering, enabling researchers to analyze social media trends through post content and enriched features.

**Secondary Consideration**: Basic network storage (follows/followers) is included for future network analysis capabilities, but complex network queries and graph analysis are explicitly out of scope for the initial implementation.

The technical architecture leverages proven technologies (Redis, Parquet, DuckDB) while introducing modern practices (Docker, monitoring, intelligent partitioning) to create a robust foundation for long-term scalability and performance. The post data storage strategy addresses the unique challenges of social media content analysis at scale, with network analysis planned for future phases.