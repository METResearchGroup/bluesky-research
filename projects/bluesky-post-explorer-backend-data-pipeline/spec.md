# Bluesky Firehose Data Pipeline Backend - Specification

## Project Information
- **Title**: Bluesky Firehose Data Pipeline Backend
- **Author**: Mark Torres
- **Date**: 2025-01-27
- **Project Type**: Greenfield backend infrastructure
- **Timeline**: 6 weeks with single engineer (rapid prototyping)

---

## Phase 1: Problem Definition and Stakeholder Identification

### Problem Statement
The Bluesky Post Explorer frontend needs a high-performance, scalable backend data pipeline to replace the current study-specific, file-based approach. The current system writes files in real-time batches which does not scale well for high-volume data. We need to transition from study-specific data collection to generic data collection for all Bluesky users, with Redis buffering to handle the firehose throughput and batch processing for scalability. **The primary focus is on text-based queries and feature-based queries of posts, with post enrichment from in-house classifiers.**

### Stakeholders
1. **Primary Users**: Social media researchers and analysts using the Bluesky Post Explorer frontend
2. **Development Team**: Single engineer using rapid prototyping methodology
3. **Data Scientists**: Users analyzing social media trends and patterns
4. **System Administrator**: Single engineer managing Hetzner infrastructure
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
- Process data from Redis to Parquet storage efficiently
- Return query results for 1-day queries in <30 seconds
- Achieve 80%+ compression ratio with Parquet
- Maintain 99.9% availability for data pipeline and API services
- Enable fast analytics queries for frontend
- Scale to handle all Bluesky users (not just study participants)
- Cost under $100/month, ideally under $50/month
- **Initial Success**: Successfully capture and process raw data with 2-day continuous Jetstream run
- **Primary Focus**: Fast text-based queries of posts
- **Long-term**: Scale to handle 1TB every 3 months with data lifecycle management

### Rapid Prototyping Success Criteria
- **Phase 1A (Redis)**: Redis running with monitoring, can write/read test data
- **Phase 1B (Data Writer)**: Service reads from Redis, writes to Parquet, deletes from buffer
- **Phase 1C (Jetstream)**: Complete pipeline working for 2 days continuously
- **Phase 2A (DuckDB)**: Can query Parquet data with LIKE queries
- **Phase 2B (API)**: Basic search API functional with pagination
- **Phase 3A (Production)**: System deployed to Hetzner with security
- **Phase 3B (Load Test)**: System handles expected load with performance requirements met

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
- **Data Writer Service**: Service that reads from Redis buffer and writes to Parquet storage
- **Parquet Storage**: Optimized columnar storage with intelligent partitioning from system design
- **DuckDB Query Engine**: Fast analytical queries with SQL interface
- **REST API**: Basic search endpoint for frontend integration
- **Docker Deployment**: Containerized services on Hetzner VMs
- **Monitoring**: Prometheus + Grafana from day one
- **Local Development**: Environment that mirrors production
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

### Effort Estimation (6-Week Timeline)

#### Week 1-2: Core Data Pipeline
- **Week 1**: Redis foundation with monitoring + Data writer service
- **Week 2**: Jetstream integration + Complete pipeline validation

#### Week 3-4: Query Engine & API
- **Week 3**: DuckDB integration + Basic query functionality
- **Week 4**: REST API development + Search functionality

#### Week 5-6: Production Hardening
- **Week 5**: Production deployment to Hetzner
- **Week 6**: Load testing, optimization, and security

#### Detailed Breakdown:
- **Phase 1**: Core Data Pipeline (2 weeks)
  - Redis setup with Prometheus/Grafana monitoring
  - Data writer service (Redis → Parquet)
  - Jetstream integration and 2-day validation

- **Phase 2**: Query Engine & API (2 weeks)
  - DuckDB integration with Parquet storage
  - REST API with search functionality
  - Basic query performance optimization

- **Phase 3**: Production Hardening (2 weeks)
  - Hetzner deployment and security
  - Load testing and optimization
  - Production monitoring validation

### Resource Requirements
- **Team Size**: 1 engineer (rapid prototyping)
- **Infrastructure**: 1-2 Hetzner VMs (~$20-60/month, target under $50/month)
- **Storage**: ~1TB every 3 months (~$20-50/month additional storage costs)
- **Development Tools**: Docker, monitoring stack
- **Timeline**: 6 weeks (rapid prototyping with piecemeal deployment)
- **Experience**: Engineer experienced with Docker, monitoring; new to Hetzner
- **Initial Data Volume**: 1 month of data (~240M events, ~300-400GB compressed)

### Success Probability
- **High Confidence**: Core infrastructure components are well-established
- **Medium Risk**: Real-time streaming and batch processing complexity
- **Mitigation**: Phased approach with early testing and validation

---

## Implementation Plan

### Rapid Prototyping Approach - Piecemeal Deployment

**Single Engineer, Rapid Iteration, Quick Shipping**

This project will be executed by a single engineer using rapid prototyping methodology with piecemeal deployment to production as early as possible.

### **Phase 1: Core Data Pipeline (Weeks 1-2)**
**Objective**: Get raw data flowing from Redis to permanent storage

#### **Phase 1A: Redis Foundation (Week 1)**
- **Deliverable**: Working Redis instance with basic monitoring
- **Success Criteria**:
  - ✅ Redis container running and accessible
  - ✅ Basic health checks operational
  - ✅ Prometheus/Grafana monitoring active
  - ✅ Can write/read test data to/from Redis
  - ✅ Monitoring shows Redis metrics

#### **Phase 1B: Data Writer Service (Week 1-2)**
- **Deliverable**: Service that reads from Redis buffer and writes to Parquet storage
- **Success Criteria**:
  - ✅ Service can read data from Redis buffer
  - ✅ Service can write data to Parquet with partitioning
  - ✅ Service can delete processed data from Redis
  - ✅ Compression algorithms implemented and tested
  - ✅ Service scales with data volume
  - ✅ Can process test data end-to-end

#### **Phase 1C: Jetstream Integration (Week 2)**
- **Deliverable**: Jetstream writing to Redis buffer
- **Success Criteria**:
  - ✅ Jetstream connects to Redis successfully
  - ✅ Jetstream writes data to Redis buffer
  - ✅ Complete pipeline: Jetstream → Redis → Parquet
  - ✅ Can run for 2 days continuously
  - ✅ Full transparency through Grafana/Prometheus
  - ✅ Data telemetry visible and accurate

### **Phase 2: Query Engine & API (Weeks 3-4)**
**Objective**: Connect query engine to raw data and build basic API

#### **Phase 2A: DuckDB Integration (Week 3)**
- **Deliverable**: DuckDB reading from Parquet storage
- **Success Criteria**:
  - ✅ DuckDB can read from Parquet files
  - ✅ Basic LIKE queries work on post text
  - ✅ Query performance <30 seconds for 1-day data
  - ✅ Can handle concurrent queries

#### **Phase 2B: REST API (Week 3-4)**
- **Deliverable**: Basic REST API with search functionality
- **Success Criteria**:
  - ✅ GET /search endpoint functional
  - ✅ Text search working with DuckDB
  - ✅ Pagination implemented (50 results/page)
  - ✅ Basic error handling
  - ✅ API responds within performance requirements

### **Phase 3: Production Hardening (Weeks 5-6)**
**Objective**: Production deployment, security, and load testing

#### **Phase 3A: Production Deployment (Week 5)**
- **Deliverable**: System deployed to Hetzner production environment
- **Success Criteria**:
  - ✅ All services deployed to Hetzner
  - ✅ SSL/TLS certificates configured
  - ✅ Firewall and security configured
  - ✅ Monitoring and alerting operational
  - ✅ System accessible from internet

#### **Phase 3B: Load Testing & Optimization (Week 6)**
- **Deliverable**: System tested under load and optimized
- **Success Criteria**:
  - ✅ Load testing with expected data volumes
  - ✅ Performance requirements met under load
  - ✅ Security review completed
  - ✅ API documentation complete
  - ✅ Production monitoring validated

### **Incremental Validation Strategy**

**Test Early, Test Often, Deploy Early**

#### **Phase 1 Validation**
- **Redis Test**: Write/read test data, verify monitoring
- **Data Writer Test**: Process test data, verify Parquet output
- **Jetstream Test**: 2-day continuous run, verify data flow

#### **Phase 2 Validation**
- **DuckDB Test**: Query test data, verify performance
- **API Test**: Search functionality, verify responses

#### **Phase 3 Validation**
- **Production Test**: Deploy to Hetzner, verify accessibility
- **Load Test**: Test under expected load, verify performance

### **Key Principles**
- **Ship Early**: Deploy to production as soon as basic functionality works
- **Test Incrementally**: Validate each component before moving to next
- **Monitor Everything**: Prometheus/Grafana from day one
- **Fail Fast**: Identify issues early and fix quickly
- **Piecemeal Deployment**: Deploy small chunks frequently

### **Risk Mitigation**
- **Single Engineer**: Reduces coordination overhead
- **Rapid Prototyping**: Quick iteration and feedback
- **Early Production**: Real-world testing from start
- **Comprehensive Monitoring**: Visibility into all components
- **Incremental Validation**: Test each piece before proceeding

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

This specification provides a rapid prototyping roadmap for building a high-performance, scalable backend data pipeline for the Bluesky Post Explorer. The approach focuses on getting raw data flowing quickly with piecemeal deployment to production as early as possible.

**Primary Focus**: Text-based queries of posts with rapid iteration and quick shipping. The system is optimized for fast post content search, enabling researchers to analyze social media trends through post content.

**Secondary Consideration**: Basic network storage (follows/followers) is included for future network analysis capabilities, but complex network queries and graph analysis are explicitly out of scope for the initial implementation.

**Rapid Prototyping Approach**: Single engineer using rapid prototyping methodology with piecemeal deployment to production. Each phase has clear, tangible deliverables that can be shipped quickly and validated incrementally.

The technical architecture leverages proven technologies (Redis, Parquet, DuckDB) while introducing modern practices (Docker, monitoring, intelligent partitioning) to create a robust foundation for long-term scalability and performance. The rapid prototyping approach ensures quick iteration and early production deployment for real-world validation.