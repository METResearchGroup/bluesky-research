# Bluesky Post Explorer Backend Data Pipeline

## Project Overview

A high-performance, scalable backend data pipeline for the Bluesky Post Explorer frontend. This system handles the full Bluesky firehose (~8.1M events/day) with Redis buffering, Parquet storage, and DuckDB query engine.

## Quick Links

- **Linear Project**: [Bluesky Post Explorer Backend Data Pipeline](https://linear.app/metresearch/project/bluesky-post-explorer-backend-data-pipeline-f5f0ac148021)
- **Team**: Northwestern
- **Timeline**: 2 months (8 weeks)
- **Team Size**: 10 engineers

## Project Status

**Current Phase**: Planning Phase  
**Next Milestone**: Phase 1 - Core Infrastructure (Weeks 1-2)  
**Overall Progress**: 0% Complete  

## Architecture Overview

```
Bluesky Firehose → Jetstream Connector → Redis Buffer → Batch Processing → Parquet Storage → DuckDB Query Engine → REST API
```

### Key Components

- **Redis Buffer**: High-performance in-memory buffering for real-time data
- **Batch Processing**: Configurable processing (default 5 minutes)
- **Parquet Storage**: Columnar storage with intelligent partitioning
- **DuckDB Query Engine**: Fast analytical queries with SQL interface
- **REST API**: Endpoints for frontend integration

## Project Phases

### Phase 1: Core Infrastructure (Weeks 1-2)
- [ ] Redis buffer setup and Jetstream integration
- [ ] Basic monitoring setup
- **Validation**: End-to-end data flow working (Jetstream → Redis)

### Phase 2: Batch Processing & Storage (Weeks 3-4)
- [ ] Batch processing service and Parquet storage
- [ ] Data pipeline integration
- **Validation**: Basic query functionality operational

### Phase 3: Query Engine & API (Weeks 5-6)
- [ ] DuckDB query engine and API development
- [ ] API integration testing
- **Validation**: Full API integration complete

### Phase 4: Deployment & Monitoring (Weeks 7-8)
- [ ] Docker deployment and Hetzner infrastructure
- [ ] Monitoring and observability
- **Validation**: Production deployment and monitoring complete

## Incremental Testing Strategy

The project uses an incremental testing approach to validate at each scale:

1. **Phase 1 (Week 2)**: 1 day of data (~8.1M events, ~10-15GB)
2. **Phase 2 (Week 4)**: 1 week of data (~56.7M events, ~70-100GB)
3. **Phase 3 (Week 6)**: 1 month of data (~240M events, ~300-400GB)
4. **Phase 4 (Week 8)**: Continuous firehose ingestion

## Success Criteria

### Primary Success Metrics
- Handle ~8.1M events/day from Bluesky firehose
- Process batches in <30 seconds, query response in <30 seconds
- Achieve 80%+ compression ratio with Parquet
- Maintain 99.9% availability for data pipeline and API services
- Cost under $100/month, ideally under $50/month
- Successfully capture and process 1 month of data (~240M events)

### Incremental Validation Success Criteria
- **Phase 1 (1 Day)**: All components work correctly with 1 day of data
- **Phase 2 (1 Week)**: System handles 1 week of data without issues
- **Phase 3 (1 Month)**: System successfully handles 1 month of data
- **Phase 4 (Full Scale)**: Production-ready system with continuous ingestion

## Technical Stack

- **Infrastructure**: Hetzner Cloud VMs with Docker containers
- **Data Storage**: Parquet files with date-based partitioning
- **Buffer Layer**: Redis for real-time data buffering
- **Query Engine**: DuckDB for OLAP workloads
- **Monitoring**: Prometheus + Grafana
- **Orchestration**: Prefect for job orchestration

## Data Flow

1. **Ingestion**: Bluesky firehose data ingested via Jetstream connector
2. **Buffering**: Data buffered in Redis for real-time processing
3. **Batch Processing**: Data processed in configurable batches (default 5 minutes)
4. **Storage**: Processed data stored in Parquet format with intelligent partitioning
5. **Querying**: Data queried via DuckDB with basic LIKE queries for text search
6. **API**: REST API provides endpoints for frontend integration

## Project Files

- **`spec.md`**: Complete project specification
- **`braindump.md`**: Initial brainstorming and context gathering
- **`plan_bluesky_post_explorer_backend.md`**: Detailed task plan with subtasks and effort estimates
- **`todo.md`**: Checklist of subtasks synchronized with Linear issues
- **`logs.md`**: Log file for tracking progress and issues
- **`lessons_learned.md`**: Document insights and process improvements
- **`metrics.md`**: Track performance metrics and completion times
- **`tickets/`**: Directory for individual ticket documentation

## Team Structure

**Team Size**: 10 engineers  
**Team**: Northwestern  
**Project Manager**: [TBD]  
**Technical Lead**: [TBD]  

## Risk Mitigation

- **Parallel Development**: Multiple teams working on different components
- **Early Testing**: Continuous integration and testing with incremental validation phases
- **Rollback Plan**: Docker-based deployment enables quick rollbacks
- **Monitoring**: Comprehensive observability from day one
- **Incremental Scaling**: Test with 1 day → 1 week → 1 month → full scale
- **Validation Gates**: Each phase must be 100% successful before proceeding

## Dependencies

### External Dependencies
- **Jetstream Connector**: Existing implementation from services/sync/jetstream
- **Hetzner Infrastructure**: Cloud VM provisioning and SSD volumes
- **Docker Registry**: Container image storage and distribution

### Internal Dependencies
- **Team Coordination**: Parallel development across 10 engineers
- **API Design**: Frontend requirements for API endpoints
- **Monitoring Setup**: Prometheus and Grafana configuration

## Next Steps

1. **Create detailed tickets** in Linear for each subtask
2. **Assign team members** to specific phases
3. **Set up development environment** and tooling
4. **Begin Phase 1 implementation**

## Contact

For questions about this project, please refer to the Linear project or contact the project team.

---
**Last Updated**: [Date]  
**Status**: Planning Phase  
**Version**: 1.0 