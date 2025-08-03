# Phase 1: Core Data Pipeline - Linear Ticket Proposals

## Overview
**Objective**: Get raw data flowing from Redis to permanent storage
**Timeline**: Weeks 1-2
**Approach**: Rapid prototyping with piecemeal deployment

---

## MET-001: Set up Redis container with Docker and basic monitoring

### Context & Motivation
Redis buffer is required for high-throughput data buffering to handle ~8.1M events/day from the Bluesky firehose. This is the foundation for the entire data pipeline and must be operational before any data processing can begin. We need Redis running with Prometheus/Grafana monitoring from day one for visibility into system performance.

### Detailed Description & Requirements

#### Functional Requirements:
- Create Dockerfile for Redis container optimized for high-throughput buffering
- Configure Redis with appropriate memory settings for ~8.1M events/day
- Set up Redis persistence strategy (RDB + AOF) for data recovery
- Implement basic Redis configuration for performance optimization
- Create Docker Compose service definition for Redis
- Set up Prometheus Redis exporter for metrics collection
- Configure Grafana dashboard for Redis monitoring
- Implement basic health check endpoint for Redis

#### Non-Functional Requirements:
- Redis must handle 1000+ operations/second sustained
- Memory usage should not exceed 80% of allocated RAM
- Container should start within 30 seconds
- Redis should be accessible on standard port 6379
- Prometheus metrics should be available within 60 seconds of container start
- Grafana dashboard should show key Redis metrics (memory, ops/sec, connected clients)

#### Validation & Error Handling:
- Redis container starts successfully on first attempt
- Redis responds to PING commands within 100ms
- Container logs show successful startup without errors
- Memory usage stays within acceptable bounds during testing
- Prometheus can scrape Redis metrics
- Grafana dashboard displays Redis metrics correctly

### Success Criteria
- Redis container starts successfully with Docker Compose
- Redis responds to basic commands (PING, INFO, etc.)
- Container logs show no errors during startup
- Redis configuration optimized for high-throughput operations
- Prometheus Redis exporter running and collecting metrics
- Grafana dashboard showing Redis performance metrics
- Basic health check endpoint returns 200 OK
- Docker Compose file committed to repository

### Test Plan
- `test_redis_startup`: Docker container starts → Redis responds to PING
- `test_redis_performance`: 1000 SET operations → All complete within 5 seconds
- `test_redis_memory`: Load test data → Memory usage stays under 80%
- `test_redis_persistence`: Write data → Data persists after container restart
- `test_prometheus_metrics`: Prometheus can scrape Redis metrics
- `test_grafana_dashboard`: Grafana displays Redis metrics correctly
- `test_health_check`: Health check endpoint returns 200 OK

📁 Test file: `services/redis/tests/test_redis_setup.py`

### Dependencies
- None (foundational component)

### Suggested Implementation Plan
- Create `services/redis/Dockerfile` with Redis 7.0+ base image
- Configure `redis.conf` for high-throughput operations
- Create `docker-compose.yml` with Redis service
- Add Prometheus Redis exporter container
- Configure Grafana dashboard for Redis metrics
- Add health check endpoint for Redis
- Document Redis configuration and usage

### Effort Estimate
- Estimated effort: **6 hours**
- Assumes Docker environment is already set up
- Includes configuration optimization, monitoring setup, and testing

### Priority & Impact
- Priority: **High**
- Rationale: Blocks all downstream data processing components

### Acceptance Checklist
- [ ] Redis Dockerfile created and optimized
- [ ] Docker Compose service defined with monitoring
- [ ] Redis starts successfully on first attempt
- [ ] Basic performance tests pass
- [ ] Prometheus metrics collection working
- [ ] Grafana dashboard displaying Redis metrics
- [ ] Health check endpoint functional
- [ ] Configuration documented in README
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Redis Documentation: https://redis.io/documentation
- Prometheus Redis Exporter: https://github.com/oliver006/redis_exporter
- Related tickets: MET-002, MET-003

---

## MET-002: Create data writer service for Redis to Parquet conversion

### Context & Motivation
The data writer service is the core component that reads data from the Redis buffer and writes it to permanent Parquet storage with intelligent partitioning. This service must handle the high-throughput data flow from Redis to Parquet files efficiently, ensuring no data loss and optimal compression.

### Detailed Description & Requirements

#### Functional Requirements:
- Create Python service that connects to Redis and reads data from buffer
- Implement Parquet file writing with intelligent partitioning (year/month/day/hour)
- Service must delete processed data from Redis buffer after successful write
- Support all firehose data types (posts, likes, follows, reposts)
- Implement configurable batch processing (default: process every 5 minutes)
- Add compression algorithms (Snappy, GZIP) with performance testing
- Implement error handling and retry logic for failed operations
- Add logging for all operations (reads, writes, deletes, errors)

#### Non-Functional Requirements:
- Service must process batches in <30 seconds
- Achieve 80%+ compression ratio with Parquet
- Handle memory efficiently (stream processing for large datasets)
- Service should be restartable without data loss
- Log all operations with appropriate log levels
- Service should scale with data volume

#### Validation & Error Handling:
- Service can read data from Redis without errors
- Parquet files are written with correct partitioning structure
- Data is deleted from Redis only after successful write
- Service handles Redis connection failures gracefully
- Service can resume processing after restart
- Compression ratios meet performance targets

### Success Criteria
- Service can read data from Redis buffer successfully
- Service can write data to Parquet with correct partitioning
- Service can delete processed data from Redis buffer
- Compression algorithms implemented and tested
- Service scales with data volume
- Can process test data end-to-end without errors
- Logging provides visibility into all operations
- Service is restartable without data loss

### Test Plan
- `test_redis_read`: Read test data from Redis → Data retrieved correctly
- `test_parquet_write`: Write data to Parquet → Files created with correct structure
- `test_redis_delete`: Delete processed data → Data removed from Redis
- `test_compression`: Test different compression algorithms → 80%+ compression achieved
- `test_batch_processing`: Process batches → All batches complete within 30 seconds
- `test_error_handling`: Simulate Redis failures → Service handles gracefully
- `test_restart`: Restart service → Processing resumes correctly
- `test_partitioning`: Verify partitioning structure → Files organized correctly

📁 Test file: `services/data_writer/tests/test_data_writer.py`

### Dependencies
- Depends on: MET-001 (Redis setup)

### Suggested Implementation Plan
- Create `services/data_writer/main.py` with Redis connection logic
- Implement Parquet writing with Polars or pandas
- Add partitioning logic based on timestamp
- Implement compression algorithm testing
- Add error handling and retry mechanisms
- Create configuration for batch processing intervals
- Add comprehensive logging
- Create Docker container for the service

### Effort Estimate
- Estimated effort: **12 hours**
- Includes Redis integration, Parquet writing, compression testing, and error handling

### Priority & Impact
- Priority: **High**
- Rationale: Core component for data pipeline, blocks Jetstream integration

### Acceptance Checklist
- [ ] Data writer service implemented
- [ ] Redis read/write/delete operations working
- [ ] Parquet writing with correct partitioning
- [ ] Compression algorithms tested and optimized
- [ ] Error handling and retry logic implemented
- [ ] Comprehensive logging added
- [ ] Service is restartable without data loss
- [ ] Performance requirements met
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Parquet Documentation: https://parquet.apache.org/
- Polars Documentation: https://pola.rs/
- Related tickets: MET-001, MET-003

---

## MET-003: Implement Jetstream integration with Redis buffer

### Context & Motivation
Jetstream integration is the final component needed to complete the data pipeline. This connects the Bluesky firehose to the Redis buffer, enabling real-time data ingestion. The integration must be robust and handle the high-throughput nature of the firehose data.

### Detailed Description & Requirements

#### Functional Requirements:
- Integrate existing Jetstream connector with Redis buffer
- Configure Jetstream to write all firehose data types to Redis
- Implement proper error handling for Jetstream connection issues
- Add monitoring for Jetstream connection status and data flow
- Configure Jetstream to handle reconnection scenarios
- Implement data validation before writing to Redis
- Add logging for Jetstream operations and data flow

#### Non-Functional Requirements:
- Jetstream must handle ~8.1M events/day without dropping data
- Connection should be resilient to network issues
- Data should be written to Redis within 1 second of receipt
- Monitoring should show real-time data flow metrics
- Service should be restartable without data loss

#### Validation & Error Handling:
- Jetstream connects to Redis successfully
- Jetstream writes data to Redis buffer without errors
- Complete pipeline: Jetstream → Redis → Parquet works end-to-end
- Can run for 2 days continuously without issues
- Full transparency through Grafana/Prometheus
- Data telemetry visible and accurate

### Success Criteria
- Jetstream connects to Redis successfully
- Jetstream writes data to Redis buffer
- Complete pipeline: Jetstream → Redis → Parquet functional
- Can run for 2 days continuously
- Full transparency through Grafana/Prometheus
- Data telemetry visible and accurate
- Error handling works for connection issues
- Monitoring shows real-time data flow

### Test Plan
- `test_jetstream_connection`: Connect to Bluesky firehose → Connection established
- `test_redis_write`: Write data to Redis → Data appears in Redis buffer
- `test_end_to_end`: Complete pipeline test → Data flows from Jetstream to Parquet
- `test_continuous_run`: Run for 2 days → No data loss or errors
- `test_monitoring`: Verify metrics → Grafana shows data flow
- `test_error_handling`: Simulate connection issues → Service handles gracefully
- `test_data_validation`: Validate data format → Invalid data handled correctly

📁 Test file: `services/jetstream/tests/test_jetstream_integration.py`

### Dependencies
- Depends on: MET-001 (Redis setup), MET-002 (Data writer service)

### Suggested Implementation Plan
- Review existing Jetstream connector code
- Modify connector to write to Redis instead of files
- Add Redis connection configuration
- Implement data validation logic
- Add monitoring and logging
- Test with small data volumes first
- Scale up to full firehose data
- Monitor for 2 days continuously

### Effort Estimate
- Estimated effort: **8 hours**
- Includes Jetstream modification, Redis integration, and testing

### Priority & Impact
- Priority: **High**
- Rationale: Final component needed for complete data pipeline

### Acceptance Checklist
- [ ] Jetstream connects to Redis successfully
- [ ] Data flows from Jetstream to Redis buffer
- [ ] Complete pipeline functional end-to-end
- [ ] 2-day continuous run successful
- [ ] Monitoring shows data flow metrics
- [ ] Error handling implemented
- [ ] Data validation working
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Existing Jetstream Code: `services/sync/jetstream/`
- Related tickets: MET-001, MET-002

---

## MET-004: Create comprehensive monitoring dashboard for data pipeline

### Context & Motivation
Comprehensive monitoring is essential for the data pipeline to provide visibility into system performance, data flow, and potential issues. This dashboard will show Redis metrics, data flow rates, Parquet writing performance, and Jetstream connection status.

### Detailed Description & Requirements

#### Functional Requirements:
- Create Grafana dashboard showing Redis performance metrics
- Add data flow metrics (events/second, data volume)
- Display Parquet writing performance and compression ratios
- Show Jetstream connection status and data ingestion rates
- Implement alerting for critical issues (Redis memory, connection failures)
- Add system resource monitoring (CPU, memory, disk usage)
- Create data quality metrics (records processed, errors)

#### Non-Functional Requirements:
- Dashboard should load within 5 seconds
- Metrics should update every 30 seconds
- Alerts should trigger within 1 minute of issues
- Dashboard should be accessible via web interface
- Historical data should be retained for 30 days

#### Validation & Error Handling:
- Dashboard displays all required metrics correctly
- Alerts trigger appropriately for critical issues
- Historical data is retained and accessible
- Dashboard performance meets requirements

### Success Criteria
- Grafana dashboard displays all pipeline metrics
- Redis performance metrics visible and accurate
- Data flow rates shown in real-time
- Parquet writing performance monitored
- Jetstream connection status displayed
- Alerting configured for critical issues
- System resources monitored
- Dashboard accessible and performant

### Test Plan
- `test_dashboard_load`: Load dashboard → All panels display correctly
- `test_metrics_accuracy`: Verify metrics → Data matches actual performance
- `test_alerting`: Trigger alerts → Notifications sent correctly
- `test_historical_data`: Check retention → 30 days of data available
- `test_performance`: Dashboard load time → Under 5 seconds

📁 Test file: `services/monitoring/tests/test_dashboard.py`

### Dependencies
- Depends on: MET-001 (Redis setup), MET-002 (Data writer), MET-003 (Jetstream)

### Suggested Implementation Plan
- Configure Prometheus data sources
- Create Grafana dashboard panels
- Set up alerting rules
- Configure data retention policies
- Test dashboard with real data
- Document dashboard usage

### Effort Estimate
- Estimated effort: **4 hours**
- Includes dashboard creation, alerting setup, and testing

### Priority & Impact
- Priority: **Medium**
- Rationale: Essential for monitoring but not blocking core functionality

### Acceptance Checklist
- [ ] Grafana dashboard created with all metrics
- [ ] Redis performance metrics displayed
- [ ] Data flow rates monitored
- [ ] Alerting configured for critical issues
- [ ] Dashboard performance meets requirements
- [ ] Historical data retention configured
- [ ] Dashboard accessible via web interface
- [ ] Documentation created

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Grafana Documentation: https://grafana.com/docs/
- Related tickets: MET-001, MET-002, MET-003

---

## MET-005: Implement data validation and error handling for data pipeline

### Context & Motivation
Robust error handling and data validation are critical for a production data pipeline. This ensures data quality, prevents data loss, and provides clear visibility into any issues that occur during processing.

### Detailed Description & Requirements

#### Functional Requirements:
- Implement data validation for all firehose data types
- Add error handling for Redis connection failures
- Create retry logic for failed Parquet writes
- Implement dead letter queue for unprocessable data
- Add comprehensive logging for all error scenarios
- Create data quality metrics and reporting
- Implement circuit breaker pattern for external dependencies

#### Non-Functional Requirements:
- Error handling should not impact pipeline performance
- Failed data should be retried up to 3 times
- Unprocessable data should be logged and stored separately
- Error logs should be searchable and actionable
- Data quality metrics should be available in monitoring

#### Validation & Error Handling:
- Invalid data is handled gracefully without pipeline failure
- Connection failures are handled with retry logic
- Failed operations are logged with sufficient detail
- Data quality metrics are accurate and actionable

### Success Criteria
- Data validation prevents invalid data from corrupting pipeline
- Error handling works for all failure scenarios
- Retry logic successfully processes failed operations
- Dead letter queue captures unprocessable data
- Comprehensive logging provides visibility into issues
- Data quality metrics are accurate and actionable
- Pipeline continues operating despite individual failures

### Test Plan
- `test_data_validation`: Invalid data → Handled gracefully
- `test_connection_failures`: Redis failures → Retry logic works
- `test_parquet_failures`: Write failures → Retry and dead letter queue
- `test_error_logging`: Error scenarios → Logged with sufficient detail
- `test_data_quality`: Quality metrics → Accurate and actionable

📁 Test file: `services/data_writer/tests/test_error_handling.py`

### Dependencies
- Depends on: MET-002 (Data writer service)

### Suggested Implementation Plan
- Add data validation schemas for all data types
- Implement retry logic with exponential backoff
- Create dead letter queue storage
- Add comprehensive error logging
- Create data quality metrics collection
- Test with various failure scenarios

### Effort Estimate
- Estimated effort: **6 hours**
- Includes validation logic, error handling, and testing

### Priority & Impact
- Priority: **Medium**
- Rationale: Important for production reliability but not blocking core functionality

### Acceptance Checklist
- [ ] Data validation implemented for all data types
- [ ] Error handling works for all failure scenarios
- [ ] Retry logic implemented and tested
- [ ] Dead letter queue functional
- [ ] Comprehensive error logging added
- [ ] Data quality metrics implemented
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Related tickets: MET-002

---

## Phase 1 Summary

### Total Tickets: 5
### Estimated Effort: 36 hours
### Critical Path: MET-001 → MET-002 → MET-003
### Key Deliverables:
- Redis instance with monitoring
- Data writer service (Redis → Parquet)
- Jetstream integration
- Complete data pipeline (Jetstream → Redis → Parquet)
- Comprehensive monitoring dashboard
- Error handling and validation

### Exit Criteria:
- Complete pipeline working for 2 days continuously
- All data flowing from Jetstream to Parquet storage
- Monitoring showing accurate metrics
- Error handling working for all scenarios
- Ready for Phase 2 (Query Engine & API) 