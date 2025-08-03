# Phase 1: Core Data Pipeline - Linear Ticket Proposals

## Overview
**Objective**: Get raw data flowing from Redis to permanent storage with rapid prototyping approach
**Timeline**: Weeks 1-2
**Approach**: Rapid prototyping with piecemeal deployment to Hetzner as early as possible

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

## MET-002: Deploy Redis infrastructure to Hetzner with CI/CD

### Context & Motivation
Early deployment to Hetzner is critical for rapid prototyping and real-world validation. This ensures we can test the Redis infrastructure in a production-like environment immediately, rather than waiting until Phase 3. This deployment will serve as the foundation for all subsequent data pipeline components.

### Detailed Description & Requirements

#### Functional Requirements:
- Set up Hetzner VM with appropriate specifications for Redis workload
- Configure Docker and Docker Compose for production deployment
- Deploy Redis container with monitoring stack to Hetzner
- Set up automated CI/CD pipeline for deployment
- Configure production environment variables and secrets
- Set up SSL/TLS certificates for secure access
- Configure firewall and network security
- Implement automated deployment scripts

#### Non-Functional Requirements:
- VM should have sufficient resources (2+ CPU cores, 4GB+ RAM, 80GB+ SSD)
- Deployment should complete within 15 minutes
- SSL certificates should be automatically renewed
- Firewall should block unnecessary ports
- CI/CD pipeline should deploy on every main branch push
- System should be accessible via HTTPS

#### Validation & Error Handling:
- Redis container starts successfully in Hetzner environment
- SSL certificates are valid and working
- Firewall blocks unauthorized access
- CI/CD pipeline deploys successfully
- System is accessible from internet
- Deployment scripts are idempotent

### Success Criteria
- Redis deployed to Hetzner successfully
- SSL/TLS certificates configured and working
- Firewall and security configured
- CI/CD pipeline functional and automated
- System accessible from internet
- Monitoring stack operational in production
- Deployment automated and repeatable

### Test Plan
- `test_hetzner_deployment`: Deploy to Hetzner → Redis starts successfully
- `test_ssl_certificates`: HTTPS access → Certificates valid and working
- `test_firewall_security`: Unauthorized access → Blocked appropriately
- `test_cicd_pipeline`: Push to main → Automatic deployment successful
- `test_internet_access`: External access → System accessible via HTTPS
- `test_monitoring_production`: Monitoring stack → Operational in production

📁 Test file: `deployment/tests/test_hetzner_deployment.py`

### Dependencies
- Depends on: MET-001 (Redis setup)

### Suggested Implementation Plan
- Provision Hetzner VM with appropriate specifications
- Install Docker and Docker Compose
- Configure production environment variables
- Set up SSL certificates with Let's Encrypt
- Configure firewall rules
- Set up GitHub Actions CI/CD pipeline
- Deploy Redis with monitoring stack
- Test deployment and accessibility

### Effort Estimate
- Estimated effort: **8 hours**
- Includes VM setup, CI/CD configuration, security setup, and testing

### Priority & Impact
- Priority: **High**
- Rationale: Enables rapid prototyping in production environment

### Acceptance Checklist
- [ ] Hetzner VM provisioned with appropriate specs
- [ ] Redis deployed successfully to production
- [ ] SSL certificates configured and working
- [ ] Firewall and security configured
- [ ] CI/CD pipeline functional and automated
- [ ] System accessible from internet
- [ ] Monitoring stack operational
- [ ] Deployment automated and repeatable
- [ ] Tests written and passing
- [ ] Documentation created

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Hetzner Documentation: https://docs.hetzner.com/
- Related tickets: MET-001, MET-003

---

## MET-003: Create data writer service for Redis to Parquet conversion

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
- Depends on: MET-001 (Redis setup), MET-002 (Hetzner deployment)

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
- Related tickets: MET-001, MET-002, MET-004

---

## MET-004: Implement load testing with mock data stream

### Context & Motivation
Load testing is essential to validate that the Redis buffer and data writer service can handle the expected production workload. This includes testing high-throughput data ingestion, buffer performance under load, and data writer processing capabilities. We need to simulate the ~8.1M events/day throughput to ensure system stability.

### Detailed Description & Requirements

#### Functional Requirements:
- Create mock data generator that simulates Bluesky firehose data
- Implement load testing scenarios for Redis buffer throughput
- Test data writer service performance under high load
- Monitor system resources during load testing
- Validate data integrity during high-load scenarios
- Test system recovery after load spikes
- Implement automated load testing scripts with Locust
- Create load testing reports and analysis

#### Non-Functional Requirements:
- Mock data should simulate realistic Bluesky firehose patterns
- Load testing should generate 100,000+ records per minute
- System should handle sustained load for 1+ hours
- Response times should remain acceptable under load
- System should recover within 5 minutes after load spikes
- Resource usage should stay within acceptable limits
- Data integrity should be maintained during all tests

#### Validation & Error Handling:
- Mock data accurately simulates real firehose data
- Redis buffer handles high-throughput without data loss
- Data writer processes all data without errors
- System resources stay within limits during load
- Data integrity is maintained throughout testing
- System recovers gracefully from load spikes

### Success Criteria
- Mock data generator creates realistic firehose data
- Load testing scenarios designed and implemented
- System handles 100,000+ records/minute without data loss
- Data writer performance meets requirements under load
- Data integrity maintained during all tests
- System recovery tested and validated
- Load testing automated and repeatable
- Comprehensive testing reports generated

### Test Plan
- `test_mock_data_generation`: Generate mock data → Realistic firehose patterns
- `test_redis_buffer_load`: High-volume data ingestion → No data loss
- `test_data_writer_load`: High-load processing → Performance maintained
- `test_system_resources`: Resource monitoring → Usage within limits
- `test_data_integrity`: Data validation → Integrity maintained
- `test_recovery_scenarios`: Load spike recovery → System recovers gracefully
- `test_sustained_load`: 1+ hour load test → System stable throughout

📁 Test file: `testing/tests/test_load_testing.py`

### Dependencies
- Depends on: MET-001 (Redis setup), MET-003 (Data writer service)

### Suggested Implementation Plan
- Create mock data generator for Bluesky firehose data
- Implement Locust load testing scenarios
- Set up monitoring for load testing
- Execute load tests with various scenarios
- Monitor system performance and resources
- Validate data integrity during tests
- Analyze results and identify bottlenecks
- Generate comprehensive testing reports

### Effort Estimate
- Estimated effort: **8 hours**
- Includes mock data generation, load testing implementation, and analysis

### Priority & Impact
- Priority: **Medium**
- Rationale: Important for validation but not blocking core functionality

### Acceptance Checklist
- [ ] Mock data generator implemented
- [ ] Load testing scenarios designed
- [ ] Automated load testing implemented
- [ ] System handles expected production load
- [ ] Data integrity maintained during tests
- [ ] System recovery tested
- [ ] Performance bottlenecks identified
- [ ] Comprehensive reports generated
- [ ] Tests documented and repeatable
- [ ] Results analyzed and documented

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Locust Documentation: https://locust.io/
- Related tickets: MET-001, MET-003

---

## MET-005: Implement Jetstream integration with Redis buffer

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
- Depends on: MET-001 (Redis setup), MET-002 (Hetzner deployment), MET-003 (Data writer service), MET-004 (Load testing)

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
- Related tickets: MET-001, MET-002, MET-003, MET-004

---

## MET-006: Create comprehensive monitoring dashboard for data pipeline

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
- Depends on: MET-001 (Redis setup), MET-002 (Hetzner deployment), MET-003 (Data writer), MET-005 (Jetstream)

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
- Related tickets: MET-001, MET-002, MET-003, MET-005

---

## Phase 1 Summary

### Total Tickets: 6
### Estimated Effort: 46 hours
### Critical Path: MET-001 → MET-002 → MET-003 → MET-005
### Key Deliverables:
- Redis instance with monitoring (local and Hetzner)
- Hetzner deployment with CI/CD pipeline
- Data writer service (Redis → Parquet)
- Load testing with mock data stream
- Jetstream integration
- Complete data pipeline (Jetstream → Redis → Parquet)
- Comprehensive monitoring dashboard

### Exit Criteria:
- Complete pipeline working for 2 days continuously
- All data flowing from Jetstream to Parquet storage
- Monitoring showing accurate metrics
- Load testing validates system performance
- System deployed to Hetzner with automated CI/CD
- Ready for Phase 2 (Query Engine & API) 