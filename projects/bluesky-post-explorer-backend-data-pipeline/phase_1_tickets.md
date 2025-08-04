# Phase 1: Core Data Pipeline - Linear Ticket Proposals

## Overview
**Objective**: Get raw data flowing from Redis to permanent storage with rapid prototyping approach
**Timeline**: Weeks 1-2
**Approach**: Rapid prototyping with piecemeal deployment to Hetzner as early as possible

---

## MET-001: Set up Redis container with Docker and basic monitoring

### Context & Motivation
Redis serves as a 5-minute buffer for high-throughput data buffering to handle ~8.1M events/day from the Bluesky firehose. The data writer service runs every 5 minutes to clear the Redis buffer after successful Parquet writes. This buffer strategy provides data durability during data writer failures while maintaining optimal performance. The complete data pipeline will run on a single Hetzner Cloud server (CX41) with Redis buffer, Parquet storage, and query engine API, providing a cost-effective MVP solution that can scale to handle the full Bluesky firehose data volume. This is the foundation for the entire data pipeline and must be operational before any data processing can begin. We need Redis running with comprehensive monitoring from day one for visibility into system performance and buffer overflow detection.

### Detailed Description & Requirements

#### Functional Requirements:
- Create Dockerfile for Redis container optimized for high-throughput buffering
- Configure Redis with 2GB memory allocation for 8-hour worst-case buffer capacity
- Set up Redis persistence strategy (AOF-only with `appendfsync everysec`) for data durability
- Implement Redis configuration optimized for buffer use case with `allkeys-lru` eviction policy
- Create Docker Compose service definition for Redis with monitoring stack
- Set up Prometheus Redis exporter for comprehensive metrics collection
- Configure Grafana dashboard for Redis buffer monitoring and overflow detection
- Implement health check endpoint for Redis with buffer status
- Set up buffer overflow detection and alerting via telemetry
- Implement coordinated error handling between Redis and data writer services
- **Infrastructure**: Deploy to Hetzner Cloud CX41 VM (4 vCPU, 8GB RAM, 160GB SSD) for complete data pipeline
- **Networking**: Configure private network (10.0.0.0/24) with firewall rules for secure service access
- **Storage**: Use Hetzner Cloud SSD volumes (500GB) for Parquet data storage and Redis persistence
- **Automation**: Implement Terraform infrastructure as code for automated deployment
- **Query Engine**: Implement query engine API for Parquet data access with DuckDB/Polars integration
- **Data Pipeline**: Set up complete data pipeline services (data writer, query engine, monitoring)

#### Non-Functional Requirements:
- Redis must handle 1000+ operations/second sustained
- Memory usage should not exceed 80% of allocated 2GB RAM (1.6GB)
- Container should start within 30 seconds
- Redis should be accessible on standard port 6379
- Prometheus metrics should be available within 60 seconds of container start
- Grafana dashboard should show comprehensive Redis buffer metrics
- Buffer overflow detection should trigger alerts within 5 minutes of overflow
- Data persistence should survive Redis restarts for up to 8 hours of buffer data
- Buffer clearing operations should complete within 30 seconds
- **Infrastructure**: VM should handle complete data pipeline workload with <80% CPU utilization
- **Storage**: SSD storage should provide <10ms latency for Redis persistence and Parquet operations
- **Network**: Private network should provide <1ms latency between services
- **Cost**: Total infrastructure cost should be <$50/month for complete data pipeline
- **Query Performance**: 1-day queries should return results in <30 seconds
- **Data Processing**: Parquet compression should achieve 80%+ compression ratio
- **Availability**: Maintain 99.9% availability for data pipeline and API services

#### Validation & Error Handling:
- Redis container starts successfully on first attempt
- Redis responds to PING commands within 100ms
- Container logs show successful startup without errors
- Memory usage stays within acceptable bounds during testing
- Prometheus can scrape Redis metrics
- Grafana dashboard displays Redis metrics correctly
- Buffer overflow detection works correctly
- Data persistence survives Redis restarts
- Buffer clearing operations complete successfully
- Error coordination between Redis and data writer functions properly
- **Infrastructure**: Hetzner Cloud VM deployment succeeds with Terraform
- **Network**: Private network connectivity and firewall rules work correctly
- **Storage**: SSD storage performance meets Redis persistence and Parquet requirements
- **Cost**: Infrastructure costs stay within budget (<$50/month)
- **Query Engine**: Query engine API responds to requests within acceptable latency
- **Data Pipeline**: Complete pipeline processes data from Redis to Parquet successfully
- **Performance**: Query performance meets <30 second requirement for 1-day queries

### Success Criteria
- Redis container starts successfully with Docker Compose
- Redis responds to basic commands (PING, INFO, etc.)
- Container logs show no errors during startup
- Redis configuration optimized for buffer operations with 2GB memory allocation
- Prometheus Redis exporter running and collecting comprehensive metrics
- Grafana dashboard showing Redis buffer metrics and overflow detection
- Health check endpoint returns 200 OK with buffer status information
- Buffer overflow detection and alerting system functional
- Data persistence survives Redis restarts with up to 8 hours of buffer data
- Buffer clearing operations complete within 30 seconds
- Error coordination between Redis and data writer services working
- **Infrastructure**: Hetzner Cloud VM deployed successfully with Terraform automation
- **Performance**: VM handles complete data pipeline workload with <80% CPU utilization under normal load
- **Storage**: SSD storage provides <10ms latency for Redis persistence and Parquet operations
- **Network**: Private network provides <1ms latency between all services
- **Cost**: Total infrastructure cost stays under $50/month
- **Query Engine**: Query engine API responds to requests with <30 second latency for 1-day queries
- **Data Pipeline**: Complete pipeline processes 8.1M events/day from Redis to Parquet successfully
- **Compression**: Parquet compression achieves 80%+ compression ratio
- **Availability**: System maintains 99.9% availability for data pipeline and API services
- Docker Compose file committed to repository

### Test Plan
- `test_redis_startup`: Docker container starts → Redis responds to PING
- `test_redis_performance`: 1000 SET operations → All complete within 5 seconds
- `test_redis_memory`: Load test data → Memory usage stays under 80% of 2GB
- `test_redis_persistence`: Write data → Data persists after container restart
- `test_prometheus_metrics`: Prometheus can scrape Redis metrics
- `test_grafana_dashboard`: Grafana displays Redis metrics correctly
- `test_health_check`: Health check endpoint returns 200 OK with buffer status
- `test_buffer_overflow_detection`: Fill buffer to 90% → Overflow alert triggered
- `test_buffer_overflow_handling`: Fill buffer to 100% → System handles gracefully
- `test_data_writer_integration`: Simulate data writer clearing → Buffer cleared successfully
- `test_data_writer_failure_recovery`: Simulate writer failure → Buffer retains data for 8 hours
- `test_redis_persistence_recovery`: Redis crash/restart → Data recovered correctly
- `test_buffer_clearing_performance`: Clear 2.7M events → Completes within 30 seconds
- `test_error_coordination`: Simulate various failures → Error handling works correctly
- `test_infrastructure_deployment`: Terraform deployment → Hetzner Cloud VM created successfully
- `test_vm_performance`: Load test → VM CPU utilization stays under 80%
- `test_storage_performance`: Redis persistence → SSD latency under 10ms
- `test_network_connectivity`: Private network → Latency under 1ms between services
- `test_firewall_rules`: Security testing → Redis accessible only via private network
- `test_cost_monitoring`: Resource usage → Monthly cost stays under $15

#### Network Performance Testing & Benchmarking (MVP Implementation):
```yaml
# Network Performance Test Suite
network_performance_tests:
  # Baseline Network Performance Tests
  - test_name: "inter_service_latency"
    description: "Measure latency between all services"
    command: "ping -c 100 -i 0.1"
    target: "<1ms average latency"
    frequency: "daily"
    
  - test_name: "network_throughput"
    description: "Test network throughput between services"
    command: "iperf3 -c target_service -t 30"
    target: ">1000 Mbps"
    frequency: "weekly"
    
  - test_name: "connection_establishment"
    description: "Test TCP connection establishment time"
    command: "timeout 5 bash -c '</dev/tcp/service_ip/port'"
    target: "<10ms"
    frequency: "daily"
    
  - test_name: "packet_loss"
    description: "Test packet loss rate"
    command: "ping -c 1000 -i 0.1 -q"
    target: "<0.1% packet loss"
    frequency: "daily"
    
  - test_name: "network_utilization"
    description: "Monitor network interface utilization"
    command: "cat /proc/net/dev"
    target: "<80% utilization"
    frequency: "continuous"
    
  - test_name: "bandwidth_capacity"
    description: "Test maximum bandwidth capacity"
    command: "iperf3 -c target_service -t 60 -R"
    target: ">1000 Mbps sustained"
    frequency: "weekly"
    
  - test_name: "concurrent_connections"
    description: "Test maximum concurrent connections"
    command: "ab -n 10000 -c 100 http://service:port/"
    target: "Handle 1000+ concurrent connections"
    frequency: "weekly"

# Network Security Testing
network_security_tests:
  - test_name: "port_scanning"
    description: "Verify only expected ports are open"
    command: "nmap -sT -p- target_ip"
    expected: "Only ports 22, 6379, 8000, 8001, 9090, 9121 open"
    frequency: "daily"
    
  - test_name: "unauthorized_access"
    description: "Test unauthorized access attempts"
    command: "telnet target_ip 80"  # Should fail
    expected: "Connection refused"
    frequency: "daily"
    
  - test_name: "service_isolation"
    description: "Verify services can only communicate on allowed ports"
    command: "telnet service_ip 6379"  # From unauthorized service
    expected: "Connection refused"
    frequency: "daily"

# Network Monitoring Implementation
network_monitoring_implementation:
  # Prometheus Node Exporter Configuration
  node_exporter_network_metrics:
    - metric: "node_network_receive_bytes_total"
      description: "Network bytes received"
    - metric: "node_network_transmit_bytes_total"
      description: "Network bytes transmitted"
    - metric: "node_network_receive_packets_total"
      description: "Network packets received"
    - metric: "node_network_transmit_packets_total"
      description: "Network packets transmitted"
    - metric: "node_network_receive_errs_total"
      description: "Network receive errors"
    - metric: "node_network_transmit_errs_total"
      description: "Network transmit errors"
    - metric: "node_network_receive_drop_total"
      description: "Network receive drops"
    - metric: "node_network_transmit_drop_total"
      description: "Network transmit drops"

  # Custom Network Metrics (via custom exporter)
  custom_network_metrics:
    - metric: "network_inter_service_latency_ms"
      description: "Latency between services"
      collection_method: "Custom script with ping/curl"
    - metric: "network_connection_count"
      description: "Active TCP connections"
      collection_method: "netstat -an | grep ESTABLISHED | wc -l"
    - metric: "network_bandwidth_utilization_percent"
      description: "Network bandwidth utilization"
      collection_method: "Calculate from bytes_in/bytes_out"

# Network Performance Benchmarking Tools
network_benchmarking_tools:
  - tool: "iperf3"
    purpose: "Network throughput testing"
    installation: "apt-get install iperf3"
    usage: "iperf3 -s (server) / iperf3 -c server_ip (client)"
    
  - tool: "ping"
    purpose: "Latency and packet loss testing"
    installation: "Built-in"
    usage: "ping -c count -i interval target_ip"
    
  - tool: "netstat"
    purpose: "Network connection monitoring"
    installation: "Built-in"
    usage: "netstat -an | grep ESTABLISHED"
    
  - tool: "ss"
    purpose: "Socket statistics"
    installation: "Built-in"
    usage: "ss -tuln"
    
  - tool: "nmap"
    purpose: "Port scanning and network discovery"
    installation: "apt-get install nmap"
    usage: "nmap -sT -p- target_ip"
    
  - tool: "tcpdump"
    purpose: "Network packet analysis"
    installation: "apt-get install tcpdump"
    usage: "tcpdump -i any -w capture.pcap"

# Network Performance Baseline Establishment
network_baseline_establishment:
  # Phase 1: Establish baselines under normal load
  normal_load_baselines:
    - metric: "inter_service_latency_ms"
      target: "<1ms"
      measurement_duration: "24 hours"
      acceptable_variance: "±20%"
      
  - metric: "network_throughput_mbps"
    target: ">1000 Mbps"
    measurement_duration: "1 hour"
    acceptable_variance: "±10%"
    
  - metric: "packet_loss_percent"
    target: "<0.1%"
    measurement_duration: "24 hours"
    acceptable_variance: "±50%"
    
  - metric: "connection_establishment_ms"
    target: "<10ms"
    measurement_duration: "1 hour"
    acceptable_variance: "±30%"

  # Phase 2: Establish baselines under high load
  high_load_baselines:
    - metric: "inter_service_latency_ms"
      target: "<5ms"
      measurement_duration: "1 hour"
      load_condition: "8.1M events/day equivalent"
      
  - metric: "network_throughput_mbps"
    target: ">800 Mbps"
    measurement_duration: "1 hour"
    load_condition: "Peak load simulation"
    
  - metric: "packet_loss_percent"
    target: "<1%"
    measurement_duration: "1 hour"
    load_condition: "High load stress test"

# Network Performance Monitoring Dashboard
network_monitoring_dashboard:
  grafana_panels:
    - panel: "Network Throughput"
      metrics: ["network_bytes_in", "network_bytes_out"]
      visualization: "Time series graph"
      refresh_rate: "30s"
      
  - panel: "Network Latency"
    metrics: ["network_inter_service_latency_ms"]
    visualization: "Time series graph"
    refresh_rate: "30s"
    
  - panel: "Network Errors"
    metrics: ["network_packets_dropped", "network_connection_errors"]
    visualization: "Time series graph"
    refresh_rate: "30s"
    
  - panel: "Network Utilization"
    metrics: ["network_interface_utilization_percent"]
    visualization: "Gauge"
    refresh_rate: "30s"
    
  - panel: "Active Connections"
    metrics: ["network_connection_count"]
    visualization: "Stat panel"
    refresh_rate: "30s"
```

📁 Test file: `services/redis/tests/test_redis_setup.py`

### Dependencies
- None (foundational component)

### Technical Implementation Strategy

#### Server Architecture (Single Server MVP):
```
Hetzner Cloud CX41 Server (4 vCPU, 8GB RAM, 160GB SSD + 500GB SSD Volume)
├── Redis Buffer (2GB RAM) - High-throughput data buffering
├── Data Writer Service (1GB RAM) - Redis to Parquet conversion
├── Jetstream Service (512MB RAM) - Bluesky Firehose connector
├── Prometheus (256MB RAM) - Metrics collection
├── Redis Exporter (64MB RAM) - Redis metrics for Prometheus
├── Parquet Storage (500GB SSD Volume)
└── Total Memory Allocation: ~3.8GB (within 8GB limit)
```

#### Service Dependencies & Startup Order:
```
1. Redis (Foundation) - No dependencies
   ├── Health check: redis-cli ping (10s interval)
   └── Startup time: 30s

2. Data Writer (Depends on Redis)
   ├── Requires Redis to be running and healthy
   ├── Health check: HTTP /health endpoint (30s interval)
   └── Startup time: 60s

3. Jetstream (Depends on Redis)
   ├── Requires Redis to be running and healthy
   ├── Health check: HTTP /health endpoint (30s interval)
   └── Startup time: 60s

4. Prometheus (Depends on all services)
   ├── Requires all services to be running
   ├── Health check: HTTP /-/healthy endpoint (30s interval)
   └── Startup time: 45s

5. Redis Exporter (Depends on Redis)
   ├── Requires Redis to be running and healthy
   ├── Health check: HTTP /metrics endpoint (30s interval)
   └── Startup time: 15s
```

#### Data Volume Analysis:
- **Current**: 8.1M events/day = ~4GB/day = ~120GB/month
- **Target**: 1TB every 3 months = ~333GB/month = ~11GB/day
- **Growth Factor**: 2.7x increase from current to target volume
- **Storage Requirements**: 500GB SSD volume for 3+ months of data storage

#### Buffer Strategy & Data Volume:
- **Primary Use Case**: 5-minute buffer cleared by data writer every 5 minutes
- **Worst-Case Scenario**: 8-hour buffer capacity for data writer failures
- **Data Volume**: ~2.7M events in 8 hours (93.75 events/second × 28,800 seconds)
- **Memory Requirements**: ~1.35GB for 8-hour buffer (500 bytes/event average)
- **Recommended Allocation**: 2GB Redis memory with 50% headroom for overhead

#### Redis Configuration for 8-Hour Buffer:
```redis
# Memory management
maxmemory 2gb
maxmemory-policy allkeys-lru

# Persistence (AOF-only for buffer use case)
appendonly yes
appendfsync everysec
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# Performance optimization
tcp-keepalive 300
timeout 0
tcp-backlog 511
databases 1
save ""  # Disable RDB for buffer use case
```

#### Buffer Overflow Detection via Telemetry:
```python
# Redis monitoring metrics to track
redis_buffer_utilization_bytes = redis.memory_used()
redis_buffer_events_count = redis.llen('firehose_buffer')
redis_buffer_oldest_event_age_seconds = time.time() - oldest_event_timestamp

# Alerting thresholds
if redis_buffer_utilization_bytes > 1.8gb:  # 90% of 2GB
    alert("REDIS_BUFFER_HIGH_UTILIZATION")
    
if redis_buffer_oldest_event_age_seconds > 300:  # 5 minutes
    alert("REDIS_BUFFER_OVERFLOW_DETECTED")
    
if redis_buffer_events_count > 2.5M:  # Approaching 8-hour limit
    alert("REDIS_BUFFER_APPROACHING_CAPACITY")
```

#### Data Writer Integration Strategy:
```python
class DataWriter:
    def __init__(self):
        self.redis_client = redis.Redis()
        self.last_processed_id = None
        
    def process_buffer(self):
        """Process Redis buffer using streaming pattern to avoid memory issues"""
        batch_size = 1000
        total_processed = 0
        
        while True:
            # 1. Get batch of records from Redis buffer (streaming approach)
            batch_records = self.redis_client.lrange('firehose_buffer', 0, batch_size - 1)
            
            if not batch_records:
                break  # No more records to process
                
            # 2. Write batch to Parquet
            written_ids = self.write_batch_to_parquet(batch_records)
            
            if written_ids:
                # 3. Remove only successfully written records atomically
                self.remove_written_records(written_ids)
                total_processed += len(written_ids)
                
            # 4. Continue with next batch
            if len(batch_records) < batch_size:
                break  # Last batch processed
                
        logger.info(f"Processed {total_processed} records from buffer")
        
    def write_to_parquet(self, records):
        """Write records to Parquet and return IDs of written records"""
        written_ids = []
        
        for batch in self.create_batches(records, 1000):
            try:
                # Write batch to Parquet
                batch_ids = self.write_batch_to_parquet(batch)
                written_ids.extend(batch_ids)
            except Exception as e:
                # Log error but continue with next batch
                logger.error(f"Failed to write batch: {e}")
                continue
                
        return written_ids
        
    def remove_written_records(self, written_ids):
        """Remove only successfully written records from Redis"""
        # Use Redis pipeline for efficiency
        pipe = self.redis_client.pipeline()
        
        for record_id in written_ids:
            # Remove specific record by ID
            pipe.lrem('firehose_buffer', 0, record_id)
            
        pipe.execute()
```

#### Error Coordination Strategy:
```python
class ErrorCoordinator:
    def __init__(self):
        self.redis_client = redis.Redis()
        self.alert_manager = AlertManager()
        
    def handle_data_writer_failure(self, error):
        """Handle data writer failures"""
        # 1. Log error
        logger.error(f"Data writer failure: {error}")
        
        # 2. Check buffer state
        buffer_stats = self.get_buffer_stats()
        
        # 3. Alert based on severity
        if buffer_stats['oldest_event_age'] > 3600:  # > 1 hour
            self.alert_manager.send_critical_alert("DATA_WRITER_FAILURE_CRITICAL")
        elif buffer_stats['oldest_event_age'] > 300:  # > 5 minutes
            self.alert_manager.send_warning_alert("DATA_WRITER_FAILURE_WARNING")
            
        # 4. Attempt recovery
        self.attempt_recovery()
        
    def handle_redis_failure(self, error):
        """Handle Redis failures"""
        # 1. Log error
        logger.error(f"Redis failure: {error}")
        
        # 2. Alert immediately
        self.alert_manager.send_critical_alert("REDIS_FAILURE")
        
        # 3. Attempt Redis restart/recovery
        self.attempt_redis_recovery()
        
    def get_buffer_stats(self):
        """Get comprehensive buffer statistics"""
        try:
            return {
                'size': self.redis_client.llen('firehose_buffer'),
                'memory_used': self.redis_client.memory_used(),
                'oldest_event_age': self.get_oldest_event_age(),
                'redis_connected': True
            }
        except Exception as e:
            return {
                'redis_connected': False,
                'error': str(e)
            }
```

#### Buffer Overflow Testing Scheme:
```python
class BufferOverflowTests:
    def test_buffer_fills_up_gradually(self):
        """Test buffer behavior as it fills up over time"""
        # 1. Start data writer
        # 2. Stop data writer
        # 3. Inject data for 8 hours (simulated)
        # 4. Verify buffer doesn't lose data
        # 5. Restart data writer
        # 6. Verify all data is processed
        
    def test_buffer_overflow_alerting(self):
        """Test overflow detection and alerting"""
        # 1. Fill buffer to 90% capacity
        # 2. Verify alert is triggered
        # 3. Fill buffer to 100% capacity
        # 4. Verify critical alert is triggered
        
    def test_data_writer_recovery(self):
        """Test data writer recovery after failure"""
        # 1. Fill buffer with 4 hours of data
        # 2. Simulate data writer failure
        # 3. Continue filling buffer for 4 more hours
        # 4. Restart data writer
        # 5. Verify all 8 hours of data is processed
        
    def test_redis_persistence_recovery(self):
        """Test Redis persistence and recovery"""
        # 1. Fill buffer with data
        # 2. Simulate Redis crash
        # 3. Restart Redis
        # 4. Verify data is recovered
        # 5. Verify data writer can continue processing
```

#### Infrastructure Specifications:
```yaml
# VM Configuration (Single Server MVP)
vm_type: "cx41"
cpu_cores: 4
ram_gb: 8
storage_gb: 160
storage_type: "ssd"
location: "nbg1"  # Nuremberg, Germany
cost_per_month: "~€16.80 (~$18/month)"

# Additional Storage Volume
volume_size: "500GB"
volume_type: "ssd"
volume_cost_per_month: "~€25.00 (~$27/month)"
volume_mount_path: "/data"

# Network Configuration
private_network: "10.0.0.0/24"
service_ips:
  redis: "10.0.0.10"
  data_writer: "10.0.0.11"
  jetstream: "10.0.0.12"
  prometheus: "10.0.0.13"
  redis_exporter: "10.0.0.14"

# Storage Configuration
storage_paths:
  redis_data: "/data/redis"
  parquet_storage: "/data/parquet"
  prometheus_data: "/data/prometheus"

# Firewall Rules
allowed_ports:
  - port: 22
    protocol: "tcp"
    source: "your_ip_only"
  - port: 6379
    protocol: "tcp"
    source: "private_network_only"
  - port: 9090
    protocol: "tcp"
    source: "private_network_only"
  - port: 8000
    protocol: "tcp"
    source: "private_network_only"  # Data Writer API
  - port: 8001
    protocol: "tcp"
    source: "private_network_only"  # Jetstream API

# Resource Allocation Strategy
resource_limits:
  redis:
    memory: "2G"
    cpus: "1.0"
  data_writer:
    memory: "1G"
    cpus: "0.5"
  jetstream:
    memory: "512M"
    cpus: "0.5"
  prometheus:
    memory: "256M"
    cpus: "0.25"
  redis_exporter:
    memory: "64M"
    cpus: "0.1"

# Storage Configuration
redis_persistence_path: "/data/redis/appendonly.aof"
parquet_storage_path: "/data/parquet"
backup_strategy: "daily_to_object_storage"
estimated_storage_growth: "~4GB/day"
total_cost_per_month: "~€41.80 (~$45/month)"
```

#### Network Security & Policies (MVP Implementation):
```yaml
# Docker Network Security Policies
docker_network_policies:
  # Service-to-service communication rules
  allowed_connections:
    - from: "data-writer"
      to: "redis"
      ports: [6379]
      protocol: "tcp"
    - from: "jetstream"
      to: "redis"
      ports: [6379]
      protocol: "tcp"
    - from: "redis-exporter"
      to: "redis"
      ports: [6379]
      protocol: "tcp"
    - from: "prometheus"
      to: "redis-exporter"
      ports: [9121]
      protocol: "tcp"
    - from: "prometheus"
      to: "data-writer"
      ports: [8000]
      protocol: "tcp"
    - from: "prometheus"
      to: "jetstream"
      ports: [8001]
      protocol: "tcp"

  # Deny all other inter-service communication
  default_policy: "deny"

# Network Security Implementation Strategy:
# 1. Use Docker's built-in network isolation
# 2. Implement iptables rules for additional security
# 3. Use service-specific networks for isolation
# 4. Monitor network traffic for anomalies

# iptables Rules for Additional Security:
iptables_rules:
  - rule: "Allow established connections"
    command: "iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT"
  - rule: "Allow loopback traffic"
    command: "iptables -A INPUT -i lo -j ACCEPT"
  - rule: "Allow SSH from specific IPs"
    command: "iptables -A INPUT -p tcp --dport 22 -s YOUR_IP -j ACCEPT"
  - rule: "Allow internal service communication"
    command: "iptables -A INPUT -s 10.0.0.0/24 -j ACCEPT"
  - rule: "Drop all other traffic"
    command: "iptables -A INPUT -j DROP"

# Network Monitoring Strategy:
network_monitoring:
  # Traffic monitoring
  - metric: "network_bytes_in"
    description: "Incoming network traffic"
    alert_threshold: ">100MB/s sustained for 5 minutes"
  - metric: "network_bytes_out"
    description: "Outgoing network traffic"
    alert_threshold: ">100MB/s sustained for 5 minutes"
  - metric: "network_packets_dropped"
    description: "Dropped network packets"
    alert_threshold: ">1000 packets in 1 minute"
  - metric: "network_connection_count"
    description: "Active network connections"
    alert_threshold: ">1000 connections"
  - metric: "network_latency_ms"
    description: "Inter-service network latency"
    alert_threshold: ">10ms average over 5 minutes"

# Network Performance Baselines:
network_baselines:
  inter_service_latency_ms: "<1ms"
  network_throughput_mbps: ">1000 Mbps"
  connection_establishment_ms: "<10ms"
  packet_loss_percent: "<0.1%"
  network_utilization_percent: "<80%"
```

#### Parquet Storage Layout:
```yaml
# Parquet Storage Structure
/data/parquet/
├── year=2024/
│   ├── month=01/
│   │   ├── day=01/
│   │   │   ├── hour=00/
│   │   │   │   ├── posts.parquet
│   │   │   │   ├── likes.parquet
│   │   │   │   └── follows.parquet
│   │   │   └── hour=01/
│   │   └── day=02/
│   └── month=02/
└── year=2025/

# Storage Optimization
partitioning_strategy: "year/month/day/hour"
compression: "snappy"
file_size_target: "100MB per file"
compression_ratio_target: "80%+"
```

#### Query Engine Integration:
```python
# Query Engine Implementation
class QueryEngine:
    def __init__(self):
        self.parquet_path = "/data/parquet"
        self.duckdb_connection = duckdb.connect()
        
    def query_posts(self, start_date, end_date, filters=None):
        """Query posts with date range and optional filters"""
        query = f"""
        SELECT * FROM read_parquet('{self.parquet_path}/**/*.parquet')
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        AND record_type = 'post'
        """
        if filters:
            query += f" AND {filters}"
        return self.duckdb_connection.execute(query).fetchdf()
        
    def get_daily_stats(self, date):
        """Get daily statistics for a specific date"""
        query = f"""
        SELECT 
            COUNT(*) as total_posts,
            COUNT(DISTINCT author) as unique_authors,
            AVG(length(text)) as avg_post_length
        FROM read_parquet('{self.parquet_path}/year={date.year}/month={date.month:02d}/day={date.day:02d}/**/*.parquet')
        WHERE record_type = 'post'
        """
        return self.duckdb_connection.execute(query).fetchdf()
        
    def search_posts(self, search_term, date_range=None):
        """Search posts by text content"""
        query = f"""
        SELECT * FROM read_parquet('{self.parquet_path}/**/*.parquet')
        WHERE record_type = 'post'
        AND text ILIKE '%{search_term}%'
        """
        if date_range:
            query += f" AND date BETWEEN '{date_range[0]}' AND '{date_range[1]}'"
        return self.duckdb_connection.execute(query).fetchdf()
```

#### Monitoring Requirements:
```yaml
# Essential Metrics
redis_buffer_size_events: Number of events in buffer
redis_buffer_memory_bytes: Memory usage of buffer
redis_buffer_oldest_event_age_seconds: Age of oldest event
redis_buffer_utilization_percent: Buffer utilization percentage

# Data Writer Metrics
data_writer_processing_duration_seconds: Time to process buffer
data_writer_events_processed_per_second: Processing rate
data_writer_success_rate_percent: Success rate of writes
data_writer_failure_count: Number of failures

# Query Engine Metrics
query_engine_response_time_ms: Query response time
query_engine_requests_per_second: Query throughput
query_engine_error_rate_percent: Query error rate
query_engine_cache_hit_rate_percent: Cache hit rate

# Storage Metrics
parquet_storage_size_bytes: Total Parquet storage size
parquet_compression_ratio_percent: Compression ratio achieved
parquet_files_count: Number of Parquet files
storage_utilization_percent: Storage volume utilization

# System Health Metrics
redis_connected: Redis connection status
data_writer_healthy: Data writer health status
query_engine_healthy: Query engine health status
buffer_overflow_detected: Overflow detection flag

# Infrastructure Metrics
vm_cpu_utilization_percent: VM CPU usage
vm_memory_utilization_percent: VM memory usage
vm_disk_io_latency_ms: Storage I/O latency
network_latency_ms: Private network latency
infrastructure_cost_usd: Monthly infrastructure cost

# Network Monitoring Metrics (MVP Implementation)
network_bytes_in: Incoming network traffic in bytes
network_bytes_out: Outgoing network traffic in bytes
network_packets_in: Incoming network packets
network_packets_out: Outgoing network packets
network_packets_dropped: Dropped network packets
network_connection_count: Active network connections
network_connection_errors: Network connection errors
network_interface_utilization_percent: Network interface utilization
network_inter_service_latency_ms: Inter-service network latency
network_bandwidth_utilization_percent: Network bandwidth utilization
network_security_events: Network security events (failed connections, etc.)
```

#### Cost Breakdown & Analysis:
```yaml
# Single Server MVP (CX41 + 500GB Storage)
vm_cost: "€16.80/month (~$18/month)"
storage_cost: "€25.00/month (~$27/month)"
network_cost: "€0/month (included)"
total_cost: "€41.80/month (~$45/month)"

# Cost Optimization
- VM provides 4 vCPU, 8GB RAM for complete data pipeline
- 500GB SSD storage for 3+ months of data
- All services run on single server (no network costs)
- Cost stays well under $50/month budget

# Scaling Considerations
- Can handle current 8.1M events/day easily
- Can scale to ~20M events/day before needing upgrade
- Storage can be expanded to 1TB+ as needed
- Clear migration path to multi-server when needed
```

#### Migration Path:
```yaml
# Phase 1 (Months 1-3): Single Server MVP
- Deploy everything on CX41 server
- Validate data pipeline and query performance
- Monitor resource usage and costs
- Target: 8.1M events/day processing

# Phase 2 (Months 4-6): Optimize Single Server
- Add more storage as needed (up to 1TB)
- Optimize query performance with caching
- Consider upgrading to CX51 if needed
- Target: 15M events/day processing

# Phase 3 (Months 7+): Multi-Server Migration
- Split into dedicated servers when needed
- Implement data lifecycle management
- Add hot/warm storage tiers
- Target: 22M+ events/day processing

# Migration Triggers
- CPU utilization consistently >80%
- Memory utilization consistently >85%
- Query response times >30 seconds
- Storage utilization >80%
- Data volume >15M events/day
```

#### Alerting Rules:
```yaml
# Critical Alerts
- alert: RedisBufferOverflow
  condition: redis_buffer_oldest_event_age_seconds > 300
  severity: critical
  
- alert: DataWriterFailure
  condition: data_writer_healthy == false
  severity: critical
  
- alert: RedisConnectionLost
  condition: redis_connected == false
  severity: critical

- alert: QueryEngineFailure
  condition: query_engine_healthy == false
  severity: critical

# Warning Alerts
- alert: BufferHighUtilization
  condition: redis_buffer_utilization_percent > 80
  severity: warning
  
- alert: DataWriterSlow
  condition: data_writer_processing_duration_seconds > 240
  severity: warning

- alert: QueryEngineSlow
  condition: query_engine_response_time_ms > 30000
  severity: warning

- alert: StorageHighUtilization
  condition: storage_utilization_percent > 80
  severity: warning

# Infrastructure Alerts
- alert: VMHighCPU
  condition: vm_cpu_utilization_percent > 80
  severity: warning
  
- alert: VMHighMemory
  condition: vm_memory_utilization_percent > 85
  severity: warning
  
- alert: StorageHighLatency
  condition: vm_disk_io_latency_ms > 10
  severity: warning
  
- alert: InfrastructureCostHigh
  condition: infrastructure_cost_usd > 50
  severity: warning

# Network Alerts (MVP Implementation)
- alert: NetworkHighUtilization
  condition: network_interface_utilization_percent > 80
  severity: warning

- alert: NetworkHighLatency
  condition: network_inter_service_latency_ms > 10
  severity: warning

- alert: NetworkPacketLoss
  condition: network_packets_dropped > 1000
  severity: critical

- alert: NetworkBandwidthExceeded
  condition: network_bandwidth_utilization_percent > 90
  severity: warning

- alert: NetworkConnectionErrors
  condition: network_connection_errors > 100
  severity: warning

- alert: NetworkSecurityBreach
  condition: network_security_events > 50
  severity: critical
```

### Suggested Implementation Plan

#### Service Implementation:
- Install and configure Redis 7.2+ on Hetzner VM with buffer-optimized settings
- Create Data Writer service in Python with Redis connection and Parquet writing
- Create Jetstream service in Python with Bluesky firehose integration
- Add health check endpoints for all services (HTTP /health endpoints)
- Implement proper logging and error handling for all services

#### Infrastructure & Deployment:
- Create Terraform configuration for Hetzner Cloud CX41 deployment
- Configure network and storage mounting on host
- Set up firewall rules for service access (ports 6379, 9090, 8000, 8001)
- Mount 500GB SSD volume to `/data` for persistent storage
- Implement automated deployment scripts
- Configure iptables rules for additional network security
- Set up network monitoring and performance testing tools

#### Monitoring & Observability:
- Install and configure Prometheus for metrics collection
- Add Redis exporter for Redis metrics
- Configure Prometheus to scrape metrics from all services
- Set up comprehensive health checks and monitoring
- Implement buffer overflow detection and alerting system
- Add coordinated error handling between all services
- Configure Node Exporter for network metrics collection
- Set up custom network monitoring scripts and exporters
- Implement network performance testing and benchmarking
- Configure network-specific alerting rules

#### Testing & Validation:
- Test service startup order and dependency resolution
- Validate inter-service communication and network connectivity
- Test data persistence across service restarts
- Verify resource allocation and performance under load
- Test complete pipeline end-to-end functionality
- Execute network performance benchmarking tests
- Validate network security policies and firewall rules
- Test network monitoring and alerting functionality
- Establish network performance baselines
- Validate network capacity under expected load

### Effort Estimate
- Estimated effort: **21-26 hours**
- Assumes Hetzner VM is already set up
- Includes service implementation, infrastructure setup, monitoring configuration, and network security implementation

#### Detailed Breakdown:
- **Service Implementation**: 8-10 hours
  - Redis installation and configuration optimization
  - Data Writer service development with health endpoints
  - Jetstream service integration with Redis
  - Health check endpoints for all services
  - Error handling and logging implementation

- **Infrastructure & Deployment**: 5-6 hours
  - Terraform configuration for Hetzner Cloud CX41
  - Network and storage setup on host
  - Firewall configuration and security setup
  - Automated deployment scripts
  - iptables rules configuration for network security
  - Network monitoring tools installation

- **Monitoring & Testing**: 6-7 hours
  - Prometheus installation and configuration
  - Redis exporter setup and metrics collection
  - Health check validation and monitoring setup
  - Service dependency testing and validation
  - End-to-end pipeline testing and validation
  - Node Exporter configuration for network metrics
  - Custom network monitoring scripts implementation
  - Network performance testing and benchmarking setup

- **Documentation & Integration**: 2-3 hours
  - Service documentation and configuration guides
  - Integration testing and validation
  - Performance optimization and tuning
  - Network security and monitoring documentation

### Priority & Impact
- Priority: **High**
- Rationale: Blocks all downstream data processing components

### Acceptance Checklist
- [ ] Redis Dockerfile created and optimized for buffer operations
- [ ] Docker Compose service defined with monitoring stack
- [ ] Redis starts successfully on first attempt with 2GB memory allocation
- [ ] Basic performance tests pass with buffer-specific requirements
- [ ] Prometheus metrics collection working with custom buffer metrics
- [ ] Grafana dashboard displaying Redis buffer metrics and overflow detection
- [ ] Health check endpoint functional with buffer status information
- [ ] Buffer overflow detection and alerting system implemented
- [ ] Data persistence survives Redis restarts with up to 8 hours of buffer data
- [ ] Buffer clearing operations complete within 30 seconds
- [ ] Error coordination between Redis and data writer services working
- [ ] **Infrastructure**: Terraform configuration created and Hetzner Cloud CX41 VM deployed successfully
- [ ] **Networking**: Private network configured with proper firewall rules for all services
- [ ] **Network Security**: iptables rules configured and network policies implemented
- [ ] **Network Monitoring**: Node Exporter and custom network metrics collection configured
- [ ] **Network Performance**: Network performance testing and benchmarking tools installed and configured
- [ ] **Storage**: 500GB SSD volume configured for Parquet data storage with optimal performance
- [ ] **Query Engine**: Query engine API implemented with DuckDB/Polars for Parquet queries
- [ ] **Data Pipeline**: Complete pipeline processes data from Redis to Parquet successfully
- [ ] **Performance**: VM handles complete data pipeline workload with <80% CPU utilization under normal load
- [ ] **Query Performance**: Query engine responds to 1-day queries with <30 second latency
- [ ] **Compression**: Parquet compression achieves 80%+ compression ratio
- [ ] **Cost**: Infrastructure costs stay within budget (<$50/month)
- [ ] **Monitoring**: Comprehensive monitoring for VM, storage, query engine, network, and costs configured
- [ ] **Network Baselines**: Network performance baselines established and documented
- [ ] Configuration documented in README with complete system architecture and implementation details
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

**Note**: This implementation uses a simplified deployment approach with direct production deployment via GitHub Actions. Advanced deployment strategies (blue-green deployments, rollback mechanisms, staging environments) will be implemented in future phases as the system matures.

### Detailed Description & Requirements

#### Functional Requirements:
- Set up Hetzner VM with appropriate specifications for Redis workload
- Configure Docker and Docker Compose for production deployment
- Deploy Redis container with monitoring stack to Hetzner
- Set up automated CI/CD pipeline using GitHub Actions for deployment
- Configure production environment variables and secrets
- Set up SSL/TLS certificates for secure access
- Configure firewall and network security
- Implement automated deployment scripts

#### Non-Functional Requirements:
- VM should have sufficient resources (2+ CPU cores, 4GB+ RAM, 80GB+ SSD)
- Deployment should complete within 15 minutes
- SSL certificates should be automatically renewed
- Firewall should block unnecessary ports
- GitHub Actions CI/CD pipeline should deploy on every main branch push
- System should be accessible via HTTPS

#### Validation & Error Handling:
- Redis container starts successfully in Hetzner environment
- SSL certificates are valid and working
- Firewall blocks unauthorized access
- GitHub Actions CI/CD pipeline deploys successfully
- System is accessible from internet
- Deployment scripts are idempotent

### Success Criteria
- Redis deployed to Hetzner successfully
- SSL/TLS certificates configured and working
- Firewall and security configured
- GitHub Actions CI/CD pipeline functional and automated
- System accessible from internet
- Monitoring stack operational in production
- Deployment automated and repeatable

### Test Plan
- `test_hetzner_deployment`: Deploy to Hetzner → Redis starts successfully
- `test_ssl_certificates`: HTTPS access → Certificates valid and working
- `test_firewall_security`: Unauthorized access → Blocked appropriately
- `test_github_actions_pipeline`: Push to main → Automatic deployment successful
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
- Set up GitHub Actions CI/CD pipeline with automated deployment
- Deploy Redis with monitoring stack
- Test deployment and accessibility

#### GitHub Actions Implementation Details:
```yaml
# .github/workflows/deploy.yml
name: Deploy to Hetzner
on:
  push:
    branches: [main]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
          
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          
      - name: Run tests
        run: |
          pytest tests/
          
      - name: Deploy to Hetzner
        env:
          HETZNER_TOKEN: ${{ secrets.HETZNER_TOKEN }}
          HETZNER_SERVER_IP: ${{ secrets.HETZNER_SERVER_IP }}
          SSH_PRIVATE_KEY: ${{ secrets.SSH_PRIVATE_KEY }}
        run: |
          # Deploy using Terraform or direct SSH deployment
          # Implementation details to be added
```

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
- [ ] GitHub Actions CI/CD pipeline functional and automated
- [ ] System accessible from internet
- [ ] Monitoring stack operational
- [ ] Deployment automated and repeatable
- [ ] Tests written and passing
- [ ] Documentation created

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Hetzner Documentation: https://docs.hetzner.com/
- GitHub Actions Documentation: https://docs.github.com/en/actions
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

---

## MET-007: Containerize Phase 1 services with Docker Compose orchestration

### Context & Motivation
After successfully implementing and validating the core data pipeline services individually, we need to containerize the entire system using Docker Compose for improved deployment consistency, service isolation, and operational efficiency. This ticket focuses on creating a production-ready containerized environment that maintains the same functionality while providing better resource management, service discovery, and deployment automation.

### Detailed Description & Requirements

#### Functional Requirements:
- Create Dockerfiles for all Phase 1 services (Redis, Data Writer, Jetstream, Prometheus, Redis Exporter)
- Design comprehensive Docker Compose configuration with proper service dependencies
- Implement service discovery and inter-service communication patterns
- Configure volume management for persistent data storage
- Set up resource limits and reservations for all services
- Implement health checks and monitoring integration
- Create environment-specific configurations (development, staging, production)
- Add secrets management and secure configuration handling

#### Non-Functional Requirements:
- All services must start within 2 minutes of `docker-compose up`
- Service dependencies must be properly managed with health checks
- Resource usage must stay within VM limits (8GB RAM, 4 vCPU)
- Volume persistence must survive container restarts
- Network isolation must prevent unauthorized access
- Configuration must be environment-agnostic
- Deployment must be automated and repeatable

#### Validation & Error Handling:
- All services start successfully in Docker environment
- Service dependencies resolve correctly with health checks
- Inter-service communication works through Docker network
- Volume data persists across container restarts
- Resource limits prevent system overload
- Health checks provide accurate service status
- Monitoring integration works in containerized environment

### Success Criteria
- Complete Docker Compose configuration for all Phase 1 services
- All services containerized and running successfully
- Service dependencies and health checks working correctly
- Volume persistence and data integrity maintained
- Resource allocation optimized and within limits
- Monitoring and alerting functional in containerized environment
- Deployment automated and repeatable across environments
- Documentation complete for containerized deployment

### Test Plan
- `test_docker_build`: Build all containers → All images created successfully
- `test_service_startup`: Start all services → All services healthy within 2 minutes
- `test_dependencies`: Verify service dependencies → Health checks resolve correctly
- `test_networking`: Test inter-service communication → All services can communicate
- `test_volumes`: Test volume persistence → Data survives container restarts
- `test_resources`: Monitor resource usage → All services within limits
- `test_monitoring`: Verify monitoring integration → Metrics collection working
- `test_deployment`: Test automated deployment → Repeatable across environments

📁 Test file: `deployment/tests/test_docker_orchestration.py`

### Dependencies
- Depends on: MET-001, MET-002, MET-003, MET-004, MET-005, MET-006 (All Phase 1 services implemented and validated)

### Technical Implementation Strategy

#### Docker Compose Architecture:
```
Docker Compose Services:
├── redis (Redis 7.2-alpine)
│   ├── Memory: 2GB limit, 1GB reservation
│   ├── Health check: redis-cli ping
│   └── Volume: redis_data → /data
├── data-writer (Python service)
│   ├── Memory: 1GB limit, 512MB reservation
│   ├── Health check: HTTP /health
│   ├── Depends on: redis (healthy)
│   └── Volume: parquet_storage → /data/parquet
├── jetstream (Python service)
│   ├── Memory: 512MB limit, 256MB reservation
│   ├── Health check: HTTP /health
│   ├── Depends on: redis (healthy)
│   └── Volume: logs → /app/logs
├── prometheus (Prometheus)
│   ├── Memory: 256MB limit, 128MB reservation
│   ├── Health check: HTTP /-/healthy
│   ├── Depends on: all services
│   └── Volume: prometheus_data → /prometheus
└── redis-exporter (Redis Exporter)
    ├── Memory: 64MB limit, 32MB reservation
    ├── Health check: HTTP /metrics
    ├── Depends on: redis (healthy)
    └── No persistent volumes

Docker Networks:
└── bluesky_private (172.20.0.0/16)
    ├── Service isolation
    ├── Internal DNS resolution
    └── Custom subnet configuration

Docker Volumes:
├── redis_data (bind mount to /data/redis)
├── parquet_storage (bind mount to /data/parquet)
├── prometheus_data (bind mount to /data/prometheus)
└── service_logs (bind mount to /data/logs)
```

#### Service Dependencies & Health Checks:
```
Startup Order:
1. Redis (no dependencies)
   ├── Health check: redis-cli ping (10s interval, 3s timeout)
   └── Startup time: 30s

2. Data Writer & Jetstream (depend on Redis)
   ├── depends_on: redis (condition: service_healthy)
   ├── Health check: HTTP /health (30s interval, 10s timeout)
   └── Startup time: 60s

3. Redis Exporter (depends on Redis)
   ├── depends_on: redis (condition: service_healthy)
   ├── Health check: HTTP /metrics (30s interval, 10s timeout)
   └── Startup time: 15s

4. Prometheus (depends on all services)
   ├── depends_on: redis, data-writer, jetstream, redis-exporter
   ├── Health check: HTTP /-/healthy (30s interval, 10s timeout)
   └── Startup time: 45s
```

#### Resource Allocation Strategy:
```yaml
Total VM Resources: 8GB RAM, 4 vCPU
Service Allocation:
- Redis: 2GB RAM, 1 vCPU (25% of total)
- Data Writer: 1GB RAM, 0.5 vCPU (12.5% of total)
- Jetstream: 512MB RAM, 0.5 vCPU (12.5% of total)
- Prometheus: 256MB RAM, 0.25 vCPU (6.25% of total)
- Redis Exporter: 64MB RAM, 0.1 vCPU (2.5% of total)
- System Overhead: ~1.2GB RAM, ~1.65 vCPU (remaining)
- Total Allocated: ~3.8GB RAM, ~2.35 vCPU (within limits)
```

#### Volume Management Strategy:
```yaml
Persistent Storage:
- redis_data: /data/redis (Redis AOF persistence)
- parquet_storage: /data/parquet (Parquet files)
- prometheus_data: /data/prometheus (Prometheus TSDB)
- service_logs: /data/logs (Application logs)

Volume Configuration:
- Type: bind mounts to host paths
- Permissions: appropriate user/group ownership
- Backup: daily snapshots to object storage
- Monitoring: disk usage alerts at 80% threshold
```

### Suggested Implementation Plan

#### Docker Compose Setup:
- Create comprehensive `docker-compose.yml` with all Phase 1 services
- Implement proper service dependencies with `depends_on` and health checks
- Configure custom Docker network (`bluesky_private`) for service isolation
- Set up bind-mounted volumes for persistent storage
- Implement resource limits and reservations for all services
- Add environment variable management and secrets handling

#### Service Containerization:
- Create Dockerfiles for all services with optimized base images
- Implement health check endpoints for all services
- Configure proper logging and error handling
- Add service discovery and inter-service communication
- Optimize container images for size and security

#### Infrastructure Integration:
- Update Terraform configuration for Docker environment
- Configure Docker network and volume mounting on host
- Set up firewall rules for containerized services
- Implement automated deployment with Docker Compose
- Add container monitoring and resource tracking

#### Testing & Validation:
- Test service startup order and dependency resolution
- Validate inter-service communication and network connectivity
- Test volume persistence across container restarts
- Verify resource allocation and performance under load
- Test complete pipeline end-to-end in containerized environment

### Effort Estimate
- Estimated effort: **12-16 hours**
- Assumes all Phase 1 services are implemented and validated
- Includes Docker Compose orchestration, containerization, and testing

#### Detailed Breakdown:
- **Docker Compose Orchestration**: 4-5 hours
  - Multi-service architecture design and implementation
  - Service dependency configuration with health checks
  - Network configuration and service discovery
  - Volume management and resource allocation

- **Service Containerization**: 4-5 hours
  - Dockerfile creation for all services
  - Health check endpoint implementation
  - Container optimization and security hardening
  - Environment variable and secrets management

- **Infrastructure Integration**: 2-3 hours
  - Terraform updates for Docker environment
  - Network and volume configuration
  - Deployment automation and monitoring

- **Testing & Validation**: 2-3 hours
  - Container orchestration testing
  - End-to-end pipeline validation
  - Performance and resource testing
  - Documentation and deployment guides

### Priority & Impact
- Priority: **Medium**
- Rationale: Important for production readiness but not blocking core functionality

### Acceptance Checklist
- [ ] Docker Compose configuration created for all Phase 1 services
- [ ] All services containerized and running successfully
- [ ] Service dependencies and health checks working correctly
- [ ] Volume persistence and data integrity maintained
- [ ] Resource allocation optimized and within VM limits
- [ ] Monitoring and alerting functional in containerized environment
- [ ] Deployment automated and repeatable across environments
- [ ] Container security and optimization implemented
- [ ] Documentation complete for containerized deployment
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Docker Compose Documentation: https://docs.docker.com/compose/
- Related tickets: MET-001, MET-002, MET-003, MET-004, MET-005, MET-006

---

## Phase 1 Summary

### Total Tickets: 7
### Estimated Effort: 58 hours
### Critical Path: MET-001 → MET-002 → MET-003 → MET-005 → MET-007
### Key Deliverables:
- Redis instance with monitoring (local and Hetzner)
- Hetzner deployment with CI/CD pipeline
- Data writer service (Redis → Parquet)
- Load testing with mock data stream
- Jetstream integration
- Complete data pipeline (Jetstream → Redis → Parquet)
- Comprehensive monitoring dashboard
- Containerized deployment with Docker Compose

### Exit Criteria:
- Complete pipeline working for 2 days continuously
- All data flowing from Jetstream to Parquet storage
- Monitoring showing accurate metrics
- Load testing validates system performance
- System deployed to Hetzner with automated GitHub Actions CI/CD
- All services containerized and orchestrated with Docker Compose
- Ready for Phase 2 (Query Engine & API)
