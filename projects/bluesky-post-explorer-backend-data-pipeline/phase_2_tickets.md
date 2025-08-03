# Phase 2: Query Engine & API - Linear Ticket Proposals

## Overview
**Objective**: Connect query engine to raw data and build basic API
**Timeline**: Weeks 3-4
**Approach**: Rapid prototyping with piecemeal deployment

---

## MET-006: Set up DuckDB integration with Parquet storage

### Context & Motivation
DuckDB is the core query engine that will enable fast analytical queries against the Parquet data. This integration must provide efficient access to the stored data and support basic LIKE queries for text search, which is the primary use case for the Bluesky Post Explorer.

### Detailed Description & Requirements

#### Functional Requirements:
- Set up DuckDB connection to Parquet storage directory
- Implement automatic schema discovery from Parquet files
- Create optimized table definitions for all data types (posts, likes, follows)
- Implement basic LIKE queries for post text search
- Add support for date range queries with partitioning optimization
- Create connection pooling for concurrent query handling
- Implement query result caching for frequently accessed data
- Add query performance monitoring and logging

#### Non-Functional Requirements:
- Query performance <30 seconds for 1-day data queries
- Support for concurrent queries (minimum 5 concurrent users)
- Memory usage should not exceed 2GB for query operations
- Connection establishment should complete within 5 seconds
- Query results should be streamed for large result sets
- DuckDB should handle Parquet files with 80%+ compression efficiently

#### Validation & Error Handling:
- DuckDB can read from Parquet files without errors
- Basic LIKE queries work on post text with acceptable performance
- Date range queries utilize partitioning effectively
- Concurrent queries don't cause memory issues
- Large result sets are handled efficiently
- Connection failures are handled gracefully

### Success Criteria
- DuckDB can read from Parquet files successfully
- Basic LIKE queries work on post text
- Query performance <30 seconds for 1-day data
- Can handle concurrent queries
- Date range queries optimized with partitioning
- Connection pooling functional
- Query performance monitoring active
- Memory usage stays within limits

### Test Plan
- `test_parquet_connection`: Connect to Parquet files → Schema discovered correctly
- `test_like_queries`: Text search queries → Results returned within 30 seconds
- `test_date_range`: Date range queries → Partitioning utilized effectively
- `test_concurrent_queries`: Multiple users → All queries complete successfully
- `test_memory_usage`: Large queries → Memory stays under 2GB
- `test_connection_pooling`: Multiple connections → Pool manages efficiently
- `test_performance_monitoring`: Query metrics → Performance data collected

📁 Test file: `services/query_engine/tests/test_duckdb_integration.py`

### Dependencies
- Depends on: MET-002 (Data writer service - Parquet storage)

### Suggested Implementation Plan
- Install and configure DuckDB with Python bindings
- Create connection management module
- Implement schema discovery from Parquet files
- Add query optimization for partitioning
- Implement connection pooling
- Add performance monitoring
- Create query result streaming
- Test with various query patterns

### Effort Estimate
- Estimated effort: **10 hours**
- Includes DuckDB setup, query optimization, and performance testing

### Priority & Impact
- Priority: **High**
- Rationale: Core component for query functionality, blocks API development

### Acceptance Checklist
- [ ] DuckDB connected to Parquet storage
- [ ] Schema discovery working correctly
- [ ] LIKE queries functional with good performance
- [ ] Date range queries optimized
- [ ] Concurrent query handling implemented
- [ ] Connection pooling functional
- [ ] Performance monitoring active
- [ ] Memory usage within limits
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- DuckDB Documentation: https://duckdb.org/docs/
- Related tickets: MET-002, MET-007

---

## MET-007: Implement basic text search functionality with DuckDB

### Context & Motivation
Text search is the primary use case for the Bluesky Post Explorer. Users need to search post content using basic LIKE queries to find relevant posts. This functionality must be fast and efficient, providing results within the performance requirements.

### Detailed Description & Requirements

#### Functional Requirements:
- Implement LIKE query functionality for post text search
- Support case-insensitive text search
- Add wildcard support (*, %) for flexible matching
- Implement search result ranking (by date, relevance)
- Add search result pagination (50 results per page)
- Support search within date ranges
- Implement search result caching for repeated queries
- Add search analytics and metrics collection

#### Non-Functional Requirements:
- Search queries must complete within 30 seconds
- Support for complex search patterns (multiple terms, wildcards)
- Search results should be ranked by relevance and date
- Pagination should work efficiently for large result sets
- Search cache should improve performance for repeated queries
- Search analytics should track popular search terms

#### Validation & Error Handling:
- LIKE queries return accurate results
- Case-insensitive search works correctly
- Wildcard patterns are handled properly
- Pagination works for large result sets
- Search performance meets requirements
- Invalid search patterns are handled gracefully

### Success Criteria
- LIKE queries work correctly for post text search
- Case-insensitive search functional
- Wildcard support implemented
- Search results ranked appropriately
- Pagination works efficiently
- Search within date ranges functional
- Search caching improves performance
- Search analytics tracking active

### Test Plan
- `test_basic_search`: Simple text search → Results returned correctly
- `test_case_insensitive`: Case variations → Same results returned
- `test_wildcard_search`: Wildcard patterns → Results match expectations
- `test_date_range_search`: Date filtering → Results filtered correctly
- `test_pagination`: Large result sets → Pagination works efficiently
- `test_search_ranking`: Result ordering → Results ranked appropriately
- `test_search_cache`: Repeated queries → Cache improves performance
- `test_search_analytics`: Query tracking → Analytics data collected

📁 Test file: `services/query_engine/tests/test_text_search.py`

### Dependencies
- Depends on: MET-006 (DuckDB integration)

### Suggested Implementation Plan
- Implement LIKE query builder for DuckDB
- Add case-insensitive search support
- Implement wildcard pattern handling
- Create search result ranking logic
- Add pagination support
- Implement search caching
- Add search analytics collection
- Test with various search patterns

### Effort Estimate
- Estimated effort: **8 hours**
- Includes search functionality, ranking, caching, and analytics

### Priority & Impact
- Priority: **High**
- Rationale: Primary use case for the application

### Acceptance Checklist
- [ ] LIKE queries functional for text search
- [ ] Case-insensitive search working
- [ ] Wildcard support implemented
- [ ] Search result ranking functional
- [ ] Pagination working efficiently
- [ ] Date range search implemented
- [ ] Search caching active
- [ ] Search analytics tracking
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Related tickets: MET-006, MET-008

---

## MET-008: Create REST API with search endpoint

### Context & Motivation
The REST API is the interface between the frontend and the query engine. It needs to provide a simple, efficient way for users to search posts and retrieve results. The API should be designed for the Vercel frontend and support the primary use case of text-based post search.

### Detailed Description & Requirements

#### Functional Requirements:
- Create FastAPI application with search endpoint
- Implement GET /search endpoint with query parameters
- Support search parameters: text, username, start_date, end_date, page, limit
- Return JSON responses with search results and metadata
- Implement proper error handling and validation
- Add request/response logging for debugging
- Implement rate limiting (1000 requests/hour per IP)
- Add API documentation with OpenAPI/Swagger
- Support CORS for frontend integration

#### Non-Functional Requirements:
- API response time <5 seconds for typical queries
- Support for concurrent requests (minimum 10 concurrent users)
- API should be stateless and horizontally scalable
- Rate limiting should be configurable
- API documentation should be auto-generated
- Error responses should be consistent and informative

#### Validation & Error Handling:
- Search endpoint returns correct results for valid queries
- Invalid parameters return appropriate error responses
- Rate limiting works correctly
- API documentation is accurate and complete
- CORS headers are set correctly for frontend
- Error responses provide useful debugging information

### Success Criteria
- GET /search endpoint functional
- Text search working with DuckDB
- Pagination implemented (50 results/page)
- Basic error handling
- API responds within performance requirements
- Rate limiting functional
- API documentation complete
- CORS configured for frontend

### Test Plan
- `test_search_endpoint`: Valid search query → Results returned correctly
- `test_invalid_parameters`: Invalid input → Appropriate error response
- `test_pagination`: Large result sets → Pagination works correctly
- `test_rate_limiting`: Excessive requests → Rate limiting enforced
- `test_concurrent_requests`: Multiple users → All requests handled
- `test_api_documentation`: Swagger docs → Documentation accurate
- `test_cors_headers`: Frontend requests → CORS headers present
- `test_error_handling`: Various errors → Consistent error responses

📁 Test file: `services/api/tests/test_search_api.py`

### Dependencies
- Depends on: MET-007 (Text search functionality)

### Suggested Implementation Plan
- Create FastAPI application structure
- Implement search endpoint with parameter validation
- Add DuckDB integration for queries
- Implement pagination logic
- Add rate limiting middleware
- Create error handling and logging
- Generate OpenAPI documentation
- Configure CORS for frontend
- Test with various scenarios

### Effort Estimate
- Estimated effort: **10 hours**
- Includes API development, integration, documentation, and testing

### Priority & Impact
- Priority: **High**
- Rationale: Required for frontend integration

### Acceptance Checklist
- [ ] GET /search endpoint implemented
- [ ] Text search working with DuckDB
- [ ] Pagination functional (50 results/page)
- [ ] Error handling implemented
- [ ] API responds within performance requirements
- [ ] Rate limiting configured
- [ ] API documentation complete
- [ ] CORS configured for frontend
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- FastAPI Documentation: https://fastapi.tiangolo.com/
- Related tickets: MET-007, MET-009

---

## MET-009: Implement API authentication and security

### Context & Motivation
While the initial implementation is internal-only, we need to prepare for future authentication requirements. This includes basic security measures, API key management, and infrastructure for future auth integration.

### Detailed Description & Requirements

#### Functional Requirements:
- Implement API key authentication for internal access
- Add request validation and sanitization
- Implement basic security headers (CORS, CSP, etc.)
- Add request logging for security monitoring
- Create API key management system
- Implement IP-based access controls
- Add security monitoring and alerting
- Prepare infrastructure for future OAuth integration

#### Non-Functional Requirements:
- API key validation should add <100ms to response time
- Security headers should not impact functionality
- Request logging should be searchable and auditable
- API key management should be secure and scalable
- Security monitoring should provide real-time alerts

#### Validation & Error Handling:
- Invalid API keys are rejected appropriately
- Security headers are set correctly
- Request logging captures necessary information
- API key management works securely
- Security monitoring detects suspicious activity

### Success Criteria
- API key authentication functional
- Request validation and sanitization working
- Security headers configured correctly
- Request logging implemented
- API key management system operational
- IP-based access controls functional
- Security monitoring active
- Infrastructure ready for future auth

### Test Plan
- `test_api_key_auth`: Valid/invalid keys → Appropriate responses
- `test_request_validation`: Invalid requests → Proper error handling
- `test_security_headers`: Response headers → Security headers present
- `test_request_logging`: API requests → Logged with sufficient detail
- `test_ip_controls`: IP restrictions → Access controlled appropriately
- `test_security_monitoring`: Suspicious activity → Alerts triggered

📁 Test file: `services/api/tests/test_security.py`

### Dependencies
- Depends on: MET-008 (REST API)

### Suggested Implementation Plan
- Implement API key middleware
- Add request validation and sanitization
- Configure security headers
- Set up request logging
- Create API key management
- Implement IP-based controls
- Add security monitoring
- Prepare OAuth infrastructure

### Effort Estimate
- Estimated effort: **6 hours**
- Includes authentication, security, monitoring, and testing

### Priority & Impact
- Priority: **Medium**
- Rationale: Important for production but not blocking core functionality

### Acceptance Checklist
- [ ] API key authentication implemented
- [ ] Request validation working
- [ ] Security headers configured
- [ ] Request logging functional
- [ ] API key management operational
- [ ] IP-based controls implemented
- [ ] Security monitoring active
- [ ] OAuth infrastructure prepared
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Related tickets: MET-008

---

## MET-010: Create API performance optimization and caching

### Context & Motivation
API performance is critical for user experience. We need to implement caching strategies and query optimization to ensure fast response times, especially for frequently accessed data and repeated queries.

### Detailed Description & Requirements

#### Functional Requirements:
- Implement Redis caching for search results
- Add query result caching with TTL (5 minutes)
- Implement cache invalidation for data updates
- Add query optimization for common search patterns
- Implement response compression (GZIP)
- Add database connection pooling optimization
- Create cache warming for popular searches
- Implement cache monitoring and metrics

#### Non-Functional Requirements:
- Cached responses should be <100ms
- Cache hit ratio should be >70% for repeated queries
- Query optimization should improve performance by >50%
- Response compression should reduce size by >60%
- Cache monitoring should provide real-time metrics

#### Validation & Error Handling:
- Cached responses are faster than direct queries
- Cache invalidation works correctly when data updates
- Query optimization improves performance
- Response compression works without errors
- Cache monitoring provides accurate metrics

### Success Criteria
- Redis caching functional for search results
- Query result caching with TTL working
- Cache invalidation implemented
- Query optimization improving performance
- Response compression active
- Database connection pooling optimized
- Cache warming functional
- Cache monitoring providing metrics

### Test Plan
- `test_cache_performance`: Cached vs direct queries → Cache faster
- `test_cache_invalidation`: Data updates → Cache invalidated correctly
- `test_query_optimization`: Optimized queries → Performance improved
- `test_response_compression`: Compressed responses → Size reduced
- `test_cache_monitoring`: Cache metrics → Accurate data collected
- `test_cache_warming`: Popular searches → Cache warmed appropriately

📁 Test file: `services/api/tests/test_performance.py`

### Dependencies
- Depends on: MET-008 (REST API), MET-001 (Redis setup)

### Suggested Implementation Plan
- Implement Redis caching middleware
- Add query result caching with TTL
- Create cache invalidation logic
- Optimize common query patterns
- Add response compression
- Optimize database connections
- Implement cache warming
- Add cache monitoring

### Effort Estimate
- Estimated effort: **8 hours**
- Includes caching, optimization, monitoring, and testing

### Priority & Impact
- Priority: **Medium**
- Rationale: Important for performance but not blocking core functionality

### Acceptance Checklist
- [ ] Redis caching implemented
- [ ] Query result caching functional
- [ ] Cache invalidation working
- [ ] Query optimization active
- [ ] Response compression configured
- [ ] Database pooling optimized
- [ ] Cache warming implemented
- [ ] Cache monitoring active
- [ ] Tests written and passing
- [ ] Code reviewed and merged

### Links & References
- Plan: `projects/bluesky-post-explorer-backend-data-pipeline/spec.md`
- Related tickets: MET-001, MET-008

---

## Phase 2 Summary

### Total Tickets: 5
### Estimated Effort: 42 hours
### Critical Path: MET-006 → MET-007 → MET-008
### Key Deliverables:
- DuckDB integration with Parquet storage
- Basic text search functionality
- REST API with search endpoint
- API authentication and security
- Performance optimization and caching

### Exit Criteria:
- DuckDB can query Parquet data with LIKE queries
- Basic search API functional with pagination
- API responds within performance requirements
- Security measures implemented
- Ready for Phase 3 (Production Hardening) 