#!/bin/bash

# End-to-End DataWriter Pipeline Test Runner
# This script runs a comprehensive test of the complete DataWriter pipeline

set -euo pipefail  # Exit on error, treat unset vars as errors, fail pipelines

# Configuration
TEST_DURATION_MINUTES=${1:-10}
OUTPUT_DIR="./data"
LOG_DIR="./logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Resolve OUTPUT_DIR and LOG_DIR to absolute paths to remain stable across cd
INITIAL_CWD="$(pwd)"
case "$OUTPUT_DIR" in
  /*) ;; # already absolute
  *) OUTPUT_DIR="$INITIAL_CWD/${OUTPUT_DIR#./}" ;;
esac
case "$LOG_DIR" in
  /*) ;;
  *) LOG_DIR="$INITIAL_CWD/${LOG_DIR#./}" ;;
esac

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    printf "%b\n" "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    printf "%b\n" "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    printf "%b\n" "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    printf "%b\n" "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

# Create directories (absolute)
mkdir -p "$OUTPUT_DIR"
mkdir -p "$LOG_DIR"

# Log file (absolute)
LOG_FILE="$LOG_DIR/end_to_end_test_$TIMESTAMP.log"

# Function to log to both console and file
log() {
    printf "%s\n" "$1" | tee -a "$LOG_FILE"
}

# Header
log "=================================================================="
log "🚀 END-TO-END DATAWRITER PIPELINE TEST"
log "=================================================================="
log "⏱️ Test Duration: $TEST_DURATION_MINUTES minutes"
log "📁 Output Directory: $OUTPUT_DIR"
log "📄 Log File: $LOG_FILE"
log "🕐 Start Time: $(date)"
log "=================================================================="

# Step 1: Check prerequisites
log_info "Step 1: Checking prerequisites..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if conda environment is activated (allow override/CI)
if [[ "$CONDA_DEFAULT_ENV" != "bluesky-research" ]]; then
    if [[ -n "${ALLOW_ANY_ENV:-}" || -n "${CI:-}" ]]; then
        log_warning "Conda env 'bluesky-research' not active; continuing due to ALLOW_ANY_ENV/CI."
    else
        log_warning "Conda environment 'bluesky-research' is not activated."
        log_info "Please run: conda activate bluesky-research (or set ALLOW_ANY_ENV=1 to override)"
        exit 1
    fi
fi

# Check if required Python packages are installed
log_info "Checking Python dependencies..."
python -c "import redis, polars, prefect, requests" 2>/dev/null || {
    log_error "Missing required Python packages. Please install them:"
    log_error "pip install redis polars prefect requests"
    exit 1
}

log_success "Prerequisites check passed"

# Step 2: Start infrastructure
log_info "Step 2: Starting infrastructure..."

# Navigate to backend directory
cd "$(dirname "$0")/.."

# Start Prefect infrastructure
log_info "Starting Prefect infrastructure..."
docker compose -f docker-compose.prefect.yml up -d

# Wait helpers
wait_for_container_healthy() {
    local container_name="$1"
    local timeout_seconds="${2:-180}"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for container '$container_name' to be healthy (timeout ${timeout_seconds}s)..."
    while true; do
        if ! docker inspect "$container_name" >/dev/null 2>&1; then
            log_warning "Container '$container_name' not found yet. Retrying..."
        else
            # Try health status if Health exists; otherwise fall back to running state
            local health_status
            health_status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}nohealth{{end}}' "$container_name" 2>/dev/null || echo "unknown")
            if [ "$health_status" = "healthy" ]; then
                log_success "Container '$container_name' is healthy"
                break
            elif [ "$health_status" = "unhealthy" ]; then
                log_error "Container '$container_name' is unhealthy"
                return 1
            elif [ "$health_status" = "nohealth" ]; then
                # No HEALTHCHECK; treat running state as healthy
                local state_status
                state_status=$(docker inspect -f '{{.State.Status}}' "$container_name" 2>/dev/null || echo "unknown")
                if [ "$state_status" = "running" ]; then
                    log_success "Container '$container_name' is running (no HEALTHCHECK)"
                    break
                fi
            fi
        fi
        
        local now
        now=$(date +%s)
        local elapsed
        elapsed=$(( now - start_time ))
        if [ $elapsed -ge $timeout_seconds ]; then
            log_error "Timed out waiting for '$container_name' to become healthy"
            return 1
        fi
        sleep 2
    done
}

wait_for_http_ok() {
    local url="$1"
    local timeout_seconds="${2:-120}"
    local start_time
    start_time=$(date +%s)

    log_info "Waiting for HTTP 200 from $url (timeout ${timeout_seconds}s)..."
    while true; do
        if curl -fsS --connect-timeout 2 -m 5 "$url" >/dev/null 2>&1; then
            log_success "HTTP OK from $url"
            break
        fi
        local now
        now=$(date +%s)
        local elapsed
        elapsed=$(( now - start_time ))
        if [ $elapsed -ge $timeout_seconds ]; then
            log_error "Timed out waiting for $url"
            return 1
        fi
        sleep 2
    done
}

# Wait for key services to be healthy/responding
log_info "Waiting for services to be ready..."
wait_for_container_healthy bluesky_redis 120
wait_for_container_healthy redis_exporter 120
wait_for_container_healthy prometheus 150
wait_for_container_healthy alertmanager 150
wait_for_container_healthy grafana 180
wait_for_container_healthy prefect_server 180
wait_for_container_healthy prefect_worker 180

# Also verify critical HTTP endpoints
wait_for_http_ok "http://localhost:4200/api/health" 120

# Validate infrastructure
log_info "Validating infrastructure..."
if ! python redis_testing/08_prefect_infrastructure_setup.py; then
    log_error "Infrastructure validation failed"
    exit 1
fi

log_success "Infrastructure started and validated"

# Step 3: Deploy DataWriter flow
log_info "Step 3: Deploying DataWriter flow..."

# Move to project root to run module with absolute imports
cd ../..
if ! python -m bluesky_database.backend.prefect.deploy_datawriter; then
    log_error "DataWriter flow deployment failed"
    exit 1
fi

log_success "DataWriter flow deployed successfully"

# Step 4: Run end-to-end test
log_info "Step 4: Running end-to-end test..."

# Move to redis_testing directory
cd bluesky_database/backend/redis_testing

# Run the comprehensive end-to-end test
# Temporarily disable exit-on-error to capture test exit code
set +e
python 10_end_to_end_test.py --duration "$TEST_DURATION_MINUTES" --output-dir "$OUTPUT_DIR"
TEST_EXIT_CODE=$?
set -e

# Step 5: Collect results
log_info "Step 5: Collecting test results..."

# Find the latest test results file in OUTPUT_DIR
LATEST_RESULTS=$(ls -t "$OUTPUT_DIR"/end_to_end_test_results_*.json 2>/dev/null | head -n1)

if [ -n "$LATEST_RESULTS" ]; then
    log_info "Test results file: $LATEST_RESULTS"
    
    # Extract key metrics from results
    if command -v jq >/dev/null 2>&1; then
        OVERALL_SUCCESS=$(jq -r '.overall_success' "$LATEST_RESULTS" 2>/dev/null || echo "unknown")
        SUCCESS_RATE=$(jq -r '.success_rate' "$LATEST_RESULTS" 2>/dev/null || echo "unknown")
        TOTAL_TESTS=$(jq -r '.total_tests' "$LATEST_RESULTS" 2>/dev/null || echo "unknown")
        PASSED_TESTS=$(jq -r '.passed_tests' "$LATEST_RESULTS" 2>/dev/null || echo "unknown")
        
        log "📊 Test Results Summary:"
        log "   Overall Success: $OVERALL_SUCCESS"
        log "   Success Rate: $SUCCESS_RATE%"
        log "   Tests Passed: $PASSED_TESTS/$TOTAL_TESTS"
    fi
else
    log_warning "No test results file found"
fi

# Step 6: Validate output
log_info "Step 6: Validating output..."

# Check if Parquet files were created
PARQUET_COUNT=$(find "$OUTPUT_DIR" -name "*.parquet" 2>/dev/null | wc -l)

if [ "$PARQUET_COUNT" -gt 0 ]; then
    log_success "Found $PARQUET_COUNT Parquet files"
    
    # Show file structure
    log_info "Parquet file structure:"
    find "$OUTPUT_DIR" -name "*.parquet" | head -10 | while read file; do
        log "   $file"
    done
    
    if [ "$PARQUET_COUNT" -gt 10 ]; then
        log_info "   ... and $((PARQUET_COUNT - 10)) more files"
    fi
else
    log_warning "No Parquet files found"
fi

# Step 7: Cleanup (optional)
log_info "Step 7: Cleanup..."

# Decide whether to stop infrastructure (non-interactive safe)
AUTO_STOP=${AUTO_STOP:-}
if [ -t 0 ] && [ -z "$AUTO_STOP" ]; then
    # Interactive session and no AUTO_STOP override: prompt
    read -p "Do you want to stop the infrastructure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Stopping infrastructure..."
        cd ..
        docker compose -f docker-compose.prefect.yml down
        log_success "Infrastructure stopped"
    else
        log_info "Infrastructure left running. You can stop it manually with:"
        log_info "docker compose -f docker-compose.prefect.yml down"
    fi
else
    # Non-interactive or AUTO_STOP is set
    if [[ "$AUTO_STOP" =~ ^([Yy][Ee]?[Ss]?|1|true)$ ]]; then
        log_info "AUTO_STOP enabled. Stopping infrastructure..."
        cd ..
        docker compose -f docker-compose.prefect.yml down
        log_success "Infrastructure stopped"
    elif [[ "$AUTO_STOP" =~ ^([Nn][Oo]?|0|false)$ ]]; then
        log_info "AUTO_STOP disabled. Leaving infrastructure running."
        log_info "docker compose -f docker-compose.prefect.yml down"
    else
        # Default in CI/non-interactive: stop to avoid leaks
        log_info "Non-interactive session without AUTO_STOP set. Stopping infrastructure by default..."
        cd ..
        docker compose -f docker-compose.prefect.yml down
        log_success "Infrastructure stopped"
    fi
fi

# Final summary
log "=================================================================="
log "🏁 END-TO-END TEST COMPLETED"
log "=================================================================="
log "⏱️ Duration: $TEST_DURATION_MINUTES minutes"
log "📁 Output Directory: $OUTPUT_DIR"
log "📄 Log File: $LOG_FILE"
log "🕐 End Time: $(date)"

if [ "$TEST_EXIT_CODE" -eq 0 ]; then
    log_success "✅ Test completed successfully!"
    log_success "🎉 DataWriter pipeline is working correctly!"
else
    log_error "❌ Test failed with exit code $TEST_EXIT_CODE"
    log_error "📄 Check the log file for details: $LOG_FILE"
fi

log "=================================================================="

# Exit with the test exit code
exit $TEST_EXIT_CODE
