#!/bin/bash

# End-to-End DataWriter Pipeline Test Runner
# This script runs a comprehensive test of the complete DataWriter pipeline

set -e  # Exit on any error

# Configuration
TEST_DURATION_MINUTES=${1:-10}
OUTPUT_DIR="./data"
LOG_DIR="./logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$LOG_DIR"

# Log file
LOG_FILE="$LOG_DIR/end_to_end_test_$TIMESTAMP.log"

# Function to log to both console and file
log() {
    echo "$1" | tee -a "$LOG_FILE"
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

# Check if conda environment is activated
if [[ "$CONDA_DEFAULT_ENV" != "bluesky-research" ]]; then
    log_warning "Conda environment 'bluesky-research' is not activated."
    log_info "Please run: conda activate bluesky-research"
    exit 1
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

# Wait for services to be ready
log_info "Waiting for services to be ready..."
sleep 30

# Validate infrastructure
log_info "Validating infrastructure..."
python redis_testing/08_prefect_infrastructure_setup.py

if [ $? -ne 0 ]; then
    log_error "Infrastructure validation failed"
    exit 1
fi

log_success "Infrastructure started and validated"

# Step 3: Deploy DataWriter flow
log_info "Step 3: Deploying DataWriter flow..."

cd prefect
python deploy_datawriter.py

if [ $? -ne 0 ]; then
    log_error "DataWriter flow deployment failed"
    exit 1
fi

log_success "DataWriter flow deployed successfully"

# Step 4: Run end-to-end test
log_info "Step 4: Running end-to-end test..."

cd ../redis_testing

# Run the comprehensive end-to-end test
python 10_end_to_end_test.py --duration "$TEST_DURATION_MINUTES" --output-dir "$OUTPUT_DIR"

TEST_EXIT_CODE=$?

# Step 5: Collect results
log_info "Step 5: Collecting test results..."

# Find the latest test results file
LATEST_RESULTS=$(ls -t end_to_end_test_results_*.json 2>/dev/null | head -n1)

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

# Ask user if they want to stop the infrastructure
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
