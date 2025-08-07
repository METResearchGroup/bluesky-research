#!/usr/bin/env bash

# Bluesky Redis Backend - Complete Load Test Runner
# This script runs the entire Redis setup and Jetstream load test using Docker

set -euo pipefail  # Exit on any error, unset var, or failed pipe

# Load Redis password from environment or Docker secret
if [ -z "${REDIS_PASSWORD:-}" ]; then
    if [ -f ".env" ]; then
        # Source .env file if it exists
        set -a  # automatically export all variables
        source .env
        set +a
    fi
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Function to check if Docker Compose is available
check_docker_compose() {
    if ! command_exists docker-compose && ! docker compose version >/dev/null 2>&1; then
        print_error "Neither docker-compose nor docker compose CLI found."
        exit 1
    fi
    print_success "Docker Compose is available"
}

# Function to get the appropriate docker compose command
get_docker_compose_cmd() {
    if command_exists docker-compose; then
        echo "docker-compose"
    else
        echo "docker compose"
    fi
}

# Function to get Redis authentication arguments
get_redis_auth_args() {
    if [ -n "${REDIS_PASSWORD:-}" ]; then
        echo "-a \"$REDIS_PASSWORD\""
    else
        echo ""
    fi
}

# Function to check if conda environment exists
check_conda_env() {
    # Check if conda command exists
    if ! command_exists conda; then
        print_error "Conda is not installed or not in PATH. Please install conda and try again."
        exit 1
    fi
    
    # Check for exact environment name match
    if ! conda env list | awk '{print $1}' | grep -xq "bluesky-research"; then
        print_warning "Conda environment 'bluesky-research' not found. Creating it..."
        conda create -n bluesky-research python=3.11 -y
    fi
    print_success "Conda environment 'bluesky-research' is available"
}

# Function to activate conda environment
activate_conda() {
    print_status "Activating conda environment..."
    eval "$(conda shell.bash hook)"
    conda activate bluesky-research
    print_success "Conda environment activated"
}

# Function to install dependencies
install_dependencies() {
    print_status "Installing Python dependencies..."
    
    # Check if uv is available, otherwise use pip
    if command_exists uv; then
        print_status "Using uv to install dependencies..."
        uv pip install -r requirements.txt
    else
        print_warning "uv not found, using pip instead..."
        pip install -r requirements.txt
    fi
    
    print_success "Dependencies installed"
}

# Function to create data directories
create_data_directories() {
    print_status "Creating data directories..."
    mkdir -p data/redis
    mkdir -p data/logs
    print_success "Data directories created"
}

# Function to start Redis
start_redis() {
    print_status "Starting Redis container..."
    
    # Get the appropriate docker compose command
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd)
    
    # Get Redis authentication arguments
    local redis_auth_args
    redis_auth_args=$(get_redis_auth_args)
    
    # Stop any existing containers
    $docker_compose_cmd down >/dev/null 2>&1 || true
    
    # Start Redis
    $docker_compose_cmd up -d redis
    
    # Wait for Redis to be ready
    print_status "Waiting for Redis to be ready..."
    for i in {1..30}; do
        if $docker_compose_cmd exec -T redis redis-cli $redis_auth_args ping >/dev/null 2>&1; then
            print_success "Redis is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            print_error "Redis failed to start within 30 seconds"
            $docker_compose_cmd logs redis
            exit 1
        fi
        sleep 1
    done
    
    # Display Redis information
    print_status "Redis Information:"
    $docker_compose_cmd exec -T redis redis-cli $redis_auth_args info server | grep -E "(redis_version|uptime_in_seconds|connected_clients)"
    $docker_compose_cmd exec -T redis redis-cli $redis_auth_args info memory | grep -E "(used_memory_human|maxmemory_human)"
}

# Function to test Redis
test_redis() {
    print_status "Testing Redis functionality..."
    if REDIS_PASSWORD="${REDIS_PASSWORD:-}" python test_redis_server.py; then
        print_success "Redis tests passed"
    else
        print_error "Redis tests failed"
        exit 1
    fi
}

# Function to run Jetstream load test
run_load_test() {
    print_status "Running Jetstream load test..."
    print_status "This will simulate the Bluesky firehose and process data through Redis to Parquet files."
    print_status "Press Ctrl+C to stop the test early."
    echo ""
    
    # Run the load test with Redis password
    REDIS_PASSWORD="${REDIS_PASSWORD:-}" python jetstream_load_test.py
}

# Function to show results
show_results() {
    print_status "Load test completed!"
    echo ""
    
    # Check if data files were created
    if [ -d "data" ] && [ "$(ls -A data 2>/dev/null)" ]; then
        print_success "Data files generated:"
        find data -name "*.parquet" -type f | head -10 | while read -r file; do
            echo "  📄 $file"
        done
        
        # Count total files
        total_files=$(find data -name "*.parquet" -type f | wc -l)
        echo ""
        print_status "Total Parquet files generated: $total_files"
        
        # Show directory structure
        echo ""
        print_status "Data directory structure:"
        tree data -I "*.aof" 2>/dev/null || find data -type d | head -10
    else
        print_warning "No data files found in data/ directory"
    fi
    
    echo ""
    print_status "Redis container is still running. To stop it, run:"
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd)
    echo "  $docker_compose_cmd down"
    echo ""
    print_status "To view Redis logs, run:"
    echo "  $docker_compose_cmd logs -f redis"
}

# Function to cleanup on exit
cleanup() {
    print_status "Cleaning up..."
    # Don't stop Redis by default, let user decide
    local docker_compose_cmd
    docker_compose_cmd=$(get_docker_compose_cmd 2>/dev/null || echo "docker compose")
    print_status "Redis container is still running. Use '$docker_compose_cmd down' to stop it."
}

# Set up trap for cleanup
trap cleanup EXIT

# Main execution
main() {
    echo "🚀 Bluesky Redis Backend - Complete Load Test Runner"
    echo "=================================================="
    echo ""
    
    # Check prerequisites
    print_status "Checking prerequisites..."
    check_docker
    check_docker_compose
    check_conda_env
    
    # Change to script directory
    cd "$(dirname "$0")"
    
    # Setup environment
    activate_conda
    install_dependencies
    create_data_directories
    
    # Start and test Redis
    start_redis
    test_redis
    
    # Run the load test
    run_load_test
    
    # Show results
    show_results
}

# Run main function
main "$@"
