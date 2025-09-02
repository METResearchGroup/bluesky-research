#!/bin/bash

# Manuscript R Analyses Runner Script
# This script automates the setup and execution of the R analysis

set -e  # Exit on any error

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

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
print_status "Working directory: $SCRIPT_DIR"

# Check if R is installed
print_status "Checking if R is installed..."
if ! command -v R &> /dev/null; then
    print_error "R is not installed. Please install R first:"
    print_error "  brew install r"
    exit 1
fi
print_success "R is installed"

# Check if RStudio is installed (optional, but good to know)
if command -v rstudio &> /dev/null; then
    print_success "RStudio is also available"
else
    print_warning "RStudio not found (optional, but recommended for development)"
fi

# Check if required R files exist
print_status "Checking for required R scripts..."
if [ ! -f "$SCRIPT_DIR/setup.R" ]; then
    print_error "setup.R not found in $SCRIPT_DIR"
    exit 1
fi

if [ ! -f "$SCRIPT_DIR/feed_analyses_updated.R" ]; then
    print_error "feed_analyses_updated.R not found in $SCRIPT_DIR"
    exit 1
fi
print_success "All required R scripts found"

# Check if data file exists
DATA_FILE="/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv"
print_status "Checking for data file..."
if [ ! -f "$DATA_FILE" ]; then
    print_warning "Data file not found: $DATA_FILE"
    print_warning "The analysis may fail if this file is required"
    print_warning "Continuing anyway..."
else
    print_success "Data file found"
fi

# Create results directory if it doesn't exist
RESULTS_DIR="$SCRIPT_DIR/results"
if [ ! -d "$RESULTS_DIR" ]; then
    print_status "Creating results directory..."
    mkdir -p "$RESULTS_DIR"
    print_success "Results directory created: $RESULTS_DIR"
fi

# Run the setup script
print_status "Running R package setup..."
print_status "This may take a few minutes if packages need to be installed..."

cd "$SCRIPT_DIR"
R --slave --no-restore --no-save << 'EOF'
# Source the simple setup script first
source("setup_simple.R")
EOF

if [ $? -eq 0 ]; then
    print_success "Package setup completed successfully"
else
    print_warning "Simple setup failed, trying alternative method..."
    R --slave --no-restore --no-save << 'EOF'
    # Source the original setup script
    source("setup.R")
EOF
    if [ $? -eq 0 ]; then
        print_success "Alternative package setup completed successfully"
    else
        print_error "All setup methods failed"
        exit 1
    fi
fi

# Run the analysis script
print_status "Running the analysis script..."
print_status "This will generate plots and save them to a timestamped directory..."

cd "$SCRIPT_DIR"
R --slave --no-restore --no-save << 'EOF'
# Source the analysis script
source("feed_analyses_updated.R")
EOF

if [ $? -eq 0 ]; then
    print_success "Analysis completed successfully!"
    
    # Find the most recent timestamped directory
    LATEST_DIR=$(find "$RESULTS_DIR" -type d -name "20*" | sort | tail -1)
    if [ -n "$LATEST_DIR" ]; then
        print_success "Results saved to: $LATEST_DIR"
        
        # Count the number of PNG files generated
        PNG_COUNT=$(find "$LATEST_DIR" -name "*.png" | wc -l)
        print_success "Generated $PNG_COUNT plot files"
        
        # List the generated files
        print_status "Generated files:"
        find "$LATEST_DIR" -name "*.png" -exec basename {} \; | sort | while read file; do
            echo "  - $file"
        done
    else
        print_warning "Could not find timestamped results directory"
    fi
else
    print_error "Analysis failed"
    exit 1
fi

print_success "All done! Check the results directory for your plots."
print_status "You can also open RStudio and run the scripts interactively if you prefer."
