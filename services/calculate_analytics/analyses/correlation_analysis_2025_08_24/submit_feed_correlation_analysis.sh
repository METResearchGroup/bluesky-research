#!/bin/bash

# Submit Feed Correlation Analysis Job
# This script runs the feed correlation analysis on the latest CSV files from the results directory

# Set job parameters
JOB_NAME="feed_correlation_analysis"
SCRIPT_NAME="feed_correlation_analysis.py"
OUTPUT_DIR="results"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Submit the job
sbatch \
    --job-name="$JOB_NAME" \
    --output="$OUTPUT_DIR/${JOB_NAME}_%j.out" \
    --error="$OUTPUT_DIR/${JOB_NAME}_%j.err" \
    --time=01:00:00 \
    --mem=8G \
    --cpus-per-task=2 \
    --wrap="python $SCRIPT_NAME"

echo "Submitted feed correlation analysis job: $JOB_NAME"
echo "Check results in: $OUTPUT_DIR/"
