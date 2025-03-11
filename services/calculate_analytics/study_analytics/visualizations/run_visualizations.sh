#!/bin/bash

# Navigate to the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $SCRIPT_DIR

# Create output directory if it doesn't exist
mkdir -p output

echo "Activating conda environment..."
# Ensure the conda environment is activated
source $(conda info --base)/etc/profile.d/conda.sh
conda activate bluesky-research

# Check for required packages and install if needed
echo "Checking dependencies..."
python -c "import statsmodels" || pip install statsmodels

echo "Running toxicity visualization..."
# Run the toxicity visualization first (quicker)
python plot_toxicity.py

echo "Running full feature visualizations..."
# Run the full feature visualizations
python time_series.py

echo "Visualization complete! Results are in the 'output' directory." 