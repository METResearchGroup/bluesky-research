# Manuscript R Analyses 2025-09-01

## Overview

This repository contains R scripts for analyzing feed content data as part of a research manuscript on engagement-based algorithms and their impact on content amplification. The analysis focuses on examining how different feed algorithms affect the distribution of various content types, including toxicity, intergroup content, moral content, emotion, outrage, constructive content, and political content.

## Research Question

**RQ1: Do engagement-based algorithms amplify IME (Intergroup, Moral, Emotional) content?**

This analysis compares different feed conditions:
- **Default**: Reverse-chronological without personalization
- **Reverse Chronological**: Standard chronological feed
- **Engagement-Based**: Algorithm optimized for user engagement
- **Diversified Extremity**: Algorithm designed to diversify content exposure

## Repository Structure

```
manuscript_R_analyses_2025_09_01/
├── README.md                    # This file
├── setup.R                      # Package installation and setup script
├── feed_analyses_updated.R      # Main analysis script
├── original/                    # Original analysis files
│   └── feed_analyses.R         # Original R script
└── results/                     # Output directory (timestamped subdirectories)
    └── <timestamp>/            # Timestamped results from each run
        ├── toxicity_time_series_smoothed.png
        ├── intergroup_time_series_smoothed.png
        ├── moral_time_series_smoothed.png
        ├── emotion_time_series_smoothed.png
        ├── outrage_time_series_smoothed.png
        ├── constructive_time_series_smoothed.png
        └── political_time_series_smoothed.png
```

## Prerequisites

- **R** (version 4.0 or higher)
- **RStudio** (recommended for beginners)
- **macOS** (tested on macOS 23.1.0)

## Setup Instructions

### Step 1: Install R and RStudio

#### Option A: Using Homebrew (Recommended)
```bash
# Install R
brew install r

# Install RStudio
brew install --cask rstudio
```

#### Option B: Manual Installation
1. **Install R**: Download from [CRAN](https://cran.r-project.org/bin/macosx/)
2. **Install RStudio**: Download from [Posit](https://posit.co/download/rstudio-desktop/)

### Step 2: Install Required R Packages

#### Option A: Run the setup script
```bash
# Navigate to the repository directory
cd /Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/manuscript_R_analyses_2025_09_01

# Start R and run setup
R
source("setup.R")
```

#### Option B: Manual package installation
Open R or RStudio and run:
```r
install.packages(c("tidyverse", "lmerTest", "broom.mixed", "knitr", "ggpubr", "emmeans"))
```

### Step 3: Verify Installation

Run the setup script to verify all packages are installed correctly:
```r
source("setup.R")
```

You should see confirmation messages for each package.

## Running the Analysis

### Prerequisites
Ensure you have the required data file:
```
/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv
```

### Method 1: Automated Bash Script (Recommended)
```bash
# Navigate to the repository directory
cd /Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/manuscript_R_analyses_2025_09_01

# Run the automated script (handles setup and analysis)
./run_analysis.sh
```

This script will:
- Check if R is installed
- Run the package setup automatically
- Execute the analysis
- Show you where the results are saved

### Method 2: Using RStudio (Interactive)
1. Open RStudio
2. Open `feed_analyses_updated.R`
3. Click the "Source" button or press `Cmd+Shift+S`

### Method 3: Manual Command Line
```bash
# Navigate to the repository directory
cd /Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/manuscript_R_analyses_2025_09_01

# First run setup (if not done already)
R
source("setup.R")
quit()

# Then run the analysis
Rscript feed_analyses_updated.R
```

## Output

The script will:
1. Create a timestamped output directory in `results/`
2. Load and process the feed data
3. Generate 7 time series plots with LOESS smoothing
4. Save all plots as high-resolution PNG files (300 DPI)

### Generated Plots
- **Toxicity**: Average toxicity probability over time
- **Intergroup Content**: Average intergroup content probability
- **Moral Content**: Average moral content probability  
- **Emotion Content**: Average emotion content probability
- **Outrage Content**: Average moral outrage content probability
- **Constructive Content**: Average constructive content probability
- **Political Content**: Average political content probability

Each plot shows:
- Individual data points (jittered for visibility)
- LOESS smoothed trend lines with confidence bands
- Color-coded conditions (Black: Reverse Chronological, Red: Engagement-Based, Green: Diversified Extremity)

## Data Requirements

The analysis expects a CSV file with the following structure:
- **Date column**: Date in YYYY-MM-DD format
- **Condition column**: Feed algorithm type
- **Average columns**: `feed_average_*` prefixed columns for continuous metrics
- **Proportion columns**: `feed_proportion_*` prefixed columns for binary classifications

## Troubleshooting

### Package Installation Issues
```r
# If packages fail to install, try:
install.packages("tidyverse", dependencies = TRUE)

# Or install from a different CRAN mirror:
install.packages("tidyverse", repos = "https://cran.rstudio.com/")
```

### Data File Not Found
If you get an error about the data file not being found:
1. Verify the file path is correct
2. Check that the CSV file exists
3. Ensure you have read permissions for the file

### Permission Errors
```bash
# Ensure write permissions for output directory
chmod -R 755 /Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/manuscript_R_analyses_2025_09_01/results
```

### RStudio Issues
- Make sure RStudio is using the correct R installation
- Check that the working directory is set correctly
- Verify all packages are loaded without errors

## Dependencies

### Required R Packages
- **tidyverse**: Data manipulation and visualization
- **lmerTest**: Linear mixed-effects models
- **broom.mixed**: Tidying mixed model outputs
- **knitr**: Dynamic report generation
- **ggpubr**: Publication-ready plots
- **emmeans**: Estimated marginal means

### System Requirements
- R 4.0+
- macOS (tested on 23.1.0)
- Sufficient disk space for output plots (~50MB per run)

## Contributing

When modifying the analysis:
1. Update the timestamp in output filenames
2. Document any changes to the analysis methodology
3. Test with sample data before running on full dataset
4. Update this README if new dependencies are added

## Contact

For questions about this analysis, please refer to the main project documentation or contact the research team.