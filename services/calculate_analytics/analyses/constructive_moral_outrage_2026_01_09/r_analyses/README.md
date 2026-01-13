# Constructiveness-Only RQ1 Analyses

**NOTE: this folder's code (i.e., all code in `r_analyses`) is an AI-generated version of the `RQ1_analyses.R` file (which we coded up manually) refactored to work specifically on the constructiveness data ONLY. The actual human-curated code is `RQ1_analyses.R`; use other files here at your own risk. This README is also AI-generated as well (with some manual curation).**

This directory contains a refactored, modular R analysis pipeline focused exclusively on **constructive content** analyses. This version does **not** require baseline firehose data (`baseline_prop`), making it simpler to run with just the constructiveness dataset.

## Key Features

- **Narrow scope**: Only analyzes constructive content (no other dependent variables)
- **No baseline required**: Works without baseline firehose data
- **Clean modular structure**: Split into focused modules for maintainability
- **Reproducible**: All outputs saved to a dedicated directory

## Directory Structure

```markdown
r_analyses/
├── README.md                              # This file
├── Makefile                               # Build automation
├── RQ1_constructiveness_only_analyses.R   # Main entry script
├── RQ1_analyses.R                         # Original full analysis script
└── lib/
    ├── bootstrap.R                        # Setup utilities (working dir, packages)
    ├── config.R                           # Central configuration
    ├── io.R                               # Data loading functions
    ├── transform.R                        # Data cleaning/transformation
    ├── models.R                           # Mixed-effects model fitting
    ├── contrasts.R                        # Contrast computations (emmeans)
    ├── plotting.R                         # Visualization functions
    └── reporting.R                        # Output file generation
```

## Prerequisites

### Required R Packages

Install the following R packages before running:

```r
install.packages(c(
  "tidyverse",  # dplyr, readr, ggplot2, etc.
  "lmerTest",   # Mixed-effects models with p-values
  "emmeans",    # Estimated marginal means and contrasts
  "scales",     # Plotting utilities
  "tibble"      # Modern data frames (usually via tidyverse)
))
```

Or install all at once:
```r
install.packages(c("tidyverse", "lmerTest", "emmeans", "scales", "tibble"))
```

### Required Input Files

Place these CSV files in the **parent directory** (one level up from `r_analyses/`):

1. **`per_user_per_day_average_constructiveness.csv`**
   - Columns: `handle`, `condition`, `date`, `feed_average_constructiveness`
   - Contains per-user-per-day constructiveness averages aggregated from per-feed data
   - **Note**: This file was previously named `daily_feed_content_aggregated_results_per_user.csv`

2. **`qualtrics_final_impute.csv`**
   - Must contain a `handle` column
   - Used to filter to valid study participants only

## Running the Analysis

### Option 1: Using Make (Recommended)

From the project root or the analysis directory:

```bash
cd services/calculate_analytics/analyses/constructive_moral_outrage_2026_01_09/r_analyses
make run
```

Or from anywhere (using absolute path):

```bash
make -C /path/to/r_analyses run
```

**Available Make targets:**

- `make run` - Run the main analysis script
- `make clean` - Remove the output directory
- `make check-inputs` - Check if required input files exist (doesn't run analysis)
- `make help` - Show help message with available targets

### Option 2: Using Rscript Directly

```bash
cd services/calculate_analytics/analyses/constructive_moral_outrage_2026_01_09/r_analyses
Rscript RQ1_constructiveness_only_analyses.R
```

### Option 3: Interactive R/RStudio

1. Open R or RStudio
2. Navigate to the `r_analyses` directory
3. Source the main script:
   ```r
   source("RQ1_constructiveness_only_analyses.R")
   ```
4. The script will automatically call `main()` when sourced

## Outputs

All outputs are written to `outputs_constructiveness_only/` (relative to `r_analyses/`):

### Plots
- **`constructive_time_series_conditions_only.png`**: Time series plot showing constructive content proportions over time by condition (with LOESS smoothing)
- **`prepost_contrasts_constructive.png`**: Visual comparison of pre/post-election contrasts between conditions

### Tables (CSV)
- **`model_fit_comparison_constructive.csv`**: Model fit statistics comparing base vs. pre/post models (AIC, BIC, log-likelihood, LRT)
- **`contrasts_prepost_constructive.csv`**: Detailed contrast results for pre/post-election periods
- **`contrasts_base_constructive.csv`**: Overall contrasts across all time (base model)
- **`polynomial_model_selection_constructive.csv`**: Polynomial time model selection (linear, quadratic, cubic)

## Differences from Original `RQ1_analyses.R`

- **Scope**: Only analyzes constructive content (not intergroup, moral, toxic, etc.)
- **No baseline**: Does not require or use baseline firehose data
- **Modular**: Split into focused modules for easier maintenance
- **Cleaner**: More readable main function with clear sections

## Troubleshooting

### Missing R Packages
If you see an error about missing packages, install them:
```r
install.packages(c("tidyverse", "lmerTest", "emmeans", "scales", "tibble"))
```

### Missing Input Files
Ensure both CSV files are in the parent directory (one level up from `r_analyses/`):
- `../per_user_per_day_average_constructiveness.csv`
- `../qualtrics_final_impute.csv`

### Working Directory Issues
The script automatically sets the working directory to its own location. If you encounter path errors, ensure you're running from within the `r_analyses/` directory or using the Makefile.

## Notes

- The script automatically sets the working directory to the script's location
- All file paths in the config are relative to the parent directory (where the CSV files live)
- Outputs are written to `outputs_constructiveness_only/` (created automatically)
- The script does **not** modify input files
