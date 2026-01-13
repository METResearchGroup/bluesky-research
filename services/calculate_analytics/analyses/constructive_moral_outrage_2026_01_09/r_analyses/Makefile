# Makefile for running RQ1 Constructiveness-Only Analyses
#
# Usage:
#   make run           - Run the main analysis script
#   make clean         - Remove output directory
#   make help          - Show this help message

# Default target
.PHONY: run clean check-inputs help

# Rscript command (adjust if needed)
RSCRIPT := Rscript

# Main script name
MAIN_SCRIPT := RQ1_constructiveness_only_analyses.R

# Output directory (matches config.R default)
OUTPUT_DIR := outputs_constructiveness_only

# Default target
.DEFAULT_GOAL := help

help:
	@echo "Makefile for RQ1 Constructiveness-Only Analyses"
	@echo ""
	@echo "Available targets:"
	@echo "  make run          - Run the main R analysis script"
	@echo "  make clean        - Remove the output directory"
	@echo "  make check-inputs - Check if required input files exist"
	@echo "  make help         - Show this help message"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - R must be installed and available in PATH"
	@echo "  - Required R packages: tidyverse, lmerTest, emmeans, scales, tibble"
	@echo "  - Input CSV files must be in parent directory:"
	@echo "    - per_user_per_day_average_constructiveness.csv"
	@echo "    - qualtrics_final_impute.csv"

run:
	@echo "Running RQ1 Constructiveness-Only Analyses..."
	@echo "Working directory: $$(pwd)"
	@echo ""
	@if [ ! -f "$(MAIN_SCRIPT)" ]; then \
		echo "Error: $(MAIN_SCRIPT) not found. Are you in the r_analyses directory?"; \
		exit 1; \
	fi
	@if ! command -v $(RSCRIPT) >/dev/null 2>&1; then \
		echo "Error: $(RSCRIPT) not found. Please install R and ensure it's in your PATH."; \
		exit 1; \
	fi
	@echo "Executing: $(RSCRIPT) $(MAIN_SCRIPT)"
	@echo ""
	@$(RSCRIPT) $(MAIN_SCRIPT) || { \
		echo ""; \
		echo "Analysis failed. Check error messages above."; \
		exit 1; \
	}
	@echo ""
	@echo "Analysis complete! Outputs written to: $(OUTPUT_DIR)/"

clean:
	@echo "Removing output directory: $(OUTPUT_DIR)/"
	@if [ -d "$(OUTPUT_DIR)" ]; then \
		rm -rf "$(OUTPUT_DIR)"; \
		echo "Output directory removed."; \
	else \
		echo "Output directory does not exist."; \
	fi

# Check if input files exist (informative, not blocking)
check-inputs:
	@echo "Checking for required input files in parent directory..."
	@if [ -f "../per_user_per_day_average_constructiveness.csv" ]; then \
		echo "  ✓ Found: per_user_per_day_average_constructiveness.csv"; \
	else \
		echo "  ✗ Missing: per_user_per_day_average_constructiveness.csv"; \
	fi
	@if [ -f "../qualtrics_final_impute.csv" ]; then \
		echo "  ✓ Found: qualtrics_final_impute.csv"; \
	else \
		echo "  ✗ Missing: qualtrics_final_impute.csv"; \
	fi

