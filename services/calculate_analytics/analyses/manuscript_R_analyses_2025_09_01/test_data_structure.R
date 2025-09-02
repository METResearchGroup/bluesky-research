# Test Data Structure Script
# This script loads the data and shows the actual column structure

cat("=== Testing Data Structure ===\n")

# Load required packages
library(tidyverse)

# Load data
data_file <- "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv"

if (!file.exists(data_file)) {
  cat("❌ Data file not found:", data_file, "\n")
  cat("Please ensure the data file exists before running the analysis.\n")
} else {
  cat("✅ Data file found\n")
  
  # Load the data
  combined_data <- read_csv(data_file, show_col_types = FALSE)
  cat("Loaded", nrow(combined_data), "rows\n")
  
  # Show all column names
  cat("\n=== All Column Names ===\n")
  all_cols <- colnames(combined_data)
  print(all_cols)
  
  # Show average columns
  cat("\n=== Average Columns ===\n")
  avg_cols <- all_cols[grepl("feed_average_", all_cols)]
  print(avg_cols)
  
  # Show proportion columns
  cat("\n=== Proportion Columns ===\n")
  prop_cols <- all_cols[grepl("feed_proportion_", all_cols)]
  print(prop_cols)
  
  # Show what columns would be available after renaming
  cat("\n=== Columns After Renaming (feed_average_* -> *) ===\n")
  renamed_avg_cols <- gsub("feed_average_", "", avg_cols)
  print(renamed_avg_cols)
  
  # Show sample data
  cat("\n=== Sample Data (first 3 rows) ===\n")
  print(head(combined_data, 3))
  
  # Show unique conditions
  cat("\n=== Unique Conditions ===\n")
  if ("condition" %in% colnames(combined_data)) {
    print(unique(combined_data$condition))
  } else {
    cat("No 'condition' column found\n")
  }
  
  # Show date range
  cat("\n=== Date Range ===\n")
  if ("date" %in% colnames(combined_data)) {
    cat("Date range:", min(combined_data$date), "to", max(combined_data$date), "\n")
  } else {
    cat("No 'date' column found\n")
  }
}

cat("\n=== Data Structure Test Complete ===\n")
