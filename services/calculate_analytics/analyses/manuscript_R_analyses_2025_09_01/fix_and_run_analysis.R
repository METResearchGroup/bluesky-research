# Fix and Run Analysis Script
# This script will install missing dependencies and run the analysis

cat("=== Fixing R Package Dependencies and Running Analysis ===\n")

# Step 1: Install CMake (required for compiling R packages)
cat("Step 1: Installing CMake...\n")
system("brew install cmake", intern = FALSE)

# Step 2: Set CRAN mirror
options(repos = c(CRAN = "https://cran.rstudio.com/"))

# Step 3: Install required packages one by one
cat("Step 2: Installing R packages...\n")

required_packages <- c("tidyverse", "lmerTest", "broom.mixed", "knitr", "ggpubr", "emmeans")

for (pkg in required_packages) {
  cat(paste("Installing", pkg, "...\n"))
  tryCatch({
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      install.packages(pkg, dependencies = TRUE, type = "source")
      cat(paste("✓", pkg, "installed successfully\n"))
    } else {
      cat(paste("✓", pkg, "already installed\n"))
    }
  }, error = function(e) {
    cat(paste("✗ Error installing", pkg, ":", e$message, "\n"))
  })
}

# Step 4: Load packages and verify
cat("Step 3: Loading packages...\n")

# Load packages one by one and check for errors
packages_to_load <- c("tidyverse", "lmerTest", "broom.mixed", "knitr", "ggpubr", "emmeans")

for (pkg in packages_to_load) {
  cat(paste("Loading", pkg, "...\n"))
  tryCatch({
    library(pkg, character.only = TRUE)
    cat(paste("✓", pkg, "loaded successfully\n"))
  }, error = function(e) {
    cat(paste("✗ Error loading", pkg, ":", e$message, "\n"))
  })
}

# Step 5: Verify key functions are available
cat("Step 4: Verifying functions...\n")

key_functions <- c("read_csv", "%>%", "ggplot", "ggsave", "mutate", "filter")

for (func in key_functions) {
  if (exists(func)) {
    cat(paste("✓", func, "is available\n"))
  } else {
    cat(paste("✗", func, "is NOT available\n"))
  }
}

# Step 6: Run the analysis
cat("Step 5: Running analysis...\n")

# Clear workspace
rm(list = ls())

# Load packages again to ensure they're available
library(tidyverse)
library(lmerTest) 
library(broom.mixed)
library(knitr)
library(ggpubr)
library(emmeans)

# Timestamp functionality
generate_timestamp <- function() {
  format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
}

create_output_dir <- function() {
  timestamp <- generate_timestamp()
  base_output_dir <- "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/manuscript_R_analyses_2025_09_01/results"
  output_dir <- file.path(base_output_dir, timestamp)
  
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    cat("Created output directory:", output_dir, "\n")
  }
  
  return(output_dir)
}

# Create output directory
output_dir <- create_output_dir()

# Load data
data_file <- "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv"

if (!file.exists(data_file)) {
  stop("Data file not found: ", data_file)
}

# Load the data
combined_data <- read_csv(data_file)
cat("Loaded", nrow(combined_data), "rows from", basename(data_file), "\n")

# Get column names
all_cols <- colnames(combined_data)
cat("Column names:\n")
print(all_cols)

# Identify average and proportion columns
avg_cols <- all_cols[grepl("feed_average_", all_cols)]
prop_cols <- all_cols[grepl("feed_proportion_", all_cols)]

cat("Average columns found:", length(avg_cols), "\n")
cat("Proportion columns found:", length(prop_cols), "\n")

# Create separate datasets for averages and proportions
condition_feed <- combined_data %>% 
  select(all_of(c("date", "condition", avg_cols))) %>%
  rename_with(~gsub("feed_average_", "", .x), starts_with("feed_average_"))

condition_prop <- combined_data %>% 
  select(all_of(c("date", "condition", prop_cols))) %>%
  rename_with(~gsub("feed_proportion_", "", .x), starts_with("feed_proportion_"))

# Data wrangling for condition_feed
condition_feed <- condition_feed %>% 
  mutate(condition = ifelse(is.na(condition) == TRUE, "default", condition)) %>%
  mutate(condition = recode(condition,
                            "default" = "Default",
                            "reverse_chronological" = "Reverse Chronological",
                            "representative_diversification" = "Diversified Extremity",
                            "engagement" = "Engagement-Based")) %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d")) %>%
  mutate(day = as.numeric(date - min(date))) %>%
  mutate(day_centered = as.numeric(date - (min(date) + (max(date) - min(date)) / 2)))

# Set aside default condition
default <- condition_feed %>% filter(condition == "Default")
condition_feed <- condition_feed %>% filter(condition != "Default")

# Data wrangling for condition_prop
condition_prop <- condition_prop %>% 
  mutate(condition = ifelse(is.na(condition) == TRUE, "default", condition)) %>%
  mutate(condition = recode(condition,
                            "default" = "Default",
                            "reverse_chronological" = "Reverse Chronological",
                            "representative_diversification" = "Diversified Extremity",
                            "engagement" = "Engagement-Based")) %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d")) %>%
  mutate(day = as.numeric(date - min(date))) %>%
  mutate(day_centered = as.numeric(date - (min(date) + (max(date) - min(date)) / 2)))

# Set aside default condition
default_prop <- condition_prop %>% filter(condition == "Default")
condition_prop <- condition_prop %>% filter(condition != "Default")

# Plotting function
plot_time_series <- function(dv, y_label) {
  if (is.character(dv)) {
    dv_sym <- sym(dv)
  } else {
    dv_sym <- enquo(dv)
  }
  
  p <- ggplot(condition_feed, aes(x = date, y = !!dv_sym, color = condition, fill = condition)) +
    geom_jitter(color = "grey", alpha = 0.05, size = 0.5, width = 0.2, height = 0.02) +
    geom_smooth(method = "loess", se = TRUE, 
                method.args = list(span = 0.75, control = loess.control(surface = "direct")),
                linetype = "solid", size = 1, alpha = 0.2) +
    labs(
      title = paste("Time Series of", y_label, "by Condition"),
      x = "Date",
      y = y_label,
      color = "Condition",
      fill = "Condition"
    ) +
    scale_color_manual(values = c("Reverse Chronological" = "black", 
                                  "Engagement-Based" = "red3", 
                                  "Diversified Extremity" = "green4")) +
    scale_fill_manual(values = c("Reverse Chronological" = "black", 
                                 "Engagement-Based" = "red3", 
                                 "Diversified Extremity" = "green4")) +
    theme_classic() +
    theme(
      text = element_text(size = 12),
      legend.title = element_text(size = 12),
      legend.text = element_text(size = 10)
    )
  
  return(p)
}

# Generate plots
cat("Generating plots...\n")

# Check which columns are available for plotting
available_cols <- colnames(condition_feed)
cat("Available columns for plotting:\n")
print(available_cols)

# Create plots for available columns
# Note: Column names were renamed from feed_average_* to * format
plot_vars <- list(
  "toxic" = "Average Toxicity Probability",
  "intergroup" = "Average Intergroup Content Probability", 
  "moral" = "Average Moral Content Probability",
  "emotion" = "Average Emotion Content Probability",
  "moral_outrage" = "Average Outrage Content Probability",
  "constructive" = "Average Constructive Content Probability",
  "is_sociopolitical" = "Average Political Content Probability"
)

for (var_name in names(plot_vars)) {
  if (var_name %in% available_cols) {
    cat(paste("Creating plot for", var_name, "\n"))
    plot_obj <- plot_time_series(var_name, plot_vars[[var_name]])
    filename <- paste0(var_name, "_time_series_smoothed.png")
    ggsave(file.path(output_dir, filename), plot_obj, dpi = 300, width = 10, height = 6)
    cat(paste("✓ Saved", filename, "\n"))
  } else {
    cat(paste("✗ Column", var_name, "not found in data\n"))
  }
}

cat("Analysis completed successfully!\n")
cat("Results saved to:", output_dir, "\n")
cat("Generated", length(list.files(output_dir, pattern = "\\.png$")), "plot files\n")
