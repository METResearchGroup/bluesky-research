#### load libraries ----
library(tidyverse)
library(lmerTest) 
library(broom.mixed)
library(knitr)
library(ggpubr)
library(emmeans)

#### set up working environment ----
# clear workspace
rm(list = ls())  

#### timestamp functionality ----
# Create timestamp for output directory (similar to Python's generate_current_datetime_str)
generate_timestamp <- function() {
  format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
}

# Create timestamped output directory
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

# Create the output directory
output_dir <- create_output_dir()

#### load data ----
# Load the combined data file that contains both averages and proportions
data_file <- "/Users/mark/Documents/work/bluesky-research/services/calculate_analytics/analyses/user_feed_analysis_2025_04_08/results/daily_feed_content_aggregated_results_per_user.csv"

# Check if file exists
if (!file.exists(data_file)) {
  stop("Data file not found: ", data_file)
}

# Load the data
combined_data <- read_csv(data_file, show_col_types = FALSE)
cat("Loaded", nrow(combined_data), "rows from", basename(data_file), "\n")

# Separate the data into averages and proportions based on column names
# Assuming columns follow the pattern: feed_average_* and feed_proportion_*

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

# Comment out baseline data loading since files don't exist
# baseline_feed <- read_csv("firehose_baseline_averages_per_day.csv")
# baseline_prop <- read_csv("firehose_baseline_aggregated_proportion.csv")

#########################################################.
# RQ1: Do engagement-based algorithms amplify IME content?
#########################################################.

#### condition feed wrangling (prob) ----

# add default condition -- reverse-chronological without any personalization
condition_feed <- condition_feed %>% mutate(condition = ifelse(is.na(condition) == TRUE, "default", condition))

# edit feed names
condition_feed <- condition_feed %>%
  mutate(condition = recode(condition,
                            "default" = "Default",
                            "reverse_chronological" = "Reverse Chronological",
                            "representative_diversification" = "Diversified Extremity",
                            "engagement" = "Engagement-Based"))

# convert date to time object
condition_feed <- condition_feed %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

# calculate the midpoint date for centering of day later
midpoint_date <- min(condition_feed$date) + (max(condition_feed$date) - min(condition_feed$date)) / 2

# create numeric time variable (days since first date) for time modeling later
condition_feed <- condition_feed %>%
  mutate(day = as.numeric(date - min(date)))

# create a centered day variable: days since the midpoint of the study
condition_feed <- condition_feed %>%
  mutate(day_centered = as.numeric(date - midpoint_date))

# we want to set aside "default" for later comparison
default <- condition_feed %>% 
  filter(condition == "Default")

# remove rows with condition "Default" from main df
condition_feed <- condition_feed %>% 
  filter(condition != "Default")

# Comment out baseline content wrangling since files don't exist
# #### baseline content wrangling (prob) ----
# 
# # add condition label for later
# baseline_feed <- baseline_feed %>%
#   mutate(condition = "Baseline Firehose")
# 
# # convert date to time object
# baseline_feed <- baseline_feed %>%
#   mutate(date = as.Date(date, format = "%Y-%m-%d"))

#### condition feed wrangling (prop) ----

# add default condition -- reverse-chronological without any personalization
condition_prop <- condition_prop %>% mutate(condition = ifelse(is.na(condition) == TRUE, "default", condition))

# edit feed names
condition_prop <- condition_prop %>%
  mutate(condition = recode(condition,
                            "default" = "Default",
                            "reverse_chronological" = "Reverse Chronological",
                            "representative_diversification" = "Diversified Extremity",
                            "engagement" = "Engagement-Based"))

# convert date to time object
condition_prop <- condition_prop %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

# calculate the midpoint date for centering of day later
midpoint_date <- min(condition_prop$date) + (max(condition_prop$date) - min(condition_prop$date)) / 2

# create numeric time variable (days since first date) for time modeling later
condition_prop <- condition_prop %>%
  mutate(day = as.numeric(date - min(date)))

# create a centered day variable: days since the midpoint of the study
condition_prop <- condition_prop %>%
  mutate(day_centered = as.numeric(date - midpoint_date))

# we want to set aside "default" for later comparison
default <- condition_prop %>% 
  filter(condition == "Default")

# remove rows with condition "Default" from main df
condition_prop <- condition_prop %>% 
  filter(condition != "Default")

# Comment out baseline content wrangling since files don't exist
# #### baseline content wrangling (prop) ----
# 
# # add condition label for later
# baseline_prop <- baseline_prop %>%
#   mutate(condition = "Baseline Firehose")
# 
# # convert date to time object
# baseline_prop <- baseline_prop %>%
#   mutate(date = as.Date(date, format = "%Y-%m-%d"))

#### plotting DVs over time with LOESS fit and error bands ----

# since we have multiple DVs, this function will plot with custom Y axis
plot_time_series <- function(dv, y_label) {
  # Accept the DV as a string or an unquoted column name
  if (is.character(dv)) {
    dv_sym <- sym(dv)
  } else {
    dv_sym <- enquo(dv)
  }
  
  # Build the plot using the full data (condition_feed)
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
    # Use a constant color scheme
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
  
  # Return the plot object so it can be saved or further modified
  return(p)
}

# Generate plots
cat("Generating plots...\n")

# Check which columns are available for plotting
available_cols <- colnames(condition_feed)
cat("Available columns for plotting:\n")
print(available_cols)

# Create plots for available columns (using actual column names after renaming)
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
