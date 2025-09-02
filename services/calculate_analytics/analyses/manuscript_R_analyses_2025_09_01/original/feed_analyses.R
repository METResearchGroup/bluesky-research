#### load libraries ----
library(tidyverse)
library(lmerTest) 
library(broom.mixed)
library(knitr)
library(ggpubr)
library(emmeans)

#### set up working environment ----
# automatically set the working directory to the script's location
setwd(dirname(rstudioapi::getSourceEditorContext()$path))

# clear workspace
rm(list = ls())  



#### load data ----

# mean probabilities (continuous)
condition_feed <- read_csv("condition_aggregated.csv")
baseline_feed <- read_csv("firehose_baseline_averages_per_day.csv")

# mean proportions (makes all classifications at post level binary)
condition_prop <- read_csv("condition_aggregated_proportion.csv")
baseline_prop <- read_csv("firehose_baseline_aggregated_proportion.csv")

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


#### baseline content wrangling (prob) ----

# add condition label for later
baseline_feed <- baseline_feed %>%
  mutate(condition = "Baseline Firehose")

# convert date to time object
baseline_feed <- baseline_feed %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))


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



#### baseline content wrangling (prop) ----

# add condition label for later
baseline_prop <- baseline_prop %>%
  mutate(condition = "Baseline Firehose")

# convert date to time object
baseline_prop <- baseline_prop %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))




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

# toxicity
plot_toxic <- plot_time_series("avg_prob_toxic", "Average Toxicity Probability")
print(plot_toxic)
ggsave("toxicity_time_series_smoothed.png", plot_toxic, dpi = 300, width = 10, height = 6)

# intergroup
plot_intergroup <- plot_time_series("avg_prob_intergroup", "Average Intergroup Content Probability")
ggsave("intergroup_time_series_smoothed.png", plot_intergroup, dpi = 300, width = 10, height = 6)

# moral
plot_moral <- plot_time_series("avg_prob_moral", "Average Moral Content Probability")
ggsave("moral_time_series_smoothed.png", plot_moral, dpi = 300, width = 10, height = 6)

# emotion
plot_emotion <- plot_time_series("avg_prob_emotion", "Average Emotion Content Probability")
ggsave("emotion_time_series_smoothed.png", plot_emotion, dpi = 300, width = 10, height = 6)

# outrage
plot_outrage <- plot_time_series("avg_prob_moral_outrage", "Average Outrage Content Probability")
ggsave("outrage_time_series_smoothed.png", plot_outrage, dpi = 300, width = 10, height = 6)

# constructive
plot_constructive <- plot_time_series("avg_prob_constructive", "Average Constructive Content Probability")
ggsave("constructive_time_series_smoothed.png", plot_constructive, dpi = 300, width = 10, height = 6)

# political
plot_political <- plot_time_series("avg_is_political", "Average Political Content Probability")
print(plot_political)
ggsave("political_time_series_smoothed.png", plot_constructive, dpi = 300, width = 10, height = 6)