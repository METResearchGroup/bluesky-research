#### load libraries ----
library(tidyverse)
library(lmerTest) 
library(broom.mixed)
library(knitr)
library(ggpubr)
library(emmeans)
library(splines)
library(scales)

#### set up working environment ----
# automatically set the working directory to the script's location
setwd(dirname(rstudioapi::getSourceEditorContext()$path))

# clear workspace
rm(list = ls())  



#### load data ----

# proportions and probabilities of content per user per day
condition_feed <- read_csv("daily_feed_content_aggregated_results_per_user.csv")

# baseline firehose sample of content per day
baseline_prop <- read_csv("daily_baseline_updated.csv")



#### analyze only valid users ----
qualtrics_final_impute <- read_csv("qualtrics_final_impute.csv") %>% 
  group_by(handle) %>% summarize (n = n())

# Extract the list of valid user handles
valid_handles <- qualtrics_final_impute$handle

# Filter condition_feed to only include valid users
condition_feed <- condition_feed %>%
  filter(handle %in% valid_handles)


#########################################################.
# RQ1: Do engagement-based algorithms amplify IME content?
#########################################################.


#### condition feed wrangling ----
names(condition_feed)
# edit feed names
condition_feed <- condition_feed %>%
  mutate(condition = recode(condition,
                            "reverse_chronological" = "Reverse Chronological",
                            "representative_diversification" = "Diversified Extremity",
                            "engagement" = "Engagement-Based"))

# convert date to time object
condition_feed <- condition_feed %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

# calculate the midpoint date for centering of day for exploratory models
midpoint_date <- min(condition_feed$date) + (max(condition_feed$date) - min(condition_feed$date)) / 2

# create numeric time variable (days since first date) for exploratory time modeling
condition_feed <- condition_feed %>%
  mutate(day = as.numeric(date - min(date)))

# create a centered day variable: days since the midpoint of the study
condition_feed <- condition_feed %>%
  mutate(day_centered = as.numeric(date - midpoint_date))



#### baseline content wrangling (prop) ----

# add condition label for later
baseline_prop <- baseline_prop %>%
  mutate(condition = "Baseline Firehose")

# convert date to time object
baseline_prop <- baseline_prop %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

# Update baseline_prop column names to match your main data naming convention
baseline_prop <- baseline_prop %>%
  rename(
    feed_proportion_intergroup = baseline_proportion_intergroup,
    feed_proportion_moral = baseline_proportion_moral,
    feed_proportion_is_sociopolitical = baseline_proportion_is_sociopolitical,
    feed_proportion_toxic = baseline_proportion_toxic,
    feed_proportion_constructive = baseline_proportion_constructive,
    feed_proportion_moral_outrage = baseline_proportion_moral_outrage,
    feed_proportion_is_valence_negative = baseline_proportion_is_valence_negative, 
    feed_proportion_is_valence_positive = baseline_proportion_is_valence_positive   
  )


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
plot_toxic <- plot_time_series("feed_proportion_toxic", "Average Toxicity Proportion")
print(plot_toxic)
ggsave("toxicity_time_series_smoothed.png", plot_toxic, dpi = 300, width = 10, height = 6)

# intergroup
plot_intergroup <- plot_time_series("avg_prob_intergroup", "Average Intergroup Content Proportion")
ggsave("intergroup_time_series_smoothed.png", plot_intergroup, dpi = 300, width = 10, height = 6)

# moral
plot_moral <- plot_time_series("avg_prob_moral", "Average Moral Content Proportion")
ggsave("moral_time_series_smoothed.png", plot_moral, dpi = 300, width = 10, height = 6)

# emotion
plot_emotion <- plot_time_series("avg_prob_emotion", "Average Emotion Content Proportion")
ggsave("emotion_time_series_smoothed.png", plot_emotion, dpi = 300, width = 10, height = 6)

# outrage
plot_outrage <- plot_time_series("feed_proportion_moral_outrage", "Average Outrage Content Proportion")
print(plot_outrage)
ggsave("outrage_time_series_smoothed.png", plot_outrage, dpi = 300, width = 10, height = 6)

# constructive
plot_constructive <- plot_time_series("avg_prob_constructive", "Average Constructive Content Proportion")
ggsave("constructive_time_series_smoothed.png", plot_constructive, dpi = 300, width = 10, height = 6)

# political
plot_political <- plot_time_series("avg_is_political", "Average Political Content Proportion")
print(plot_political)
ggsave("political_time_series_smoothed.png", plot_constructive, dpi = 300, width = 10, height = 6)



#### Base vs Pre/Post Election Models for Proportions ----

# Election date
election_date <- as.Date("2024-11-05")

# Create pre/post election variables in proportion data
condition_feed <- condition_feed %>%
  mutate(
    is_post_election = as.numeric(date > election_date),
    period = factor(if_else(date > election_date, "Post", "Pre"), levels = c("Pre", "Post"))
  )

# set RC as ref group
# condition_feed <- condition_feed %>%
 # mutate(condition = factor(condition, 
                          #  levels = c("Reverse Chronological", 
                                #       "Diversified Extremity", 
                                 #      "Engagement-Based")))

# Function to fit base and pre/post models
fit_base_prepost_models <- function(dv, data = condition_feed) {
  
  # MODEL 1: Base model (no time component)
  base_formula <- as.formula(paste(dv, "~ condition + (1 | handle)"))
  model_base <- lmer(base_formula, data = data)
  
  # MODEL 2: Pre/post election interaction
  prepost_formula <- as.formula(paste(dv, "~ condition * is_post_election + (1 | handle)"))
  model_prepost <- lmer(prepost_formula, data = data)
  
  # Return both models
  list(
    base = model_base,
    prepost = model_prepost
  )
}

## Fit models for all proportion variables
# pre-reg
models_prop_intergroup <- fit_base_prepost_models("feed_proportion_intergroup")
models_prop_moral <- fit_base_prepost_models("feed_proportion_moral")
models_prop_emotion_neg <- fit_base_prepost_models("feed_proportion_is_valence_negative")
models_prop_emotion_pos <- fit_base_prepost_models("feed_proportion_is_valence_positive")
models_prop_toxic <- fit_base_prepost_models("feed_proportion_toxic")
models_prop_political <- fit_base_prepost_models("feed_proportion_is_sociopolitical")

# exploratory
models_prop_outrage <- fit_base_prepost_models("feed_proportion_moral_outrage")
models_prop_constructive <- fit_base_prepost_models("feed_proportion_constructive")

#### compare model fits (base vs prepost) ----

# Function to compare model fit
compare_base_prepost_fit <- function(models, dv_name = "") {
  
  model_base <- models$base
  model_prepost <- models$prepost
  
  # Model comparison table
  comparison <- data.frame(
    Model = c("Base (Condition Only)", "Pre/Post Election"),
    AIC = c(AIC(model_base), AIC(model_prepost)),
    BIC = c(BIC(model_base), BIC(model_prepost)),
    LogLik = c(as.numeric(logLik(model_base)), as.numeric(logLik(model_prepost))),
    df = c(attr(logLik(model_base), "df"), attr(logLik(model_prepost), "df"))
  ) %>%
    mutate(
      delta_AIC = AIC - min(AIC),
      delta_BIC = BIC - min(BIC),
      AIC_better = ifelse(AIC == min(AIC), "***", ""),
      BIC_better = ifelse(BIC == min(BIC), "***", "")
    )
  
  # Likelihood ratio test
  lr_test <- anova(model_base, model_prepost)
  
  # Results
  results <- list(
    comparison_table = comparison,
    lr_test = lr_test,
    aic_winner = comparison$Model[which.min(comparison$AIC)],
    bic_winner = comparison$Model[which.min(comparison$BIC)]
  )
  
  # Print results
  cat(paste("=== MODEL FIT COMPARISON:", toupper(dv_name), "===\n"))
  print(comparison)
  
  cat(paste("\nBest model by AIC:", results$aic_winner))
  cat(paste("\nBest model by BIC:", results$bic_winner))
  
  cat("\n\n=== LIKELIHOOD RATIO TEST ===\n")
  print(lr_test)
  
  # Interpretation
  p_value <- lr_test$`Pr(>Chisq)`[2]
  if (p_value < 0.001) {
    cat("\nResult: Pre/post model significantly better than base model (p < .001)\n")
  } else if (p_value < 0.05) {
    cat(paste("\nResult: Pre/post model significantly better than base model (p =", round(p_value, 3), ")\n"))
  } else {
    cat(paste("\nResult: No significant improvement over base model (p =", round(p_value, 3), ")\n"))
  }
  
  return(results)
}

# Compare model fits for all variables
fit_intergroup <- compare_base_prepost_fit(models_prop_intergroup, "Intergroup Posts")
cat("\n", rep("=", 60), "\n\n")

fit_moral <- compare_base_prepost_fit(models_prop_moral, "Moral Posts")
cat("\n", rep("=", 60), "\n\n")

fit_emotion_neg <- compare_base_prepost_fit(models_prop_emotion_neg, "Negative Emotion Posts")
cat("\n", rep("=", 60), "\n\n")

fit_emotion_pos <- compare_base_prepost_fit(models_prop_emotion_pos, "Positive Emotion Posts")
cat("\n", rep("=", 60), "\n\n")

fit_toxic <- compare_base_prepost_fit(models_prop_toxic, "Toxic Posts")
cat("\n", rep("=", 60), "\n\n")

fit_outrage <- compare_base_prepost_fit(models_prop_outrage, "Moral Outrage Posts")
cat("\n", rep("=", 60), "\n\n")

fit_constructive <- compare_base_prepost_fit(models_prop_constructive, "Constructive Posts")
cat("\n", rep("=", 60), "\n\n")

fit_political <- compare_base_prepost_fit(models_prop_political, "Political Posts")
cat("\n", rep("=", 60), "\n\n")

#### Summary table across variables: AIC/BIC/LLR and preferred model ----
# Collect the paired models you already fit
models_list <- list(
  Toxic         = models_prop_toxic,
  Intergroup    = models_prop_intergroup,
  Moral         = models_prop_moral,
  Emotion_Neg   = models_prop_emotion_neg,
  Emotion_Pos   = models_prop_emotion_pos,
  Outrage       = models_prop_outrage,
  Constructive  = models_prop_constructive,
  Political     = models_prop_political
)

# Helper to safely extract LRT results
safe_lrt <- function(m_base, m_prepost) {
  lrt <- suppressWarnings(anova(m_base, m_prepost))
  if (is.null(lrt) || nrow(lrt) < 2) {
    list(LLR = NA_real_, p = NA_real_)
  } else {
    list(LLR = as.numeric(lrt$Chisq[2]),
         p   = as.numeric(lrt$`Pr(>Chisq)`[2]))
  }
}

# Build one tidy row per variable
summ_rows <- lapply(names(models_list), function(var) {
  m <- models_list[[var]]
  AIC_base <- AIC(m$base);     AIC_pre  <- AIC(m$prepost)
  BIC_base <- BIC(m$base);     BIC_pre  <- BIC(m$prepost)
  lrt <- safe_lrt(m$base, m$prepost)
  
  data.frame(
    Variable        = var,
    AIC_Base        = AIC_base,
    AIC_PrePost     = AIC_pre,
    BIC_Base        = BIC_base,
    BIC_PrePost     = BIC_pre,
    LLR             = lrt$LLR,
    LLR_p           = lrt$p,
    Preferred_Model = ifelse(AIC_pre < AIC_base, "Pre/Post",
                             ifelse(AIC_pre > AIC_base, "Base", "Tie")),
    stringsAsFactors = FALSE
  )
})

summary_table <- do.call(rbind, summ_rows)


##  create pub-ready table for SI Appendix
library(gt)

publication_table_gt <- summary_table %>%
  # Clean up variable names for display
  mutate(
    Variable = case_when(
      Variable == "Emotion_Neg" ~ "Negative Emotion",
      Variable == "Emotion_Pos" ~ "Positive Emotion", 
      TRUE ~ Variable
    ),
    # Format p-values
    LLR_p_formatted = case_when(
      is.na(LLR_p) ~ "—",
      LLR_p < 0.001 ~ "< .001",
      LLR_p < 0.01 ~ paste0("< .01"),
      TRUE ~ sprintf("%.3f", LLR_p)
    ),
    # Color code preferred model
    Preferred_Model = ifelse(Preferred_Model == "Pre/Post", 
                             paste0("**", Preferred_Model, "**"), 
                             Preferred_Model)
  ) %>%
  gt() %>%
  # Format columns
  fmt_number(
    columns = c(AIC_Base, AIC_PrePost, BIC_Base, BIC_PrePost, LLR),
    decimals = 2
  ) %>%
  # Add column labels
  cols_label(
    Variable = "Content Type",
    AIC_Base = "Base Model",
    AIC_PrePost = "Pre/Post Model", 
    BIC_Base = "Base Model",
    BIC_PrePost = "Pre/Post Model",
    LLR = "χ²",
    LLR_p_formatted = "p-value",
    Preferred_Model = "Preferred Model"
  ) %>%
  # Add column spanners
  tab_spanner(
    label = "AIC",
    columns = c(AIC_Base, AIC_PrePost)
  ) %>%
  tab_spanner(
    label = "BIC", 
    columns = c(BIC_Base, BIC_PrePost)
  ) %>%
  tab_spanner(
    label = "Likelihood Ratio Test",
    columns = c(LLR, LLR_p_formatted)
  ) %>%
  # Add title and subtitle
  tab_header(
    title = "Model Comparison: Base vs. Pre/Post Election Models",
    subtitle = "Lower AIC/BIC values indicate better model fit"
  ) %>%
  # Style the table
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_labels()
  ) %>%
  tab_style(
    style = cell_text(weight = "bold"),
    locations = cells_column_spanners()
  ) %>%
  tab_style(
    style = cell_fill(color = "lightgray"),
    locations = cells_column_spanners()
  ) %>%
  # Highlight preferred models
  tab_style(
    style = cell_text(weight = "bold", color = "darkblue"),
    locations = cells_body(
      columns = Preferred_Model,
      rows = Preferred_Model == "**Pre/Post**"
    )
  ) %>%
  # Remove p-value formatting column  
  cols_hide(columns = LLR_p) %>%
  # Add footnote
  tab_footnote(
    footnote = "Bold indicates statistically preferred model based on AIC / BIC",
    locations = cells_column_labels(columns = Preferred_Model)
  )

# Save the gt table as an image
gtsave(publication_table_gt, "model_comparison_table.png", 
       vwidth = 1000, vheight = 600)

# Print to console
publication_table_gt

ggsave()

# Print or pretty-print
knitr::kable(summary_table, digits = 3, align = "lrrrrrcc", caption = "Model comparison summary")






#### reporting of contrasts in text (pre/post) ----

# Fast emmeans (z-based; no Satterthwaite/KR)
emmeans::emm_options(lmerTest.limit = 0, pbkrtest.limit = 0)

# Your exact condition labels
LVL_RC <- "Reverse Chronological"
LVL_EB <- "Engagement-Based"
LVL_DE <- "Diversified Extremity"

# Three pairwise contrasts (names must match factor levels exactly)
contr_defs_all <- list(
  "EB vs RC"      = setNames(c(-1,  1,  0), c(LVL_RC, LVL_EB, LVL_DE)),
  "DE vs RC"      = setNames(c(-1,  0,  1), c(LVL_RC, LVL_EB, LVL_DE)),
  "EB vs DE"      = setNames(c( 0,  1, -1), c(LVL_RC, LVL_EB, LVL_DE))
)

# CI column standardizer (for emmeans summaries)
std_ci_cols <- function(df) {
  if (!("lower.CL" %in% names(df))) {
    if (all(c("asymp.LCL","asymp.UCL") %in% names(df))) {
      df <- dplyr::rename(df, lower.CL = asymp.LCL, upper.CL = asymp.UCL)
    } else {
      df <- dplyr::mutate(df,
                          lower.CL = estimate - qnorm(0.975) * SE,
                          upper.CL = estimate + qnorm(0.975) * SE
      )
    }
  }
  df
}

# Pretty p-values (Nature-style)
ptxt <- function(p) {
  if (is.na(p)) return("p = NA")
  if (p < 0.001) "p < .001" else paste0("p = ", formatC(p, digits = 3, format = "f"))
}

# ---- MAIN FUNCTION
# dv_baseline_col: column in baseline_prop (e.g., "prop_moral")
# model_prepost:   your fitted pre/post model (e.g., models_prop_moral$prepost)
# baseline_df:     baseline_prop (global prevalence TS; no condition)
# dv_label:        pretty label for printing
interpret_dv_prepost_global_onebase_allpairs <- function(dv_baseline_col,
                                                         model_prepost,
                                                         baseline_df,
                                                         dv_label = dv_baseline_col) {
  stopifnot(dv_baseline_col %in% names(baseline_df))
  # 1) Global baseline (pooled across time)
  global_mean <- mean(baseline_df[[dv_baseline_col]], na.rm = TRUE)
  
  # 2) emmeans: condition means in Pre (0) and Post (1); all pairwise contrasts
  emm <- emmeans(
    model_prepost,
    ~ condition | is_post_election,
    at = list(is_post_election = c(0, 1)),  # 0=Pre, 1=Post
    re.form = NA
  )
  contr <- contrast(emm, method = contr_defs_all, by = "is_post_election")
  contr_df <- summary(contr, infer = c(TRUE, TRUE), df = Inf) %>%
    as.data.frame() %>%
    std_ci_cols() %>%
    mutate(period = if_else(is_post_election == 0, "Pre", "Post")) %>%
    select(period, contrast, estimate, SE, lower.CL, upper.CL, p.value)
  
  # 3) Printer using single global baseline with explicit reference group
  fmt_row <- function(row, comparison_name) {
    # For "EB vs DE", flip the interpretation to "DE compared to EB"
    if (comparison_name == "EB vs DE") {
      abs_pp <- (-row$estimate) * 100  # Flip the sign
      rel_pct <- if (!is.na(global_mean) && global_mean > 0) ((-row$estimate) / global_mean) * 100 else NA_real_
      
      paste0(
        "DE",
        ifelse(abs_pp >= 0, " increased by ", " decreased by "),
        sprintf("%.2f", abs(abs_pp)), " percentage points compared to EB",
        if (!is.na(rel_pct)) paste0(" (a ", sprintf("%.1f", abs(rel_pct)),
                                    "% relative ", ifelse(rel_pct >= 0, "increase", "decrease"), ")") else "",
        ", ", ptxt(row$p.value)
      )
    } else {
      # Regular processing for other contrasts
      abs_pp  <- row$estimate * 100
      rel_pct <- if (!is.na(global_mean) && global_mean > 0) (row$estimate / global_mean) * 100 else NA_real_
      
      first_condition <- strsplit(comparison_name, " vs ")[[1]][1]
      second_condition <- strsplit(comparison_name, " vs ")[[1]][2]
      
      paste0(
        first_condition,
        ifelse(abs_pp >= 0, " increased by ", " decreased by "),
        sprintf("%.2f", abs(abs_pp)), " percentage points compared to ", second_condition,
        if (!is.na(rel_pct)) paste0(" (a ", sprintf("%.1f", abs(rel_pct)),
                                    "% relative ", ifelse(rel_pct >= 0, "increase", "decrease"), ")") else "",
        ", ", ptxt(row$p.value)
      )
    }
  }
  # Extract neatly for Pre and Post
  get_one <- function(contrast_name, period_name) {
    contr_df %>% filter(contrast == contrast_name, period == period_name) %>% slice(1)
  }
  
  EB_RC_pre   <- get_one("EB vs RC", "Pre")
  DE_RC_pre   <- get_one("DE vs RC", "Pre")
  EB_DE_pre   <- get_one("EB vs DE", "Pre")
  EB_RC_post  <- get_one("EB vs RC", "Post")
  DE_RC_post  <- get_one("DE vs RC", "Post")
  EB_DE_post  <- get_one("EB vs DE", "Post")
  
  # 4) Print
  cat("=== ", toupper(dv_label), " ===\n", sep = "")
  cat("Global baseline prevalence (pooled): ", sprintf("%.2f", global_mean*100), "%\n", sep = "")
  cat("Pre-election contrasts:\n")
  cat("  • EB vs RC: ", fmt_row(EB_RC_pre, "EB vs RC"),  "\n", sep = "")
  cat("  • DE vs RC: ", fmt_row(DE_RC_pre, "DE vs RC"),  "\n", sep = "")
  cat("  • EB vs DE: ", fmt_row(EB_DE_pre, "EB vs DE"),  "\n", sep = "")
  cat("Post-election contrasts:\n")
  cat("  • EB vs RC: ", fmt_row(EB_RC_post, "EB vs RC"), "\n", sep = "")
  cat("  • DE vs RC: ", fmt_row(DE_RC_post, "DE vs RC"), "\n", sep = "")
  cat("  • EB vs DE: ", fmt_row(EB_DE_post, "EB vs DE"), "\n\n", sep = "")
  
  # 5) Return tidy tibble invisibly (handy for tables)
  to_pct <- function(x) x * 100
  rel_pct <- function(est) ifelse(global_mean > 0, (est / global_mean) * 100, NA_real_)
  invisible(tibble::tibble(
    DV = dv_label,
    Global_Baseline = global_mean,
    # Pre
    EB_vs_RC_Pre_Est = EB_RC_pre$estimate, EB_vs_RC_Pre_pp = to_pct(EB_RC_pre$estimate),
    EB_vs_RC_Pre_rel = rel_pct(EB_RC_pre$estimate), EB_vs_RC_Pre_p = EB_RC_pre$p.value,
    DE_vs_RC_Pre_Est = DE_RC_pre$estimate, DE_vs_RC_Pre_pp = to_pct(DE_RC_pre$estimate),
    DE_vs_RC_Pre_rel = rel_pct(DE_RC_pre$estimate), DE_vs_RC_Pre_p = DE_RC_pre$p.value,
    EB_vs_DE_Pre_Est = EB_DE_pre$estimate, EB_vs_DE_Pre_pp = to_pct(EB_DE_pre$estimate),
    EB_vs_DE_Pre_rel = rel_pct(EB_DE_pre$estimate), EB_vs_DE_Pre_p = EB_DE_pre$p.value,
    # Post
    EB_vs_RC_Post_Est = EB_RC_post$estimate, EB_vs_RC_Post_pp = to_pct(EB_RC_post$estimate),
    EB_vs_RC_Post_rel = rel_pct(EB_RC_post$estimate), EB_vs_RC_Post_p = EB_RC_post$p.value,
    DE_vs_RC_Post_Est = DE_RC_post$estimate, DE_vs_RC_Post_pp = to_pct(DE_RC_post$estimate),
    DE_vs_RC_Post_rel = rel_pct(DE_RC_post$estimate), DE_vs_RC_Post_p = DE_RC_post$p.value,
    EB_vs_DE_Post_Est = EB_DE_post$estimate, EB_vs_DE_Post_pp = to_pct(EB_DE_post$estimate),
    EB_vs_DE_Post_rel = rel_pct(EB_DE_post$estimate), EB_vs_DE_Post_p = EB_DE_post$p.value
  ))
}

## call function

# Intergroup (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_intergroup", models_prop_intergroup$prepost, baseline_prop, "intergroup content"
)

# Moral (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_moral", models_prop_moral$prepost, baseline_prop, "moral content"
)

# Negative Emotional (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_is_valence_negative", models_prop_emotion_neg$prepost, baseline_prop, "negative emotional content"
)

# Positive Emotional (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_is_valence_positive", models_prop_emotion_pos$prepost, baseline_prop, "positive emotional content"
)

# Toxic (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_toxic", models_prop_toxic$prepost, baseline_prop, "toxic content"
)

# Political (pre-registered)
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_is_sociopolitical", models_prop_political$prepost, baseline_prop, "political content"
)

# Exploratory: Moral outrage
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_moral_outrage", models_prop_outrage$prepost, baseline_prop, "moral outrage content"
)

# Exploratory: Constructive
interpret_dv_prepost_global_onebase_allpairs(
  "feed_proportion_constructive", models_prop_constructive$prepost, baseline_prop, "constructive content"
)




#### model results summary (pre/post) ----

options(scipen = 999)

# Intergroup (pre-registered)
summary(models_prop_intergroup$prepost)

# Moral (pre-registered)
summary(models_prop_moral$prepost)

# Negative Emotional (pre-registered)
summary(models_prop_emotion_neg$prepost)

# Positive Emotional (pre-registered)
summary(models_prop_emotion_pos$prepost)

# Toxic (pre-registered)
summary(models_prop_toxic$prepost)

# Political (pre-registered)
summary(models_prop_political$prepost)

# Exploratory: Moral outrage
summary(models_prop_outrage$prepost)

# Exploratory: Constructive
summary(models_prop_constructive$prepost)

#### reporting of contrasts (baseline model) ----
# Fast emmeans (z-based; no Satterthwaite/KR)
emmeans::emm_options(lmerTest.limit = 0, pbkrtest.limit = 0)

# Ensure RC is the reference in your data (safe no-op if already true)
condition_feed <- condition_feed %>%
  mutate(condition = relevel(factor(condition), ref = "Reverse Chronological"))

# Exact display labels (must match factor levels in your models)
LVL_RC <- "Reverse Chronological"
LVL_EB <- "Engagement-Based"
LVL_DE <- "Diversified Extremity"

# Contrast definitions (order & names must match factor levels)
contr_defs_all <- list(
  "Engagement-Based vs Reverse Chronological" = setNames(c(-1,  1,  0), c(LVL_RC, LVL_EB, LVL_DE)),
  "Diversified Extremity vs Reverse Chronological" = setNames(c(-1,  0,  1), c(LVL_RC, LVL_EB, LVL_DE)),
  "Engagement-Based vs Diversified Extremity" = setNames(c( 0,  1, -1), c(LVL_RC, LVL_EB, LVL_DE))
)

# CI column standardizer (for emmeans summaries)
std_ci_cols <- function(df) {
  if (!("lower.CL" %in% names(df))) {
    if (all(c("asymp.LCL","asymp.UCL") %in% names(df))) {
      df <- dplyr::rename(df, lower.CL = asymp.LCL, upper.CL = asymp.UCL)
    } else {
      df <- dplyr::mutate(
        df,
        lower.CL = estimate - qnorm(0.975) * SE,
        upper.CL = estimate + qnorm(0.975) * SE
      )
    }
  }
  df
}

# Pretty p-values
ptxt <- function(p) {
  if (is.na(p)) return("p = NA")
  if (p < 0.001) "p < .001" else paste0("p = ", formatC(p, digits = 3, format = "f"))
}

# ============= BASE-MODEL INTERPRETER (no pre/post split)
# dv_baseline_col: column name in baseline_prop (e.g., "feed_proportion_moral")
# model_base     : your lmer base model (e.g., models_prop_moral$base)
# baseline_df    : baseline_prop (global prevalence; no condition)
# dv_label       : pretty name for printing
interpret_dv_base_global_allpairs <- function(dv_baseline_col,
                                              model_base,
                                              baseline_df,
                                              dv_label = dv_baseline_col) {
  stopifnot(dv_baseline_col %in% names(baseline_df))
  # 1) Global baseline (pooled)
  global_mean <- mean(baseline_df[[dv_baseline_col]], na.rm = TRUE)
  
  # 2) emmeans of condition (overall), with all pairwise contrasts
  emm <- emmeans(model_base, ~ condition, re.form = NA)
  contr <- contrast(emm, method = contr_defs_all)
  
  contr_df <- summary(contr, infer = c(TRUE, TRUE), df = Inf) %>%
    as.data.frame() %>%
    std_ci_cols() %>%
    select(contrast, estimate, SE, lower.CL, upper.CL, p.value)
  
  # Helper to print a row as text with percentage points + relative %
  fmt_row <- function(row, comparison_name) {
    # Parse left/right condition names from the long label "A vs B"
    parts <- strsplit(comparison_name, " vs ", fixed = TRUE)[[1]]
    cond_left  <- parts[1]
    cond_right <- parts[2]
    
    abs_pp  <- row$estimate * 100
    rel_pct <- if (!is.na(global_mean) && global_mean > 0) (row$estimate / global_mean) * 100 else NA_real_
    
    paste0(
      cond_left,
      ifelse(abs_pp >= 0, " increased by ", " decreased by "),
      sprintf("%.2f", abs(abs_pp)), " percentage points compared to ", cond_right,
      if (!is.na(rel_pct)) paste0(" (a ", sprintf("%.1f", abs(rel_pct)),
                                  "% relative ", ifelse(rel_pct >= 0, "increase", "decrease"), ")") else "",
      ", ", ptxt(row$p.value)
    )
  }
  
  # 3) Pull rows by label
  get_one <- function(name) contr_df %>% filter(contrast == name) %>% slice(1)
  
  EB_RC <- get_one("Engagement-Based vs Reverse Chronological")
  DE_RC <- get_one("Diversified Extremity vs Reverse Chronological")
  EB_DE <- get_one("Engagement-Based vs Diversified Extremity")
  
  # 4) Print
  cat("=== ", toupper(dv_label), " — BASE MODEL (overall) ===\n", sep = "")
  cat("Global baseline prevalence (pooled): ", sprintf("%.2f", global_mean*100), "%\n", sep = "")
  cat("Overall contrasts:\n")
  cat("  • ", fmt_row(EB_RC, "Engagement-Based vs Reverse Chronological"), "\n", sep = "")
  cat("  • ", fmt_row(DE_RC, "Diversified Extremity vs Reverse Chronological"), "\n", sep = "")
  cat("  • ", fmt_row(EB_DE, "Engagement-Based vs Diversified Extremity"), "\n\n", sep = "")
  
  # 5) Return tidy tibble invisibly
  to_pp <- function(x) x * 100
  rel_pct <- function(est) ifelse(global_mean > 0, (est / global_mean) * 100, NA_real_)
  invisible(tibble::tibble(
    DV = dv_label,
    Global_Baseline = global_mean,
    EB_vs_RC_Est  = EB_RC$estimate,  EB_vs_RC_pp  = to_pp(EB_RC$estimate),
    EB_vs_RC_rel  = rel_pct(EB_RC$estimate),      EB_vs_RC_p  = EB_RC$p.value,
    DE_vs_RC_Est  = DE_RC$estimate,  DE_vs_RC_pp  = to_pp(DE_RC$estimate),
    DE_vs_RC_rel  = rel_pct(DE_RC$estimate),      DE_vs_RC_p  = DE_RC$p.value,
    EB_vs_DE_Est  = EB_DE$estimate,  EB_vs_DE_pp  = to_pp(EB_DE$estimate),
    EB_vs_DE_rel  = rel_pct(EB_DE$estimate),      EB_vs_DE_p  = EB_DE$p.value
  ))
}

# =
# EXAMPLE CALLS — BASE MODELS
# (You already have these fitted via fit_base_prepost_models; use $base)
# =

# Pre-registered
interpret_dv_base_global_allpairs(
  "feed_proportion_intergroup", models_prop_intergroup$base, baseline_prop, "intergroup content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_moral", models_prop_moral$base, baseline_prop, "moral content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_is_valence_negative", models_prop_emotion_neg$base, baseline_prop, "negative emotional content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_is_valence_positive", models_prop_emotion_pos$base, baseline_prop, "positive emotional content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_toxic", models_prop_toxic$base, baseline_prop, "toxic content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_is_sociopolitical", models_prop_political$base, baseline_prop, "political content"
)

# Exploratory
interpret_dv_base_global_allpairs(
  "feed_proportion_moral_outrage", models_prop_outrage$base, baseline_prop, "moral outrage content"
)
interpret_dv_base_global_allpairs(
  "feed_proportion_constructive", models_prop_constructive$base, baseline_prop, "constructive content"
)











#### Plot all pre post variables into one (UPDATED) ----
# Fast emmeans (z-based; no Satterthwaite/KR)
emmeans::emm_options(lmerTest.limit = 0, pbkrtest.limit = 0)

# --- Define the two contrasts using fully spelled labels ---
LVL_RC <- "Reverse Chronological"
LVL_EB <- "Engagement-Based"
LVL_DE <- "Diversified Extremity"

contr_defs <- list(
  "Engagement-Based vs Reverse Chronological" = setNames(c(-1,  1,  0), c(LVL_RC, LVL_EB, LVL_DE)),
  "Diversified Extremity vs Reverse Chronological" = setNames(c(-1,  0,  1), c(LVL_RC, LVL_EB, LVL_DE))
)

# --- CI standardizer for emmeans outputs
std_ci_cols <- function(df) {
  if (!("lower.CL" %in% names(df))) {
    if (all(c("asymp.LCL","asymp.UCL") %in% names(df))) {
      df <- dplyr::rename(df, lower.CL = asymp.LCL, upper.CL = asymp.UCL)
    } else {
      df <- dplyr::mutate(
        df,
        lower.CL = estimate - qnorm(0.975) * SE,
        upper.CL = estimate + qnorm(0.975) * SE
      )
    }
  }
  df
}

# --- Helper: get pre/post contrasts from a single pre/post model
get_contrasts_for_model <- function(model_obj, var_label) {
  emm <- emmeans(
    model_obj,
    ~ condition | is_post_election,
    at = list(is_post_election = c(0, 1)),  # 0=Pre, 1=Post
    re.form = NA
  )
  contrast(emm, method = contr_defs, by = "is_post_election") |>
    summary(infer = c(TRUE, TRUE), df = Inf) |>
    as.data.frame() |>
    std_ci_cols() |>
    mutate(
      Variable = var_label,
      period   = factor(if_else(is_post_election == 0, "Pre", "Post"), levels = c("Pre","Post")),
      comp     = factor(contrast, levels = names(contr_defs))
    ) |>
    select(Variable, period, comp, estimate, SE, lower.CL, upper.CL, p.value)
}

# --- Collect the pre/post models you want to visualize (UPDATED OBJECTS)
models_prepost <- list(
  "Intergroup"         = models_prop_intergroup$prepost,
  "Moral"              = models_prop_moral$prepost,
  "Negative Emotional" = models_prop_emotion_neg$prepost,
  "Positive Emotional" = models_prop_emotion_pos$prepost,
  "Toxic"              = models_prop_toxic$prepost,
  "Political"          = models_prop_political$prepost,
  "Moral Outrage"      = models_prop_outrage$prepost,
  "Constructive"       = models_prop_constructive$prepost
)

# --- Build one tidy data frame of all contrasts
contr_all <- purrr::imap_dfr(models_prepost, ~ get_contrasts_for_model(.x, .y))

# --- Facet order (UPDATED to include split emotional and exploratory)
requested_order <- c("Intergroup", "Moral", "Negative Emotional", "Positive Emotional", "Toxic", "Political", "Moral Outrage", "Constructive")
existing <- levels(factor(contr_all$Variable))
facet_levels <- unique(c(requested_order, setdiff(existing, requested_order)))

contr_all <- contr_all %>%
  mutate(
    Variable = factor(Variable, levels = facet_levels),
    comp     = factor(comp, levels = names(contr_defs)),
    period   = factor(period, levels = c("Pre","Post"))
  )

# --- Shared y-limits including zero (optional, consistent across facets)
ylims <- {
  rng <- range(c(contr_all$lower.CL, contr_all$upper.CL, 0), na.rm = TRUE)
  if (diff(rng) == 0) rng <- rng + c(-0.01, 0.01)
  pad <- 0.06 * diff(rng)
  c(rng[1] - pad, rng[2] + pad)
}

# --- Color-blind–safe (Okabe–Ito; not blue/orange)
pal_contrasts <- c(
  "Engagement-Based vs Reverse Chronological"      = "#D55E00", # Vermillion
  "Diversified Extremity vs Reverse Chronological" = "#009E73"  # Bluish Green
)

# --- Position dodge to separate comparisons within each period
pd <- position_dodge(width = 0.48)

# --- Plot
plot_prepost_facets <- ggplot(
  contr_all,
  aes(x = period, y = estimate, color = comp)
) +
  # Emphasized zero line across all panels (bold but not too thick)
  geom_hline(yintercept = 0, linetype = "dotted", linewidth = 0.7, color = "black") +
  # 95% CI bars and points for each comparison
  geom_errorbar(
    aes(ymin = lower.CL, ymax = upper.CL),
    width = 0.16, linewidth = 0.9, position = pd
  ) +
  geom_point(size = 2.5, stroke = 0.4, position = pd) +
  # Scales & labels
  scale_color_manual(
    name = NULL,
    values = pal_contrasts,
    labels = names(pal_contrasts)
  ) +
  scale_x_discrete(labels = c("Pre-election", "Post-election")) +
  scale_y_continuous(
    name = "Marginal effect on proportion of posts",
    limits = ylims,
    expand = expansion(mult = c(0.04, 0.06)),
    breaks = scales::pretty_breaks(n = 6),
    labels = scales::label_number(accuracy = 0.01)
  ) +
  labs(
    x = NULL,
    title = "Pre-/Post-Election Marginal Effects by Outcome",
    subtitle = "Estimated contrasts vs reverse-chronological feed with 95% CIs"
  ) +
  facet_wrap(~ Variable, ncol = 2, scales = "fixed") +
  theme_classic(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 13, margin = margin(b = 4)),
    plot.subtitle      = element_text(size = 10.5, margin = margin(b = 8)),
    strip.text         = element_text(face = "bold", size = 11, margin = margin(t = 6, b = 6)),
    axis.title.y       = element_text(size = 11, margin = margin(r = 8)),
    axis.text.x        = element_text(size = 10.5, margin = margin(t = 2)),
    axis.text.y        = element_text(size = 10.5),
    legend.position    = "bottom",
    legend.direction   = "horizontal",
    legend.text        = element_text(size = 10),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank(),
    plot.margin        = margin(6, 10, 6, 10)
  ) +
  guides(color = guide_legend(override.aes = list(linewidth = 1.0, size = 3.0), nrow = 1))

# Draw (and optionally save)
print(plot_prepost_facets)
ggsave("prepost_marginal_effects_faceted_2col.png", plot_prepost_facets, width = 7.5, height = 8.5, dpi = 600)




















#### Plot all pre post variables into one (UPDATED) ----
# Fast emmeans (z-based; no Satterthwaite/KR)
emmeans::emm_options(lmerTest.limit = 0, pbkrtest.limit = 0)

# --- Define the two contrasts using fully spelled labels ---
LVL_RC <- "Reverse Chronological"
LVL_EB <- "Engagement-Based"
LVL_DE <- "Diversified Extremity"

contr_defs <- list(
  "Engagement-Based vs Reverse Chronological" = setNames(c(-1,  1,  0), c(LVL_RC, LVL_EB, LVL_DE)),
  "Diversified Extremity vs Reverse Chronological" = setNames(c(-1,  0,  1), c(LVL_RC, LVL_EB, LVL_DE))
)

# --- CI standardizer for emmeans outputs
std_ci_cols <- function(df) {
  if (!("lower.CL" %in% names(df))) {
    if (all(c("asymp.LCL","asymp.UCL") %in% names(df))) {
      df <- dplyr::rename(df, lower.CL = asymp.LCL, upper.CL = asymp.UCL)
    } else {
      df <- dplyr::mutate(
        df,
        lower.CL = estimate - qnorm(0.975) * SE,
        upper.CL = estimate + qnorm(0.975) * SE
      )
    }
  }
  df
}

# --- Helper: get pre/post contrasts from a single pre/post model
get_contrasts_for_model <- function(model_obj, var_label) {
  emm <- emmeans(
    model_obj,
    ~ condition | is_post_election,
    at = list(is_post_election = c(0, 1)),  # 0=Pre, 1=Post
    re.form = NA
  )
  contrast(emm, method = contr_defs, by = "is_post_election") |>
    summary(infer = c(TRUE, TRUE), df = Inf) |>
    as.data.frame() |>
    std_ci_cols() |>
    mutate(
      Variable = var_label,
      period   = factor(if_else(is_post_election == 0, "Pre", "Post"), levels = c("Pre","Post")),
      comp     = factor(contrast, levels = names(contr_defs))
    ) |>
    select(Variable, period, comp, estimate, SE, lower.CL, upper.CL, p.value)
}

# --- Collect the pre/post models you want to visualize (UPDATED OBJECTS)
models_prepost <- list(
  "Intergroup"         = models_prop_intergroup$prepost,
  "Moral"              = models_prop_moral$prepost,
  "Negative Emotional" = models_prop_emotion_neg$prepost,
  "Positive Emotional" = models_prop_emotion_pos$prepost,
  "Toxic"              = models_prop_toxic$prepost,
  "Political"          = models_prop_political$prepost,
  "Moral Outrage"      = models_prop_outrage$prepost,
  "Constructive"       = models_prop_constructive$prepost
)

# --- Build one tidy data frame of all contrasts
contr_all <- purrr::imap_dfr(models_prepost, ~ get_contrasts_for_model(.x, .y))

# --- Facet order (UPDATED to include split emotional and exploratory)
requested_order <- c("Intergroup", "Moral", "Negative Emotional", "Positive Emotional", "Toxic", "Political", "Moral Outrage", "Constructive")
existing <- levels(factor(contr_all$Variable))
facet_levels <- unique(c(requested_order, setdiff(existing, requested_order)))

contr_all <- contr_all %>%
  mutate(
    Variable = factor(Variable, levels = facet_levels),
    comp     = factor(comp, levels = names(contr_defs)),
    period   = factor(period, levels = c("Pre","Post"))
  )

# --- Shared y-limits including zero (optional, consistent across facets)
ylims <- {
  rng <- range(c(contr_all$lower.CL, contr_all$upper.CL, 0), na.rm = TRUE)
  if (diff(rng) == 0) rng <- rng + c(-0.01, 0.01)
  pad <- 0.06 * diff(rng)
  c(rng[1] - pad, rng[2] + pad)
}

# --- Color-blind–safe (Okabe–Ito; not blue/orange)
pal_contrasts <- c(
  "Engagement-Based vs Reverse Chronological"      = "#D55E00", # Vermillion
  "Diversified Extremity vs Reverse Chronological" = "#009E73"  # Bluish Green
)

# --- Position dodge to separate comparisons within each period
pd <- position_dodge(width = 0.48)

# --- Plot
plot_prepost_facets <- ggplot(
  contr_all,
  aes(x = period, y = estimate, color = comp)
) +
  # Emphasized zero line across all panels (bold but not too thick)
  geom_hline(yintercept = 0, linetype = "dotted", linewidth = 0.7, color = "black") +
  # 95% CI bars and points for each comparison
  geom_errorbar(
    aes(ymin = lower.CL, ymax = upper.CL),
    width = 0.16, linewidth = 0.9, position = pd
  ) +
  geom_point(size = 2.5, stroke = 0.4, position = pd) +
  # Scales & labels
  scale_color_manual(
    name = NULL,
    values = pal_contrasts,
    labels = names(pal_contrasts)
  ) +
  scale_x_discrete(labels = c("Pre-election", "Post-election")) +
  scale_y_continuous(
    name = "Marginal effect on proportion of posts",
    limits = ylims,
    expand = expansion(mult = c(0.04, 0.06)),
    breaks = scales::pretty_breaks(n = 6),
    labels = scales::label_number(accuracy = 0.01)
  ) +
  labs(
    x = NULL,
    title = "Pre-/Post-Election Marginal Effects by Outcome",
    subtitle = "Estimated contrasts vs reverse-chronological feed with 95% CIs"
  ) +
  facet_wrap(~ Variable, ncol = 2, scales = "fixed") +
  theme_classic(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 13, margin = margin(b = 4)),
    plot.subtitle      = element_text(size = 10.5, margin = margin(b = 8)),
    strip.text         = element_text(face = "bold", size = 11, margin = margin(t = 6, b = 6)),
    axis.title.y       = element_text(size = 11, margin = margin(r = 8)),
    axis.text.x        = element_text(size = 10.5, margin = margin(t = 2)),
    axis.text.y        = element_text(size = 10.5),
    legend.position    = "bottom",
    legend.direction   = "horizontal",
    legend.text        = element_text(size = 10),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank(),
    plot.margin        = margin(6, 10, 6, 10)
  ) +
  guides(color = guide_legend(override.aes = list(linewidth = 1.0, size = 3.0), nrow = 1))

# Draw (and optionally save)
print(plot_prepost_facets)
ggsave("prepost_marginal_effects_faceted_2col.png", plot_prepost_facets, width = 7.5, height = 8.5, dpi = 600)



#### Correlation Matrix Analysis (Updated Vars) ----

library(ggcorrplot)
# Columns to use (deduplicated, ordered)
var_cols <- c(
  "feed_proportion_intergroup",
  "feed_proportion_moral",
  "feed_proportion_is_sociopolitical",
  "feed_proportion_constructive",
  "feed_proportion_moral_outrage",
  "feed_proportion_is_valence_negative",
  "feed_proportion_is_valence_positive"
)

# Pretty labels (same length/order as var_cols)
var_labels <- c(
  "Intergroup",
  "Moral",
  "Sociopolitical",
  "Constructive",
  "Outrage",
  "Emotion: Negative",
  "Emotion: Positive"
)

# Helper to assert required columns exist
check_required_cols <- function(df, cols, df_name){
  missing <- setdiff(cols, names(df))
  if(length(missing)){
    stop(sprintf("Missing columns in %s: %s", df_name, paste(missing, collapse = ", ")))
  }
}

# ========= By-Condition (condition_feed)
check_required_cols(condition_feed, c("condition", var_cols), "condition_feed")

conditions <- unique(condition_feed$condition)

for (cond in conditions) {
  # Filter & select
  cond_data <- condition_feed %>%
    filter(condition == cond) %>%
    select(all_of(var_cols))
  
  # Drop rows where *all* selected vars are NA
  cond_data <- cond_data[rowSums(is.na(cond_data)) < ncol(cond_data), , drop = FALSE]
  if (nrow(cond_data) < 3) {
    message(sprintf("Skipping '%s' (insufficient rows after NA removal).", cond))
    next
  }
  
  # Correlation matrix
  cor_matrix <- cor(cond_data, method = "pearson", use = "pairwise.complete.obs")
  rownames(cor_matrix) <- var_labels
  colnames(cor_matrix) <- var_labels
  
  # Plot
  p <- ggcorrplot(
    cor_matrix,
    hc.order = TRUE,
    type = "upper",
    lab = TRUE,
    lab_size = 3,
    colors = c("red3", "white", "blue3"),
    title = paste("Correlation Matrix:", cond)
  ) +
    theme(plot.title = element_text(hjust = 0.5))
  
  # Save
  filename <- paste0(
    "correlation_matrix_", 
    str_replace_all(tolower(cond), "[^a-z0-9]+", "_"),
    ".png"
  )
  ggsave(filename, p, dpi = 300, width = 10, height = 8, bg = "white")
}

# ========= Baseline (baseline_prop)
check_required_cols(baseline_prop, var_cols, "baseline_prop")

base_data <- baseline_prop %>%
  select(all_of(var_cols))

# Drop rows where *all* selected vars are NA
base_data <- base_data[rowSums(is.na(base_data)) < ncol(base_data), , drop = FALSE]
if (nrow(base_data) >= 3) {
  cor_baseline <- cor(base_data, method = "pearson", use = "pairwise.complete.obs")
  rownames(cor_baseline) <- var_labels
  colnames(cor_baseline) <- var_labels
  
  p_baseline <- ggcorrplot(
    cor_baseline,
    hc.order = TRUE,
    type = "upper",
    lab = TRUE,
    lab_size = 3,
    colors = c("red3", "white", "blue3"),
    title = "Correlation Matrix: Baseline (Feed Proportions)"
  ) +
    theme(plot.title = element_text(hjust = 0.5))
  
  ggsave("correlation_matrix_baseline.png", p_baseline, dpi = 300, width = 10, height = 8, bg = "white")
} else {
  message("Baseline skipped (insufficient rows after NA removal).")
}


#### Descriptive tables for each condition ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
#### Create descriptive statistics table with kable ----
# Identify proportion variables
proportion_vars <- c("feed_proportion_intergroup", "feed_proportion_moral", 
                     "feed_proportion_is_sociopolitical", "feed_proportion_toxic", 
                     "feed_proportion_constructive", "feed_proportion_moral_outrage",
                     "feed_proportion_is_valence_negative", "feed_proportion_is_valence_positive")

# Function to calculate descriptive statistics
calc_descriptives <- function(data, group_var) {
  data %>%
    select(all_of(c(group_var, proportion_vars))) %>%
    pivot_longer(cols = all_of(proportion_vars), 
                 names_to = "variable", 
                 values_to = "value") %>%
    group_by(!!sym(group_var), variable) %>%
    summarise(
      n = sum(!is.na(value)),
      mean = mean(value, na.rm = TRUE),
      sd = sd(value, na.rm = TRUE),
      median = median(value, na.rm = TRUE),
      .groups = "drop"
    )
}

# Calculate descriptives for condition feed
condition_descriptives <- calc_descriptives(condition_feed, "condition")

# Calculate descriptives for baseline
baseline_descriptives <- calc_descriptives(baseline_prop, "condition")

# Combine the datasets
combined_descriptives <- bind_rows(condition_descriptives, baseline_descriptives)

# Create clean variable names mapping
variable_labels <- c(
  "feed_proportion_intergroup" = "Intergroup Content",
  "feed_proportion_moral" = "Moral Content", 
  "feed_proportion_is_valence_negative" = "Negative Valence Content",
  "feed_proportion_is_valence_positive" = "Positive Valence Content",
  "feed_proportion_toxic" = "Toxic Content",
  "feed_proportion_is_sociopolitical" = "Sociopolitical Content",
  "feed_proportion_moral_outrage" = "Moral Outrage Content",
  "feed_proportion_constructive" = "Constructive Content"
)

# Define the desired order for rows
row_order <- c(
  "Intergroup Content",
  "Moral Content", 
  "Negative Valence Content",
  "Positive Valence Content",
  "Toxic Content",
  "Sociopolitical Content",
  "Moral Outrage Content",
  "Constructive Content"
)

# Format the table for publication
descriptive_table <- combined_descriptives %>%
  mutate(
    variable = recode(variable, !!!variable_labels),
    # Format statistics to APA style
    mean_sd = paste0(sprintf("%.3f", mean), " (", sprintf("%.3f", sd), ")"),
    median_fmt = sprintf("%.3f", median)
  ) %>%
  # Reorder conditions
  mutate(condition = factor(condition, levels = c("Reverse Chronological", 
                                                  "Diversified Extremity", 
                                                  "Engagement-Based", 
                                                  "Baseline Firehose"))) %>%
  # Apply custom row ordering
  mutate(variable = factor(variable, levels = row_order)) %>%
  arrange(variable, condition) %>%
  select(variable, condition, n, mean_sd, median_fmt)

# Create the table with content types as rows, conditions as columns
table_for_kable <- combined_descriptives %>%
  mutate(
    variable = recode(variable, !!!variable_labels),
    mean_sd = sprintf("%.3f (%.3f)", mean, sd),
    median_fmt = sprintf("%.3f", median)
  ) %>%
  mutate(condition = factor(condition, levels = c("Reverse Chronological", 
                                                  "Diversified Extremity", 
                                                  "Engagement-Based", 
                                                  "Baseline Firehose"))) %>%
  # Apply custom row ordering
  mutate(variable = factor(variable, levels = row_order)) %>%
  arrange(variable, condition) %>%
  select(variable, condition, mean_sd, median_fmt) %>%
  pivot_wider(names_from = condition, 
              values_from = c(mean_sd, median_fmt),
              names_glue = "{condition}_{.value}") %>%
  # Reorder columns: each condition gets M(SD) then Median
  select(variable,
         `Reverse Chronological_mean_sd`, `Reverse Chronological_median_fmt`,
         `Diversified Extremity_mean_sd`, `Diversified Extremity_median_fmt`, 
         `Engagement-Based_mean_sd`, `Engagement-Based_median_fmt`,
         `Baseline Firehose_mean_sd`, `Baseline Firehose_median_fmt`)

# Create the publication-ready kable table
descriptive_kable <- table_for_kable %>%
  kable(format = "html",
        col.names = c("Content Type",
                      rep(c("M (SD)", "Median"), 4)),
        caption = "Descriptive Statistics for Content Proportions by Algorithmic Condition",
        escape = FALSE,
        align = c("l", rep("c", 8))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed"),
                full_width = FALSE,
                font_size = 11) %>%
  add_header_above(c(" " = 1, 
                     "Reverse Chronological" = 2, 
                     "Diversified Extremity" = 2, 
                     "Engagement-Based" = 2, 
                     "Baseline Firehose" = 2)) %>%
  column_spec(1, bold = TRUE, width = "3cm") %>%
  column_spec(2:9, width = "2cm") %>%
  footnote(general = "Note. Proportions range from 0 to 1. M = Mean; SD = Standard Deviation.",
           general_title = "",
           footnote_as_chunk = TRUE)

# Display the table
print(descriptive_kable)

# Save high-resolution HTML table
save_kable(descriptive_kable, 
           file = "descriptive_statistics_table.html", 
           zoom = 3,  # Increases resolution
           vwidth = 1200, vheight = 800)  # Set viewport dimensions

# Alternative: Save as PNG for high-resolution image
library(webshot2)
save_kable(descriptive_kable, 
           file = "descriptive_statistics_table.png", 
           zoom = 3,  # 3x resolution
           vwidth = 1200, vheight = 800)

# Alternative: Save as PDF (vector format, infinitely scalable)
save_kable(descriptive_kable, 
           file = "descriptive_statistics_table.pdf", 
           zoom = 2,  # PDF doesn't need as high zoom
           vwidth = 1200, vheight = 800)



#### (FOR SUPP) Pre/Post Election Spline Models for Moral Content Proportions ----

# Election date
election_date <- as.Date("2024-11-05")

# Create pre/post election variables in proportion data
condition_prop <- condition_prop %>%
  mutate(
    is_post_election = as.numeric(date > election_date),
    period = factor(if_else(date > election_date, "Post", "Pre"), levels = c("Pre", "Post")),
    day_from_election = as.numeric(date - election_date)
  )

# Function to compare base model vs spline models for proportions
compare_proportion_spline_models <- function(dv, data = condition_prop) {
  
  cat(paste("Comparing models for", dv, "...\n"))
  
  # Create enhanced dataset with all spline variables upfront
  spline_basis <- ns(data$day_from_election, knots = 0, df = 3)
  
  data_enhanced <- data %>%
    mutate(
      pre_election_slope = pmin(day_from_election, 0),   # negative values before election
      post_election_slope = pmax(day_from_election, 0)   # positive values after election
    ) %>%
    bind_cols(as.data.frame(spline_basis))
  
  # Dynamically rename spline columns based on actual number created
  spline_cols <- ncol(spline_basis)
  spline_names <- paste0("spline", 1:spline_cols)
  colnames(data_enhanced)[(ncol(data_enhanced) - spline_cols + 1):ncol(data_enhanced)] <- spline_names
  
  # Fit all models to the same enhanced dataset
  # MODEL 1: Base model (no time component)
  base_formula <- as.formula(paste(dv, "~ condition + (1 | bluesky_handle)"))
  model_base <- lmer(base_formula, data = data_enhanced)
  
  # MODEL 2: Simple pre/post election
  prepost_formula <- as.formula(paste(dv, "~ condition * is_post_election + (1 | bluesky_handle)"))
  model_prepost <- lmer(prepost_formula, data = data_enhanced)
  
  # MODEL 3: Piecewise linear spline (separate slopes before/after election)
  piecewise_formula <- as.formula(paste(dv, "~ condition * (pre_election_slope + post_election_slope) + (1 | bluesky_handle)"))
  model_piecewise <- lmer(piecewise_formula, data = data_enhanced)
  
  # MODEL 4: Natural spline with election knot
  spline_terms <- paste(spline_names, collapse = " + ")
  natural_formula <- as.formula(paste(dv, "~ condition * (", spline_terms, ") + (1 | bluesky_handle)"))
  model_natural <- lmer(natural_formula, data = data_enhanced)
  
  # Model comparison
  comparison <- data.frame(
    Model = c("Base (No Time)", "Pre/Post Election", "Piecewise Linear", "Natural Spline"),
    AIC = c(AIC(model_base), AIC(model_prepost), AIC(model_piecewise), AIC(model_natural)),
    BIC = c(BIC(model_base), BIC(model_prepost), BIC(model_piecewise), BIC(model_natural)),
    LogLik = c(as.numeric(logLik(model_base)), as.numeric(logLik(model_prepost)), 
               as.numeric(logLik(model_piecewise)), as.numeric(logLik(model_natural))),
    df = c(attr(logLik(model_base), "df"), attr(logLik(model_prepost), "df"),
           attr(logLik(model_piecewise), "df"), attr(logLik(model_natural), "df"))
  ) %>%
    mutate(
      delta_AIC = AIC - min(AIC),
      delta_BIC = BIC - min(BIC),
      best_AIC = (delta_AIC == 0),
      best_BIC = (delta_BIC == 0),
      AIC_winner = ifelse(best_AIC, "***", ""),
      BIC_winner = ifelse(best_BIC, "***", "")
    )
  
  # Likelihood ratio tests comparing to base model
  lr_prepost <- anova(model_base, model_prepost)
  lr_piecewise <- anova(model_base, model_piecewise) 
  lr_natural <- anova(model_base, model_natural)
  
  # Results list
  results <- list(
    models = list(
      base = model_base,
      prepost = model_prepost,
      piecewise = model_piecewise,
      natural = model_natural
    ),
    comparison = comparison,
    lr_tests = list(
      prepost_vs_base = lr_prepost,
      piecewise_vs_base = lr_piecewise,
      natural_vs_base = lr_natural
    ),
    data_enhanced = data_enhanced
  )
  
  # Print results
  cat(paste("\n=== MODEL COMPARISON RESULTS FOR", toupper(dv), "===\n"))
  print(comparison)
  
  best_model_name <- comparison$Model[which.min(comparison$AIC)]
  cat(paste("\nBest model by AIC:", best_model_name, "\n"))
  
  cat("\n=== LIKELIHOOD RATIO TESTS vs BASE MODEL ===\n")
  cat("Pre/Post vs Base:\n")
  print(lr_prepost)
  cat("\nPiecewise vs Base:\n") 
  print(lr_piecewise)
  cat("\nNatural Spline vs Base:\n")
  print(lr_natural)
  
  return(results)
}



# Run spline model comparison for moral content proportions
cat("=== PROPORTION MORAL POSTS MODEL COMPARISON ===\n")
prop_moral_results <- compare_proportion_spline_models("prop_moral_posts")

# Choose your model based on the comparison results:
summary(prop_moral_results$models$piecewise)

# For pre/post model (simpler):  
# summary(prop_moral_results$models$prepost)

# For natural spline model:
# summary(prop_moral_results$models$natural)

# For base model:
# summary(prop_moral_results$models$base)












#### (FOR SUPP) testing data driven polynomial ----
#### feed data analysis stats: test data-driven polynomial terms (up to cubic) ----
# Function to fit polynomial models of different orders (up to cubic)
#### feed data analysis stats: test data-driven polynomial terms (up to cubic) ----
# Function to fit polynomial models of different orders (up to cubic)
fit_polynomial_models <- function(dv, max_order = 3) {
  
  # Storage for models and fit statistics
  models <- list()
  fit_stats <- data.frame(
    order = 0:max_order,
    model_type = c("Base (time interaction)", "Linear", "Quadratic", "Cubic"),
    AIC = NA,
    BIC = NA,
    logLik = NA,
    deviance = NA,
    df_resid = NA
  )
  
  # Fit models of increasing polynomial order
  for (i in 0:max_order) {
    
    cat("Fitting", fit_stats$model_type[i + 1], "model for", dv, "...\n")
    
    # Create formula based on model type
    if (i == 0) {
      # Base model: condition * day (time interaction but no polynomial)
      formula_text <- paste(dv, "~ condition * day + (1 | handle)")
    } else {
      # Polynomial model of order i
      formula_text <- paste(dv, "~ condition * poly(day,", i, ") + (1 | handle)")
    }
    
    formula_obj <- as.formula(formula_text)
    
    # Fit model
    model <- lmer(formula_obj, data = condition_feed)
    
    # Store model
    models[[paste0("order_", i)]] <- model
    
    # Extract fit statistics
    fit_stats$AIC[i + 1] <- AIC(model)
    fit_stats$BIC[i + 1] <- BIC(model)
    fit_stats$logLik[i + 1] <- as.numeric(logLik(model))
    fit_stats$deviance[i + 1] <- deviance(model)
    fit_stats$df_resid[i + 1] <- df.residual(model)
  }
  
  # Add delta AIC and BIC (relative to best model)
  fit_stats <- fit_stats %>%
    mutate(
      delta_AIC = AIC - min(AIC),
      delta_BIC = BIC - min(BIC),
      best_AIC = (delta_AIC == 0),
      best_BIC = (delta_BIC == 0)
    )
  
  return(list(models = models, fit_stats = fit_stats))
}

# Pre-registered DVs
cat("=== PRE-REGISTERED MODELS ===\n")
models_poly_intergroup <- fit_polynomial_models("feed_proportion_intergroup")
models_poly_moral <- fit_polynomial_models("feed_proportion_moral")
models_poly_emotion_neg <- fit_polynomial_models("feed_proportion_is_valence_negative")
models_poly_emotion_pos <- fit_polynomial_models("feed_proportion_is_valence_positive")
models_poly_toxic <- fit_polynomial_models("feed_proportion_toxic")
models_poly_political <- fit_polynomial_models("feed_proportion_is_sociopolitical")

# Exploratory DVs
cat("\n=== EXPLORATORY MODELS ===\n")
models_poly_outrage <- fit_polynomial_models("feed_proportion_moral_outrage")
models_poly_constructive <- fit_polynomial_models("feed_proportion_constructive")

# Function to create summary table across all DVs
create_polynomial_summary <- function() {
  
  # List of all model results
  all_models <- list(
    "Intergroup" = models_poly_intergroup,
    "Moral" = models_poly_moral,
    "Negative Emotion" = models_poly_emotion_neg,
    "Positive Emotion" = models_poly_emotion_pos,
    "Toxic" = models_poly_toxic,
    "Political" = models_poly_political,
    "Outrage" = models_poly_outrage,
    "Constructive" = models_poly_constructive
  )
  
  # Create summary table
  summary_table <- map_dfr(names(all_models), function(dv_name) {
    fit_stats <- all_models[[dv_name]]$fit_stats
    
    # Find best models
    best_aic <- fit_stats$model_type[which.min(fit_stats$AIC)]
    best_bic <- fit_stats$model_type[which.min(fit_stats$BIC)]
    
    # Get delta values for each model
    deltas <- fit_stats %>%
      select(model_type, delta_AIC, delta_BIC) %>%
      pivot_wider(names_from = model_type, 
                  values_from = c(delta_AIC, delta_BIC),
                  names_sep = "_")
    
    tibble(
      DV = dv_name,
      Best_AIC = best_aic,
      Best_BIC = best_bic,
      .before = 1
    ) %>%
      bind_cols(deltas)
  })
  
  return(summary_table)
}

# Create and display summary
polynomial_summary <- create_polynomial_summary()
cat("\n=== POLYNOMIAL MODEL SELECTION SUMMARY ===\n")
print(polynomial_summary)

# Function to create comparison plot for a specific DV
plot_polynomial_comparison <- function(poly_results, dv_name) {
  poly_results$fit_stats %>%
    select(order, model_type, AIC, BIC) %>%
    pivot_longer(cols = c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
    ggplot(aes(x = factor(order), y = value, color = criterion, group = criterion)) +
    geom_line(size = 1) +
    geom_point(size = 3) +
    scale_x_discrete(labels = c("Base\n(time interaction)", "Linear", "Quadratic", "Cubic")) +
    scale_color_manual(values = c("AIC" = "#E69F00", "BIC" = "#0072B2")) +
    labs(
      title = paste("Polynomial Model Comparison:", dv_name),
      subtitle = "Lower values indicate better fit",
      x = "Model Type",
      y = "Information Criterion Value",
      color = "Criterion"
    ) +
    theme_classic() +
    theme(
      text = element_text(size = 12),
      axis.text.x = element_text(angle = 0, hjust = 0.5),
      legend.position = "bottom"
    )
}

# Example: Create plot for intergroup content
intergroup_plot <- plot_polynomial_comparison(models_poly_intergroup, "Intergroup Content")
print(intergroup_plot)
ggsave("polynomial_comparison_intergroup.png", intergroup_plot, 
       dpi = 300, width = 10, height = 6)


# Also create a more detailed summary with delta values
detailed_summary <- polynomial_summary %>%
  select(DV, Best_AIC, Best_BIC, 
         `delta_AIC_Base (time interaction)`, delta_AIC_Linear, delta_AIC_Quadratic, delta_AIC_Cubic,
         `delta_BIC_Base (time interaction)`, delta_BIC_Linear, delta_BIC_Quadratic, delta_BIC_Cubic) %>%
  rename_with(~str_replace(.x, "delta_AIC_", "AIC_Δ_"), starts_with("delta_AIC")) %>%
  rename_with(~str_replace(.x, "delta_BIC_", "BIC_Δ_"), starts_with("delta_BIC"))

# Create a detailed formatted table with delta values
detailed_formatted_summary <- detailed_summary %>%
  kable(format = "html", 
        col.names = c("Dependent Variable", "Best Model (AIC)", "Best Model (BIC)",
                      "Base", "Linear", "Quadratic", "Cubic",
                      "Base", "Linear", "Quadratic", "Cubic"),
        caption = "Detailed Polynomial Model Selection Summary") %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed"),
                full_width = FALSE) %>%
  column_spec(1, bold = TRUE) %>%
  add_header_above(c(" " = 1, "Best Models" = 2, "AIC Delta Values" = 4, "BIC Delta Values" = 4))

# Save detailed formatted table as HTML
save_kable(detailed_formatted_summary, "polynomial_detailed_summary_table.html")


## produce model summary tables ---

# Function to create formatted model summary tables
create_model_summary_table <- function(model, model_name = "Model", save_file = TRUE) {
  
  # Extract model summary
  model_summary <- summary(model)
  
  # Get coefficients table (fixed effects only)
  coef_table <- model_summary$coefficients
  
  # Calculate 95% confidence intervals for fixed effects only
  ci_95 <- confint(model, method = "Wald", parm = "beta_")
  
  # Ensure we have matching rows by using fixed effects only
  fixed_effect_names <- rownames(coef_table)
  ci_fixed <- ci_95[rownames(ci_95) %in% fixed_effect_names, , drop = FALSE]
  
  # Create summary dataframe
  summary_df <- data.frame(
    Term = rownames(coef_table),
    Coefficient = coef_table[, "Estimate"],
    SE = coef_table[, "Std. Error"],
    CI_Lower = ci_fixed[, "2.5 %"],
    CI_Upper = ci_fixed[, "97.5 %"],
    t_value = coef_table[, "t value"],
    p_value = coef_table[, "Pr(>|t|)"],
    stringsAsFactors = FALSE
  )
  
  # Format values
  summary_df <- summary_df %>%
    mutate(
      # Round coefficients and CIs to 3 decimal places
      Coefficient_fmt = sprintf("%.3f", Coefficient),
      SE_fmt = sprintf("%.3f", SE),
      CI_fmt = paste0("[", sprintf("%.3f", CI_Lower), ", ", sprintf("%.3f", CI_Upper), "]"),
      
      # Format t-values to 2 decimal places
      t_value_fmt = sprintf("%.2f", t_value),
      
      # Format p-values with special handling for p < .001
      p_value_fmt = ifelse(p_value < 0.001, 
                           "p < .001", 
                           paste0("p = ", sprintf("%.3f", p_value)))
    ) %>%
    # Select final columns for display
    select(Term, Coefficient_fmt, CI_fmt, t_value_fmt, p_value_fmt) %>%
    rename(
      "Term" = Term,
      "Coefficient" = Coefficient_fmt,
      "95% CI" = CI_fmt,
      "t" = t_value_fmt,
      "p-value" = p_value_fmt
    )
  
  # Create formatted kable table
  kable_table <- summary_df %>%
    kable(format = "html",
          caption = paste("Model Summary:", model_name),
          align = c("l", "r", "c", "r", "r")) %>%
    kable_styling(bootstrap_options = c("striped", "hover", "condensed"),
                  full_width = FALSE,
                  position = "left") %>%
    column_spec(1, bold = TRUE, width = "3cm") %>%
    column_spec(2:5, width = "2cm") %>%
    row_spec(0, bold = TRUE, background = "#f8f9fa")
  
  # Save table if requested
  if (save_file) {
    filename <- paste0("model_summary_", gsub("[^A-Za-z0-9]", "_", model_name), ".html")
    save_kable(kable_table, filename)
    cat(paste("Table saved as:", filename, "\n"))
  }
  
  # Return both the table and raw data
  return(list(
    table = kable_table,
    data = summary_df,
    raw_summary = summary_df
  ))
}

# Function to create comparison table for multiple models
create_model_comparison_table <- function(model_list, model_names = NULL, save_file = TRUE) {
  
  if (is.null(model_names)) {
    model_names <- paste("Model", seq_along(model_list))
  }
  
  # Extract summaries for all models
  all_summaries <- map2_dfr(model_list, model_names, function(model, name) {
    
    model_summary <- summary(model)
    coef_table <- model_summary$coefficients
    ci_95 <- confint(model, method = "Wald")
    
    data.frame(
      Model = name,
      Term = rownames(coef_table),
      Coefficient = coef_table[, "Estimate"],
      CI_Lower = ci_95[, "2.5 %"],
      CI_Upper = ci_95[, "97.5 %"],
      p_value = coef_table[, "Pr(>|t|)"],
      stringsAsFactors = FALSE
    )
  })
  
  # Format the comparison table
  comparison_df <- all_summaries %>%
    mutate(
      Coef_CI = paste0(sprintf("%.3f", Coefficient), 
                       " [", sprintf("%.3f", CI_Lower), 
                       ", ", sprintf("%.3f", CI_Upper), "]"),
      p_value_fmt = ifelse(p_value < 0.001, 
                           "< .001", 
                           sprintf("%.3f", p_value))
    ) %>%
    select(Model, Term, Coef_CI, p_value_fmt) %>%
    rename(
      "Model" = Model,
      "Term" = Term,
      "Coefficient [95% CI]" = Coef_CI,
      "p-value" = p_value_fmt
    )
  
  # Create kable table
  comparison_table <- comparison_df %>%
    kable(format = "html",
          caption = "Model Comparison Summary") %>%
    kable_styling(bootstrap_options = c("striped", "hover", "condensed"),
                  full_width = FALSE) %>%
    column_spec(1, bold = TRUE) %>%
    column_spec(2, italic = TRUE) %>%
    row_spec(0, bold = TRUE, background = "#f8f9fa") %>%
    collapse_rows(columns = 1, valign = "top")
  
  # Save if requested
  if (save_file) {
    save_kable(comparison_table, "model_comparison_summary.html")
    cat("Comparison table saved as: model_comparison_summary.html\n")
  }
  
  return(list(
    table = comparison_table,
    data = comparison_df
  ))
}

# Create cubic model summary tables for all DVs

# Pre-registered models - cubic summaries
cat("=== CREATING CUBIC MODEL SUMMARY TABLES ===\n")

# Pre-registered DVs
intergroup_cubic <- create_model_summary_table(models_poly_intergroup$models$order_3, 
                                               "Intergroup Content - Cubic Model")

moral_cubic <- create_model_summary_table(models_poly_moral$models$order_3, 
                                          "Moral Content - Cubic Model")

emotion_neg_cubic <- create_model_summary_table(models_poly_emotion_neg$models$order_3, 
                                                "Negative Emotion - Cubic Model")

emotion_pos_cubic <- create_model_summary_table(models_poly_emotion_pos$models$order_3, 
                                                "Positive Emotion - Cubic Model")

toxic_cubic <- create_model_summary_table(models_poly_toxic$models$order_3, 
                                          "Toxic Content - Cubic Model")

political_cubic <- create_model_summary_table(models_poly_political$models$order_3, 
                                              "Political Content - Cubic Model")

# Exploratory DVs
outrage_cubic <- create_model_summary_table(models_poly_outrage$models$order_3, 
                                            "Moral Outrage - Cubic Model")

constructive_cubic <- create_model_summary_table(models_poly_constructive$models$order_3, 
                                                 "Constructive Content - Cubic Model")



## create predicted values from cubic model plots ---
# Function to create prediction plots for cubic models
create_cubic_prediction_plot <- function(model, dv_name, save_file = TRUE) {
  
  # Create prediction data frame
  # Get the range of days from the original data
  day_range <- range(condition_feed$day, na.rm = TRUE)
  
  # Create prediction grid
  pred_data <- expand_grid(
    day = seq(day_range[1], day_range[2], length.out = 100),
    condition = unique(condition_feed$condition)
  ) %>%
    # Add a dummy handle for prediction (will be averaged over)
    mutate(handle = "dummy_user")
  
  # Generate predictions with confidence intervals
  pred_data$predicted <- predict(model, newdata = pred_data, re.form = NA)
  
  # Get prediction intervals using bootMer (more robust for mixed models)
  # Note: This is computationally intensive, so we'll use a simpler approach
  pred_matrix <- predictInterval(model, newdata = pred_data, 
                                 level = 0.95, n.sims = 100, 
                                 stat = "median", returnSims = FALSE)
  
  # Combine predictions with intervals
  plot_data <- pred_data %>%
    bind_cols(pred_matrix) %>%
    mutate(
      condition = factor(condition, 
                         levels = c("Diversified Extremity", "Engagement-Based", "Reverse Chronological"))
    )
  
  # Create the plot
  p <- ggplot(plot_data, aes(x = day, y = fit, color = condition, fill = condition)) +
    geom_ribbon(aes(ymin = lwr, ymax = upr), alpha = 0.2, color = NA) +
    geom_line(size = 1.2) +
    scale_color_manual(values = c("Diversified Extremity" = "#1f77b4", 
                                  "Engagement-Based" = "#ff7f0e", 
                                  "Reverse Chronological" = "#2ca02c")) +
    scale_fill_manual(values = c("Diversified Extremity" = "#1f77b4", 
                                 "Engagement-Based" = "#ff7f0e", 
                                 "Reverse Chronological" = "#2ca02c")) +
    labs(
      title = paste("Cubic Time Trends:", dv_name),
      subtitle = "Predicted values with 95% confidence intervals",
      x = "Day",
      y = "Predicted Proportion",
      color = "Feed Condition",
      fill = "Feed Condition"
    ) +
    theme_classic() +
    theme(
      text = element_text(size = 12),
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11, color = "gray50"),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      strip.background = element_blank(),
      strip.text = element_text(face = "bold")
    ) +
    guides(color = guide_legend(override.aes = list(alpha = 1, size = 1.5)))
  
  # Save plot if requested
  if (save_file) {
    filename <- paste0("cubic_predictions_", gsub("[^A-Za-z0-9]", "_", dv_name), ".png")
    ggsave(filename, p, dpi = 300, width = 12, height = 8)
    cat(paste("Plot saved as:", filename, "\n"))
  }
  
  return(list(
    plot = p,
    data = plot_data
  ))
}

# Alternative simpler function if predictInterval is not available
create_cubic_prediction_plot_simple <- function(model, dv_name, save_file = TRUE) {
  
  # Create prediction data frame
  day_range <- range(condition_feed$day, na.rm = TRUE)
  
  pred_data <- expand_grid(
    day = seq(day_range[1], day_range[2], length.out = 100),
    condition = unique(condition_feed$condition)
  ) %>%
    mutate(handle = "dummy_user")
  
  # Generate predictions (without confidence intervals for simplicity)
  pred_data$predicted <- predict(model, newdata = pred_data, re.form = NA)
  
  # Create the plot
  p <- ggplot(pred_data, aes(x = day, y = predicted, color = condition)) +
    geom_line(size = 1.2) +
    scale_color_manual(values = c("Diversified Extremity" = "#1f77b4", 
                                  "Engagement-Based" = "#ff7f0e", 
                                  "Reverse Chronological" = "#2ca02c")) +
    labs(
      title = paste("Cubic Time Trends:", dv_name),
      subtitle = "Predicted values from cubic polynomial model",
      x = "Day",
      y = "Predicted Proportion",
      color = "Feed Condition"
    ) +
    theme_classic() +
    theme(
      text = element_text(size = 12),
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11, color = "gray50"),
      legend.position = "bottom",
      legend.title = element_text(face = "bold")
    )
  
  # Save plot if requested
  if (save_file) {
    filename <- paste0("cubic_predictions_", gsub("[^A-Za-z0-9]", "_", dv_name), ".png")
    ggsave(filename, p, dpi = 300, width = 12, height = 8)
    cat(paste("Plot saved as:", filename, "\n"))
  }
  
  return(list(
    plot = p,
    data = pred_data
  ))
}

# Create cubic prediction plots for all DVs
cat("=== CREATING CUBIC MODEL PREDICTION PLOTS ===\n")

# Try the full version first, fall back to simple if needed
tryCatch({
  # Load required library for prediction intervals
  library(merTools)
  use_intervals <- TRUE
}, error = function(e) {
  cat("merTools not available, using simplified plots without confidence intervals\n")
  use_intervals <- FALSE
})

# Pre-registered DVs
if (use_intervals) {
  intergroup_plot <- create_cubic_prediction_plot(models_poly_intergroup$models$order_3, 
                                                  "Intergroup Content")
  moral_plot <- create_cubic_prediction_plot(models_poly_moral$models$order_3, 
                                             "Moral Content")
  emotion_neg_plot <- create_cubic_prediction_plot(models_poly_emotion_neg$models$order_3, 
                                                   "Negative Emotion")
  emotion_pos_plot <- create_cubic_prediction_plot(models_poly_emotion_pos$models$order_3, 
                                                   "Positive Emotion")
  toxic_plot <- create_cubic_prediction_plot(models_poly_toxic$models$order_3, 
                                             "Toxic Content")
  political_plot <- create_cubic_prediction_plot(models_poly_political$models$order_3, 
                                                 "Political Content")
  
  # Exploratory DVs
  outrage_plot <- create_cubic_prediction_plot(models_poly_outrage$models$order_3, 
                                               "Moral Outrage")
  constructive_plot <- create_cubic_prediction_plot(models_poly_constructive$models$order_3, 
                                                    "Constructive Content")
} else {
  # Use simple version
  intergroup_plot <- create_cubic_prediction_plot_simple(models_poly_intergroup$models$order_3, 
                                                         "Intergroup Content")
  moral_plot <- create_cubic_prediction_plot_simple(models_poly_moral$models$order_3, 
                                                    "Moral Content")
  emotion_neg_plot <- create_cubic_prediction_plot_simple(models_poly_emotion_neg$models$order_3, 
                                                          "Negative Emotion")
  emotion_pos_plot <- create_cubic_prediction_plot_simple(models_poly_emotion_pos$models$order_3, 
                                                          "Positive Emotion")
  toxic_plot <- create_cubic_prediction_plot_simple(models_poly_toxic$models$order_3, 
                                                    "Toxic Content")
  political_plot <- create_cubic_prediction_plot_simple(models_poly_political$models$order_3, 
                                                        "Political Content")
  
  # Exploratory DVs
  outrage_plot <- create_cubic_prediction_plot_simple(models_poly_outrage$models$order_3, 
                                                      "Moral Outrage")
  constructive_plot <- create_cubic_prediction_plot_simple(models_poly_constructive$models$order_3, 
                                                           "Constructive Content")
}

cat("All cubic prediction plots created and saved!\n")

# Optional: Create a combined plot showing all DVs
create_combined_prediction_plot <- function(save_file = TRUE) {
  
  # List of all models and names
  models_list <- list(
    models_poly_intergroup$models$order_3,
    models_poly_moral$models$order_3,
    models_poly_emotion_neg$models$order_3,
    models_poly_emotion_pos$models$order_3,
    models_poly_toxic$models$order_3,
    models_poly_political$models$order_3,
    models_poly_outrage$models$order_3,
    models_poly_constructive$models$order_3
  )
  
  dv_names <- c("Intergroup", "Moral", "Negative Emotion", "Positive Emotion",
                "Toxic", "Political", "Outrage", "Constructive")
  
  # Create prediction data for all models
  day_range <- range(condition_feed$day, na.rm = TRUE)
  
  all_predictions <- map2_dfr(models_list, dv_names, function(model, name) {
    pred_data <- expand_grid(
      day = seq(day_range[1], day_range[2], length.out = 50),
      condition = unique(condition_feed$condition)
    ) %>%
      mutate(handle = condition_feed$handle[1])
    
    pred_data$predicted <- predict(model, newdata = pred_data, re.form = NA)
    pred_data$DV <- name
    return(pred_data)
  })
  
  # Create faceted plot
  # Compute global y range
  y_range <- range(all_predictions$predicted, na.rm = TRUE)
  
  # Create faceted plot with fixed y limits
  combined_plot <- all_predictions %>%
    mutate(
      condition = factor(condition, 
                         levels = c("Diversified Extremity", "Engagement-Based", "Reverse Chronological")),
      DV = factor(DV, levels = dv_names)
    ) %>%
    ggplot(aes(x = day, y = predicted, color = condition)) +
    geom_line(size = 1) +
    facet_wrap(~DV, scales = "free_y", ncol = 2) +
    scale_color_manual(values = c("Diversified Extremity" = "#1f77b4", 
                                  "Engagement-Based" = "#ff7f0e", 
                                  "Reverse Chronological" = "#2ca02c")) +
    labs(
      title = "Cubic Time Trends Across All Content Types",
      subtitle = "Predicted values from cubic polynomial models",
      x = "Day",
      y = "Predicted Proportion",
      color = "Feed Condition"
    ) +
    theme_classic() +
    theme(
      text = element_text(size = 10),
      plot.title = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11, color = "gray50"),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      strip.background = element_rect(fill = "gray95", color = NA),
      strip.text = element_text(face = "bold", size = 9)
    )
  
  
  if (save_file) {
    ggsave("cubic_predictions_combined.png", combined_plot, 
           dpi = 300, width = 16, height = 12)
    cat("Combined plot saved as: cubic_predictions_combined.png\n")
  }
  
  return(combined_plot)
}

# Create the combined plot
combined_plot <- create_combined_prediction_plot()
combined_plot

ggsave("cubic_predictions_combined_publication.png", combined_plot, 
       dpi = 600, width = 12, height = 10, units = "in", 
       bg = "white", device = "png")

#### feed data analysis stats: test data-driven polynomial terms ----

# Function to fit polynomial models of different orders
fit_polynomial_models <- function(dv, max_order = 5) {
  
  # Storage for models and fit statistics
  models <- list()
  fit_stats <- data.frame(
    order = 1:max_order,
    AIC = NA,
    BIC = NA,
    logLik = NA,
    deviance = NA,
    df_resid = NA
  )
  
  # Fit models of increasing polynomial order
  for (i in 1:max_order) {
    
    cat("Fitting polynomial order", i, "...\n")
    
    # Create formula with polynomial of order i
    if (i == 1) {
      # Linear model (no polynomial)
      formula_text <- paste(dv, "~ condition * day + (1 | bluesky_handle)")
    } else {
      # Polynomial model of order i
      formula_text <- paste(dv, "~ condition * poly(day,", i, ") + (1 | bluesky_handle)")
    }
    
    formula_obj <- as.formula(formula_text)
    
    # Fit model
    model <- lmer(formula_obj, data = condition_feed)
    
    # Store model
    models[[paste0("poly_", i)]] <- model
    
    # Extract fit statistics
    fit_stats$AIC[i] <- AIC(model)
    fit_stats$BIC[i] <- BIC(model)
    fit_stats$logLik[i] <- as.numeric(logLik(model))
    fit_stats$deviance[i] <- deviance(model)
    fit_stats$df_resid[i] <- df.residual(model)
  }
  
  # Add delta AIC and BIC (relative to best model)
  fit_stats <- fit_stats %>%
    mutate(
      delta_AIC = AIC - min(AIC),
      delta_BIC = BIC - min(BIC),
      best_AIC = (delta_AIC == 0),
      best_BIC = (delta_BIC == 0)
    )
  
  return(list(models = models, fit_stats = fit_stats))
}

# Fit polynomial models for IME
cat("Testing polynomial orders 1-5 for avg_prob_ime...\n")
ime_poly_results <- fit_polynomial_models("avg_prob_ime", max_order = 5)

# Display fit statistics
cat("\n=== MODEL COMPARISON RESULTS ===\n")
print(ime_poly_results$fit_stats)

# Identify best models
best_aic_order <- ime_poly_results$fit_stats$order[which.min(ime_poly_results$fit_stats$AIC)]
best_bic_order <- ime_poly_results$fit_stats$order[which.min(ime_poly_results$fit_stats$BIC)]

cat(paste("\nBest model by AIC: Polynomial order", best_aic_order))
cat(paste("\nBest model by BIC: Polynomial order", best_bic_order))

# Create visualization of model comparison
fit_comparison_plot <- ime_poly_results$fit_stats %>%
  select(order, AIC, BIC) %>%
  pivot_longer(cols = c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = order, y = value, color = criterion)) +
  geom_line(size = 1) +
  geom_point(size = 3) +
  scale_x_continuous(breaks = 1:5, labels = c("Linear", "Quadratic", "Cubic", "Quartic", "Quintic")) +
  scale_color_manual(values = c("AIC" = "#E69F00", "BIC" = "#0072B2")) +
  labs(
    title = "Model Comparison: Polynomial Order Selection",
    subtitle = "Lower values indicate better fit",
    x = "Polynomial Order",
    y = "Information Criterion Value",
    color = "Criterion"
  ) +
  theme_classic() +
  theme(
    text = element_text(size = 12),
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.position = "bottom"
  )


print(fit_comparison_plot)
ggsave("polynomial_order_comparison.png", fit_comparison_plot, 
       dpi = 300, width = 10, height = 6)

# Extract and visualize daily marginal effects from quintic IME model ----

# Use the cubic model (3rd order) from our polynomial testing
ime_cubic_model <- ime_poly_results$models$poly_3

# DAILY MARGINAL EFFECTS FROM CUBIC MODEL
day_grid <- condition_feed %>% distinct(date, day) %>% arrange(day)

# Get estimated marginal means for each condition on each day
emm_by_day_cubic <- emmeans(
  ime_cubic_model,
  ~ condition | day,
  at = list(day = day_grid$day),
  re.form = NA
)

# Contrast definitions (RC, EB, DE factor order)
contr_defs <- list(
  "Engagement-Based vs Reverse Chronological"       = c(-1,  1,  0),
  "Diversified Extremity vs Reverse Chronological"  = c(-1,  0,  1)
)

# Compute contrasts
contr_df_cubic <- contrast(emm_by_day_cubic, method = contr_defs)
contr_by_day_cubic <- summary(contr_df_cubic, infer = c(TRUE, TRUE)) %>%
  as.data.frame() %>%
  left_join(day_grid, by = "day") %>%
  mutate(comp = factor(contrast, levels = names(contr_defs)))

# Fallback for CI columns if needed
if (!all(c("lower.CL","upper.CL") %in% names(contr_by_day_cubic))) {
  contr_by_day_cubic <- contr_by_day_cubic %>%
    mutate(
      lower.CL = estimate - qnorm(0.975) * SE,
      upper.CL = estimate + qnorm(0.975) * SE
    )
}

# Constants for plotting
election_date <- as.Date("2024-11-05")
pal_contrasts <- c(
  "Engagement-Based vs Reverse Chronological"       = "#E69F00",
  "Diversified Extremity vs Reverse Chronological"  = "#0072B2"
)

# Calculate y-axis limits including zero
all_values <- c(contr_by_day_cubic$lower.CL, contr_by_day_cubic$upper.CL, 0)
rng  <- range(all_values, na.rm = TRUE)
if (diff(rng) == 0) rng <- rng + c(-0.01, 0.01)
pad  <- 0.05 * diff(rng)
ylims <- c(rng[1] - pad, rng[2] + pad)

# CREATE DAILY MARGINAL EFFECTS PLOT
plot_cubic_daily <- ggplot(contr_by_day_cubic, aes(x = date, y = estimate, color = comp)) +
  geom_hline(yintercept = 0, linetype = "solid", color = "black", size = 0.5) +
  geom_vline(xintercept = as.numeric(election_date), 
             linetype = "dashed", color = "gray30", size = 0.8) +
  annotate("text", x = election_date, y = max(ylims) * 0.85, 
           label = "Election", angle = 90, vjust = -0.5,
           size = 3.2, color = "gray30", fontface = "bold") +
  geom_errorbar(aes(ymin = lower.CL, ymax = upper.CL), width = 0.5, alpha = 0.6) +
  geom_point(size = 1.5, alpha = 0.8) +
  scale_y_continuous(
    name = "Δ IME Content Probability",
    expand = expansion(mult = c(0.05, 0.05)),
    limits = ylims,
    breaks = scales::pretty_breaks(n = 6),
    labels = scales::number_format(accuracy = 0.01)
  ) +
  scale_x_date(
    name = "Date",
    date_labels = "%b %d",
    date_breaks = "2 weeks",
    expand = expansion(mult = c(0.02, 0.02))
  ) +
  scale_color_manual(
    name = "Comparison",
    values = pal_contrasts
  ) +
  labs(
    title = "Daily Marginal Effects: Algorithm Impact on IME Content",
    subtitle = "Cubic polynomial model predictions with 95% confidence intervals"
  ) +
  theme_classic() +
  theme(
    text = element_text(color = "black"),
    plot.title = element_text(size = 14, face = "bold"),
    plot.subtitle = element_text(size = 12),
    axis.title = element_text(size = 11),
    axis.text = element_text(size = 10),
    axis.text.x = element_text(angle = 45, hjust = 1),
    legend.title = element_text(size = 11, face = "bold"),
    legend.text = element_text(size = 10),
    panel.grid.major.y = element_line(color = "gray95", size = 0.3),
    panel.grid.minor.y = element_blank(),
    axis.line = element_line(color = "black", size = 0.5),
    axis.ticks = element_line(color = "black", size = 0.4),
    legend.position = "bottom"
  )

# Display the plot
print(plot_cubic_daily)

# Save high-quality version
ggsave("cubic_ime_daily_marginal_effects.png", plot_cubic_daily, 
       dpi = 600, width = 12, height = 8, bg = "white")

# SUMMARY STATISTICS FOR REPORTING
cat("\n=== CUBIC MODEL MARGINAL EFFECTS SUMMARY ===\n")

# Find maximum effect sizes
max_effects <- contr_by_day_cubic %>%
  group_by(comp) %>%
  summarise(
    max_effect = max(estimate),
    max_effect_date = date[which.max(estimate)],
    min_effect = min(estimate),
    min_effect_date = date[which.min(estimate)],
    mean_effect = mean(estimate),
    .groups = "drop"
  )

print(max_effects)

# Effects around election (±7 days)
election_window <- contr_by_day_cubic %>%
  filter(abs(as.numeric(date - election_date)) <= 7) %>%
  group_by(comp) %>%
  summarise(
    pre_election_effect = mean(estimate[date < election_date]),
    post_election_effect = mean(estimate[date > election_date]),
    election_change = post_election_effect - pre_election_effect,
    .groups = "drop"
  )

cat("\n=== EFFECTS AROUND ELECTION (±7 days) ===\n")
print(election_window)

# Range of study period effects
study_range <- contr_by_day_cubic %>%
  group_by(comp) %>%
  summarise(
    effect_range = max(estimate) - min(estimate),
    early_period = mean(estimate[date <= as.Date("2024-10-15")]),
    late_period = mean(estimate[date >= as.Date("2024-11-20")]),
    .groups = "drop"
  )

cat("\n=== STUDY PERIOD EFFECTS SUMMARY ===\n")
print(study_range)


#### comparing condition feeds vs. baseline content ----

# create combined df (prop) - updated variable names
condition_baseline_prop <- condition_feed %>% 
  dplyr::select(date, condition, 
                feed_proportion_toxic, feed_proportion_intergroup, feed_proportion_moral,
                feed_proportion_is_valence_negative, feed_proportion_is_valence_positive,
                feed_proportion_moral_outrage, feed_proportion_constructive, 
                feed_proportion_is_sociopolitical) %>% 
  rbind(., baseline_prop %>% 
          dplyr::select(date, condition, 
                        feed_proportion_toxic, feed_proportion_intergroup, feed_proportion_moral,
                        feed_proportion_is_valence_negative, feed_proportion_is_valence_positive,
                        feed_proportion_moral_outrage, feed_proportion_constructive, 
                        feed_proportion_is_sociopolitical))

#### plotting DVs over time with LOESS fit and error bands (w/ baseline condition, proportion) ----

# Publication-ready plotting function for Nature
plot_time_series_baseline_prop <- function(dv, y_label) {
  # Accept the DV as a string or an unquoted column name
  if (is.character(dv)) {
    dv_sym <- sym(dv)
  } else {
    dv_sym <- enquo(dv)
  }
  
  # Election date
  election_date <- as.Date("2024-11-05")
  
  # Build the plot using the combined data
  p <- ggplot(condition_baseline_prop, aes(x = date, y = !!dv_sym, color = condition, fill = condition)) +
    # Raw data points (very subtle)
    geom_jitter(color = "grey90", alpha = 0.3, size = 0.3, width = 0.2, height = 0.01) +
    
    # LOESS fits with custom styling per condition
    geom_smooth(data = . %>% filter(condition != "Baseline Firehose"),
                method = "loess", se = TRUE, 
                method.args = list(span = 0.75, control = loess.control(surface = "direct")),
                linetype = "solid", linewidth = 1.2, alpha = 0.15) +
    
    # Baseline with dotted line and lighter error band
    geom_smooth(data = . %>% filter(condition == "Baseline Firehose"),
                method = "loess", se = TRUE, 
                method.args = list(span = 0.75, control = loess.control(surface = "direct")),
                linetype = "dotted", linewidth = 1.2, alpha = 0.1) +
    
    # Election reference line (subtle)
    geom_vline(xintercept = as.numeric(election_date), 
               linetype = "dotted", color = "gray40", linewidth = 0.7) +
    
    # Election label (refined positioning)
    annotate("text", x = election_date, y = Inf, 
             label = "Election", angle = 90, vjust = 1.3, hjust = 1.1,
             size = 3.5, color = "gray30", fontface = "italic") +
    
    # Clean axis formatting
    scale_x_date(name = "Date", 
                 date_labels = "%b %Y",
                 date_breaks = "1 month",
                 expand = expansion(mult = c(0.02, 0.02))) +
    
    scale_y_continuous(name = y_label,
                       labels = scales::percent_format(accuracy = 0.1),
                       expand = expansion(mult = c(0.02, 0.05))) +
    
    # Publication color scheme
    scale_color_manual(
      name = "Feed Type",
      values = c("Reverse Chronological" = "black", 
                 "Engagement-Based" = "#D55E00",     # Vermillion
                 "Diversified Extremity" = "#009E73", # Bluish Green
                 "Baseline Firehose" = "#0072B2"),   # Blue for baseline
      breaks = c("Reverse Chronological", "Engagement-Based", "Diversified Extremity", "Baseline Firehose")
    ) +
    
    scale_fill_manual(
      name = "Feed Type",
      values = c("Reverse Chronological" = "black", 
                 "Engagement-Based" = "#D55E00",     
                 "Diversified Extremity" = "#009E73", 
                 "Baseline Firehose" = "#0072B2"),
      breaks = c("Reverse Chronological", "Engagement-Based", "Diversified Extremity", "Baseline Firehose")
    ) +
    
    # Clean minimal theme
    theme_classic(base_size = 11) +
    theme(
      # Typography
      text = element_text(color = "black"),
      axis.title = element_text(size = 11, color = "black"),
      axis.text = element_text(size = 10, color = "black"),
      axis.text.x = element_text(angle = 45, hjust = 1),
      
      # Legend
      legend.title = element_text(size = 10, face = "bold"),
      legend.text = element_text(size = 9),
      legend.position = "bottom",
      legend.key.width = unit(1.2, "cm"),
      
      # Panel
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.border = element_blank(),
      
      # Axes
      axis.line = element_line(color = "black", linewidth = 0.5),
      axis.ticks = element_line(color = "black", linewidth = 0.4),
      
      # Plot area
      plot.margin = margin(10, 10, 10, 10),
      plot.title = element_blank()  # Remove title for cleaner look
    )
  
  return(p)
}

# Plot all variables with updated names
# Toxic
plot_toxic_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_toxic", "Toxic Content Proportion")
ggsave("toxic_time_series_baseline_prop.png", plot_toxic_baseline_prop, dpi = 300, width = 10, height = 6)

# Intergroup
plot_intergroup_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_intergroup", "Intergroup Content Proportion")
ggsave("intergroup_time_series_baseline_prop.png", plot_intergroup_baseline_prop, dpi = 300, width = 10, height = 6)

# Moral
plot_moral_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_moral", "Moral Content Proportion")
ggsave("moral_time_series_baseline_prop.png", plot_moral_baseline_prop, dpi = 300, width = 10, height = 6)

# Negative Emotion
plot_emotion_neg_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_is_valence_negative", "Negative Emotional Content Proportion")
ggsave("emotion_neg_time_series_baseline_prop.png", plot_emotion_neg_baseline_prop, dpi = 300, width = 10, height = 6)

# Positive Emotion
plot_emotion_pos_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_is_valence_positive", "Positive Emotional Content Proportion")
ggsave("emotion_pos_time_series_baseline_prop.png", plot_emotion_pos_baseline_prop, dpi = 300, width = 10, height = 6)

# Moral Outrage
plot_outrage_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_moral_outrage", "Moral Outrage Content Proportion")
ggsave("outrage_time_series_baseline_prop.png", plot_outrage_baseline_prop, dpi = 300, width = 10, height = 6)

# Constructive
plot_constructive_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_constructive", "Constructive Content Proportion")
ggsave("constructive_time_series_baseline_prop.png", plot_constructive_baseline_prop, dpi = 300, width = 10, height = 6)

# Political
plot_political_baseline_prop <- plot_time_series_baseline_prop("feed_proportion_is_sociopolitical", "Political Content Proportion")
ggsave("political_time_series_baseline_prop.png", plot_political_baseline_prop, dpi = 300, width = 10, height = 6)






#### exclusion robustness here ----

## ----------------------------
# Load threshold handle sets
# ----------------------------
handle_sets <- readRDS("valid_handle_sets_by_threshold.rds")

# ----------------------------
# DVs to run (RQ1)
# ----------------------------
dv_list <- c(
  "feed_proportion_intergroup",
  "feed_proportion_moral",
  "feed_proportion_is_valence_negative",
  "feed_proportion_is_valence_positive",
  "feed_proportion_toxic",
  "feed_proportion_is_sociopolitical",
  "feed_proportion_moral_outrage",
  "feed_proportion_constructive"
)

# --- model runner: returns EB-RC, DE-RC, EB-DE for Pre and Post
run_rq1_model2_one_dv_allcontrasts <- function(data, dv, ref_condition = "Reverse Chronological") {
  
  data <- data %>%
    mutate(
      condition = factor(condition, levels = c(ref_condition,
                                               "Diversified Extremity",
                                               "Engagement-Based")),
      period = factor(.data$period, levels = c("Pre", "Post"))
    )
  
  fml <- as.formula(paste0(dv, " ~ condition * period + (1 | handle)"))
  mod <- lmer(fml, data = data)
  
  emm <- emmeans(mod, ~ condition | period)
  
  # Contrasts vs RC
  ctr_rc <- contrast(emm, method = "trt.vs.ctrl", ref = 1) %>%
    summary(infer = TRUE) %>%
    as_tibble()
  
  # Pairwise contrasts to get EB-DE directly
  ctr_pw <- contrast(emm, method = "pairwise") %>%
    summary(infer = TRUE) %>%
    as_tibble()
  
  # Keep only EB - DE from pairwise set
  ctr_ebde <- ctr_pw %>%
    filter(contrast %in% c("Engagement-Based - Diversified Extremity",
                           "Diversified Extremity - Engagement-Based")) %>%
    mutate(
      # Force direction EB - DE
      estimate = if_else(contrast == "Diversified Extremity - Engagement-Based", -estimate, estimate),
      p.value  = if_else(contrast == "Diversified Extremity - Engagement-Based", p.value, p.value),
      contrast = "Engagement-Based - Diversified Extremity"
    )
  
  bind_rows(ctr_rc, ctr_ebde) %>%
    transmute(
      dv = dv,
      period = period,
      contrast = contrast,
      estimate = estimate,
      p.value = p.value
    )
}


rq1_robust_results2 <- imap_dfr(handle_sets, function(valid_handles, threshold_name) {
  
  dat_thr <- condition_feed %>%
    filter(handle %in% valid_handles)
  
  map_dfr(dv_list, ~ run_rq1_model2_one_dv_allcontrasts(dat_thr, dv = .x)) %>%
    mutate(
      threshold = threshold_name,
      n_handles = n_distinct(dat_thr$handle)
    )
})



# ----------------------------
# Sanity check Ns by threshold
# ----------------------------
rq1_robust_results2 %>%
  distinct(threshold, n_handles) %>%
  arrange(desc(n_handles)) %>%
  print(n = Inf)

# ----------------------------
# Save results
# ----------------------------
saveRDS(rq1_robust_results2, "rq1_model2_robustness_all_contrasts.rds")


#### create comparison table for SI ----
make_dv_table <- function(results_df, dv_name, dv_title = NULL) {
  
  df <- results_df %>%
    filter(dv == dv_name) %>%
    mutate(
      thr_num = as.numeric(str_extract(threshold, "\\d")),
      thr_label = paste0("≥", thr_num, " weeks (n=", n_handles, ")"),
      contrast_short = case_when(
        str_detect(contrast, "Engagement-Based") & str_detect(contrast, "Reverse Chronological") ~ "EB − RC",
        str_detect(contrast, "Diversified Extremity") & str_detect(contrast, "Reverse Chronological") ~ "DE − RC",
        str_detect(contrast, "Engagement-Based") & str_detect(contrast, "Diversified Extremity") ~ "EB − DE",
        TRUE ~ contrast
      ),
      beta = fmt_beta(estimate),
      sig = p.value < .05
    ) %>%
    arrange(desc(thr_num))
  
  # Build a cell string per threshold x period
  cell_df <- df %>%
    mutate(
      line = if_else(sig,
                     paste0("**", contrast_short, ": ", beta, "**"),
                     paste0(contrast_short, ": ", beta))
    ) %>%
    group_by(thr_label, period) %>%
    summarise(cell = paste(line, collapse = "<br>"), .groups = "drop") %>%
    pivot_wider(names_from = period, values_from = cell)
  
  gt_tbl <- cell_df %>%
    gt() %>%
    cols_label(
      thr_label = "Inclusion threshold",
      Pre = "Pre-election",
      Post = "Post-election"
    ) %>%
    tab_header(
      title = md(paste0("**Robustness across inclusion thresholds: ", ifelse(is.null(dv_title), dv_name, dv_title), "**")),
      subtitle = "Entries are β coefficients; bold indicates p < .05."
    ) %>%
    fmt_markdown(columns = c(Pre, Post)) %>%
    cols_align(align = "left", columns = c(thr_label, Pre, Post)) %>%
    tab_options(
      table.font.size = px(13),
      column_labels.font.size = px(13),
      heading.title.font.size = px(14),
      heading.subtitle.font.size = px(12),
      data_row.padding = px(6)
    ) %>%
    cols_width(
      thr_label ~ px(220),
      Pre ~ px(260),
      Post ~ px(260)
    )
  
  gt_tbl
}


dv_titles <- c(
  feed_proportion_intergroup = "Intergroup content",
  feed_proportion_moral = "Moral content",
  feed_proportion_is_valence_negative = "Negative emotion content",
  feed_proportion_is_valence_positive = "Positive emotion content",
  feed_proportion_toxic = "Toxic content",
  feed_proportion_is_sociopolitical = "Political content",
  feed_proportion_moral_outrage = "Outrage content",
  feed_proportion_constructive = "Constructive content"
)

fmt_beta <- function(x) sprintf("%.2f", as.numeric(x))

# Create and save one table per DV
tables <- map(dv_list, ~ make_dv_table(rq1_robust_results2, .x, dv_titles[[.x]]))

tables[[which(dv_list == "feed_proportion_toxic")]]
tables[[which(dv_list == "feed_proportion_moral")]]
tables[[which(dv_list == "feed_proportion_is_valence_negative")]]
tables[[which(dv_list == "feed_proportion_is_valence_positive")]]
tables[[which(dv_list == "feed_proportion_toxic")]]
tables[[which(dv_list == "feed_proportion_is_sociopolitical")]]
tables[[which(dv_list == "feed_proportion_moral_outrage")]]
tables[[which(dv_list == "feed_proportion_constructive")]]



# diagnostic
check_ns <- imap_dfr(handle_sets, \(h, nm) {
  dat_thr <- condition_feed %>% filter(handle %in% h)
  
  tibble(
    threshold = nm,
    n_handles = n_distinct(dat_thr$handle),
    n_rows = nrow(dat_thr)
  )
})

check_ns %>% print(n = Inf)


rq1_robust_results2 %>%
  distinct(contrast) %>%
  arrange(contrast) %>%
  print(n = Inf)

levels(condition_feed$condition)
