# Central configuration for constructiveness-only RQ1 analyses.

cfg_default <- function() {
  list(
    # ---- Inputs ----
    # NOTE: This file was previously: "daily_feed_content_aggregated_results_per_user.csv"
    # This newer file is per-user-per-day constructiveness derived from per-feed averages.
    input_condition_feed_csv = "per_user_per_day_average_constructiveness.csv",
    input_valid_users_csv = "qualtrics_final_impute.csv",

    # ---- Key dates ----
    election_date = as.Date("2024-11-05"),

    # ---- Output ----
    output_dir = "outputs_constructiveness_only",
    overwrite_outputs = TRUE,

    # ---- Plot knobs ----
    loess_span = 0.75
  )
}


