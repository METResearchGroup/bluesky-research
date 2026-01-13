# RQ1 (Constructiveness Only): Analyses without baseline_prop
#
# This script is intentionally narrow:
# - Input: per-user-per-day constructiveness file (no baseline firehose)
# - Output: plots + CSV summaries for constructiveness only
#
# NOTE: Do NOT source `RQ1_analyses.R` here; this file is a refactor focused on one DV.

source("lib/bootstrap.R")
set_working_dir_to_script()

source("lib/config.R")
source("lib/io.R")
source("lib/transform.R")
source("lib/models.R")
source("lib/contrasts.R")
source("lib/plotting.R")
source("lib/reporting.R")

main <- function(cfg = cfg_default()) {
  require_packages(
    c(
      "tidyverse", # dplyr/readr/ggplot2/etc.
      "lmerTest",
      "emmeans",
      "scales",
      "tibble"
    )
  )

  ensure_output_dir(cfg$output_dir, overwrite = cfg$overwrite_outputs)

  # ----------------------------
  # Load data
  # ----------------------------
  condition_feed <- read_condition_feed_constructiveness(cfg$input_condition_feed_csv)
  valid_handles <- read_valid_handles(cfg$input_valid_users_csv)

  # ----------------------------
  # Clean + filter
  # ----------------------------
  condition_feed <- condition_feed |>
    filter_to_valid_handles(valid_handles) |>
    recode_condition_labels() |>
    add_time_columns() |>
    add_prepost_columns(cfg$election_date) |>
    dplyr::mutate(
      # Ensure consistent factor ordering (RC as ref by convention)
      condition = factor(
        .data$condition,
        levels = c("Reverse Chronological", "Diversified Extremity", "Engagement-Based")
      )
    )

  # ----------------------------
  # Plot: time series (conditions only)
  # ----------------------------
  p_ts <- plot_time_series_constructive(condition_feed, loess_span = cfg$loess_span)
  ggplot2::ggsave(
    filename = file.path(cfg$output_dir, "constructive_time_series_conditions_only.png"),
    plot = p_ts,
    dpi = 300,
    width = 10,
    height = 6
  )

  # ----------------------------
  # Mixed models: base vs pre/post
  # ----------------------------
  model_base <- fit_base_model_constructive(condition_feed)
  model_prepost <- fit_prepost_model_constructive(condition_feed)

  fit <- compare_model_fit(model_base, model_prepost)
  write_model_fit_table(fit, file.path(cfg$output_dir, "model_fit_comparison_constructive.csv"))

  # ----------------------------
  # Contrasts (no baseline normalization)
  # ----------------------------
  ctr_prepost <- emmeans_contrasts_prepost(model_prepost)
  ctr_base <- emmeans_contrasts_base(model_base)

  write_contrasts_table(ctr_prepost, file.path(cfg$output_dir, "contrasts_prepost_constructive.csv"))
  write_contrasts_table(ctr_base, file.path(cfg$output_dir, "contrasts_base_constructive.csv"))

  p_ctr <- plot_prepost_contrasts_constructive(ctr_prepost)
  ggplot2::ggsave(
    filename = file.path(cfg$output_dir, "prepost_contrasts_constructive.png"),
    plot = p_ctr,
    dpi = 300,
    width = 8,
    height = 5
  )

  # ----------------------------
  # Optional: polynomial time models (constructiveness only)
  # ----------------------------
  poly_fit <- fit_polynomial_time_models_constructive(condition_feed, max_order = 3)
  write_polynomial_fit_table(poly_fit, file.path(cfg$output_dir, "polynomial_model_selection_constructive.csv"))

  invisible(
    list(
      cfg = cfg,
      data = condition_feed,
      model_base = model_base,
      model_prepost = model_prepost,
      fit = fit,
      contrasts = list(prepost = ctr_prepost, base = ctr_base),
      poly_fit = poly_fit
    )
  )
}

# Intentionally not auto-running `main()`.
# Run manually when ready:
result <- main()
