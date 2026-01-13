recode_condition_labels <- function(df) {
  # Standardize condition display names. Input values expected (from CSV):
  # - reverse_chronological
  # - representative_diversification
  # - engagement
  dplyr::mutate(
    df,
    condition = dplyr::recode(
      .data$condition,
      "reverse_chronological" = "Reverse Chronological",
      "representative_diversification" = "Diversified Extremity",
      "engagement" = "Engagement-Based",
      .default = as.character(.data$condition)
    )
  )
}

add_time_columns <- function(df) {
  df <- dplyr::mutate(df, date = as.Date(.data$date, format = "%Y-%m-%d"))

  min_date <- min(df$date, na.rm = TRUE)
  max_date <- max(df$date, na.rm = TRUE)
  midpoint_date <- min_date + (max_date - min_date) / 2

  dplyr::mutate(
    df,
    day = as.numeric(.data$date - min_date),
    day_centered = as.numeric(.data$date - midpoint_date)
  )
}

add_prepost_columns <- function(df, election_date) {
  dplyr::mutate(
    df,
    is_post_election = as.numeric(.data$date > election_date),
    period = factor(
      dplyr::if_else(.data$date > election_date, "Post", "Pre"),
      levels = c("Pre", "Post")
    )
  )
}

filter_to_valid_handles <- function(df, valid_handles) {
  dplyr::filter(df, .data$handle %in% valid_handles)
}


