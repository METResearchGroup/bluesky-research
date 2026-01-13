read_condition_feed_constructiveness <- function(path) {
  # Expected columns in per_user_per_day_average_constructiveness.csv:
  # - handle
  # - condition
  # - date
  # - feed_average_constructiveness
  df <- readr::read_csv(path, show_col_types = FALSE)

  required <- c("handle", "condition", "date", "feed_average_constructiveness")
  missing <- setdiff(required, names(df))
  if (length(missing) > 0) {
    stop(
      sprintf(
        "Condition feed CSV is missing required columns: %s",
        paste(missing, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  # Normalize column name to match the naming convention used by the original analyses.
  dplyr::rename(df, feed_proportion_constructive = feed_average_constructiveness)
}

read_valid_handles <- function(path) {
  df <- readr::read_csv(path, show_col_types = FALSE)
  if (!("handle" %in% names(df))) {
    stop("Valid users CSV must contain a `handle` column.", call. = FALSE)
  }
  unique(df$handle)
}

ensure_output_dir <- function(path, overwrite = TRUE) {
  if (dir.exists(path) && overwrite) {
    # Keep it simple: don't delete the directory; just ensure it exists.
    # Individual files are overwritten by ggsave/write_csv calls.
    return(invisible(path))
  }
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}


