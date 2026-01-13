write_model_fit_table <- function(fit, out_path) {
  # fit: output of compare_model_fit()
  readr::write_csv(fit$comparison_table, out_path)
  invisible(out_path)
}

write_contrasts_table <- function(df, out_path) {
  readr::write_csv(df, out_path)
  invisible(out_path)
}

write_polynomial_fit_table <- function(poly_fit, out_path) {
  readr::write_csv(poly_fit$fit_stats, out_path)
  invisible(out_path)
}


