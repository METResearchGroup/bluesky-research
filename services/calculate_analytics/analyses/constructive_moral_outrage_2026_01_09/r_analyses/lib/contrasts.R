contrast_defs_threeway <- function() {
  # Levels must match the factor levels in the fitted model matrix.
  lvl_rc <- "Reverse Chronological"
  lvl_eb <- "Engagement-Based"
  lvl_de <- "Diversified Extremity"

  list(
    levels = c(lvl_rc, lvl_eb, lvl_de),
    defs = list(
      "EB vs RC" = stats::setNames(c(-1, 1, 0), c(lvl_rc, lvl_eb, lvl_de)),
      "DE vs RC" = stats::setNames(c(-1, 0, 1), c(lvl_rc, lvl_eb, lvl_de)),
      "EB vs DE" = stats::setNames(c(0, 1, -1), c(lvl_rc, lvl_eb, lvl_de))
    )
  )
}

emmeans_contrasts_prepost <- function(model_prepost) {
  cd <- contrast_defs_threeway()

  emm <- emmeans::emmeans(
    model_prepost,
    ~ condition | is_post_election,
    at = list(is_post_election = c(0, 1)),
    re.form = NA
  )

  contr <- emmeans::contrast(emm, method = cd$defs, by = "is_post_election")
#   emmeans::summary(contr, infer = c(TRUE, TRUE), df = Inf) |>
  summary(contr, infer = c(TRUE, TRUE), df = Inf) |>
    as.data.frame() |>
    dplyr::mutate(
      period = dplyr::if_else(.data$is_post_election == 0, "Pre", "Post"),
      period = factor(.data$period, levels = c("Pre", "Post"))
    ) |>
    dplyr::select(.data$period, .data$contrast, .data$estimate, .data$SE, .data$lower.CL, .data$upper.CL, .data$p.value)
}

emmeans_contrasts_base <- function(model_base) {
  cd <- contrast_defs_threeway()

  emm <- emmeans::emmeans(model_base, ~ condition, re.form = NA)
  contr <- emmeans::contrast(emm, method = cd$defs)
#   emmeans::summary(contr, infer = c(TRUE, TRUE), df = Inf) |>
  summary(contr, infer = c(TRUE, TRUE), df = Inf) |>
    as.data.frame() |>
    dplyr::select(.data$contrast, .data$estimate, .data$SE, .data$lower.CL, .data$upper.CL, .data$p.value)
}
