fit_base_model_constructive <- function(df) {
  stats::as.formula("feed_proportion_constructive ~ condition + (1 | handle)") |>
    lmerTest::lmer(data = df)
}

fit_prepost_model_constructive <- function(df) {
  stats::as.formula("feed_proportion_constructive ~ condition * is_post_election + (1 | handle)") |>
    lmerTest::lmer(data = df)
}

compare_model_fit <- function(model_base, model_prepost) {
  comparison <- tibble::tibble(
    Model = c("Base (Condition Only)", "Pre/Post Election"),
    AIC = c(AIC(model_base), AIC(model_prepost)),
    BIC = c(BIC(model_base), BIC(model_prepost)),
    LogLik = c(as.numeric(logLik(model_base)), as.numeric(logLik(model_prepost))),
    df = c(attr(logLik(model_base), "df"), attr(logLik(model_prepost), "df"))
  ) |>
    dplyr::mutate(
      delta_AIC = .data$AIC - min(.data$AIC),
      delta_BIC = .data$BIC - min(.data$BIC)
    )

  lrt <- suppressWarnings(anova(model_base, model_prepost))

  list(
    comparison_table = comparison,
    lr_test = lrt,
    aic_winner = comparison$Model[which.min(comparison$AIC)],
    bic_winner = comparison$Model[which.min(comparison$BIC)]
  )
}

# Optional: polynomial time models (useful to check sensitivity to time trends).
fit_polynomial_time_models_constructive <- function(df, max_order = 3) {
  stopifnot(max_order >= 0)

  models <- list()
  fit_stats <- tibble::tibble(
    order = 0:max_order,
    model_type = c("Base (condition × day)", "Linear", "Quadratic", "Cubic")[1:(max_order + 1)],
    AIC = NA_real_,
    BIC = NA_real_,
    LogLik = NA_real_,
    df = NA_real_
  )

  for (i in 0:max_order) {
    formula_text <- if (i == 0) {
      "feed_proportion_constructive ~ condition * day + (1 | handle)"
    } else {
      sprintf("feed_proportion_constructive ~ condition * poly(day, %d) + (1 | handle)", i)
    }

    mod <- lmerTest::lmer(stats::as.formula(formula_text), data = df)
    models[[paste0("order_", i)]] <- mod

    fit_stats$AIC[i + 1] <- AIC(mod)
    fit_stats$BIC[i + 1] <- BIC(mod)
    fit_stats$LogLik[i + 1] <- as.numeric(logLik(mod))
    fit_stats$df[i + 1] <- attr(logLik(mod), "df")
  }

  fit_stats <- dplyr::mutate(
    fit_stats,
    delta_AIC = .data$AIC - min(.data$AIC),
    delta_BIC = .data$BIC - min(.data$BIC),
    best_AIC = .data$delta_AIC == 0,
    best_BIC = .data$delta_BIC == 0
  )

  list(models = models, fit_stats = fit_stats)
}


