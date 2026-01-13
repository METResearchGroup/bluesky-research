plot_time_series_constructive <- function(df, loess_span = 0.75) {
  ggplot2::ggplot(
    df,
    ggplot2::aes(x = .data$date, y = .data$feed_proportion_constructive, color = .data$condition, fill = .data$condition)
  ) +
    ggplot2::geom_jitter(color = "grey70", alpha = 0.12, size = 0.5, width = 0.2, height = 0.0) +
    ggplot2::geom_smooth(
      method = "loess",
      se = TRUE,
      method.args = list(span = loess_span, control = stats::loess.control(surface = "direct")),
      linewidth = 1.0,
      alpha = 0.18
    ) +
    ggplot2::scale_color_manual(
      values = c(
        "Reverse Chronological" = "black",
        "Engagement-Based" = "#D55E00",
        "Diversified Extremity" = "#009E73"
      )
    ) +
    ggplot2::scale_fill_manual(
      values = c(
        "Reverse Chronological" = "black",
        "Engagement-Based" = "#D55E00",
        "Diversified Extremity" = "#009E73"
      )
    ) +
    ggplot2::scale_y_continuous(
      name = "Average constructiveness (per user-day)",
      labels = scales::label_number(accuracy = 0.01)
    ) +
    ggplot2::labs(
      title = "Constructiveness over time by feed condition",
      x = "Date",
      color = "Condition",
      fill = "Condition"
    ) +
    ggplot2::theme_classic(base_size = 12) +
    ggplot2::theme(
      legend.position = "bottom",
      plot.title = ggplot2::element_text(face = "bold")
    )
}

plot_prepost_contrasts_constructive <- function(contrasts_df) {
  # contrasts_df: output of emmeans_contrasts_prepost()
  ggplot2::ggplot(
    contrasts_df,
    ggplot2::aes(x = .data$period, y = .data$estimate, color = .data$contrast)
  ) +
    ggplot2::geom_hline(yintercept = 0, linetype = "dotted", linewidth = 0.7, color = "black") +
    ggplot2::geom_errorbar(
      ggplot2::aes(ymin = .data$lower.CL, ymax = .data$upper.CL),
      width = 0.16,
      linewidth = 0.9,
      position = ggplot2::position_dodge(width = 0.48)
    ) +
    ggplot2::geom_point(size = 2.6, position = ggplot2::position_dodge(width = 0.48)) +
    ggplot2::scale_color_manual(
      name = NULL,
      values = c(
        "EB vs RC" = "#D55E00",
        "DE vs RC" = "#009E73",
        "EB vs DE" = "#0072B2"
      )
    ) +
    ggplot2::labs(
      title = "Pre-/post-election marginal effects (constructiveness)",
      subtitle = "Estimated contrasts with 95% CIs (mixed model; random intercept per handle)",
      x = NULL,
      y = "Marginal effect on constructiveness"
    ) +
    ggplot2::theme_classic(base_size = 12) +
    ggplot2::theme(
      legend.position = "bottom",
      plot.title = ggplot2::element_text(face = "bold")
    )
}


