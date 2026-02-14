#' Make Forecast Figure
#'
#' @param dat a data frame containing the data to be plotted
#' @param geo_value character matching the geo_value column in `dat` and `ci`
#' @param disease character matching the disease column in `dat` and `ci`
#' @param .variable character matching the .variable column in `dat` and `ci`
#' @param resolution character matching the resolution column in `dat` and `ci`
#' @param aggregated_numerator character matching the aggregated_numerator
#' column in `dat` and `ci`
#' @param aggregated_denominator character matching the aggregated_denominator
#' column in `dat` and `ci`
#' @param ci a data frame containing the credible intervals to be plotted
#' @param data_vintage_date date that the data was collected
#' @param y_transform a character passed as the transform argument to
#' [ggplot2::scale_y_continuous()].
#' @param highlight_dates a vector of dates to highlight on the plot
#' @param highlight_labels a vector of labels to display at the
#' `highlight_dates`
#' @param display_cutpoints a logical indicating whether to include cutpoints
#' relevant to the `target_variable`.
#' @param max_lab_site_index an integer indicating the maximum lab site index to
#' plot. Default is 5.
#'
#' @return a ggplot object
#' @export
make_forecast_figure <- function(
  dat,
  geo_value,
  disease,
  .variable,
  resolution,
  aggregated_numerator,
  aggregated_denominator,
  y_transform,
  ci,
  data_vintage_date,
  highlight_dates = NULL,
  highlight_labels = NULL,
  display_cutpoints = TRUE,
  max_lab_site_index = 5
) {
  tbl_for_join <- tibble::tibble(
    geo_value = geo_value,
    disease = disease,
    .variable = .variable,
    resolution = resolution,
    aggregated_numerator = aggregated_numerator,
    aggregated_denominator = aggregated_denominator
  )
  # Join is used because NA == NA evaluates to NA

  fig_ci <- dplyr::left_join(
    tbl_for_join,
    ci,
    by = c(
      "geo_value",
      "disease",
      ".variable",
      "resolution",
      "aggregated_numerator",
      "aggregated_denominator"
    )
  )

  fig_dat <- dplyr::left_join(
    tbl_for_join |>
      dplyr::select(-tidyselect::starts_with("agg")),
    dat,
    by = c("geo_value", "disease", ".variable", "resolution")
  )

  disease_name_pretty <- c(
    "COVID-19" = "COVID-19",
    "Influenza" = "Flu",
    "RSV" = "RSV"
  )[[disease]]

  parsed_variable_name <- parse_variable_name(.variable)

  y_axis_label <- parsed_variable_name[["full_name"]]
  y_axis_labels <- parsed_variable_name[["y_axis_labels"]]
  core_name <- parsed_variable_name[["core_name"]]

  title_prefix <- ifelse(
    stringr::str_starts(.variable, "other"),
    "Other",
    disease_name_pretty
  )

  title <- glue::glue("{title_prefix} {core_name} in {geo_value}")

  last_training_date <- dat |>
    dplyr::filter(.data$data_type == "train") |>
    dplyr::pull(date) |>
    max()

  lineribbon_dat <- fig_ci

  point_dat <- fig_dat |>
    dplyr::filter(.data$date <= max(lineribbon_dat$date)) |>
    dplyr::mutate(data_type = forcats::fct_rev(.data$data_type)) |>
    dplyr::arrange(dplyr::desc(.data$data_type))

  ## Processing for wastewater plots
  if (.variable == "site_level_log_ww_conc") {
    lineribbon_dat <- lineribbon_dat |>
      dplyr::filter(.data$lab_site_index <= max_lab_site_index)

    point_dat <- point_dat |>
      dplyr::filter(.data$lab_site_index <= max_lab_site_index)

    facet_components <- ggplot2::facet_wrap(~lab_site_index)
  } else {
    facet_components <- list()
  }

  ## Processing for proportion plots
  if (
    display_cutpoints &&
      .variable == "prop_disease_ed_visits"
  ) {
    max_y <- max(lineribbon_dat$.upper, point_dat$.value)

    full_prism_cutpoints <- forecasttools::get_prism_cutpoints(
      geo_value,
      disease
    ) |>
      unlist() |>
      utils::head(-1) |>
      utils::tail(-1)

    prism_df <-
      full_prism_cutpoints |>
      tibble::enframe(
        name = "category",
        value = "cutpoint"
      ) |>
      dplyr::mutate(
        "category" = .data$category |>
          stringr::str_remove("^prop_") |>
          stringr::str_replace_all("_", " ") |>
          stringr::str_to_title()
      ) |>
      dplyr::filter(.data$cutpoint <= max_y)

    cutpoint_plot_components <- list(
      ggplot2::geom_hline(
        data = prism_df,
        mapping = ggplot2::aes(
          yintercept = .data$cutpoint,
          color = .data$category
        ),
        linetype = "solid",
        linewidth = 1
      ),
      forecasttools::scale_color_prism("PRISM Category"),
      ggnewscale::new_scale_color()
    )
  } else {
    cutpoint_plot_components <- list()
  }

  # Processing for highlight dates
  if (!is.null(highlight_dates)) {
    highlight_components <-
      list(
        ggplot2::geom_vline(
          xintercept = highlight_dates,
          linetype = "dashed"
        ),
        ggplot2::annotate(
          geom = "text",
          x = highlight_dates,
          y = -Inf,
          label = stringr::str_c(highlight_labels, "\n"),
          vjust = "bottom"
        )
      )
  } else {
    highlight_components <- list()
  }

  forecast_highlight_components <-
    list(
      ggplot2::geom_vline(
        xintercept = last_training_date,
        linetype = "dashed"
      ),
      ggplot2::annotate(
        geom = "text",
        x = last_training_date,
        y = -Inf,
        label = "Fit Period \u2190\n",
        hjust = "right",
        vjust = "bottom"
      ),
      ggplot2::annotate(
        geom = "text",
        x = last_training_date,
        y = -Inf,
        label = "\u2192 Forecast Period\n",
        hjust = "left",
        vjust = "bottom",
      )
    )

  ggplot2::ggplot(mapping = ggplot2::aes(.data$date, .data$.value)) +
    cutpoint_plot_components +
    ggdist::geom_lineribbon(
      data = lineribbon_dat,
      mapping = ggplot2::aes(ymin = .data$.lower, ymax = .data$.upper),
      color = "#08519c",
      key_glyph = ggplot2::draw_key_polygon,
      step = "mid"
    ) +
    ggplot2::scale_fill_brewer(
      name = "Credible Interval Width",
      labels = ~ scales::label_percent()(as.numeric(.))
    ) +
    ggplot2::geom_point(
      mapping = ggplot2::aes(color = .data$data_type),
      size = 1.5,
      data = point_dat
    ) +
    ggplot2::scale_color_manual(
      name = "Data Type",
      values = c("olivedrab1", "deeppink"),
      labels = stringr::str_to_title
    ) +
    forecast_highlight_components +
    highlight_components +
    ggplot2::ggtitle(
      title,
      subtitle = glue::glue("as of {data_vintage_date}")
    ) +
    ggplot2::scale_y_continuous(
      y_axis_label,
      labels = y_axis_labels,
      transform = y_transform
    ) +
    ggplot2::scale_x_date("Date") +
    facet_components +
    cowplot::theme_minimal_grid() +
    ggplot2::theme(
      legend.position = "bottom",
      legend.direction = "vertical",
      legend.justification = "center"
    )
}
