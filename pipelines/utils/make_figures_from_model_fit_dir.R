library(forecasttools)
library(ggplot2)
library(dplyr)
library(ggdist)
library(fs)
library(cowplot)
library(hewr)
library(argparser)
library(tidyr)
library(purrr)

get_ci <- function(model_fit_dir, save = FALSE) {
  ci_path <- path(model_fit_dir, "ci", ext = "parquet")

  ci <- if (file_exists(ci_path)) {
    read_tabular(ci_path)
  } else {
    cli::cli_alert("CI file not found at: {ci_path}")
    samples_path <- path(model_fit_dir, "samples", ext = "parquet")
    if (!file_exists(samples_path)) {
      cli::cli_abort("Samples file not found at: {samples_path}")
    }
    ci_widths <- c(0.5, 0.8, 0.95)
    cli::cli_alert("Creating CI from samples at: {samples_path}")
    read_tabular(samples_path) |>
      dplyr::select(-tidyselect::any_of(c(".chain", ".iteration", ".draw"))) |>
      dplyr::group_by(dplyr::across(-".value")) |>
      ggdist::median_qi(.width = ci_widths)
  }
  if (save) {
    write_tabular(ci, ci_path)
  }
  ci
}

make_forecast_figure <- function(
  for_plotting_tbl,
  dat,
  ci,
  y_transform = "identity"
) {
  .variable <- for_plotting_tbl$.variable
  disease <- for_plotting_tbl$disease
  geo_value <- for_plotting_tbl$geo_value

  disease_name_pretty <- c(
    "COVID-19" = "COVID-19",
    "Influenza" = "Flu",
    "RSV" = "RSV"
  )[[disease]]

  parsed_variable_name <- parse_variable_name(.variable)
  y_axis_labels <- parsed_variable_name$y_axis_labels
  y_axis_name <- parsed_variable_name$full_name
  core_name <- parsed_variable_name$core_name

  title_prefix <- ifelse(
    stringr::str_starts(.variable, "other"),
    "Other",
    disease_name_pretty
  )

  title <- glue::glue("{title_prefix} {core_name} in {geo_value}")

  tmp_ci <- left_join(for_plotting_tbl, ci)
  facet_componenet <- if (n_distinct(tmp_ci[["lab_site_index"]]) > 1) {
    facet_wrap(~lab_site_index, scales = "free_y")
  } else {
    NULL
  }
  # should bake in dat resolution to dat
  # should bake in disease name to dat
  tmp_dat <- left_join(for_plotting_tbl, dat)

  # need to add the variable
  # maybe bake this into the data
  ggplot(mapping = aes(x = date, y = .value)) +
    facet_componenet +
    ggdist::geom_lineribbon(
      data = tmp_ci,
      mapping = ggplot2::aes(ymin = .data$.lower, ymax = .data$.upper),
      color = "#08519c",
      key_glyph = ggplot2::draw_key_polygon,
      step = "mid"
    ) +
    geom_point(data = tmp_dat, aes(color = data_type)) +
    ggplot2::scale_x_date("Date") +
    ggplot2::scale_y_continuous(
      name = y_axis_name,
      labels = y_axis_labels,
      transform = y_transform
    ) +
    ggplot2::scale_fill_brewer(
      name = "Credible Interval Width",
      labels = ~ scales::label_percent()(as.numeric(.))
    ) +
    ggplot2::scale_color_manual(
      name = "Data Type",
      values = c("olivedrab1", "deeppink"),
      labels = stringr::str_to_title
    ) +
    ggplot2::ggtitle(title) +
    cowplot::theme_minimal_grid() +
    ggplot2::theme(
      legend.position = "bottom",
      legend.direction = "vertical",
      legend.justification = "center"
    )
}

make_forecast_figure_from_model_fit_dir <- function(
  model_fit_dir,
  save_ci = FALSE,
  save_figs = TRUE
) {
  model_name <- path_file(model_fit_dir)
  ci <- get_ci(model_fit_dir, save = save_ci)

  dat_path <- path(model_fit_dir, "data", "combined_data", ext = "tsv")
  dat <- read_tabular(dat_path)

  fig_tbl <- ci |>
    distinct(geo_value, disease, resolution, .variable) |>
    expand_grid(y_transform = c("identity", "log10")) |>
    filter(!(.variable == "site_level_log_ww_conc" & y_transform == "log10")) |>
    rowwise() |>
    mutate(
      fig = list(make_forecast_figure(
        pick(everything(), -y_transform),
        dat,
        ci,
        y_transform = y_transform
      ))
    ) |>
    ungroup()

  if (save_figs) {
    fig_dir <- path(model_fit_dir, "figures")
    dir_create(fig_dir)

    fig_save_tbl <- fig_tbl |>
      mutate(fig_name = glue::glue("{model_name}_{.variable}_{y_transform}")) |>
      mutate(
        fig_path = path(model_fit_dir, "figures", fig_name, ext = "pdf")
      ) |>
      select(fig, fig_path)

    walk2(
      fig_save_tbl$fig,
      fig_save_tbl$fig_path,
      \(fig, fig_path) {
        save_plot(
          filename = fig_path,
          plot = fig,
          device = cairo_pdf,
          base_height = 6
        )
      }
    )
  }
  invisible(fig_tbl)
}


p <- arg_parser(
  "Generate forecast figures from model fit directory"
) |>
  add_argument(
    "model-fit-dir",
    help = "Directory containing the model data and output.",
  ) |>
  add_argument(
    "--save-ci",
    help = "Whether to save credible intervals to disk.",
    flag = TRUE
  ) |>
  add_argument(
    "--save-figs",
    help = "Whether to save figures to disk.",
    flag = TRUE
  )

argv <- parse_args(p)

make_forecast_figure_from_model_fit_dir(
  model_fit_dir = argv$model_fit_dir,
  save_figs = argv$save_figs,
  save_ci = argv$save_ci
)
