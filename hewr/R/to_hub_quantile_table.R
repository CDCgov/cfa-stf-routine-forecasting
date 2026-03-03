var_to_target <- function(variable, disease) {
  disease_abbr <- dplyr::case_match(
    disease,
    "Influenza" ~ "flu",
    "COVID-19" ~ "covid",
    "RSV" ~ "rsv",
    .default = disease
  )
  dplyr::case_match(
    variable,
    "observed_hospital_admissions" ~ glue::glue("inc {disease_abbr} hosp"),
    "observed_ed_visits" ~ glue::glue("inc {disease_abbr} ed visits"),
    "other_ed_visits" ~ glue::glue("inc other ed visits"),
    "prop_disease_ed_visits" ~ glue::glue("inc {disease_abbr} prop ed visits"),
    .default = variable
  )
}


#' Create a hubverse-format forecast quantile table
#'
#' @param model_fit_dir Model fit directory containing samples and/or quantiles
#'
#' @returns A hubverse quantile table
#' @export
#' @name to_hub_quantile_table
model_fit_dir_to_hub_q_tbl <- function(model_fit_dir) {
  model_runs_path <- path_up_to(model_fit_dir, "model_runs")
  model_batch_dir <- fs::path_dir(model_runs_path)
  batch_params <- parse_model_batch_dir_path(model_batch_dir)
  disease <- batch_params$disease
  report_date <- batch_params$report_date

  samples_paths <- fs::dir_ls(
    model_fit_dir,
    recurse = TRUE,
    glob = "*samples.parquet"
  )
  quantiles_paths <- fs::dir_ls(
    model_fit_dir,
    recurse = TRUE,
    glob = "*_quantiles_*.parquet"
  )

  quantilized_samples_forecast <- samples_paths |>
    purrr::map(forecasttools::read_tabular) |>
    purrr::map(\(x) {
      forecasttools::trajectories_to_quantiles(
        x,
        timepoint_cols = "date",
        value_col = ".value",
        id_cols = setdiff(
          colnames(x),
          c(
            "date",
            ".value",
            ".draw",
            ".chain",
            ".iteration"
          )
        )
      )
    }) |>
    tibble::enframe(name = "file_path", value = "data") |>
    dplyr::mutate(
      model_id = .data$file_path |>
        fs::path_dir() |>
        fs::path_file()
    ) |>
    dplyr::select("model_id", "data")

  quantiles_forecast <- quantiles_paths |>
    purrr::map(\(x) {
      forecasttools::read_tabular(x) |>
        dplyr::rename("quantile_value" = ".value")
    }) |>
    tibble::enframe(name = "file_path", value = "data") |>
    dplyr::mutate(
      model_id = .data$file_path |>
        fs::path_dir() |>
        fs::path_file()
    ) |>
    dplyr::select("model_id", "data")

  forecast_data <-
    dplyr::bind_rows(
      quantilized_samples_forecast,
      quantiles_forecast
    ) |>
    tidyr::unnest("data") |>
    dplyr::mutate(
      target_prefix = dplyr::if_else(
        .data$resolution == "epiweekly",
        "wk ",
        ""
      )
    ) |>
    dplyr::mutate(target_core = var_to_target(.data$.variable, disease)) |>
    dplyr::mutate(
      target = stringr::str_c(
        .data$target_prefix,
        .data$target_core
      )
    ) |>
    dplyr::mutate(reference_date = report_date) |>
    dplyr::mutate(horizon_timescale = "days") |>
    dplyr::mutate(
      horizon = forecasttools::horizons_from_target_end_dates(
        reference_date = .data$reference_date,
        horizon_timescale = .data$horizon_timescale,
        target_end_dates = .data$date
      )
    ) |>
    dplyr::mutate(
      output_type = "quantile",
      output_type_id = round(.data$quantile_level, digits = 4)
    ) |>
    dplyr::select(
      "model_id",
      "output_type",
      "output_type_id",
      value = "quantile_value",
      "reference_date",
      "target",
      "horizon",
      "horizon_timescale",
      "resolution",
      target_end_date = "date",
      location = "geo_value",
      "disease"
    )

  forecast_data
}

#' @param model_loc_dir Model location directory containing model fits
#' @export
#' @rdname to_hub_quantile_table
model_loc_dir_to_hub_q_tbl <- function(model_loc_dir) {
  fs::dir_ls(model_loc_dir, type = "directory") |>
    fs::path_filter(regexp = "data$", invert = TRUE) |>
    purrr::map(model_fit_dir_to_hub_q_tbl) |>
    dplyr::bind_rows()
}

#' @param model_runs_dir Model runs directory containing model locations
#' @export
#' @rdname to_hub_quantile_table
model_runs_dir_to_hub_q_tbl <- function(model_runs_dir) {
  model_runs_dir |>
    fs::dir_ls(type = "directory") |>
    purrr::map(model_loc_dir_to_hub_q_tbl) |>
    dplyr::bind_rows()
}
