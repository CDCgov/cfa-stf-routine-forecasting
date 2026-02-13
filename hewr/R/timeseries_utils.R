#' Utilities for timeseries forecasting scripts in the cfa-stf-routine-forecasting
#' pipeline.

#' Load Training Data for Timeseries Forecasting
#'
#' Loads and preprocesses training data for timeseries
#' forecasting models in the format expected by
#' the forecasting functions.
#'
#' @param model_dir Path to the
#' directory containing model run data.
#' @param base_data_name Base name of the data file to load.
#' @param epiweekly Logical. Indicate epiweekly (TRUE) or daily (FALSE) data.
#'   Default `FALSE`.
#'
#' @return A list with:
#' `data` (processed training data tibble),
#' `geo_value`,
#' `disease`,
#' `resolution` ("daily" or "epiweekly"), and
#' `prefix` (file prefix based on resolution).
#' @export
load_training_data <- function(
  model_dir,
  data_name = "combined_data",
  epiweekly = FALSE
) {
  resolution <- dplyr::if_else(epiweekly, "epiweekly", "daily")
  data_path <- fs::path(model_dir, "data", data_name, ext = "tsv")

  target_and_other_data <- readr::read_tsv(
    data_path,
    col_types = readr::cols(
      date = readr::col_date(),
      geo_value = readr::col_character(),
      disease = readr::col_character(),
      data_type = readr::col_character(),
      .variable = readr::col_character(),
      .value = readr::col_double()
    )
  ) |>
    dplyr::filter(.data$data_type == "train") |>
    dplyr::select(-"lab_site_index") |>
    dplyr::filter(stringr::str_ends(.data$.variable, "ed_visits")) |>
    tidyr::pivot_wider(names_from = ".variable", values_from = ".value")

  list(
    data = target_and_other_data,
    geo_value = target_and_other_data$geo_value[1],
    disease = target_and_other_data$disease[1],
    resolution = resolution
  )
}

#' Process and format timeseries forecasts into
#' a standardized format for downstream processing.
#'
#' @param forecast_data A data frame containing forecast results with date,
#'   output type identifier, and forecast variables.
#' @param geo_value Geographic identifier for the forecast location.
#' @param disease Disease name for the forecast.
#' @param resolution Temporal resolution ("daily" or "epiweekly").
#' @param output_type_id Output type identifiers
#' (e.g., ".draw" for samples, "quantile_level" for quantiles).
#'
#' @return A tibble with columns:
#' `date`,
#' `geo_value`,
#' `disease`,
#' `resolution`,
#' `aggregated_numerator`,
#' `aggregated_denominator`,
#' `.variable`,
#' `output_type_id`, and
#' `.value`.
#' @export
format_timeseries_output <- function(
  forecast_data,
  geo_value,
  disease,
  resolution,
  output_type_id
) {
  forecast_data |>
    tidyr::pivot_longer(
      -c("date", tidyselect::all_of(output_type_id)),
      names_to = ".variable",
      values_to = ".value"
    ) |>
    dplyr::mutate(
      geo_value = geo_value,
      disease = disease,
      resolution = resolution,
      aggregated_numerator = FALSE,
      aggregated_denominator = dplyr::if_else(
        stringr::str_starts(.data$.variable, "prop_"),
        FALSE,
        NA
      )
    ) |>
    dplyr::select(
      "date",
      "geo_value",
      "disease",
      "resolution",
      "aggregated_numerator",
      "aggregated_denominator",
      ".variable",
      tidyselect::all_of(output_type_id),
      ".value"
    )
}
