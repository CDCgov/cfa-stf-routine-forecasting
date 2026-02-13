script_packages <- c(
  "dplyr",
  "tidyr",
  "tibble",
  "readr",
  "stringr",
  "fs",
  "fable",
  "glue",
  "argparser",
  "rlang",
  "hewr",
  "forecasttools"
)

## load in packages without messages
purrr::walk(script_packages, \(pkg) {
  suppressPackageStartupMessages(
    library(pkg, character.only = TRUE)
  )
})

to_prop_forecast <- function(
  forecast_disease_count,
  forecast_other_count,
  disease_count_col = "observed_ed_visits",
  other_count_col = "other_ed_visits",
  output_col = "prop_disease_ed_visits"
) {
  result <- inner_join(
    forecast_disease_count,
    forecast_other_count,
    by = join_by(date, .draw)
  ) |>
    mutate(
      !!output_col := .data[[disease_count_col]] /
        (.data[[disease_count_col]] +
          .data[[other_count_col]])
    )

  return(result)
}


#' Fit and Forecast Time Series Data
#'
#' This function fits a combination ensemble model to the training data and
#' generates forecast samples for a specified number of days.
#'
#' @param data A data frame containing the time series data. It should have a
#' column named `data_type` to distinguish between training and other data.
#' @param n_forecast_days An integer specifying the number of days to forecast.
#' Default is 28.
#' @param n_samples An integer specifying the number of forecast samples to
#' generate. Default is 2000.
#' @param target_col A string specifying the name of the target column in the
#' data. Default is "ed_visits".
#' @param output_col A string specifying the name of the output column for the
#' forecasted values. Default is "other_ed_visits".
#
#' @return A tibble containing the forecast samples with columns for date,
#' draw number, and forecasted values.
fit_and_forecast_ensemble <- function(
  data,
  n_forecast_days = 28,
  n_samples = 2000,
  target_col = "ed_visits",
  output_col = "other_ed_visits"
) {
  forecast_horizon <- glue::glue("{n_forecast_days} days")
  target_sym <- rlang::sym(target_col)
  output_sym <- rlang::sym(output_col)

  offset <- 1

  fit <- data |>
    as_tsibble(index = date) |>
    filter(data_type == "train") |>
    model(
      comb_model = combination_ensemble(
        ETS(
          log(!!target_sym + !!offset) ~
            trend(
              method = c("N", "M", "A")
            )
        ),
        ARIMA(log(!!target_sym + !!offset))
      )
    )

  forecast_samples <- fit |>
    generate(h = forecast_horizon, times = n_samples) |>
    as_tibble() |>
    mutate(
      !!output_col := pmax(.data$.sim, 0), # clip values
      .draw = as.integer(.data$.rep)
    ) |>
    select("date", ".draw", all_of(output_col))

  if (any(forecast_samples[[output_col]] < 0)) {
    stop(glue::glue("Negative count forecast for {output_col}"))
  }

  forecast_samples
}

main <- function(
  model_dir,
  n_forecast_days = 28,
  n_samples = 2000,
  epiweekly = FALSE
) {
  training_data <- hewr::load_training_data(
    model_dir,
    "combined_data",
    epiweekly
  )

  target_and_other_data <- training_data$data
  geo_value <- training_data$geo_value
  disease <- training_data$disease
  resolution <- training_data$resolution

  ## Fit and forecast other (non-target-disease) ED visits using a combination
  ## ensemble model
  ts_ensemble_other_e <- fit_and_forecast_ensemble(
    target_and_other_data,
    n_forecast_days,
    n_samples,
    target_col = "other_ed_visits",
    output_col = "other_ed_visits"
  )

  ts_ensemble_count_e <- fit_and_forecast_ensemble(
    target_and_other_data,
    n_forecast_days,
    n_samples,
    target_col = "observed_ed_visits",
    output_col = "observed_ed_visits"
  )

  ts_ensemble_prop_e <- ts_ensemble_count_e |>
    to_prop_forecast(ts_ensemble_other_e)

  ts_ensemble_forecast_e <-
    ts_ensemble_prop_e |>
    hewr::format_timeseries_output(
      geo_value = geo_value,
      disease = disease,
      resolution = resolution,
      output_type_id = ".draw"
    )

  # Save the forecast
  write_tabular(
    ts_ensemble_forecast_e,
    path(
      model_dir,
      "samples",
      ext = "parquet"
    )
  )
}


p <- arg_parser(
  "Forecast other (non-target-disease) ED visits for a given location."
) |>
  add_argument(
    "--model-dir",
    help = "Directory containing the model data and output.",
  ) |>
  add_argument(
    "--n-forecast-days",
    help = "Number of days to forecast.",
    default = 28L
  ) |>
  add_argument(
    "--n-samples",
    help = "Number of samples to generate.",
    default = 2000L
  ) |>
  add_argument(
    "--epiweekly",
    help = "Whether to use epiweekly data.",
    flag = TRUE
  )

argv <- parse_args(p)
model_dir <- path(argv$model_dir)
n_forecast_days <- argv$n_forecast_days
n_samples <- argv$n_samples
epiweekly <- argv$epiweekly


main(
  model_dir,
  n_forecast_days = n_forecast_days,
  n_samples = n_samples,
  epiweekly = epiweekly
)
