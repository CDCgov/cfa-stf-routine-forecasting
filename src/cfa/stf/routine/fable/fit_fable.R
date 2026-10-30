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
  "stfroutineforecasting",
  "forecasttools",
  "urca"
)

## load in packages without messages
purrr::walk(script_packages, \(pkg) {
  suppressPackageStartupMessages(
    library(pkg, character.only = TRUE)
  )
})


#' Fit and Forecast Time Series Data
#'
#' This function fits a combination ensemble model to the training data and
#' generates forecast samples for a specified number of days.
#'
#' @details
#' This function is hardened against missing data in two ways:
#'
#' 1. Checks for missing daily dates and adds them to the data with `NA`
#' values if missing.
#' 2. Checks for missing values in the target column and imputes
#' them using an ARIMA Kalman smoother if necessary as per
#' [standard practise](https://otexts.com/fpp3/practical.html#missing-values)
#'
#' @param data A data frame containing only the training time series data.
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

  offset <- 1

  series <- data |>
    as_tsibble(index = date) |>
    tsibble::fill_gaps()
  if (all(is.na(series[[target_col]]))) {
    stop(glue::glue("All training values of {target_col} are missing"))
  }
  if (anyNA(series[[target_col]])) {
    series <- series |>
      model(ARIMA(log(!!target_sym + !!offset))) |>
      interpolate(series)
  }

  fit <- series |>
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

  if (
    !all(is.finite(forecast_samples[[output_col]])) ||
      any(forecast_samples[[output_col]] < 0)
  ) {
    stop(glue::glue("Non-finite or negative count forecast for {output_col}"))
  }

  attr(forecast_samples, "fitted") <- augment(fit)
  attr(forecast_samples, "components") <- "ETS + ARIMA"
  forecast_samples
}

main <- function(
  model_dir,
  n_forecast_days = 28,
  n_samples = 2000
) {
  training_data <- stfroutineforecasting::load_training_data(
    model_dir,
    "combined_data"
  )

  target_and_other_data <- training_data$data
  geo_value <- training_data$geo_value
  disease <- training_data$disease
  resolution <- training_data$resolution

  ## Fit and forecast other (non-target-disease) ED visits using a combination
  ## ensemble model
  fable_e_other <- fit_and_forecast_ensemble(
    target_and_other_data,
    n_forecast_days,
    n_samples,
    target_col = "other_ed_visits",
    output_col = "other_ed_visits"
  )

  readr::write_tsv(
    attr(fable_e_other, "fitted"),
    fs::path(model_dir, "fitted.tsv")
  )
  readr::write_lines(
    attr(fable_e_other, "components"),
    fs::path(model_dir, "components.txt")
  )

  fable_e_other_forecast <-
    fable_e_other |>
    stfroutineforecasting::format_timeseries_output(
      geo_value = geo_value,
      disease = disease,
      resolution = resolution,
      output_type_id = ".draw"
    )

  # Save the forecast
  write_tabular(
    fable_e_other_forecast,
    path(
      model_dir,
      "samples",
      ext = "parquet"
    )
  )
}


run_cli <- function() {
  p <- arg_parser(
    "Forecast other (non-target-disease) ED visits for a given location."
  ) |>
    add_argument(
      "--model-dir",
      help = "Directory containing the model data and output."
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
    )

  argv <- parse_args(p)
  model_dir <- path(argv$model_dir)
  n_forecast_days <- argv$n_forecast_days
  n_samples <- argv$n_samples

  main(
    model_dir,
    n_forecast_days = n_forecast_days,
    n_samples = n_samples
  )
}

if (sys.nframe() == 0L) {
  run_cli()
}
