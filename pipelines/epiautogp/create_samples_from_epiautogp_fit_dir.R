# this is a workaround for julia not being able to save dates in parquet format
# see https://github.com/CDCgov/cfa-stf-routine-forecasting/issues/898
library(argparser)

required_columns_e <- c(
  ".chain",
  ".iteration",
  ".draw",
  "date",
  "geo_value",
  "disease",
  ".variable",
  ".value",
  "resolution"
)

create_samples_from_epiautogp_fit_dir <- function(model_fit_dir) {
  model_name <- fs::path_file(model_fit_dir)

  # Read EpiAutoGP samples
  samples_path <- fs::path(model_fit_dir, "samples_raw", ext = "parquet")
  epiautogp_samples <- forecasttools::read_tabular(samples_path) |>
    dplyr::mutate(
      date = lubridate::as_date(.data$date)
    ) |>
    dplyr::select(tidyselect::any_of(required_columns_e))

  forecasttools::write_tabular(
    epiautogp_samples,
    fs::path(
      model_fit_dir,
      "samples",
      ext = "parquet"
    )
  )
}

p <- arg_parser(
  "Create samples file from an EpiAutoGP model fit directory."
) |>
  add_argument(
    "model-fit-dir",
    help = "Directory containing the model data and output.",
  )

argv <- parse_args(p)

create_samples_from_epiautogp_fit_dir(model_fit_dir = argv$model_fit_dir)
