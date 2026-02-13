library(argparser)
library(hewr)

variable_resolution_key <-
  c(
    "observed_ed_visits" = "daily",
    "other_ed_visits" = "daily",
    "observed_hospital_admissions" = "epiweekly",
    "site_level_log_ww_conc" = "daily"
  )

# put this list in hewr
required_columns_e <- c(
  ".chain",
  ".iteration",
  ".draw",
  "date",
  "geo_value",
  "disease",
  ".variable",
  ".value",
  "resolution",
  "aggregated_numerator",
  "aggregated_denominator"
)

create_samples_from_pyrenew_fit_dir <- function(model_fit_dir) {
  pyrenew_model_name <- fs::path_file(model_fit_dir)
  pyrenew_model_components <- parse_pyrenew_model_name(pyrenew_model_name)

  if (pyrenew_model_components["w"]) {
    required_columns <- c(required_columns_e, "lab_site_index")
  } else {
    required_columns <- required_columns_e
  }

  model_run_dir <- fs::path_dir(model_fit_dir)
  model_info <- parse_model_run_dir_path(model_run_dir)

  pyrenew_posterior_predictive <-
    forecasttools::read_tabular(
      fs::path(
        model_fit_dir,
        "mcmc_output",
        "tidy_posterior_predictive",
        ext = "parquet"
      )
    ) |>
    dplyr::rename("iteration" = "draw") |> # arviz -> tidybayes nomenclature
    dplyr::mutate("date" = as.Date(.data$date)) |>
    dplyr::rename_with(
      \(x) glue::glue(".{x}"),
      c("chain", "iteration", "variable", "value")
    ) |>
    dplyr::mutate(
      geo_value = model_info$location,
      disease = model_info$disease,
      resolution = variable_resolution_key[.data$.variable],
      aggregated_numerator = FALSE,
      aggregated_denominator = NA,
    ) |>
    dplyr::mutate(dplyr::across(c(".chain", ".iteration"), \(x) x + 1)) |>
    tidybayes::combine_chains() |>
    dplyr::select(tidyselect::all_of(required_columns))

  forecasttools::write_tabular(
    pyrenew_posterior_predictive,
    fs::path(
      model_fit_dir,
      "samples",
      ext = "parquet"
    )
  )
}


p <- arg_parser(
  "Create samples file from PyRenew model fit directory."
) |>
  add_argument(
    "model-fit-dir",
    help = "Directory containing the model data and output.",
  )

argv <- parse_args(p)

create_samples_from_pyrenew_fit_dir(model_fit_dir = argv$model_fit_dir)
