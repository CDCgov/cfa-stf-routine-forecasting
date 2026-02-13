library(tidyverse)
library(forecasttools)
library(fs)
library(glue)
library(argparser)

augment_samples_with_obs <- function(samples, obs) {
  first_forecast_date <- min(samples$date)
  sample_resolution <- unique(samples$resolution)

  checkmate::assert_scalar(sample_resolution)
  target_draws <- samples[[".draw"]] |> unique() |> sort()
  obs_as_samples <- obs |>
    dplyr::filter(.data[["date"]] < !!first_forecast_date) |>
    tidyr::expand_grid(.draw = target_draws) |>
    mutate(data_type = "train") |>
    bind_rows(samples) |>
    mutate(resolution = sample_resolution)
}

read_samples <- function(model_run_dir, model_name, var_name) {
  path(model_run_dir, model_name, "samples", ext = "parquet") |>
    read_tabular() |>
    filter(.variable == !!var_name) |>
    pivot_wider(names_from = ".variable", values_from = ".value") |>
    select(
      -starts_with("aggregated"),
      -any_of(c(".chain", ".iteration"))
    ) |>
    select(where(~ !all(is.na(.x))))
}

read_data <- function(model_run_dir, model_name, var_name) {
  path(model_run_dir, model_name, "data", "combined_data", ext = "tsv") |>
    read_tabular() |>
    filter(.variable == !!var_name) |>
    pivot_wider(names_from = ".variable", values_from = ".value") |>
    select(where(~ !all(is.na(.x))))
}

aggregate_to_epiweekly <- function(x, var_name) {
  daily_to_epiweekly(
    x,
    value_col = var_name,
    weekly_value_name = var_name,
    id_cols = setdiff(colnames(x), c("date", var_name)),
    strict = TRUE,
    with_epiweek_end_date = TRUE,
    epiweek_end_date_name = "date"
  ) |>
    mutate(resolution = "epiweekly") |>
    select(-all_of(c("epiweek", "epiyear")))
}

to_prop <- function(num, other, num_var_name, other_var_name, prop_var_name) {
  inner_join(num, other) |>
    mutate(
      !!prop_var_name := .data[[num_var_name]] /
        (.data[[num_var_name]] + .data[[other_var_name]])
    ) |>
    select(-all_of(c(num_var_name, other_var_name))) |>
    mutate(.variable = !!prop_var_name) |>
    rename(.value = !!prop_var_name)
}

create_prop_samples <- function(
  model_run_dir,
  num_model_name,
  other_model_name,
  num_var_name = "observed_ed_visits",
  other_var_name = "other_ed_visits",
  prop_var_name = "prop_disease_ed_visits",
  augment_num_with_obs = FALSE,
  augment_other_with_obs = TRUE,
  aggregate_num = FALSE,
  aggregate_other = FALSE,
  save = FALSE
) {
  num_samples <- read_samples(model_run_dir, num_model_name, num_var_name)
  other_samples <- read_samples(
    model_run_dir,
    other_model_name,
    other_var_name
  )

  num_data <- read_data(model_run_dir, num_model_name, num_var_name)
  other_data <- read_data(model_run_dir, other_model_name, other_var_name)

  if (augment_num_with_obs) {
    num_samples <- augment_samples_with_obs(num_samples, num_data)
  }
  if (augment_other_with_obs) {
    other_samples <- augment_samples_with_obs(other_samples, other_data)
  }
  if (aggregate_num) {
    num_samples <- aggregate_to_epiweekly(num_samples, num_var_name)
    num_data <- aggregate_to_epiweekly(num_data, num_var_name)
  }
  if (aggregate_other) {
    other_samples <- aggregate_to_epiweekly(
      other_samples,
      other_var_name
    )
    other_data <- aggregate_to_epiweekly(other_data, other_var_name)
  }

  prop_samples <- to_prop(
    num_samples,
    other_samples,
    num_var_name,
    other_var_name,
    prop_var_name
  )
  prop_data <- to_prop(
    num_data,
    other_data,
    num_var_name,
    other_var_name,
    prop_var_name
  )
  agg_model_name <- function(model_name, mod) {
    if (mod) {
      paste0("epiweekly_aggregated_", model_name, "")
    } else {
      model_name
    }
  }
  prop_model_name <- str_c(
    "prop",
    agg_model_name(num_model_name, aggregate_num),
    agg_model_name(other_model_name, aggregate_other),
    sep = "_"
  )

  if (save) {
    prop_model_dir <- path(model_run_dir, prop_model_name)
    data_dir <- path(prop_model_dir, "data")
    dir_create(data_dir, recurse = TRUE)

    write_tabular(
      prop_samples,
      path(prop_model_dir, "samples", ext = "parquet")
    )
    write_tabular(
      prop_data,
      path(data_dir, "combined_data", ext = "tsv")
    )
  }

  invisible(list(
    prop_samples = prop_samples,
    prop_data = prop_data,
    prop_model_name = prop_model_name
  ))
}

p <- arg_parser("Generate proportion samples") |>
  add_argument(
    "model-run-dir",
    help = "Directory containing the model data and output.",
  ) |>
  add_argument(
    "--num-model-name",
    help = "Name of the model containing the numerator variable.",
  ) |>
  add_argument(
    "--other-model-name",
    help = "Name of the model containing the other variable.",
  ) |>
  add_argument(
    "--num-var-name",
    help = "Name of the numerator variable.",
    default = "observed_ed_visits"
  ) |>
  add_argument(
    "--other-var-name",
    help = "Name of the other variable.",
    default = "other_ed_visits"
  ) |>
  add_argument(
    "--prop-var-name",
    help = "Name of the proportion variable.",
    default = "prop_disease_ed_visits"
  ) |>
  add_argument(
    "--augment-num-with-obs",
    help = "Whether to augment numerator samples with observations.",
    flag = TRUE
  ) |>
  add_argument(
    "--augment-other-with-obs",
    help = "Whether to augment other samples with observations.",
    flag = TRUE
  ) |>
  add_argument(
    "--aggregate-num",
    help = "Whether to aggregate numerator to epiweekly.",
    flag = TRUE
  ) |>
  add_argument(
    "--aggregate-other",
    help = "Whether to aggregate other to epiweekly.",
    flag = TRUE
  ) |>
  add_argument(
    "--save",
    help = "Whether to save the results.",
    flag = TRUE
  )

argv <- parse_args(p)

create_prop_samples(
  model_run_dir = argv$model_run_dir,
  num_model_name = argv$num_model_name,
  other_model_name = argv$other_model_name,
  num_var_name = argv$num_var_name,
  other_var_name = argv$other_var_name,
  prop_var_name = argv$prop_var_name,
  augment_num_with_obs = argv$augment_num_with_obs,
  augment_other_with_obs = argv$augment_other_with_obs,
  aggregate_num = argv$aggregate_num,
  aggregate_other = argv$aggregate_other,
  save = argv$save
)
