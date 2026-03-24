var_to_target <- function(variable, disease) {
  disease_abbr <- dplyr::replace_values(
    disease,
    "Influenza" ~ "flu",
    "COVID-19" ~ "covid",
    "RSV" ~ "rsv"
  )
  dplyr::replace_values(
    variable,
    "observed_hospital_admissions" ~ glue::glue("inc {disease_abbr} hosp"),
    "observed_ed_visits" ~ glue::glue("inc {disease_abbr} ed visits"),
    "other_ed_visits" ~ glue::glue("inc other ed visits"),
    "prop_disease_ed_visits" ~ glue::glue(
      "inc {disease_abbr} prop ed visits"
    )
  )
}


#' Convert raw samples to preliminary format
#'
#' @param samples_path The path to the raw samples file, in parquet format. The path should be of the form "model_runs/{batch_id}/{model_id}/samples.parquet", where {batch_id} is the ID of the model batch and {model_id} is the ID of the model run.
#'
#' @returns A data frame of preliminary samples.
#'
#' @export
raw_samples_to_prelim <- function(samples_path) {
  raw_samples <- duckplyr::read_parquet_duckdb(samples_path)

  batch_params <- samples_path |>
    path_up_to("model_runs") |>
    path_dir() |>
    parse_model_batch_dir_path()

  report_date <- batch_params$report_date

  model_id <- samples_path |>
    fs::path_dir() |>
    fs::path_file()

  raw_samples |>
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
    dplyr::mutate(model_id = !!model_id)
}

#' Convert preliminary samples to Hub format as samples
#'
#' @param prelim_samples A data frame of preliminary samples, in the format output by `raw_samples_to_prelim()`
#'
#' @returns A data frame in the format required by the Hub, with samples as the output type.
#'
#' @export
prelim_to_hub_samples <- function(prelim_samples) {
  prelim_samples |>
    dplyr::mutate(output_type = "sample") |>
    dplyr::rename(
      value = ".value",
      target_end_date = "date",
      location = "geo_value",
      output_type_id = ".draw"
    ) |>
    dplyr::select(
      dplyr::all_of(forecasttools::cdc_hub_std_colnames),
      "horizon_timescale",
      "resolution",
      "disease"
    )
}

#' Convert preliminary samples to Hub format as quantiles
#'
#' @param prelim_samples A data frame of preliminary samples, in the format output by `raw_samples_to_prelim()`
#'
#' @returns A data frame in the format required by the Hub, with quantiles as the output type.
#'
#' @export
prelim_to_hub_quantiles <- function(prelim_samples) {
  default_hub_quantiles <- c(
    0.01,
    0.025,
    1:19 / 20,
    0.975,
    0.99
  )
  prelim_samples |>
    prelim_to_hub_samples() |>
    hubUtils::convert_output_type(
      smht_deduped,
      to = list("quantile" = default_hub_quantiles)
    )
}


#' Convert preliminary samples to Hub format
#'
#' @param prelim_samples A data frame of preliminary samples, in the format output by `raw_samples_to_prelim()`
#' @param output_type The type of output to produce. One of "quantiles", "samples", or "both". If "quantiles", produces quantiles in the format required by the Hub. If "samples", produces samples in the format required by the Hub. If "both", produces both quantiles and samples.
#'
#' @returns A data frame in the format required by the Hub, either as quantiles, samples, or both, depending on the `output_type` parameter.
#'
#' @export
prelim_samples_to_hubverse <- function(
  prelim_samples,
  output_type = c("quantiles", "samples", "both")
) {
  output_type <- rlang::arg_match(output_type)
  if (output_type == "both") {
    output_type <- c("quantiles", "samples")
  }
  sample_processor_key <- c(
    "quantiles" = prelim_to_hub_quantiles,
    "samples" = prelim_to_hub_samples
  )
  sample_processor <- sample_processor_key[output_type]

  purrr::map(sample_processor, \(x) x(prelim_samples)) |>
    dplyr::bind_rows()
}
