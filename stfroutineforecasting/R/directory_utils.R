#' Canonical disease names
#'
#' Disease identifiers used in model batch directory names and forecast outputs.
#'
#' @format A character vector containing `"covid"`, `"flu"`, and `"rsv"`.

disease_names <- c("covid", "flu", "rsv")

#' Parse model batch directory name.
#'
#' Parse the name of a model batch directory
#' (i.e. a directory representing a single
#' disease, lookback, and omission configuration, but potentially
#' with fits for multiple locations), returning
#' a named list of quantities of interest.
#'
#' @param model_batch_dir_path Path to the model batch
#' directory to parse. Will parse only the basename.
#' @return A one-row tibble containing canonical `disease`, `n_training_days`,
#' and `exclude_last_n_days` values.
#' @export
parse_model_batch_dir_path <- function(model_batch_dir_path) {
  pattern <- "^(.+)_lookback-([0-9]+)_omit-([0-9]+)$"
  model_batch_dir_name <- fs::path_file(model_batch_dir_path)
  matches <- stringr::str_match(
    model_batch_dir_name,
    pattern
  )

  if (anyNA(matches)) {
    stop(
      "Invalid format for model batch directory name; ",
      "could not parse. Expected ",
      "'<disease>_lookback-<n_training_days>_omit-",
      "<exclude_last_n_days>'."
    )
  }

  result <-
    matches[, -1, drop = FALSE] |>
    tibble::as_tibble(.name_repair = \(x) {
      c(
        "disease",
        "n_training_days",
        "exclude_last_n_days"
      )
    }) |>
    dplyr::mutate(
      disease = dplyr::if_else(
        .data$disease %in% disease_names,
        .data$disease,
        NA_character_
      ),
      n_training_days = as.integer(.data$n_training_days),
      exclude_last_n_days = as.integer(.data$exclude_last_n_days)
    )

  if (anyNA(result)) {
    stop(
      "Could not parse extracted disease and/or integer ",
      "values expected 'disease' to be one of 'covid', 'flu', ",
      "or 'rsv'. Got: ",
      glue::glue(
        "disease: {matches[2]}, ",
        "n_training_days: {matches[3]}, ",
        "exclude_last_n_days: {matches[4]}."
      )
    )
  }

  return(result)
}

#' Parse forecast output directory name.
#'
#' Parse the report date from a directory named `<report_date>_forecasts`.
#'
#' @param forecast_output_dir_path Path to the forecast output directory.
#' Will parse only the basename.
#' @return A one-row tibble containing `report_date`.
#' @export
parse_forecast_output_dir_path <- function(forecast_output_dir_path) {
  pattern <- "^(.+)_forecasts$"
  forecast_output_dir_name <- fs::path_file(forecast_output_dir_path)
  matches <- stringr::str_match(forecast_output_dir_name, pattern)

  if (anyNA(matches)) {
    stop(
      "Invalid format for forecast output directory name; ",
      "could not parse. Expected '<report_date>_forecasts'."
    )
  }

  result <- tibble::tibble(
    report_date = lubridate::ymd(matches[, 2], quiet = TRUE)
  )
  if (anyNA(result)) {
    stop(
      "Could not parse report date from forecast output directory name. ",
      "Expected a valid date in YYYY-MM-DD format. Got: ",
      matches[, 2],
      "."
    )
  }
  return(result)
}

#' Parse model run directory path.
#'
#' Parse path to a model run directory
#' (i.e. a directory representing a run for a
#' particular location, disease, and reference
#' date, and extract key quantities of interest.
#'
#' @param model_run_dir_path Path to parse.
#' @return A one-row tibble containing `location`, canonical `disease`,
#' `report_date`, `n_training_days`, and `exclude_last_n_days` values.
#'
#' @export
parse_model_run_dir_path <- function(model_run_dir_path) {
  batch_dir <- model_run_dir_path |>
    fs::path_dir() |>
    fs::path_dir()

  batch_params <- batch_dir |>
    parse_model_batch_dir_path() |>
    dplyr::mutate(location = fs::path_file(model_run_dir_path))
  forecast_params <- batch_dir |>
    fs::path_dir() |>
    parse_forecast_output_dir_path()

  dplyr::bind_cols(batch_params, forecast_params)
}


#' Get forecast directories.
#'
#' Get all the subdirectories within a parent directory
#' that match the pattern for a forecast run for a
#' given disease and optionally a given report date.
#'
#' @param dir_of_batch_dirs Directory in which to look for
#' "model batch" directories, each of which represents an
#' individual forecast date / pathogen / dataset combination.
#' @param diseases Canonical disease identifiers to match (`"covid"`, `"flu"`,
#' or `"rsv"`), supplied as a character vector.
#' @return A vector of paths to the forecast subdirectories.
#' @export
get_all_model_batch_dirs <- function(dir_of_batch_dirs, diseases) {
  match_pattern <- stringr::str_c(
    "^(?:",
    stringr::str_c(diseases, collapse = "|"),
    ")_lookback-[0-9]+_omit-[0-9]+$"
  )

  dirs <- tibble::tibble(
    dir_path = fs::dir_ls(
      dir_of_batch_dirs,
      type = "directory"
    )
  ) |>
    dplyr::filter(stringr::str_detect(
      fs::path_file(.data$dir_path),
      !!match_pattern
    )) |>
    dplyr::pull(.data$dir_path)

  return(dirs)
}

#' Parse variable name.
#'
#' Convert a variable name into a descriptive label for display in plots.
#'
#' @param variable_name Character. Name of the variable to parse.
#' @return A list containing:
#'   - `proportion`: Logical. Indicates if the variable represents a proportion.
#'   - `core_name`: Character. A simplified name for the variable.
#'   - `full_name`: Character. A formatted name for the variable.
#'   - `y_axis_labels`: Function. A suitable label function for axis formatting.
#' @export
#'
#' @examples
#' parse_variable_name("prop_hospital_admissions")
parse_variable_name <- function(variable_name) {
  proportion <- stringr::str_starts(variable_name, "prop")

  core_name <- dplyr::case_when(
    stringr::str_detect(variable_name, "ed_visits") ~
      "Emergency Department Visits",
    stringr::str_detect(variable_name, "hospital") ~ "Hospital Admissions",
    TRUE ~ ""
  )

  full_name <- dplyr::if_else(
    proportion,
    glue::glue("Proportion of {core_name}"),
    core_name
  )

  y_axis_labels <- if (proportion) {
    scales::label_percent()
  } else {
    scales::label_comma()
  }

  list(
    proportion = proportion,
    core_name = core_name,
    full_name = full_name,
    y_axis_labels = y_axis_labels
  )
}


#' Get path up to a specific directory.
#'
#' @param path A character vector of paths.
#' @param up_to A character string specifying the directory name
#'
#' @returns A character vector of paths that go up to the specified directory.
#' @export
path_up_to <- function(path, up_to) {
  path_parts <- fs::path_split(path)
  up_to_index <- purrr::map(path_parts, \(x) which(x == up_to))
  stopifnot(lengths(up_to_index) == 1)
  purrr::map2_vec(path_parts, up_to_index, \(parts, index) {
    fs::path_join(utils::head(parts, index))
  })
}
