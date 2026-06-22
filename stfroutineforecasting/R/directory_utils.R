#' Utilities for handling and parsing directory names
#' based on cfa-stf-routine-forecasting pipeline conventions.

disease_map_lower <- c(
  "covid-19" = "COVID-19",
  "influenza" = "Influenza",
  "rsv" = "RSV"
)

#' Parse model batch directory name.
#'
#' Parse the name of a model batch directory
#' (i.e. a directory representing a single
#' report date and disease pair, but potentially
#' with fits for multiple locations), returning
#' a named list of quantities of interest.
#'
#' @param model_batch_dir_path Path to the model batch
#' directory to parse. Will parse only the basename.
#' @return A list of quantities: `disease`, `report_date`,
#' `first_training_date`, and `last_training_date`.
#' @export
parse_model_batch_dir_path <- function(model_batch_dir_path) {
  pattern <- "(.+)_r_(.+)_f_(.+)_t_(.+)"
  model_batch_dir_name <- fs::path_file(model_batch_dir_path)
  matches <- stringr::str_match(
    model_batch_dir_name,
    pattern
  )

  if (anyNA(matches)) {
    stop(
      "Invalid format for model batch directory name; ",
      "could not parse. Expected ",
      "'<disease>_r_<report_date>_f_<first_training_date>_t_",
      "<last_training_date>'."
    )
  }

  result <-
    matches[, -1, drop = FALSE] |>
    tibble::as_tibble(.name_repair = \(x) {
      c(
        "disease",
        "report_date",
        "first_training_date",
        "last_training_date"
      )
    }) |>
    dplyr::mutate(
      disease = unname(disease_map_lower[.data$disease]),
      report_date = lubridate::ymd(.data$report_date, quiet = TRUE),
      first_training_date = lubridate::ymd(
        .data$first_training_date,
        quiet = TRUE
      ),
      last_training_date = lubridate::ymd(
        .data$last_training_date,
        quiet = TRUE
      )
    )

  if (anyNA(result)) {
    stop(
      "Could not parse extracted disease and/or date ",
      "values expected 'disease' to be one of 'covid-19' ",
      "and 'influenza' and all dates to be valid dates in ",
      "YYYY-MM-DD format. Got: ",
      glue::glue(
        "disease: {matches[2]}, ",
        "report_date: {matches[3]}, ",
        "first_training_date: {matches[4]}, ",
        "last_training_date: {matches[5]}."
      )
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
#' @return A list of parsed attributes:
#' `location`, `disease`, `report_date`,
#' `first_training_date`, and `last_training_date`.
#'
#' @export
parse_model_run_dir_path <- function(model_run_dir_path) {
  batch_dir <- model_run_dir_path |>
    fs::path_dir() |>
    fs::path_dir() |>
    fs::path_file()

  batch_dir |>
    parse_model_batch_dir_path() |>
    dplyr::mutate(location = fs::path_file(model_run_dir_path))
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
#' @param diseases Names of the diseases to match, as a vector of strings,
#' or a single disease as a string.
#' @return A vector of paths to the forecast subdirectories.
#' @export
get_all_model_batch_dirs <- function(dir_of_batch_dirs, diseases) {
  # disease names are lowercase by convention
  match_patterns <- stringr::str_c(tolower(diseases), "_r", collapse = "|")

  dirs <- tibble::tibble(
    dir_path = fs::dir_ls(
      dir_of_batch_dirs,
      type = "directory"
    )
  ) |>
    dplyr::filter(stringr::str_starts(
      fs::path_file(.data$dir_path),
      !!match_patterns
    )) |>
    dplyr::pull(.data$dir_path)

  return(dirs)
}

#' Parse PyRenew Model Name
#'
#' @param pyrenew_model_name name of a pyrenew model ("pyrenew_h", "pyrenew_he",
#' "pyrnew_hew", etc)
#'
#' @returns a named logical vector indicating which components are present
#' @export
#'
#' @examples parse_pyrenew_model_name("pyrenew_h")
parse_pyrenew_model_name <- function(pyrenew_model_name) {
  pyrenew_model_tail <- stringr::str_extract(pyrenew_model_name, "(?<=_).+$") |>
    stringr::str_split_1("")
  model_components <- c("h", "e", "w")
  model_components %in% pyrenew_model_tail |> purrr::set_names(model_components)
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
    stringr::str_detect(variable_name, "ww_conc") ~
      "Viral Genomes Concentration",
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
