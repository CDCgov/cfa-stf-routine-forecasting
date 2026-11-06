#' Canonical disease names
#'
#' Disease identifiers used in directory paths and forecast outputs.
#'
#' @format A character vector containing `"covid"`, `"flu"`, and `"rsv"`.

disease_names <- c("covid", "flu", "rsv")

#' Parse model run directory path.
#'
#' Parse path to a model run directory
#' (i.e. a directory representing a run for a
#' particular location, disease, and reference
#' date, and extract key quantities of interest.
#'
#' @param model_run_dir_path Path to parse.
#' @return A tibble containing `location` and canonical `disease`.
#' Training window information is stored in each fit's metadata.json.
#'
#' @export
parse_model_run_dir_path <- function(model_run_dir_path) {
  if (!all(fs::path_file(model_run_dir_path) %in% disease_names)) {
    stop("Invalid disease directory")
  }
  tibble::tibble(
    location = fs::path_file(fs::path_dir(model_run_dir_path)),
    disease = fs::path_file(model_run_dir_path)
  )
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
