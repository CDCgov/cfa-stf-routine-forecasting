script_packages <- c(
  "argparser",
  "dplyr",
  "forecasttools",
  "fs",
  "readr",
  "lubridate",
  "stringr"
)

## load in packages without messages
purrr::walk(script_packages, \(pkg) {
  suppressPackageStartupMessages(
    library(pkg, character.only = TRUE)
  )
})

#' Convert Daily Data to Epiweekly Data
#'
#' This function reads daily data from a CSV file, converts it to epiweekly
#' data, and writes the resulting epiweekly data to a new CSV file.
#'
#' @param data_dir A string specifying the directory containing the model
#'  run data.
#' @param data_name A string specifying the name of the daily data file.
#'
#' @param strict A logical value indicating whether to enforce strict inclusion
#' of only full epiweeks. Default is TRUE.
#' @param overwrite_daily A logical value indicating whether to overwrite the
#' original daily data file with the epiweekly data. Default is TRUE. If FALSE,
#' the epiweekly data will be written to a new file with "epiweekly_" prefixed to the original file name.
#'
#' @return None. The function writes the epiweekly data to a CSV file in the
#'  specified directory.
convert_daily_to_epiweekly <- function(
  data_dir,
  data_name,
  strict = TRUE,
  overwrite_daily = TRUE
) {
  data_path <- path(data_dir, data_name)

  output_file <- ifelse(
    overwrite_daily,
    data_path,
    path(
      data_dir,
      glue::glue("epiweekly_{data_name}")
    )
  )

  daily_data <- read_tsv(
    data_path,
    col_types = cols(
      date = col_date(),
      geo_value = col_character(),
      disease = col_character(),
      data_type = col_character(),
      .variable = col_character(),
      .value = col_double(),
      resolution = col_character()
    )
  )

  daily_ed_data <- daily_data |>
    filter(str_ends(.variable, "_ed_visits"))

  other_data <- daily_data |>
    filter(str_ends(.variable, "_ed_visits", negate = TRUE))

  grouping_cols <- c("geo_value", "disease", "data_type", ".variable")

  epiweekly_ed_data <- daily_ed_data |>
    select(-"resolution") |>
    group_by(
      dplyr::across(dplyr::all_of(grouping_cols)),
      epiyear = epiyear(date),
      epiweek = epiweek(date)
    ) |>
    mutate(data_type = if_else(all(data_type == "train"), "train", "eval")) |>
    forecasttools::daily_to_epiweekly(
      value_col = ".value",
      weekly_value_name = ".value",
      id_cols = c(grouping_cols, "data_type"),
      strict = strict,
      with_epiweek_end_date = TRUE,
      epiweek_end_date_name = "date"
    ) |>
    mutate(resolution = "epiweekly") |>
    select(date, geo_value, disease, data_type, resolution, .variable, .value)

  epiweekly_data <- bind_rows(epiweekly_ed_data, other_data) |>
    arrange(date, .variable)

  write_tsv(epiweekly_data, output_file)
}

main <- function(data_dir, overwrite_daily = TRUE) {
  convert_daily_to_epiweekly(
    data_dir,
    data_name = "combined_data.tsv",
    overwrite_daily = overwrite_daily
  )
}

# Create a parser
p <- arg_parser("Create epiweekly data") |>
  add_argument(
    "data_dir",
    help = "Directory containing the model data and output."
  ) |>
  add_argument(
    "--overwrite-daily",
    help = "Whether to overwrite the original daily data file with the epiweekly data.",
    flag = TRUE
  )

argv <- parse_args(p)
main(argv$data_dir, overwrite_daily = argv$overwrite_daily)
