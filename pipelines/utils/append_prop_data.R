library(argparser)
library(forecasttools)
library(dplyr)

append_prop_data <- function(data_path) {
  original_dat <- read_tabular(data_path)

  prop_dat <- original_dat |>
    dplyr::filter(.variable %in% c("observed_ed_visits", "other_ed_visits")) |>
    tidyr::pivot_wider(
      names_from = ".variable",
      values_from = ".value"
    ) |>
    dplyr::select(dplyr::where(~ !all(is.na(.x)))) |>
    mutate(
      .variable = "prop_disease_ed_visits",
      .value = .data$observed_ed_visits /
        (.data$observed_ed_visits + .data$other_ed_visits)
    ) |>
    select(-all_of(c("observed_ed_visits", "other_ed_visits")))

  combined_dat <- bind_rows(original_dat, prop_dat) |>
    arrange(date, .variable)

  write_tabular(combined_dat, data_path)
}

p <- arg_parser("Append prop variable to combined_data.tsv") |>
  add_argument(
    "data_path",
    help = "Path to the combined_data.tsv file to which the prop variable should be added"
  )

argv <- parse_args(p)
append_prop_data(argv$data_path)
