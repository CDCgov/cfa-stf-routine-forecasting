library(argparser)
library(fs)
library(stfroutineforecasting)
library(forecasttools)


model_fit_dir_to_hub_tbl <- function(
  model_fit_dir,
  report_date,
  output_type = c("both", "quantiles", "samples")
) {
  samples_path <- path(model_fit_dir, "samples", ext = "parquet")
  hubverse_path <- path(model_fit_dir, "hubverse_table", ext = "parquet")

  hubverse_tbl <-
    samples_path |>
    raw_samples_to_prelim(report_date = report_date) |>
    prelim_samples_to_hubverse(output_type = output_type)

  forecasttools::write_tabular(hubverse_tbl, hubverse_path)
}

p <- arg_parser(
  "Create hubverse table from model fit directory."
) |>
  add_argument(
    "--report-date",
    help = "Forecast report date in YYYY-MM-DD format."
  ) |>
  add_argument(
    "model-fit-dir",
    help = "Directory containing the model data and output."
  ) |>
  add_argument(
    "--output-type",
    help = "Type of output to create. One of 'quantiles', 'samples', or 'both'.",
    default = "samples"
  )

argv <- parse_args(p)

model_fit_dir_to_hub_tbl(
  model_fit_dir = argv$model_fit_dir,
  report_date = as.Date(argv$report_date),
  output_type = argv$output_type
)
