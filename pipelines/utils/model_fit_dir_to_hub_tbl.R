library(argparser)
library(fs)
library(hewr)
library(forecasttools)


model_fit_dir_to_hub_tbl <- function(
    model_fit_dir,
    output_type = c("both", "quantiles", "samples")
) {
    samples_path <- path(model_fit_dir, "samples", ext = "parquet")
    hubverse_path <- path(model_fit_dir, "hubverse_table", ext = "parquet")

    hubverse_tbl <-
        samples_path |>
        raw_samples_to_prelim() |>
        prelim_samples_to_hubverse(output_type = output_type)

    forecasttools::write_tabular(hubverse_tbl, hubverse_path)
}

p <- arg_parser(
    "Create hubverse table from model fit directory."
) |>
    add_argument(
        "model-fit-dir",
        help = "Directory containing the model data and output.",
    ) |>
    add_argument(
        "--output-type",
        help = "Type of output to create. One of 'quantiles', 'samples', or 'both'.",
        default = "samples"
    )

argv <- parse_args(p)

model_fit_dir_to_hub_tbl(
    model_fit_dir = argv$model_fit_dir,
    output_type = argv$output_type
)
