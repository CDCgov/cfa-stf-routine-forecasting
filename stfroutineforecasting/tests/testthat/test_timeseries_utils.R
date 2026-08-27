# Common test data fixtures
common_required_columns <- c(
  ".draw",
  "date",
  "geo_value",
  "disease",
  ".variable",
  ".value",
  "resolution"
)

base_date <- as.Date("2024-01-01")

test_that("load_training_data returns training rows without data_type", {
  withr::with_tempdir({
    model_dir <- fs::path(
      "covid_r_2024-01-03_f_2024-01-01_t_2024-01-02",
      "model_runs",
      "CA",
      "fable_e_daily"
    )
    fs::dir_create(fs::path(model_dir, "data"))
    input_data <- tibble::tibble(
      date = rep(base_date + 0:2, each = 2),
      geo_value = "CA",
      disease = "covid",
      data_type = rep(c("train", "train", "eval"), each = 2),
      .variable = rep(c("observed_ed_visits", "other_ed_visits"), 3),
      .value = c(10, 90, 11, 99, 12, 108),
      resolution = "daily"
    )
    readr::write_tsv(
      input_data,
      fs::path(model_dir, "data", "combined_data.tsv")
    )

    result <- load_training_data(model_dir)

    expect_equal(result$data$date, base_date + 0:1)
    expect_false("data_type" %in% colnames(result$data))
    expect_equal(result$geo_value, "CA")
    expect_equal(result$disease, "covid")
    expect_equal(result$resolution, "daily")
  })
})

test_that("format_timeseries_output formats forecast data correctly", {
  # Create minimal test data
  forecast_data <- tibble::tibble(
    date = base_date + 0:2,
    .draw = c(1, 1, 1),
    observed_ed_visits = c(10, 15, 20),
    other_ed_visits = c(5, 7, 9)
  )

  result <- format_timeseries_output(
    forecast_data = forecast_data,
    geo_value = "US",
    disease = "covid",
    resolution = "daily",
    output_type_id = ".draw"
  )

  # Check that output has expected structure
  expect_s3_class(result, "data.frame")
  expect_true(all(
    c(
      "date",
      "geo_value",
      "disease",
      "resolution",
      ".variable",
      ".draw",
      ".value"
    ) %in%
      colnames(result)
  ))

  # Check that geo_value and disease are set correctly
  expect_true(all(result$geo_value == "US"))
  expect_true(all(result$disease == "covid"))
  expect_true(all(result$resolution == "daily"))

  # Check that data was pivoted (should have 2 variables x 3 dates = 6 rows)
  expect_equal(nrow(result), 6)
})

test_that("format_timeseries_output handles proportion variables", {
  forecast_data <- tibble::tibble(
    date = base_date,
    .draw = 1,
    prop_disease_ed_visits = 0.5
  )

  result <- format_timeseries_output(
    forecast_data = forecast_data,
    geo_value = "CA",
    disease = "flu",
    resolution = "epiweekly",
    output_type_id = ".draw"
  )
})
