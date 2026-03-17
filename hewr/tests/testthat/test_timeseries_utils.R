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
    disease = "COVID-19",
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
  expect_true(all(result$disease == "COVID-19"))
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
    disease = "Influenza",
    resolution = "epiweekly",
    output_type_id = ".draw"
  )
})
