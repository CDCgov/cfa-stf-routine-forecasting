Sys.setenv(
  DUCKPLYR_FALLBACK_COLLECT = "0",
  DUCKPLYR_FALLBACK_AUTOUPLOAD = "0"
)

default_hub_quantiles <- c(0.01, 0.025, 1:19 / 20, 0.975, 0.99)

raw_samples_fixture <- tibble::tibble(
  .draw = c(1L, 2L),
  date = as.Date(c("2024-02-04", "2024-02-10")),
  geo_value = "US",
  disease = "COVID-19",
  resolution = c("daily", "epiweekly"),
  .variable = c("observed_hospital_admissions", "observed_ed_visits"),
  .value = c(11, 22)
)

withr::with_tempdir({
  model_dir <- fs::path(
    "covid-19_r_2024-02-03_f_2021-04-01_t_2024-01-23",
    "model_runs",
    "US",
    "pyrenew_e"
  )
  samples_path <- fs::path(model_dir, "samples.parquet")

  fs::dir_create(model_dir)
  forecasttools::write_tabular(raw_samples_fixture, samples_path)
  prelim_samples <- raw_samples_to_prelim(samples_path)
})


test_that("raw_samples_to_prelim() derives report metadata from samples path", {
  expect_equal(
    dplyr::select(
      dplyr::collect(prelim_samples),
      "reference_date",
      "model_id",
      "horizon_timescale",
      "horizon",
      "target",
      ".value"
    ),
    tibble::tibble(
      reference_date = as.Date(c("2024-02-03", "2024-02-03")),
      model_id = c("pyrenew_e", "pyrenew_e"),
      horizon_timescale = c("days", "days"),
      horizon = c(1, 7),
      target = c("inc covid hosp", "wk inc covid ed visits"),
      .value = raw_samples_fixture$.value
    )
  )
})


test_that("prelim_to_hub_samples() returns the expected hub sample schema", {
  result <- prelim_to_hub_samples(prelim_samples)

  expect_equal(
    unname(colnames(result)),
    unname(c(
      forecasttools::cdc_hub_std_colnames,
      "horizon_timescale",
      "resolution",
      "disease"
    ))
  )
})

test_that("prelim_to_hub_quantiles() converts samples to default hub quantiles", {
  result <- prelim_to_hub_quantiles(prelim_samples)

  expect_true(all(result$output_type == "quantile"))
  expect_equal(sort(unique(result$output_type_id)), default_hub_quantiles)
})

test_that("prelim_samples_to_hubverse() supports samples, quantiles, and both", {
  result_samples <- prelim_samples_to_hubverse(
    prelim_samples,
    output_type = "samples"
  )
  result_quantiles <- prelim_samples_to_hubverse(
    prelim_samples,
    output_type = "quantiles"
  )
  result_both <- prelim_samples_to_hubverse(
    prelim_samples,
    output_type = "both"
  )

  expect_equal(result_samples, prelim_to_hub_samples(prelim_samples))
  expect_equal(result_quantiles, prelim_to_hub_quantiles(prelim_samples))
  expect_equal(
    result_both,
    dplyr::bind_rows(result_quantiles, result_samples)
  )
})
