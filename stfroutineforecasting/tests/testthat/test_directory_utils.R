valid_model_batch <- dplyr::bind_rows(
  tibble::tibble(
    dirname = "covid_lookback-150_omit-1",
    disease = "covid",
    n_training_days = 150L,
    exclude_last_n_days = 1L
  ),
  tibble::tibble(
    dirname = "flu_lookback-90_omit-4",
    disease = "flu",
    n_training_days = 90L,
    exclude_last_n_days = 4L
  )
)

invalid_model_batch_format <- c(
  "covid_r_2024-02-03_f_2021-04-01_t_2024-01-23",
  "flu_lookback-many_omit-4"
)

invalid_model_batch_values <- c(
  "qcovid_lookback-150_omit-1"
)

target_locations <- c("ME", "US")

valid_model_run <- valid_model_batch |>
  dplyr::mutate(
    report_date = lubridate::ymd(c("2024-02-03", "2022-12-11")),
    dirname = fs::path(
      glue::glue("{report_date}_forecasts"),
      dirname,
      "model_runs",
      target_locations
    ),
    location = target_locations,
    .after = dirname
  )

expected_model_run <- valid_model_run |>
  dplyr::select(
    disease,
    n_training_days,
    exclude_last_n_days,
    location,
    report_date
  )


test_that("parse_model_batch_dir_path() works as expected.", {
  ## should work with base dirnames that are valid
  expect_equal(
    parse_model_batch_dir_path(valid_model_batch$dirname),
    dplyr::select(valid_model_batch, -dirname)
  )

  ## should work identically with a full path rather
  ## than just base dir
  expect_equal(
    valid_model_batch |>
      dplyr::mutate(dirname = fs::path("this", "is", "a", "test", dirname)) |>
      dplyr::pull(dirname) |>
      parse_model_batch_dir_path(),
    dplyr::select(valid_model_batch, -dirname)
  )

  ## should error if the terminal directory is not
  ## what is to be parsed
  expect_error(
    valid_model_batch |>
      dplyr::mutate(dirname = fs::path(dirname, "test")) |>
      dplyr::pull(dirname) |>
      parse_model_batch_dir_path(),
    regex = "Invalid format for model batch directory name"
  )

  ## should error if the directory format does not match
  expect_error(
    parse_model_batch_dir_path(invalid_model_batch_format),
    regex = "Invalid format for model batch directory name"
  )

  ## should error if extracted entries cannot be parsed as expected
  expect_error(
    parse_model_batch_dir_path(invalid_model_batch_values),
    regex = "Could not parse extracted disease and/or integer values"
  )
})

test_that("parse_forecast_output_dir_path() works as expected.", {
  expect_equal(
    parse_forecast_output_dir_path("2024-02-03_forecasts"),
    tibble::tibble(report_date = lubridate::ymd("2024-02-03"))
  )
  expect_error(
    parse_forecast_output_dir_path("2024-02-03"),
    regex = "Invalid format for forecast output directory name"
  )
  expect_error(
    parse_forecast_output_dir_path("2024-02-30_forecasts"),
    regex = "Could not parse report date"
  )
})

test_that("parse_model_run_dir_path() works as expected.", {
  expect_equal(
    parse_model_run_dir_path(valid_model_run$dirname),
    expected_model_run
  )

  ## should work identically with a full path rather
  ## than just base dir
  expect_equal(
    valid_model_run |>
      dplyr::mutate(dirname = fs::path("this", "is", "a", "test", dirname)) |>
      dplyr::pull(dirname) |>
      parse_model_run_dir_path(),
    expected_model_run
  )

  ## should fail if there is additional terminal pathing
  expect_error(
    valid_model_run |>
      dplyr::mutate(dirname = fs::path(dirname, "test")) |>
      dplyr::pull(dirname) |>
      parse_model_run_dir_path(),
    regex = "Invalid format for model batch directory name"
  )
})

test_that("get_all_model_batch_dirs() returns expected output.", {
  withr::with_tempdir({
    ## create some directories
    valid_covid <- c(
      "covid_lookback-150_omit-1",
      "covid_lookback-90_omit-4"
    )
    valid_flu <- c(
      "flu_lookback-150_omit-1",
      "flu_lookback-90_omit-4"
    )
    valid_rsv <- c(
      "rsv_lookback-150_omit-1",
      "rsv_lookback-90_omit-4"
    )
    valid_dirs <- c(valid_flu, valid_covid, valid_rsv)

    invalid_dirs <- c(
      "this_is_not_valid",
      "covid19_lookback-",
      "covid-lookback-",
      "flu-lookback-",
      "influnza_lookback-",
      "covid_lookback-many_omit-1",
      "covid",
      "flu"
    )

    invalid_files <- c(
      "covid_lookback-.txt",
      "flu_lookback-.txt",
      "rsv_lookback-.txt"
    )
    fs::dir_create(c(valid_dirs, invalid_dirs))
    fs::file_create(invalid_files)
    expected_all_files <- c(
      valid_dirs,
      invalid_dirs,
      invalid_files
    )

    result_all <- fs::dir_ls(".") |> fs::path_file()

    result_valid <- get_all_model_batch_dirs(
      ".",
      c("covid", "flu", "rsv")
    )

    result_valid_alt <- get_all_model_batch_dirs(
      ".",
      c("flu", "rsv", "covid")
    )

    result_valid_covid <- get_all_model_batch_dirs(
      ".",
      "covid"
    )

    result_valid_flu <- get_all_model_batch_dirs(
      ".",
      "flu"
    )

    result_valid_rsv <- get_all_model_batch_dirs(
      ".",
      "rsv"
    )

    expect_setequal(result_all, expected_all_files)
    expect_setequal(result_valid, c(valid_flu, valid_covid, valid_rsv))
    expect_setequal(result_valid_alt, c(valid_flu, valid_covid, valid_rsv))
    expect_setequal(result_valid_covid, valid_covid)
    expect_setequal(result_valid_flu, valid_flu)
    expect_setequal(result_valid_rsv, valid_rsv)
  })
})
