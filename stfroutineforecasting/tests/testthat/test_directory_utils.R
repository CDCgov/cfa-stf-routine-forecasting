valid_model_batch <- dplyr::bind_rows(
  tibble::tibble(
    dirname = "covid-19_r_2024-02-03_f_2021-04-01_t_2024-01-23",
    disease = "COVID-19",
    report_date = lubridate::ymd("2024-02-03"),
    first_training_date = lubridate::ymd("2021-04-1"),
    last_training_date = lubridate::ymd("2024-01-23")
  ),
  tibble::tibble(
    dirname = "influenza_r_2022-12-11_f_2021-02-05_t_2027-12-30",
    disease = "Influenza",
    report_date = lubridate::ymd("2022-12-11"),
    first_training_date = lubridate::ymd("2021-02-5"),
    last_training_date = lubridate::ymd("2027-12-30")
  )
)

invalid_model_batch_dirs <- c(
  "qcovid-19_r_2024-02-03_f_2021-04-01_t_2024-01-23",
  "influenza_r_2022-12-33_f_2021-02-05_t_2027-12-30"
)

target_locations <- c("ME", "US")

valid_model_run <- valid_model_batch |>
  dplyr::mutate(
    dirname = fs::path(dirname, "model_runs", target_locations),
    location = target_locations
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

  ## should error if entries cannot be parsed as what is expected
  expect_error(
    parse_model_batch_dir_path(invalid_model_batch_dirs),
    regex = "Could not parse extracted disease and/or date values"
  )
})

test_that("parse_model_run_dir_path() works as expected.", {
  expect_equal(
    parse_model_run_dir_path(valid_model_run$dirname),
    dplyr::select(valid_model_run, -dirname)
  )

  ## should work identically with a full path rather
  ## than just base dir
  expect_equal(
    valid_model_run |>
      dplyr::mutate(dirname = fs::path("this", "is", "a", "test", dirname)) |>
      dplyr::pull(dirname) |>
      parse_model_run_dir_path(),
    dplyr::select(valid_model_run, -dirname)
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
      "covid-19_r_2024-02-01_f_2021-01-01_t_2024-01-31",
      "covid-19_r"
    )
    valid_flu <- c(
      "influenza_r_2022-11-12_f_2022-11-01_t_2022_11_10",
      "influenza_r"
    )
    valid_rsv <- c(
      "rsv_r_2022-11-12_f_2022-11-01_t_2022_11_10",
      "rsv_r"
    )
    valid_dirs <- c(valid_flu, valid_covid, valid_rsv)

    invalid_dirs <- c(
      "this_is_not_valid",
      "covid19_r",
      "covid-19-r",
      "influenza-r",
      "influnza_r",
      "covid-19",
      "influenza"
    )

    invalid_files <- c(
      "covid-19_r.txt",
      "influenza_r.txt",
      "rsv_r.txt"
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
      c("COVID-19", "Influenza", "RSV")
    )

    result_valid_alt <- get_all_model_batch_dirs(
      ".",
      c("Influenza", "RSV", "COVID-19")
    )

    result_valid_covid <- get_all_model_batch_dirs(
      ".",
      "COVID-19"
    )

    result_valid_flu <- get_all_model_batch_dirs(
      ".",
      "Influenza"
    )

    result_valid_rsv <- get_all_model_batch_dirs(
      ".",
      "RSV"
    )

    expect_setequal(result_all, expected_all_files)
    expect_setequal(result_valid, c(valid_flu, valid_covid, valid_rsv))
    expect_setequal(result_valid_alt, c(valid_flu, valid_covid, valid_rsv))
    expect_setequal(result_valid_covid, valid_covid)
    expect_setequal(result_valid_flu, valid_flu)
    expect_setequal(result_valid_rsv, valid_rsv)
  })
})
