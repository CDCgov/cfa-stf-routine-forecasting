target_locations <- c("ME", "US", "CA")
target_diseases <- c("covid", "flu", "rsv")

valid_model_run <- tibble::tibble(
  dirname = fs::path(
    "2026-09-02_forecasts",
    "2026-09-02-12-00-00_run",
    target_locations,
    target_diseases
  ),
  location = target_locations,
  disease = target_diseases
)


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
    regex = "Invalid disease directory"
  )
})
