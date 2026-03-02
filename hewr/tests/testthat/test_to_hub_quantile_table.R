# tests for `to_hub_quantile_table`
test_that(
  paste0(
    "to_hub_quantile_table ",
    "handles multiple locations ",
    "and multiple source files"
  ),
  {
    batch_dir_name <- "covid-19_r_2024-12-14_f_2024-12-08_t_2024-12-14"
    tempdir <- withr::local_tempdir()

    temp_batch_dir <- fs::dir_create(fs::path(tempdir, batch_dir_name))

    locations <- c("loc1", "loc2", "loc3")
    purrr::walk(locations, function(loc) {
      loc_dir <- fs::path(temp_batch_dir, "model_runs", loc, "pyrenew_e")
      fs::dir_create(loc_dir)

      date_options <- as.Date(c("2024-12-13", "2024-12-14", "2024-12-15"))
      disease_options <- "COVID-19"
      variable_options <- c("observed_ed_visits", "other_ed_visits")
      resolution_options <- c("daily", "epiweekly")
      n_draw <- 4

      create_model_results(
        file = fs::path(loc_dir, "samples", ext = "parquet"),
        model_name = fs::path_file(loc_dir),
        date_options = date_options,
        geo_value_options = loc,
        disease_options = disease_options,
        resolution_options = resolution_options,
        n_draw = n_draw
      )
    })

    result <-
      model_runs_dir_to_hub_q_tbl(temp_batch_dir) |>
      suppressMessages()

    expect_gt(nrow(result), 0)

    checkmate::expect_names(
      colnames(result),
      identical.to = c(
        "model_id",
        "output_type",
        "output_type_id",
        "value",
        "reference_date",
        "target",
        "horizon",
        "horizon_timescale",
        "resolution",
        "target_end_date",
        "location",
        "disease"
      )
    )
  }
)
