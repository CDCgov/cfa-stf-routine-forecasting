source("src/cfa/stf/routine/fable/fit_fable.R")

testthat::test_that("the ensemble handles gaps without moving calendar boundaries", {
  set.seed(1343)
  complete <- tibble::tibble(
    date = as.Date("2026-01-01") + 0:167,
    ed_visits = 100 + stats::rpois(168, 10)
  )
  masks <- list(
    beginning = 1:7,
    middle = 70:84,
    tail = 155:168,
    combined = c(1:7, 70:84, 155:168)
  )
  purrr::walk(masks, function(mask) {
    x <- complete
    x$ed_visits[mask] <- NA_real_
    set.seed(1343)
    draws <- fit_and_forecast_ensemble(x, n_samples = 20)
    testthat::expect_identical(attr(draws, "components"), "ETS + ARIMA")
    testthat::expect_true(all(is.finite(draws$other_ed_visits)))
    testthat::expect_true(all(draws$other_ed_visits >= 0))
    testthat::expect_equal(nrow(draws), 28 * 20)
    testthat::expect_equal(min(draws$date), max(x$date) + 1)
    testthat::expect_equal(max(draws$date), max(x$date) + 28)

    # Hold endpoints fixed only for the explicit/implicit internal-gap comparison.
    internal <- setdiff(mask, c(1L, nrow(x)))
    set.seed(1343)
    implicit <- fit_and_forecast_ensemble(x[-internal, ], n_samples = 20)
    testthat::expect_equal(implicit, draws)

    # Boundary rows may also be dropped: extend the horizon from the retained
    # endpoint to cover the same requested period, without requiring equal fits.
    retained <- x[-mask, ]
    forecast_start <- max(complete$date) + 1
    forecast_through <- max(complete$date) + 28
    horizon <- as.integer(forecast_through - max(retained$date))
    set.seed(1343)
    dropped <- fit_and_forecast_ensemble(
      retained,
      n_forecast_days = horizon,
      n_samples = 20
    )
    requested <- dplyr::filter(dropped, date >= forecast_start)
    testthat::expect_equal(
      sort(unique(requested$date)),
      seq(forecast_start, forecast_through, by = "day")
    )
    testthat::expect_equal(nrow(requested), 28 * 20)
    testthat::expect_equal(max(dropped$date), forecast_through)
    testthat::expect_true(all(is.finite(requested$other_ed_visits)))
    testthat::expect_true(all(requested$other_ed_visits >= 0))
  })
})

testthat::test_that("an entirely missing target fails clearly", {
  x <- tibble::tibble(
    date = as.Date("2026-01-01") + 0:83,
    ed_visits = NA_real_
  )
  testthat::expect_error(
    fit_and_forecast_ensemble(x),
    "All training values of ed_visits are missing"
  )
})
