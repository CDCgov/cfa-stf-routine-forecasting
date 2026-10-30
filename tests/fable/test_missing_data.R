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

    # Keep boundary rows so the training interval remains explicit.
    internal <- setdiff(mask, c(1L, nrow(x)))
    set.seed(1343)
    implicit <- fit_and_forecast_ensemble(x[-internal, ], n_samples = 20)
    testthat::expect_equal(implicit, draws)
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
