test_that("generate_exp_growth_pois generates correct number of samples", {
  rt <- c(0.1, 0.2, 0.15)
  initial <- 10
  samples <- generate_exp_growth_pois(rt, initial)
  expect_length(samples, length(rt))
})

test_that("generate_exp_growth_pois returns a vector of integers", {
  rt <- c(0.1, 0.2, 0.15)
  initial <- 10
  samples <- generate_exp_growth_pois(rt, initial)
  expect_type(samples, "integer")
})

test_that("generate_exp_growth_pois does not return implausible values", {
  rt <- c(0.1, 0.2, 0.15)
  initial <- 10
  analytic_av <- initial * exp(cumsum(rt))
  analytic_std <- sqrt(analytic_av)
  samples <- generate_exp_growth_pois(rt, initial)
  expect_true(all(samples <= initial + 10 * analytic_std))
  expect_true(all(samples >= initial - 10 * analytic_std))
})

test_that("generate_exp_growth_pois handles empty growth rates", {
  rt <- numeric(0)
  initial <- 10
  samples <- generate_exp_growth_pois(rt, initial)
  expect_equal(samples, numeric(0))
})
