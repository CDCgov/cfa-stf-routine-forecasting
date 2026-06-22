#' Generate Exponential Growth Process with Poisson Noise
#'
#' This function generates a sequence of samples from an exponential growth
#' process through Poisson sampling:
#' ```math
#' \begin{aligned}
#' \( \lambda_t &= I_0 \exp(\sum_{t=1}^{t} r_t) \) \\
#' I_t &\sim \text{Poisson}(\lambda_t).
#' ```
#' @param rt A numeric vector of exponential growth rates.
#' @param initial A numeric value representing the initial value of the process.
#'
#' @return An integer vector of Poisson samples generated from the exponential
#' growth process.
#'
#' @examples
#' rt <- c(0.1, 0.2, 0.15)
#' initial <- 10
#' generate_exp_growth_pois(rt, initial)
#'
#' @export
generate_exp_growth_pois <- function(rt, initial) {
  means <- initial * exp(cumsum(rt))
  samples <- stats::rpois(length(means), lambda = means)
  return(samples)
}
