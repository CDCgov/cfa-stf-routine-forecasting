create_model_results <- function(
  file,
  model_name,
  date_options,
  geo_value_options,
  disease_options,
  resolution_options,
  n_draw
) {
  model_components <- hewr::parse_pyrenew_model_name(model_name)

  components_to_variables <-
    list(
      "h" = "observed_hospital_admissions",
      "e" = c(
        "observed_ed_visits",
        "other_ed_visits",
        "prop_disease_ed_visits"
      ),
      "w" = NULL
    )

  variable_options <-
    components_to_variables |>
    purrr::keep(model_components) |>
    unname() |>
    unlist()

  data <- tidyr::expand_grid(
    .draw = 1:n_draw,
    date = date_options,
    geo_value = geo_value_options,
    disease = disease_options,
    resolution = resolution_options,
    .variable = variable_options
  ) |>
    dplyr::mutate(.value = as.double(rpois(dplyr::n(), lambda = 100)))

  forecasttools::write_tabular(data, file)
}

create_observation_data <- function(
  date_range,
  locations,
  target = "wk inc covid prop ed visits"
) {
  data <- tidyr::expand_grid(
    reference_date = date_range,
    location = locations,
    target = target
  ) |>
    dplyr::mutate(value = sample.int(100, dplyr::n(), replace = TRUE))
  return(data)
}

create_hubverse_table <- function(
  date_range,
  horizons,
  locations,
  output_type,
  output_type_id
) {
  data <- tidyr::expand_grid(
    reference_date = date_range,
    horizon = horizons,
    output_type_id = output_type_id,
    location = locations
  ) |>
    dplyr::group_by(
      reference_date,
      horizon,
      location
    ) |>
    dplyr::arrange(output_type_id, .by_group = TRUE) |>
    dplyr::mutate(
      value = sort(
        sample.int(1000, dplyr::n(), replace = TRUE),
        decreasing = FALSE
      ),
      target = "wk inc covid prop ed visits",
      # "wk inc covid hosp	"
      output_type = !!output_type,
      target_end_date = reference_date + 7 * horizon
    ) |>
    dplyr::ungroup()

  return(data)
}
