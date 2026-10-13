################ Input ################

read_data(path::AbstractString) = JSON3.read(read(path, String), MechNSSPDailyInput)

function _require(condition::Bool, message::String)
    condition || throw(ArgumentError(message))
    return nothing
end

function validate_input(input::MechNSSPDailyInput, min_observations::Integer)
    n = length(input.dates)
    _require(n > 0, "Empty data: dates cannot be empty")
    _require(
        length(input.observations) == n && length(input.raw_observations) == n &&
            length(input.reporting_fractions) == n,
        "dates, observations, raw_observations and reporting_fractions must have equal length",
    )
    _require(
        input.dates == collect(first(input.dates):Day(1):last(input.dates)),
        "dates must be a gap-free ascending daily grid; fill absent days with null",
    )
    for i in 1:n
        obs, raw, fraction = input.observations[i], input.raw_observations[i], input.reporting_fractions[i]
        if obs === nothing
            _require(
                raw === nothing && fraction === nothing,
                "observations[$i] is null, so raw_observations[$i] and reporting_fractions[$i] must be null too",
            )
            continue
        end
        _require(
            raw !== nothing && fraction !== nothing,
            "observations[$i] is present, so raw_observations[$i] and reporting_fractions[$i] must be too",
        )
        _require(isfinite(obs) && obs >= 0, "observations[$i] must be a non-negative finite number, got $obs")
        _require(isfinite(raw) && raw >= 0, "raw_observations[$i] must be a non-negative finite number, got $raw")
        _require(0 < fraction <= 1, "reporting_fractions[$i] must lie in (0, 1], got $fraction")
    end
    _require(input.observations[1] !== nothing, "the first grid date must carry an observation")
    _require(
        last(input.dates) < input.report_date,
        "the last training date $(last(input.dates)) must precede report_date $(input.report_date)",
    )
    _require(
        input.report_date <= input.forecast_through,
        "report_date $(input.report_date) must not follow forecast_through $(input.forecast_through)",
    )
    _require(input.population > 0, "population must be positive")
    n_observed = count(!isnothing, input.observations)
    _require(
        n_observed >= min_observations,
        "only $n_observed observed days; the model needs at least $min_observations (--min-observations)",
    )
    return input
end

read_and_validate_data(path::AbstractString, min_observations::Integer) =
    validate_input(read_data(path), min_observations)

# The as-of frame `reindex_to_grid` builds in the backtest: one row per grid date, `missing` at a
# day with no report, integer counts (the fitted column is the rounded nowcast).
_missing_int(x) = x === nothing ? missing : round(Int, x)
_missing_float(x) = x === nothing ? missing : Float64(x)

function asof_frame(input::MechNSSPDailyInput)
    return DataFrame(
        date = input.dates,
        counts = _missing_int.(input.observations),
        raw_counts = _missing_int.(input.raw_observations),
        reporting_fraction = _missing_float.(input.reporting_fractions),
    )
end
