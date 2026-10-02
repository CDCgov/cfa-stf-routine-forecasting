#!/usr/bin/env julia
#
# Production runner for the basic seir model with trending ascertainment fitted on daily NSSP counts.
# Features:
# - SEIRS dynamics
# - Indoor-activity seasonal forcing with a stationary latent correction
# - trending ascertainment latent
# - Plug-in weekday effects
# - Fitted by a particle filter with Liu-West on-line parameter learning
#
# Input and output:
# Reads the JSON written by `prep_mech_nssp_daily_data.py` and writes the routine
# `samples.parquet` contract plus diagnostics next to it.

using ArgParse, Dates, DataFrames, JSON3, StructTypes, Random, TOML, CSV
using ConfigurableEpi
using AlgebraicEpiMech

# The local production submodel defines `parse_epi_config`, `default_priors` and
# `build_model(ctx)` in `Main`.
include(joinpath(@__DIR__, "submodels", "basic_seir_daily_nssp_trend.jl"))

const SUBMODEL = "basic_seir_daily_nssp_trend"
const CLIMATOLOGY_PATH = joinpath(@__DIR__, "data", "indoor_activity_climatology.csv2")
const _SETTLED_TOLERANCE = 1.0e-12
const STEP_DAYS = 1.0
const SUPERSAMPLE = 2
const LIU_WEST_DISCOUNT = 0.95

"""
Input written by the Python pipeline. `dates` is the complete daily grid from the first to the
last training date; a day the vintage does not report is `null` in all three series.
`observations` are the nowcast-corrected, rounded counts the model fits; `raw_observations` and
`reporting_fractions` are kept for the weekday estimator's settled-day gate and for diagnostics.
"""
struct MechNSSPDailyInput
    dates::Vector{Date}
    observations::Vector{Union{Nothing, Float64}}
    raw_observations::Vector{Union{Nothing, Float64}}
    reporting_fractions::Vector{Union{Nothing, Float64}}
    location::String
    disease::String
    population::Int
    report_date::Date
    forecast_through::Date
end

StructTypes.StructType(::Type{MechNSSPDailyInput}) = StructTypes.Struct()

### Read, validate and prepare model input

include("input.jl")

################ Command line ################

function parse_arguments(args = ARGS)
    settings = ArgParseSettings(
        description = "Fit the mechanistic daily NSSP model and write routine forecast outputs.",
    )
    @add_arg_table! settings begin
        "--json-input"
        help = "path to the model input JSON"
        arg_type = String
        required = true

        "--output-dir"
        help = "directory in which to write forecast outputs"
        arg_type = String
        required = true

        "--n-particles"
        help = "number of particle-filter particles"
        arg_type = Int
        default = 3000
        range_tester = x -> x > 0

        "--n-forecast-draws"
        help = "number of forecast draws"
        arg_type = Int
        default = 2000
        range_tester = x -> x > 0

        "--seed"
        help = "base random seed"
        arg_type = Int
        default = 2026

        "--min-observations"
        help = "minimum number of observed days required to fit the model"
        arg_type = Int
        default = 84
        range_tester = x -> x > 0

        "--dow-window-days"
        help = "number of recent days available to the day-of-week estimator (minimum 35)"
        arg_type = Int
        default = 365
        range_tester = x -> x >= 35

        "--dow-min-exclude-days"
        help = "minimum number of recent days excluded from day-of-week estimation"
        arg_type = Int
        default = 14
        range_tester = x -> x >= 0
    end
    return parse_args(args, settings)
end


################ Model configuration ################

build_epi_config() = parse_epi_config(Dict{String, Any}(), Val(Symbol(SUBMODEL)))

################ Model pieces ################

"""A stable seed for an origin, independent of which other origins are run (run_model.jl)."""
_origin_seed(base_seed::Integer, report_date::Date) =
    Int(mod(base_seed + Dates.value(report_date - Date(1970, 1, 1)), typemax(Int32) - 1) + 1)

# Numerical diagnostics for the integration stability of the SEIR-family stage-count layout with
# the default RK4 integration scheme.
function assert_run_integration_stable(epi, prior_specs; climatology)
    assert_integration_stable(
        epi.durations_days, epi.n_E_stages, epi.n_I_stages, STEP_DAYS, SUPERSAMPLE;
        R_eff_max = prior_R_eff_bound(
            prior_specs;
            chi_max = seasonal_forcing_upper_bound(
                PRODUCTION_SEASONALITY, 0.0, prior_specs; climatology,
            ),
        ),
    )
    return nothing
end

"""
    revisable_tail_length(reporting_fraction) -> Int

Number of trailing slots from the first still-provisional day (reporting fraction below 1)
onwards, a slot with no report counting as settled (`_settled_prefix_length` in run_model.jl).
"""
function revisable_tail_length(reporting_fraction)
    fraction = coalesce.(reporting_fraction, 1.0)
    first_open = findfirst(<(1.0 - _SETTLED_TOLERANCE), fraction)
    return first_open === nothing ? 0 : length(fraction) - first_open + 1
end

"""
    estimate_weekday_effects(asof, epi, args) -> (; effects, exclude_recent_days, window_days)

The plug-in weekday effect from the latest vintage's settled days. The history contract drops the
grid's final slot (as `_daily_nssp_asof_history` does); on what remains, every day the reporting
PMF still marks as incomplete is excluded, with `--dow-min-exclude-days` as the floor, and the
window is `--dow-window-days` (a full year by default, so that it holds at least one complete
rise-peak-fall cycle whichever week the run lands on). A fallback to no effect is an error.
"""
function estimate_weekday_effects(asof::DataFrame, epi, args::Dict{String, Any})
    prepared = prepare_day_of_week_history(asof.date, asof.counts, last(asof.date) + Day(1))
    n_revisable = revisable_tail_length(asof.reporting_fraction[1:(end - 1)])
    exclude_recent_days = max(args["dow-min-exclude-days"], n_revisable)
    window_days = args["dow-window-days"]
    effects = estimate_day_of_week_effects(
        prepared.dates, prepared.counts;
        phi = epi.fixed.phi, window_days, exclude_recent_days,
    )
    effects.fallback && error(
        "day-of-week estimation fell back to no effect (window $window_days d, excluding the last " *
            "$exclude_recent_days d of $(nrow(asof) - 1) history slots); the series is too short or too sparse",
    )
    last_kept = length(prepared.dates) - exclude_recent_days
    first_kept = max(1, last_kept - window_days + 1)
    @info "Estimated day-of-week effects from the latest vintage" window_start = prepared.dates[first_kept] window_end = prepared.dates[last_kept] n_revisable exclude_recent_days n_used = effects.n_used weights = round.(effects.weights; digits = 4) extra_var = round.(effects.extra_var; digits = 5)
    return (; effects, exclude_recent_days, window_days)
end

"""
    fit_and_forecast(input, args) -> NamedTuple

Fit the production model once and forecast through the requested horizon.
"""
function fit_and_forecast(input::MechNSSPDailyInput, args::Dict{String, Any})
    started = time()
    loc = lowercase(input.location)
    asof = asof_frame(input)
    T = nrow(asof)
    n_observed = count(!ismissing, asof.counts)
    last_observation_date = maximum(asof.date[.!ismissing.(asof.counts)])

    # Forecast every day after the last training slot through `forecast_through` (for a
    # Wednesday report fitted through Tuesday that is `report_date + 0:24`, the backtest's 25).
    n_ahead = Dates.value(input.forecast_through - last(asof.date))
    target_dates = last(asof.date) .+ Day.(1:n_ahead)

    epi = build_epi_config()
    prior_specs = default_priors()
    priors = build_priors(prior_specs)
    climatology = load_indoor_activity_climatology(CLIMATOLOGY_PATH)
    assert_run_integration_stable(epi, prior_specs; climatology)

    weekday = estimate_weekday_effects(asof, epi, args)

    # Seed history: the observed days of the prepared (final slot dropped) grid on the model clock.
    prepared = prepare_day_of_week_history(asof.date, asof.counts, last(asof.date) + Day(1))
    start_date = first(asof.date)
    observed = findall(!ismissing, prepared.counts)
    history = (
        times = Float64[Dates.value(prepared.dates[i] - start_date) for i in observed],
        counts = Float64[prepared.counts[i] for i in observed],
    )

    first_count = max(Float64(first(asof.counts)), 1.0)
    # The backtest read the population from a CSV column parsed as Float64; keep that type.
    pop = Float64(input.population)
    ctx = (
        pop = pop,
        i0 = first_count,
        loc = loc,
        start_date = start_date,
        step_days = STEP_DAYS,
        priors = priors,
        prior_specs = prior_specs,
        history = history,
        day_of_week_effects = weekday.effects,
        epi = epi,
        climatology = climatology,
    )
    model = build_model(ctx)

    origin_seed = _origin_seed(args["seed"], input.report_date)
    Random.seed!(origin_seed)
    rng = Random.MersenneTwister(origin_seed)
    filter = PF(n_particles = args["n-particles"], threads = true)
    hyper = LiuWest(discount = LIU_WEST_DISCOUNT)
    engine = build_inference(
        filter, hyper, model;
        dt = STEP_DAYS, supersample = SUPERSAMPLE, n_ahead,
        n_draws = args["n-forecast-draws"], rng,
    )
    @info "ConfigurableEpi daily NSSP origin" report_date = input.report_date last_observation_date grid_slots = T reported = n_observed start_date n_ahead origin_seed n_particles = filter.n_particles threads = Threads.nthreads()
    result = fit_forecast!(engine, asof.counts, 1; emit_forecast = true)
    result.samples === nothing && error("daily NSSP inference returned no forecast draws for $(input.report_date)")

    return (;
        asof, target_dates, result, model, weekday, origin_seed, start_date, n_observed,
        last_observation_date, elapsed_seconds = time() - started,
    )
end

################ Output ################

_package_provenance(manifest_path) = begin
    manifest = TOML.parsefile(manifest_path)
    deps = manifest["deps"]
    Dict(
        name => Dict(k => v for (k, v) in only(deps[name]) if k in ("version", "git-tree-sha1", "path", "repo-url", "repo-rev"))
            for name in ("AlgebraicEpiMech", "ConfigurableEpi") if haskey(deps, name)
    )
end

function write_outputs(input::MechNSSPDailyInput, fit::NamedTuple, output_dir::AbstractString, args::Dict{String, Any})
    mkpath(output_dir)
    asof, result, target_dates = fit.asof, fit.result, fit.target_dates

    samples_path = joinpath(output_dir, "samples.parquet")
    write_forecast_samples(
        samples_path,
        forecast_sample_rows(
            result.samples, target_dates;
            geo_value = uppercase(input.location),
            disease = input.disease,
            variable = "observed_ed_visits",
            resolution = "daily",
        ),
    )

    fitted = DataFrame(
        date = asof.date,
        count = Float64.(result.fitted_means),
        observation = float.(asof.counts),
        raw_observation = float.(asof.raw_counts),
        reporting_fraction = float.(asof.reporting_fraction),
    )
    CSV.write(joinpath(output_dir, "mech_nssp_daily_fitted.csv"), fitted)

    hyper_df = copy(result.summary)
    foreach(row -> push!(hyper_df, row), day_of_week_report_rows(fit.weekday.effects))
    hyper_df[!, :origin_seed] = fill(fit.origin_seed, nrow(hyper_df))
    CSV.write(joinpath(output_dir, "mech_nssp_daily_hyperparameters.csv"), hyper_df)

    qmat = result.quantiles
    quantiles = DataFrame(
        target_date = repeat(target_dates, length(DEFAULT_QS)),
        horizon = repeat(1:length(target_dates), length(DEFAULT_QS)),
        quantile = repeat(collect(DEFAULT_QS); inner = length(target_dates)),
        value = vec(qmat),
    )
    CSV.write(joinpath(output_dir, "mech_nssp_daily_quantiles.csv"), quantiles)

    metadata = Dict(
        "submodel" => SUBMODEL,
        "location" => input.location,
        "disease" => input.disease,
        "population" => input.population,
        "report_date" => string(input.report_date),
        "forecast_through" => string(input.forecast_through),
        "start_date" => string(fit.start_date),
        "last_observation_date" => string(fit.last_observation_date),
        "grid_slots" => nrow(asof),
        "n_observed" => fit.n_observed,
        "n_ahead" => length(target_dates),
        "seed" => args["seed"],
        "origin_seed" => fit.origin_seed,
        "n_particles" => args["n-particles"],
        "n_forecast_draws" => args["n-forecast-draws"],
        "julia_threads" => Threads.nthreads(),
        "inference" => Dict(
            "filter" => "pf",
            "hyperparameter_method" => "liu_west",
            "step_days" => STEP_DAYS,
            "supersample" => SUPERSAMPLE,
            "liu_west_discount" => LIU_WEST_DISCOUNT,
            "filter_threads" => true,
            "learned_parameters" => String.(propertynames(fit.model.priors)),
        ),
        "day_of_week" => Dict(
            "window_days" => fit.weekday.window_days,
            "exclude_recent_days" => fit.weekday.exclude_recent_days,
            "n_used" => fit.weekday.effects.n_used,
            "weights" => collect(fit.weekday.effects.weights),
            "extra_var" => collect(fit.weekday.effects.extra_var),
        ),
        "julia_version" => string(VERSION),
        "packages" => _package_provenance(joinpath(@__DIR__, "Manifest.toml")),
        "fit_seconds" => round(fit.elapsed_seconds; digits = 1),
    )
    open(joinpath(output_dir, "mech_nssp_daily_run_metadata.json"), "w") do io
        JSON3.pretty(io, metadata)
    end
    @info "Wrote routine-compatible forecast samples" samples_path rows = length(result.samples) fit_seconds = round(fit.elapsed_seconds; digits = 1)
    return samples_path
end

function main()
    return try
        args = parse_arguments()
        input = read_and_validate_data(args["json-input"], args["min-observations"])
        fit = fit_and_forecast(input, args)
        write_outputs(input, fit, args["output-dir"], args)
    catch e
        println(stderr, "mech_nssp_daily pipeline run failed:")
        showerror(stderr, e, catch_backtrace())
        println(stderr)
        rethrow()
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
