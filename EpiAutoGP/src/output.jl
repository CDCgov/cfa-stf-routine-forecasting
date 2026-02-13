"""
    AbstractForecastOutput

Abstract base type for all forecast output formats in EpiAutoGP.
"""
abstract type AbstractForecastOutput end

"""
    AbstractHubverseOutput <: AbstractForecastOutput

Abstract type for hubverse-compatible forecast outputs in CSV format.
"""
abstract type AbstractHubverseOutput <: AbstractForecastOutput end

"""
    PipelineOutput <: AbstractForecastOutput

Abstract type for directly outputting forecasts as typical pipeline outputs for
    `cfa-stf-routine-forecasting`.
"""
struct PipelineOutput <: AbstractForecastOutput end

"""
    QuantileOutput <: AbstractHubverseOutput

Configuration for quantile-based forecast outputs compatible with hubverse specifications.
"""
@kwdef struct QuantileOutput <: AbstractHubverseOutput
    quantile_levels::Vector{Float64} = [
        0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
        0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.975, 0.99]
end

"""
    _make_horizon_col(target_end_dates::Vector{Date}, reference_date::Date) -> Vector{Int}

Calculate forecast horizons in weeks from reference date to target dates.

This internal helper function computes the horizon column required for hubverse
forecast tables. Horizons represent the number of weeks between the reference
date (when the forecast was made) and each target date.

# Arguments
- `target_end_dates::Vector{Date}`: Vector of forecast target dates
- `reference_date::Date`: Reference date for the forecast (forecast creation date)

# Returns
- `Vector{Int}`: Vector of horizons in weeks (integer division by 7 days)
"""
function _make_horizon_col(target_end_dates::Vector{Date}, reference_date::Date)
    return [Dates.value(d - reference_date) ÷ 7 for d in target_end_dates]
end

"""
    create_forecast_df(results::NamedTuple, output_type::QuantileOutput) -> DataFrame

Convert EpiAutoGP forecast results to a basic DataFrame with quantile summaries.

This function processes raw forecast samples from the EpiAutoGP model and computes
quantile summaries for each forecast date. The resulting DataFrame contains the
core forecast data needed for hubverse tables.

# Arguments
- `results::NamedTuple`: Model results containing `forecast_dates` and `forecasts`
  - `forecast_dates::Vector{Date}`: Dates for which forecasts were generated
  - `forecasts::Matrix`: Forecast samples (dates × samples)
- `output_type::QuantileOutput`: Configuration specifying which quantiles to compute

# Returns
- `DataFrame`: Basic forecast DataFrame with columns:
  - `output_type_id`: Quantile level (e.g., 0.5 for median)
  - `value`: Computed quantile value
  - `target_end_date`: Date for which the forecast applies
  - `output_type`: Always "quantile" for this method
"""
function create_forecast_df(results::NamedTuple, output_type::QuantileOutput)
    # Extract relevant data
    forecast_dates = results.forecast_dates
    forecasts = results.forecasts
    # Create a DataFrame with columns: output_type_id, value, target_end_date, output_type
    forecast_df = DataFrame(output_type_id=Float64[], value=Float64[], target_end_date=Date[])
    # Populate the DataFrame row by row
    for (date_idx, target_end_date) in enumerate(forecast_dates)
        date_samples = forecasts[date_idx, :]
        for q_level in output_type.quantile_levels
            q_value = quantile(date_samples, q_level)
            push!(forecast_df,
                (output_type_id=q_level,
                    value=q_value,
                    target_end_date=target_end_date))
        end
    end
    # Add constant column for output_type, this method is specifically for quantiles
    forecast_df[!, "output_type"] .= "quantile"
    return forecast_df
end

function create_forecast_df(results::NamedTuple, output_type::PipelineOutput)
    # Extract relevant data
    forecast_dates = results.forecast_dates
    forecasts = results.forecasts

    # Create a DataFrame with columns: date, .value, .draw
    forecast_df = mapreduce(vcat, enumerate(eachcol(forecasts))) do (draw, sampled_values)
        DataFrame(
            :date => forecast_dates,
            Symbol(".value") => sampled_values,
            Symbol(".draw") => fill(draw, length(sampled_values))
        )
    end

    return forecast_df
end

"""
    create_forecast_output(input, results, output_dir, output_type; kwargs...) -> DataFrame

Create complete hubverse-compatible forecast table from EpiAutoGP results.

This is the main function for generating hubverse forecast outputs. It combines
forecast results with metadata from the input to create a fully compliant
hubverse table, optionally saving it to disk.

# Arguments
- `input::EpiAutoGPInput`: Original input data containing metadata
- `results::NamedTuple`: Model forecast results with `forecast_dates` and `forecasts`
- `output_dir::String`: Directory path for saving output files
- `output_type::AbstractHubverseOutput`: Output format configuration

# Keyword Arguments
- `save_output::Bool`: Whether to save the table to a CSV file
- `disease_abbr::Dict{String, String}`: Disease name abbreviations (default: DEFAULT_PATHOGEN_DICT)
- `target_abbr::Dict{String, String}`: Target type abbreviations (default: DEFAULT_TARGET_DICT)
- `group_name::String`: Forecasting group identifier (default: DEFAULT_GROUP_NAME)
- `model_name::String`: Model identifier (default: DEFAULT_MODEL_NAME)

# Returns
- `DataFrame`: Complete hubverse-compatible forecast table with columns:
  - `output_type`: Type of forecast output ("quantile")
  - `output_type_id`: Quantile level or other output identifier
  - `value`: Forecast value
  - `reference_date`: Date when forecast was made
  - `target`: Target description (e.g., "wk inc covid hosp")
  - `horizon`: Forecast horizon in weeks
  - `target_end_date`: Date for which forecast applies
  - `location`: Geographic location identifier
"""
function create_forecast_output(
    input::EpiAutoGPInput,
    results::NamedTuple,
    output_dir::String,
    output_type::AbstractHubverseOutput;
    save_output::Bool,
    disease_abbr::Dict{String,String}=DEFAULT_PATHOGEN_DICT,
    target_abbr::Dict{String,String}=DEFAULT_TARGET_DICT,
    group_name::String=DEFAULT_GROUP_NAME,
    model_name::String=DEFAULT_MODEL_NAME
)
    # Extract relevant data
    forecast_date = input.forecast_date
    location = input.location
    pathogen = input.pathogen
    target = input.target
    target_col_string = "wk inc $(disease_abbr[pathogen]) $(target_abbr[target])"

    # Create basic forecast DataFrame
    forecast_df = create_forecast_df(results, output_type)

    # Add additional required columns
    forecast_df[!, "reference_date"] .= forecast_date
    forecast_df[!, "location"] .= location
    forecast_df[!, "target"] .= target_col_string

    # Add horizon column
    @transform!(forecast_df, :horizon = _make_horizon_col(:target_end_date, forecast_date))
    # Reorder columns and check all required columns are present
    @select!(forecast_df,
        :output_type, :output_type_id, :value, :reference_date, :target, :horizon, :target_end_date,
        :location)

    # Save as CSV to match expected format
    if save_output
        outputfilename = "$(string(forecast_date))-$(group_name)-$(model_name)-$(location)-$(disease_abbr[pathogen])-$(target).csv"
        csv_path = joinpath(output_dir, outputfilename)
        mkpath(dirname(csv_path))
        CSV.write(csv_path, forecast_df)

        @info "Saved hubverse forecast table to $csv_path"
    end

    return forecast_df
end

function create_forecast_output(
    input::EpiAutoGPInput,
    results::NamedTuple,
    output_dir::String,
    output_type::PipelineOutput;
    save_output::Bool,
    disease_abbr::Dict{String,String}=DEFAULT_PATHOGEN_DICT,
    target_abbr::Dict{String,String}=DEFAULT_TARGET_DICT,
    group_name::String=DEFAULT_GROUP_NAME,
    model_name::String=DEFAULT_MODEL_NAME
)
    # Create basic forecast DataFrame with date, .draw, .value
    forecast_df = create_forecast_df(results, output_type)

    # Determine variable name based on target and whether using percentages
    variable_name = if input.target == "nhsn"
        "observed_hospital_admissions"
    else  # nssp
        if input.use_percentage
            "prop_disease_ed_visits"
        else
            # Use ed_visit_type to determine which ED visits column
            input.ed_visit_type == "other" ? "other_ed_visits" : "observed_ed_visits"
        end
    end

    # Input is in percentage format (0-100); convert to proportion (0-1) as R expects proportions for prop_ variables
    if input.use_percentage && input.target == "nssp"
        forecast_df[!, Symbol(".value")] = forecast_df[!, Symbol(".value")] ./ 100.0
    end

    # Add .variable and resolution columns
    forecast_df[!, Symbol(".variable")] .= variable_name
    forecast_df[!, :resolution] .= input.frequency

    # Add metadata columns for hubverse compatibility
    forecast_df[!, :geo_value] .= input.location
    forecast_df[!, :disease] .= input.pathogen

    # Add metadata columns for hubverse compatibility
    forecast_df[!, :geo_value] .= input.location
    forecast_df[!, :disease] .= input.pathogen

    # Convert date column to string for parquet compatibility
    # Julia parquet does not support writing Dates
    forecast_df[!, :date] = string.(forecast_df[!, :date])

    # Save as parquet if requested
    if save_output
        # Use model-specific naming with frequency prefix
        # This matches the convention: {frequency}_{model}_samples_{target_letter}.parquet
        parquet_path = joinpath(output_dir, "samples_raw.parquet")
        mkpath(dirname(parquet_path))
        Parquet.write_parquet(parquet_path, forecast_df)

        @info "Saved pipeline forecast samples to $parquet_path"
    end

    return forecast_df
end
