"""
    struct EpiAutoGPInput

A structured input data type for EpiAutoGP epidemiological modeling.

This struct represents the complete input dataset required for running epidemiological
forecasting models in combination with nowcasting using `NowcastAutoGP.jl`. It combines historical observation data
with nowcasting requirements and forecast parameters.

# Fields
- `dates::Vector{Date}`: Vector of observation dates in chronological order
- `reports::Vector{Real}`: Vector of case counts/measurements corresponding to each date
- `pathogen::String`: Disease identifier (e.g., "COVID-19", "Influenza", "RSV")
- `location::String`: Geographic location identifier (e.g., "CA", "NY", "US")
- `target::String`: Target data type (e.g., "nssp", "nhsn")
- `frequency::String`: Temporal frequency of data ("daily" or "epiweekly")
- `ed_visit_type::String`: Type of ED visits ("observed" or "other"), only applicable for NSSP target
- `forecast_date::Date`: Reference date from which forecasting begins, often this will be a nowcast date
- `nowcast_dates::Vector{Date}`: Dates requiring nowcasting (typically recent dates with incomplete data)
- `nowcast_reports::Vector{Vector{Real}}`: Uncertainty bounds or samples for nowcast dates

"""
struct EpiAutoGPInput
    dates::Vector{Date}
    reports::Vector{Real}
    pathogen::String
    location::String
    target::String
    frequency::String
    ed_visit_type::String
    forecast_date::Date
    nowcast_dates::Vector{Date}
    nowcast_reports::Vector{Vector{Real}}
end

# Enable JSON3 serialization
StructTypes.StructType(::Type{EpiAutoGPInput}) = StructTypes.Struct()

"""
    function validate_input(data::EpiAutoGPInput)

Validate an `EpiAutoGPInput` data structure for consistency and correctness.

Performs comprehensive validation including:
- Array length consistency between dates and reports
- Chronological ordering of dates and nowcast dates
- Non-negative finite values for all reports
- Non-empty string identifiers for pathogen and location
- Reasonable forecast date relative to data range
- Proper structure of nowcast data

# Arguments
- `data::EpiAutoGPInput`: The input data structure to validate

# Returns
- `Bool`: Returns `true` if validation passes
"""
function validate_input(data::EpiAutoGPInput; valid_targets=["nhsn", "nssp"])
    @assert data.target in valid_targets "Target must be one of $(valid_targets), got '$(data.target)'"
    # Check array length consistency
    if length(data.dates) != length(data.reports)
        throw(ArgumentError("Length mismatch: dates ($(length(data.dates))) and reports ($(length(data.reports))) must have the same length"))
    end

    # Check for non-empty essential data
    if length(data.dates) == 0
        throw(ArgumentError("Empty data: dates and reports cannot be empty"))
    end

    # Check date ordering
    if !issorted(data.dates)
        throw(ArgumentError("Date ordering: dates must be sorted chronologically"))
    end

    # Check nowcast data consistency
    # If no nowcast dates, should have no nowcast reports (pure forecasting)
    if isempty(data.nowcast_dates) && !isempty(data.nowcast_reports)
        throw(ArgumentError("Nowcast consistency error: no nowcast_dates provided but nowcast_reports is not empty"))
    end

    # If nowcast dates exist, each vector in nowcast_reports should have length equal to number of nowcast_dates
    # (each vector represents one realization across all nowcast dates)
    if !isempty(data.nowcast_dates)
        for (i, report_vec) in enumerate(data.nowcast_reports)
            if length(report_vec) != length(data.nowcast_dates)
                throw(ArgumentError("Nowcast vector length mismatch at index $i: nowcast_reports[$i] has length $(length(report_vec)) but should have length $(length(data.nowcast_dates)) to match nowcast_dates"))
            end
        end
    end

    # Check nowcast date ordering
    if !isempty(data.nowcast_dates) && !issorted(data.nowcast_dates)
        throw(ArgumentError("Nowcast date ordering: nowcast_dates must be sorted chronologically"))
    end

    # Check string identifiers
    if isempty(strip(data.pathogen))
        throw(ArgumentError("Invalid pathogen: pathogen cannot be empty or whitespace"))
    end

    if isempty(strip(data.location))
        throw(ArgumentError("Invalid location: location cannot be empty or whitespace"))
    end

    # Check numerical validity
    for (i, report) in enumerate(data.reports)
        if !isfinite(report) || report < 0
            throw(ArgumentError("Invalid report value at index $i: reports must be non-negative finite numbers (got $report)"))
        end
    end

    # Check nowcast reports validity
    for (i, report_vec) in enumerate(data.nowcast_reports)
        for (j, report) in enumerate(report_vec)
            if !isfinite(report) || report < 0
                throw(ArgumentError("Invalid nowcast report value at index [$i][$j]: must be non-negative finite number (got $report)"))
            end
        end
    end

    # Check forecast date reasonableness
    if !isempty(data.dates)
        date_range = maximum(data.dates) - minimum(data.dates)
        days_buffer = max(30, Int(ceil(date_range.value / 10)))

        if data.forecast_date < minimum(data.dates) - Day(days_buffer)
            throw(ArgumentError("Forecast date ($(data.forecast_date)) is too far before the data range ($(minimum(data.dates)) to $(maximum(data.dates)))"))
        end

        if data.forecast_date > maximum(data.dates) + Day(days_buffer)
            throw(ArgumentError("Forecast date ($(data.forecast_date)) is too far after the data range ($(minimum(data.dates)) to $(maximum(data.dates)))"))
        end
    end

    return true
end

"""
    function read_data(path_to_json::String)

Read and parse epidemiological input data from a JSON file.

This function reads a JSON file and deserializes it into an `EpiAutoGPInput` struct
using JSON3.jl. The JSON file should contain all required fields matching the
struct definition.

# Arguments
- `path_to_json::String`: Path to the JSON file containing input data

# Returns
- `EpiAutoGPInput`: Parsed data structure ready for model input

# Throws
- `SystemError`: If the file cannot be read (e.g., file not found)
- `JSON3.StructuralError`: If JSON structure doesn't match expected format
- `ArgumentError`: If date parsing fails or data types are incompatible

# Examples
```julia
# Read data from a JSON file
data = read_data("path/to/input_data.json")

# The JSON file should have structure like:
# {
#   "dates": ["2024-01-01", "2024-01-02"],
#   "reports": [45.0, 52.0],
#   "pathogen": "COVID-19",
#   "location": "CA",
#   "forecast_date": "2024-01-02",
#   "nowcast_dates": [],
#   "nowcast_reports": []
# }
```

!!! note
    This function does not validate the data. Use [`read_and_validate_data`](@ref)
    for automatic validation, or call [`validate_input`](@ref) separately.
"""
function read_data(path_to_json::String)
    json_string = read(path_to_json, String)
    data = JSON3.read(json_string, EpiAutoGPInput)
    return data
end

"""
    read_and_validate_data(path_to_json::String) -> EpiAutoGPInput

Read epidemiological data from JSON file with automatic validation.
"""
function read_and_validate_data(path_to_json::String)
    data = read_data(path_to_json)
    validate_input(data)
    return data
end
