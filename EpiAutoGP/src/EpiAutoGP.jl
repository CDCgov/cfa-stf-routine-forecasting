module EpiAutoGP
using NowcastAutoGP # Core modeling package
using CSV, DataFramesMeta, Dates, JSON3, StructTypes, Parquet # Data handling packages
using ArgParse # Command-line argument parsing
using Statistics # For modeling functions

# Export command line argument parsing
export parse_arguments

# Export input data structures and functions
export EpiAutoGPInput,
    validate_input,
    read_data,
    read_and_validate_data

# Export modeling functions
export prepare_for_modelling,
    fit_base_model,
    forecast_with_epiautogp,
    forecast_with_epiautogp

# Export output functions and types
export AbstractForecastOutput,
    AbstractHubverseOutput,
    QuantileOutput,
    PipelineOutput,
    create_forecast_df,
    create_forecast_output

# constants for default pathogen and target abbreviations
const DEFAULT_PATHOGEN_DICT = Dict(
    "COVID-19" => "covid",
    "Influenza" => "flu",
    "RSV" => "rsv"
)
const DEFAULT_TARGET_DICT = Dict(
    "nhsn" => "hosp",
    "nssp" => "prop ed visits"
)
const DEFAULT_TARGET_LETTER = Dict(
    "nhsn" => "h",
    "nssp" => "e"
)
const DEFAULT_GROUP_NAME = "CFA"
const DEFAULT_MODEL_NAME = "EpiAutoGP"

# Include source files
include("parse_arguments.jl") # Function to parse command line arguments
include("input.jl")           # Functions to load and process input JSON data
include("modelling.jl")        # Main modeling and forecasting functions
include("output.jl")           # Functions for generating hubverse outputs

end
