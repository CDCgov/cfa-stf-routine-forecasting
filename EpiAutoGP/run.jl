#!/usr/bin/env julia

# Import EpiAutoGP module with all functions
using EpiAutoGP
using Logging

# Configure logging
logger = SimpleLogger()
global_logger(logger)

"""
EpiAutoGP model runner for PyRenew-HEW pipeline

This script serves as the entry point for running EpiAutoGP models in the PyRenew-HEW
forecasting pipeline. It accepts JSON input data and produces hubverse-compatible outputs.

Usage:
    julia --project=. run.jl --json-input path/to/input.json --output-dir path/to/output

Arguments:
    --json-input: Path to JSON file containing model input data
    --output-dir: Directory for saving model outputs
    --n-forecast-weeks: Number of weeks to forecast (default: 8)
    --n-particles: Number of particles for SMC (default: 24)
    --n-mcmc: Number of MCMC steps for GP kernel structure (default: 100)
    --n-hmc: Number of HMC steps for GP kernel hyperparameters (default: 50)
    --n-forecast-draws: Number of forecast draws (default: 2000)
    --transformation: Data transformation type (default: "boxcox")
    --smc-data-proportion: Proportion of data used in each SMC step (default: 0.1)
"""
function main()
    """
    Main execution function
    """
    return try
        @info "Starting EpiAutoGP model run"

        # Parse command line arguments
        args = parse_arguments()
        @info "Parsed arguments successfully"
        @info "Input file: $(args["json-input"])"
        @info "Output directory: $(args["output-dir"])"

        # Load and validate input data
        @info "Loading input data from JSON file..."
        input_data = read_and_validate_data(args["json-input"])
        @info "Successfully loaded data for $(input_data.pathogen) in $(input_data.location)"
        @info "Data contains $(length(input_data.dates)) time points"
        @info "Forecast date: $(input_data.forecast_date)"

        # Run the EpiAutoGP forecasting model
        @info "Running EpiAutoGP forecasting model..."
        results = forecast_with_epiautogp(input_data, args)
        @info "Model run completed successfully"
        @info "Generated forecasts for $(length(results.forecast_dates)) dates"

        # Create hubverse-compatible output
        @info "Creating hubverse-compatible forecast output..."
        output_type = PipelineOutput()  # Use default quantile levels

        hubverse_df = create_forecast_output(
            input_data,
            results,
            args["output-dir"],
            output_type;
            save_output = true
        )

        @info "EpiAutoGP model run completed successfully"
        @info "Results saved to $(args["output-dir"])"

    catch e
        @error "EpiAutoGP model run failed: $e"
        @error "Stack trace:" exception = (e, catch_backtrace())
        rethrow(e)
    end
end

# Run main function if script is executed directly
if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
