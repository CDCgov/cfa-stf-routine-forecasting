
@testset "Output Types and Structures Tests" begin
    @testset "QuantileOutput Construction" begin
        default_output = QuantileOutput()
        @test length(default_output.quantile_levels) == 23
        @test 0.5 in default_output.quantile_levels
        @test issorted(default_output.quantile_levels)

        custom_output = QuantileOutput(quantile_levels=[0.25, 0.5, 0.75])
        @test custom_output.quantile_levels == [0.25, 0.5, 0.75]
    end
end

@testset "create_forecast_df Function Tests" begin
    @testset "Basic Functionality" begin
        forecast_dates = [Date("2024-01-01"), Date("2024-01-02")]
        forecasts = rand(2, 100) .* 50 .+ 25
        output_type = QuantileOutput(quantile_levels=[0.25, 0.5, 0.75])

        result_df = create_forecast_df(
            (forecast_dates=forecast_dates, forecasts=forecasts), output_type)

        @test isa(result_df, DataFrame)
        @test size(result_df, 1) == 6  # 2 dates × 3 quantiles
        @test all(result_df.output_type .== "quantile")
        @test Set(unique(result_df.output_type_id)) == Set([0.25, 0.5, 0.75])

        # Test quantile ordering
        for date_obj in forecast_dates
            date_rows = result_df[result_df.target_end_date.==date_obj, :]
            q25 = date_rows[date_rows.output_type_id.==0.25, :].value[1]
            q50 = date_rows[date_rows.output_type_id.==0.5, :].value[1]
            q75 = date_rows[date_rows.output_type_id.==0.75, :].value[1]
            @test q25 <= q50 <= q75
        end
    end
end

@testset "create_forecast_output Function Tests" begin
    @testset "End-to-end Functionality" begin
        # Create input data
        input = EpiAutoGPInput(
            [Date("2024-01-01")],
            [100.0],
            "COVID-19",
            "CA",
            "nhsn",
            "epiweekly",
            "observed",
            Date("2024-01-01"),
            Date[],
            Vector{Real}[]
        )

        # Create forecast results
        forecast_dates = [Date("2024-01-08"), Date("2024-01-15")]
        forecasts = rand(2, 50) .* 100 .+ 50
        results = (forecast_dates=forecast_dates, forecasts=forecasts)

        output_type = QuantileOutput(quantile_levels=[0.5])

        tmpdir = mktempdir()
        try
            result_df = create_forecast_output(
                input, results, tmpdir, output_type;
                save_output=true
            )

            @test isa(result_df, DataFrame)
            @test size(result_df, 1) == 2
            @test all(result_df.location .== "CA")
            @test all(result_df.target .== "wk inc covid hosp")

            # Check file was saved
            csv_files = filter(f -> endswith(f, ".csv"), readdir(tmpdir))
            @test length(csv_files) == 1
        finally
            rm(tmpdir, recursive=true)
        end
    end
end
