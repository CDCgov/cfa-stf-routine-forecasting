@testset "Output Types and Structures Tests" begin
    @testset "QuantileOutput Construction" begin
        default_output = QuantileOutput()
        @test length(default_output.quantile_levels) == 23
        @test 0.5 in default_output.quantile_levels
        @test issorted(default_output.quantile_levels)

        custom_output = QuantileOutput(quantile_levels = [0.25, 0.5, 0.75])
        @test custom_output.quantile_levels == [0.25, 0.5, 0.75]
    end
end

@testset "create_forecast_df Function Tests" begin
    @testset "Basic Functionality" begin
        forecast_dates = [Date("2024-01-01"), Date("2024-01-02")]
        forecasts = rand(2, 100) .* 50 .+ 25
        output_type = QuantileOutput(quantile_levels = [0.25, 0.5, 0.75])

        result_df = create_forecast_df(
            (forecast_dates = forecast_dates, forecasts = forecasts), output_type
        )

        @test isa(result_df, DataFrame)
        @test size(result_df, 1) == 6  # 2 dates × 3 quantiles
        @test all(result_df.output_type .== "quantile")
        @test Set(unique(result_df.output_type_id)) == Set([0.25, 0.5, 0.75])

        # Test quantile ordering
        for date_obj in forecast_dates
            date_rows = result_df[result_df.target_end_date .== date_obj, :]
            q25 = date_rows[date_rows.output_type_id .== 0.25, :].value[1]
            q50 = date_rows[date_rows.output_type_id .== 0.5, :].value[1]
            q75 = date_rows[date_rows.output_type_id .== 0.75, :].value[1]
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
        results = (forecast_dates = forecast_dates, forecasts = forecasts)

        output_type = QuantileOutput(quantile_levels = [0.5])

        tmpdir = mktempdir()
        try
            result_df = create_forecast_output(
                input, results, tmpdir, output_type;
                save_output = true
            )

            @test isa(result_df, DataFrame)
            @test size(result_df, 1) == 2
            @test all(result_df.location .== "CA")
            @test all(result_df.target .== "wk inc covid hosp")

            # Check file was saved
            csv_files = filter(f -> endswith(f, ".csv"), readdir(tmpdir))
            @test length(csv_files) == 1
        finally
            rm(tmpdir, recursive = true)
        end
    end

    @testset "PipelineOutput writes samples parquet with Date column" begin
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

        forecast_dates = [Date("2024-01-08"), Date("2024-01-15")]
        forecasts = reshape([10.0, 20.0, 30.0, 40.0], 2, 2)
        results = (forecast_dates = forecast_dates, forecasts = forecasts)

        tmpdir = mktempdir()
        try
            result_df = create_forecast_output(
                input, results, tmpdir, PipelineOutput();
                save_output = true
            )

            parquet_path = joinpath(tmpdir, "samples.parquet")
            @test isfile(parquet_path)
            @test eltype(result_df.date) == Date
            @test propertynames(result_df) == [
                :date,
                Symbol(".value"),
                Symbol(".draw"),
                Symbol(".variable"),
                :resolution,
                :geo_value,
                :disease
            ]

            con = DBInterface.connect(DuckDB.DB, ":memory:")
            try
                read_df = DataFrame(DBInterface.execute(
                    con,
                    "SELECT * FROM read_parquet($(EpiAutoGP._quote_duckdb_string(parquet_path)))"
                ))

                @test eltype(read_df.date) == Date
                @test propertynames(read_df) == propertynames(result_df)
                @test read_df.date == forecast_dates[[1, 2, 1, 2]]
                @test read_df[!, Symbol(".draw")] == [1, 1, 2, 2]
                @test read_df[!, Symbol(".value")] == [10.0, 20.0, 30.0, 40.0]
                @test all(read_df[!, Symbol(".variable")] .==
                          "observed_hospital_admissions")
                @test all(read_df.resolution .== "epiweekly")
                @test all(read_df.geo_value .== "CA")
                @test all(read_df.disease .== "COVID-19")
            finally
                DBInterface.close(con)
            end
        finally
            rm(tmpdir, recursive = true)
        end
    end
end
